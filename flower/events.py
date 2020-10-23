import time
import shelve
import logging
import threading
import collections

from functools import partial

import boto3
import botocore
import celery

from tornado.ioloop import IOLoop
from tornado.ioloop import PeriodicCallback
from tornado.concurrent import run_on_executor

from celery.events import EventReceiver
from celery.events.state import State

from . import api

from collections import Counter
from concurrent.futures import ThreadPoolExecutor

from prometheus_client import Counter as PrometheusCounter, Histogram

logger = logging.getLogger(__name__)

s3 = boto3.client('s3')

class PrometheusMetrics(object):
    events = PrometheusCounter('flower_events_total', "Number of events", ['worker', 'type', 'task'])
    runtime = Histogram('flower_task_runtime_seconds', "Task runtime", ['worker', 'task'])


class EventsState(State):
    # EventsState object is created and accessed only from ioloop thread

    def __init__(self, *args, **kwargs):
        super(EventsState, self).__init__(*args, **kwargs)
        self.counter = collections.defaultdict(Counter)
        self.metrics = PrometheusMetrics()

    def event(self, event):
        worker_name = event['hostname']
        event_type = event['type']

        self.counter[worker_name][event_type] += 1

        if event_type.startswith('task-'):
            task_id = event['uuid']
            task_name = event.get('name', '')
            if not task_name and task_id in self.tasks:
                task_name = self.tasks[task_id].name or ''
            self.metrics.events.labels(worker_name, event_type, task_name).inc()

            runtime = event.get('runtime', 0)
            if runtime:
                self.metrics.runtime.labels(worker_name, task_name).observe(runtime)

        # Send event to api subscribers (via websockets)
        classname = api.events.getClassName(event_type)
        cls = getattr(api.events, classname, None)
        if cls:
            cls.send_message(event)

        # Save the event
        super(EventsState, self).event(event)


class Events(threading.Thread):
    events_enable_interval = 5000

    def __init__(self, capp, db=None, persistent=False,
                 persist_to_s3=False, s3_bucket=None,
                 enable_events=True, io_loop=None, **kwargs):
        threading.Thread.__init__(self)
        self.daemon = True

        self.io_loop = io_loop or IOLoop.instance()
        self.capp = capp


        self.db = db
        self.persistent = persistent
        self.persist_to_s3 = persist_to_s3
        self.s3_bucket = s3_bucket
        self.enable_events = enable_events
        self.state = None

        if self.persistent:
            state = None
            logger.debug("Loading state from '%s'...", self.db)
            if self.persist_to_s3:
                logger.debug("Downloading '%s' from '%s'...", self.db,
                                                           self.s3_bucket)
                try:
                    s3.download_file(Bucket=self.s3_bucket, Key=self.db,
                                      Filename=self.db)
                    state = shelve.open(self.db)
                    logger.debug("Found state: %s", self.db)
                except botocore.exceptions.ClientError as e:
                    if e.response['Error']['Code'] == "404":
                        logger.error("State file not found, moving on...")
                    if e.response['Error']['Code'] == "403":
                        logger.error("Forbidden.")
            else:
                state = shelve.open(self.db)
                logger.debug("No persistent state on S3, checking local file\
                             system.")
            if state:
                self.state = state['events']
                logger.debug("State file found: %s", self.state)
                state.close()
                logger.debug("State successfully closed")

        if not self.state:
            logger.debug("No state, writing EventState.")
            self.state = EventsState(**kwargs)


        self.timer = PeriodicCallback(self.on_enable_events,
                                      self.events_enable_interval)

    def start(self):
        threading.Thread.start(self)
        if self.enable_events:
            logger.debug("Starting enable events timer...")
            self.timer.start()

    def stop(self):
        if self.enable_events:
            logger.debug("Stopping enable events timer...")
            self.timer.stop()

        if self.persistent:
            logger.debug("Saving state to '%s'...", self.db)
            state = shelve.open(self.db)
            state['events'] = self.state
            logger.debug("State file with following state successfully saved:\
                         %s", state)
            state.close()
            if self.persist_to_s3:
                logger.debug("Putting '%s' to S3 bucket 's3://%s'...",
                             self.db, self.s3_bucket)
                s3.upload_file(Bucket=self.s3_bucket, Key=self.db,
                               Filename=self.db)

    def run(self):
        try_interval = 1
        while True:
            try:
                try_interval *= 2

                with self.capp.connection() as conn:
                    recv = EventReceiver(conn,
                                         handlers={"*": self.on_event},
                                         app=self.capp)
                    try_interval = 1
                    logger.debug("Capturing events...")
                    recv.capture(limit=None, timeout=None, wakeup=True)
            except (KeyboardInterrupt, SystemExit):
                try:
                    import _thread as thread
                except ImportError:
                    import thread
                thread.interrupt_main()
            except Exception as e:
                logger.error("Failed to capture events: '%s', "
                             "trying again in %s seconds.",
                             e, try_interval)
                logger.debug(e, exc_info=True)
                time.sleep(try_interval)

    def on_enable_events(self):
        # Periodically enable events for workers
        # launched after flower
        self.io_loop.run_in_executor(None, self.capp.control.enable_events)

    def on_event(self, event):
        # Call EventsState.event in ioloop thread to avoid synchronization
        self.io_loop.add_callback(partial(self.state.event, event))
