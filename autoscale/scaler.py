'''
EC2 Autoscaling worker
'''

import threading

import logging

logger = logging.getLogger('Scaler.run')
logger.setLevel(logging.DEBUG)

from .scalingtask import ScalingTask
from .util import ThreadWatcher


class Scaler(object):
    """
    Wrapper around Scalar object. Call .run() to start it up!
    """

    die_event = None
    threads = None

    # Task
    scale_task = None

    # collections of all threads and the thread watcher
    threads = None
    thread_watcher = None

    def __init__(self, *args, **kwargs):
        self.thread_watcher = ThreadWatcher()

        self.die_event = threading.Event()

    def run(self):
        """Start the worker"""

        # TODO(developer): THIS WOULD BE A GREAT PLACE TO ADD ANY DATABASE REGISTRATION NEEDED FOR THE SCALER
        self.scale_task = ScalingTask(self)

        self.threads = [self.scale_task]
        # TODO(developer): Add any more threads needed

        for thread in self.threads:
            thread.start()

        logger.debug('started all main threads')

        # add threads that should not die to the watcher
        self.thread_watcher.watch(self.threads, self.thread_died)

        logger.debug('spawned threads and waiting')

        ######## Now, let the threads run and wait until dieEvent is set
        try:
            while not self.die_event.isSet():
                # run any events that need to be done periodically
                self.die_event.wait(1.0)

        except KeyboardInterrupt:
            logger.warn('Keyboard interrupt. Killing scaler')
            self.die_event.set()

        ### something has triggered the dieEvent.  Clean up.

        # notify watcher that threads will be dying as expected
        self.thread_watcher.okay_to_die()

        # stop task for assigning jobs        
        with self.scale_task.poll_cv:  # force thread to wake up and die
            self.scale_task.poll_cv.notify()

        logger.debug('Waiting for task to die')
        self.scale_task.join()

        # TODO(developer): UPDATE DBS TO INDICATE SCALER IS DYING

        logger.debug('Waiting for all server spawns/teardowns to complete')
        self.scale_task.wait_for_scaling_thread_termination(wait_time=15 * 60)

        # TODO(developer): DO FINAL CLEANUP HERE

    def thread_died(self, thread):
        logger.error('Autoscaler.thread_died: %s', thread)
        self.die_event.set()
