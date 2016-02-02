"""
Various utility classes and methods
"""

import threading
import logging

class ThreadWatcher(object):
    """Generic thread monitor

    Current use case detects if threads die unexpectedly
    """

    threads = None
    thread_watchers = None
    dieEvent = None

    def __init__(self):
        self.threads = []
        self.thread_watchers = []
        self.dieEvent = threading.Event()

    def __del__(self):
        self.dieEvent.set()

    def okay_to_die(self):
        """
        Indicates to the ThreadWatcher that it is okay for threads to die.
        Callbacks are not called after this is set.
        """
        self.dieEvent.set()

    def watch(self, threads, callback=None):
        """
        Add threads to be watched for. The callback is called upon a thread's exit
        unless the okay_to_die function has been called.
        threads: A single Thread object, or a list of Thread objects.
        callback: Function that takes one argument, the thread, to be called upon
                  a thread's exit.
        """

        if not isinstance(threads, list):
            threads = [threads]

        self.threads.extend(threads)

        for thread in threads:
            thread_watcher = threading.Thread(target=self._wait, name='ThreadWatcher', args=(thread, callback))
            thread_watcher.setDaemon(True)
            self.thread_watchers.append(thread_watcher)
            thread_watcher.start()

    def _wait(self, thread, callback):
        """
        Protected method used to watch a thread.
        """

        logging.debug('ThreadWatcher: Watching %s', thread)
        thread.join()
        self.threads.remove(thread)

        if not self.dieEvent.is_set():
            logging.debug('ThreadWatcher: Thread died unexpectedly %s', thread)
            callback(thread)

