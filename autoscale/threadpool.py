from __future__ import with_statement

import ctypes
import inspect
import threading
import time
import traceback
import logging

from Queue import Queue

logger = logging.getLogger('ThreadPool')


class ThreadPool(object):
    """ThreadPool is a simple abstraction for processing a queue of arbitrary
    functions with a pool of threads.

    No results are maintained to avoid complications with tracking result sets"""

    queue_type = Queue

    def __init__(self, num_threads=10, daemon=True):
        """Creates a ThreadPool and initializes worker threads.

        *num_threads* is the number of worker threads to create.
        *daemon* specifies whether the threads should run in daemon mode.
        If True, threads will be stopped when Python is exiting."""

        # list of all active threads in the pool
        self.threads = []

        # number of active threads in the pool
        self.num_threads = num_threads

        # lock for job_in_progress, events, and waiting_on_jids
        self.lock = threading.RLock()

        # thread-safe queue for enqueuing and dequeuing jobs
        self.queue = self.queue_type()

        # total number of jobs to pass through thread pool, also the max jid
        self.num_jobs = 0

        # mapping from jid -> thread
        self.jobs_in_progress = {}

        # set of all unfinished jobs
        self.jobs_unfinished = set()

        # mapping from thread -> event objects
        self.events = {}

        # mapping from thread -> jids
        self.waiting_on_jids = {}

        # has terminate been called
        self.terminated = False

        # initialize every worker thread
        for i in range(num_threads):
            thread = Thread2(group=None, target=self._worker_loop, name='ThreadPoolWorker-%s' % i, args=(), kwargs={})
            thread.setDaemon(daemon)
            thread.start()
            self.threads.append(thread)

    def _worker_loop(self):
        """Function to be run in each separate worker thread. Gets an item
        from the queue, processes it, and then marks it as completed."""

        while True:

            # get next task from queue
            item = self.queue.get()

            if not item:  # sentinel
                with self.lock:
                    try:
                        self.threads.remove(threading.currentThread())
                    except ValueError:
                        pass
                break

            jid = item['jid']

            with self.lock:
                self.jobs_in_progress[jid] = threading.currentThread()

            # do not allow a task's exception to destroy the thread
            try:
                # process task
                item['target'](*item['args'], **item['kwargs'])
            except Exception, e:
                logger.error('Worker had exception caught: %s %s', traceback.format_exc(), e)
            finally:
                self._cleanup_job(jid)

    def _cleanup_job(self, jid):
        """Called by thread when cleaning up from a job"""

        with self.lock:
            if jid not in self.jobs_unfinished:  # async termination
                logger.warning('jid %s was already cleaned', jid)
                return
            self.jobs_unfinished.remove(jid)
            del self.jobs_in_progress[jid]
            for thread in self.waiting_on_jids.keys():
                jids = self.waiting_on_jids[thread]
                if jid in jids:
                    jids.remove(jid)
                if not jids:
                    del self.waiting_on_jids[thread]
                    # wake up any thread sleeping on join
                    self.events[thread].set()

    def add_task(self, target=None, args=(), kwargs={}):
        """Adds a *target* function to be run by workers.
        *args* should be specified as a tuple to be passed to the function.
        *kwargs* should be specified as a dict to be passed to the function.

        Watch out when using add_task() in a for loop. Due to the way closures
        work in Python (and other languages), you'll probably find that all
        your *target*s have the same input arguments. This is because the
        *target* you defined uses variables it references in the local
        environment, and only creates the closure (moves vars from the stack
        to the heap) when the function loses scope, which is after the
        last iteration of your for loop."""

        # get next jid
        with self.lock:
            self.num_jobs += 1
            jid = self.num_jobs

        with self.lock:
            self.jobs_unfinished.add(jid)

        # add task to queue
        self.queue.put({'target': target,
                        'args': args,
                        'kwargs': kwargs,
                        'jid': jid})

        return jid

    def join(self, jids, timeout=None):
        """Wait for *jids* to be completed. *jids* can be a single id or
        a list of ids. If *timeout* is reached, returns regardless of
        whether jobs have finished. If None, then waits until all have
        been finished.

        Returns True if all jobs finished, False otherwise."""

        if not hasattr(jids, '__iter__'):
            jids = [jids]
        else:
            # copy list
            jids = jids[:]

        with self.lock:
            # determine which jids are still being processed
            jids_unfinished = set()
            for jid in jids:
                if jid in self.jobs_unfinished:
                    jids_unfinished.add(jid)

            if jids_unfinished:
                current_thread = threading.currentThread()
                event = threading.Event()
                self.events[current_thread] = event
                self.waiting_on_jids[current_thread] = jids_unfinished

        if jids_unfinished:
            # sleep on event. thread will be woken up when jids are finished.
            retval = event.wait(timeout)
            del self.events[current_thread]
            if current_thread in self.waiting_on_jids:
                del self.waiting_on_jids[current_thread]
            return retval
        else:
            return True

    def kill(self, jid):
        """Kill a thread with PyThreadState_SetAsyncExc
        Beware that it is impossible to kill jobs that have not yet been started
        Not fully safe to use; use sparingly
        Returns true if killed"""

        with self.lock:
            working_thread = self.jobs_in_progress.get(jid)
            if not working_thread:
                return False
            logger.debug('Hard killing jid %s, associated with thread %s', jid, working_thread)
            #self._cleanup_job(jid)
            working_thread.terminate()
            self.threads.remove(working_thread)

            if not self.terminated:
                nthread = Thread2(group=None, target=self._worker_loop, name=working_thread.name, args=(), kwargs={})
                nthread.setDaemon(working_thread.daemon)
                logger.debug('Replacing %s with %s', working_thread, nthread)
                self.threads.append(nthread)
                nthread.start()
            return True

    def busy(self):
        """True if the ThreadPool has unfinished tasks, otherwise False."""
        return True if self.jobs_unfinished else False

    def terminate(self, join=True, join_timeout=None):
        """Shuts down all threads. Call this so that the ThreadPool will be
        garbage collected if this is a long-lived process. Set *join* to True
        if this function should block until all threads have been terminated.
        *join_timeout* if specified is the number of seconds to wait for the
        threads to terminate."""

        with self.lock:
            self.terminated = True
            for _ in xrange(self.num_threads):
                self.queue.put(None)
        if join:
            for thread in self.threads[:]:
                start = time.time()
                thread.join(join_timeout)
                if join_timeout:
                    join_timeout = max(1, join_timeout - (time.time() - start))

    def hard_terminate(self, timeout):
        """Shuts down all threads. If not terminated by *timeout*, the threads
        are force killed."""

        self.terminate(join=True, join_timeout=timeout)
        for thread in self.threads[:]:
            thread.terminate()
            try:
                self.threads.remove(thread)
            except ValueError:
                pass

class Thread2(threading.Thread):
    """Thread capable of being terminated"""
    def _get_my_tid(self):
        """determines this (self's) thread id"""
        if not self.isAlive():
            raise threading.ThreadError("the thread is not active")

        # do we have it cached?
        if hasattr(self, "_thread_id"):
            return self._thread_id

        # no, look for it in the _active dict
        for tid, tobj in threading._active.items():
            if tobj is self:
                self._thread_id = tid
                return tid

        raise AssertionError("could not determine the thread's id")

    def raise_exc(self, exctype):
        """raises the given exception type in the context of this thread"""
        #_async_raise(self._get_my_tid(), exctype)
        if self.isAlive():
            _async_raise(self.ident, exctype)


    def terminate(self):
        """raises SystemExit in the context of the given thread, which should
        cause the thread to exit silently (unless caught)"""
        try:
            self.raise_exc(SystemExit)
            logging.debug('%s async terminated', self)
        except ValueError, e:  #thread already closed
            logging.debug('%s failed to async terminate', self, exc_info = True)
            pass

def _async_raise(tid, exctype):
    """raises the exception, performs cleanup if needed"""
    if not inspect.isclass(exctype):
        raise TypeError("Only types can be raised (not instances)")
    tid = ctypes.c_long(tid)
    res = ctypes.pythonapi.PyThreadState_SetAsyncExc(tid, ctypes.py_object(exctype))
    if res == 0:
        raise ValueError("invalid thread id %s" % tid)
    elif res != 1:
        # """if it returns a number greater than one, you're in trouble,
        # and you should call it again with exc=NULL to revert the effect"""
        ctypes.pythonapi.PyThreadState_SetAsyncExc(tid, None)
        raise SystemError("PyThreadState_SetAsyncExc failed")
