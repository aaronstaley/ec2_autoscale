'''
The scalingtask is responsible for coordinating interactions with the ScalingManagers that manage
actual resources.

For speed, it controls the main AWS API querying.
'''
import itertools
import sys
import time
import traceback
import logging
import threading

try:
    from cStringIO import StringIO
except ImportError:
    from StringIO import StringIO

from collections import defaultdict

from .threadpool import ThreadPool
from .server_control.scaler_server_control import get_server_control
from .pricingmanager import PricingManager
from .pool import Pool
from .simple_scaler import SimpleScaler

logger = logging.getLogger('Scaler.ScalingTask')


class ScalingTask(threading.Thread):
    """Parent thread handles all scaling"""

    poll_time = 5  # number of seconds between successive scaling checks

    email_wait = 10 * 60  # number of seconds between successive emails w/ same subject
    num_threads = 27  # number of threads for the threadpool

    server_control = None  # Object to interface with infrastructure provider (EC2)
    managers = None  # dict mapping manager names to Types

    threadpool = None  # common threadpool handles all provisioning

    last_email = None  # dictionary mapping subject to time.time() of last email sent

    def email_warning(self, subj, msg, force=False, email_wait=None):
        """Send an email to warn admins"""
        now = time.time()
        last_email = self.last_email.get(subj, 0)
        if not email_wait:
            email_wait = self.email_wait
        if force or (now - last_email) > email_wait:
            self.last_email[subj] = now

            # TODO(developer): Connect to your own email sending logic

    def log_email(self, subj, msg, force=False, email_wait=None, logfunc=logger.error, exc_info=False):
        """Send email_warning and write error to log
        *subj*: Email subject / first part of log
        *msg*: Email body / last part of log
        *force*: Email must be sent
        *email_wait*: Customize email timeout period (default 10 minutes)
        *logfunc*: Logger function to use
        *exc_info*: Attach traceback information to msg (only valid if in exception handler) 
        """
        if exc_info:
            ei = sys.exc_info()
            sio = StringIO()
            traceback.print_exception(ei[0], ei[1], ei[2], None, sio)
            s = sio.getvalue()
            sio.close()
            if s[-1:] == "\n":
                s = s[:-1]
            msg += '\n' + s

        logfunc('%s: %s', subj, msg)

        # only email on at least warning:
        if self.server_control.effective_env == 'live':
            excluded = ['debug', 'info']
        else:
            excluded = []
        if logfunc.__name__ not in excluded:
            self.email_warning(subj, msg, force, email_wait)

    def generate_managers(self):
        """Generate all managers and pools this scalingtask will utilize"""
        # we have a unique manager for every worker scaling type
        self.managers = {}

        # The actual instance is controlled through a pool
        self.pools = []

        # TODO(developer): This is an example of an instance type
        simple_pool = Pool(self, SimpleScaler.instance_type, SimpleScaler.server_role, 'simple')
        self.pools.append(simple_pool)
        simple_manager = SimpleScaler(self, simple_pool)
        self.managers[simple_manager.name] = simple_manager
        simple_pool.add_manager(simple_manager)

    def __init__(self, worker):
        super(ScalingTask, self).__init__()
        self.worker = worker
        self.setName('ScalingTaskThread')
        self.setDaemon(True)

        self.server_control = get_server_control()
        self.generate_managers()
        self.pricing_manager = PricingManager(self.server_control)
        self.poll_cv = threading.Condition()
        self.last_email = {}
        self.threadpool = ThreadPool(num_threads=self.num_threads, daemon=False)

    def set_target(self, scaling_type, **kwargs):
        """For a given scaler, set its target"""
        self.managers[scaling_type].set_target(**kwargs)

    def update_setting(self, scaling_type, func):
        """func will modify a setting within the context of a manager.
        See docs at ???
        update_setting is the wrapper to flush to configuration server"""
        self.managers[scaling_type].update_setting(func)

    def execute(self):
        """
        Top-level event to handle system scaling        
        """

        # First determine instance filters
        # We only need to look at instances with types being managed by pools 

        instance_types = list(set([pool.instance_type for pool in self.pools]))
        filters = {self.server_control.filter_name_instance_type: instance_types}

        # obtain all server information
        try:
            instances, spot_requests = self.server_control.get_spots_and_instances(filters=filters)
        except Exception, e:
            self.log_email('Unexpected AWS result',
                           msg='get_spots_and_instances rasied exception',
                           exc_info=True)
            return

        logger.debug('execute: instances are %s. spots are %s under query filters %s', instances, spot_requests,
                     filters)

        pool_name_map = {}
        for pool in self.pools:
            pool_name_map[pool.myname] = pool

        def build_govern_dct(instances_or_spots):
            """Build a dictionary mapping pool types to list of instance-like objects to govern
            """
            governing = defaultdict(list)
            for ios in instances_or_spots:
                pool = None
                if self.server_control.effective_env == self.server_control.get_env(ios):
                    # Only consider servers in our environment
                    pool_str = self.server_control.scaler.get_pool_managing(ios)
                else:
                    pool_str = None

                if pool_str:
                    pool = pool_name_map.get(pool_str)
                    if not pool:
                        self.log_email('Invalid Pool',
                                       'Instance %s set to invalid pool %s' % (str(ios), pool_str))
                governing[pool].append(ios)
            return governing

        instances_governing = build_govern_dct(instances)
        spots_governing = build_govern_dct(spot_requests)

        self.pricing_manager.update(instances_governing, self.pools)

        # TODO(Developer); This is a basic example of using pools.
        # You can pass arbitrary kwargs into the .execute() which is forwarded to the manager.
        for pool in self.pools:
            pool.execute(instances_governing[pool], spots_governing[pool])

        not_set = list(itertools.chain(instances_governing[None],
                                       spots_governing[None]))
        if not_set:
            logging.debug('Instances not being governed by a pool - %s', not_set)

    def wait_for_scaling_thread_termination(self, wait_time=None):
        """Wait until all provisioning is complete"""
        for sm in itertools.chain(self.managers.values(), self.pools):
            start = time.time()
            logger.debug('Terminating %s', sm)
            sm.wait_for_thread_termination(wait_time)
            elasped = time.time() - start
            if wait_time:
                wait_time -= elasped
        if wait_time:
            wait_time = max(10, wait_time)
        logger.debug('Terminating threadpool')
        self.threadpool.hard_terminate(wait_time)

    tt_lock = threading.Lock()
    tt_counter = 0

    def run(self):
        with self.__class__.tt_lock:
            self.__class__.tt_counter += 1

            logging.debug('logging for %s', self.getName())
            if False:  # TODO(developer): Profile ability
                import cProfile
                cProfile.runctx('self.main()', globals(), locals(), 'storage_path')
            else:
                logging.debug('running thread %s without profiler', self.getName())
                self.main()

    def main(self):
        # TODO(developer): You may need to add startup wait logic here

        while not self.worker.die_event.isSet():
            st_time = time.time()
            self.execute()

            with self.poll_cv:  # sleep for poll_time or until woken by external event
                wait_time = max(0.1, self.poll_time - (time.time() - st_time))
                self.poll_cv.wait(wait_time)
