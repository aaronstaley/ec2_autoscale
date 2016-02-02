"""
Defines the ScalingManager parent object
A ScalingManager is responsible for managing a given resource

It generally does not deal with instances directly; such tasks are passed on to lower levels of code

Dumped code:
            
            email_warning:
            subj = self.resourceType.upper() + ': ' + subj

"""

import logging
import time

logger = logging.getLogger('Scaler.ScalingManager')

def sign(num):
    """Returns -1 for negative, 0 for 0, and 1 for positive"""
    if num < 0:
        return -1
    return int(bool(num))


class ManagerRequest(object):
    """This class defines the layout of the target object returned by manager.execute()
    """

    delta_request = 0  # difference in number of serers I want compared to ones passed in
    batches = ()  # tuple describing how requests may be grouped.  sorted from highest to lowest priority. e.g. (3,4) means make request of 3 instances and one of 4
    # Technical note: Batches cannot group different types (spot versus non-spot).
    az_minimums = {}  # dictionary mapping availability zone to minimum instances that delta_request should be applied to (be it positive or negative)
    # sum(values) must be <= abs(delta_request)
    released = []  # list of instances I am releasing to the pool (possible feature: Force termination?)
    # For purposes of calculation what I have, I am considered to no longer be holding these
    minimum_spot = 0  # minimum number of instances (including but not limited to spots) that should be connected to manager.

    # This condition should be satisfied by spawning spot instances and only if spot price <= 0.5*instance price
    # doesn't support az_minimums or batches

    def __init__(self, delta_request=0, batches=None, az_minimums=None, released=None,
                 minimum_spot=0):
        self.delta_request = delta_request
        self.batches = tuple(batches) if batches else ()
        self.az_minimums = az_minimums or {}
        self.released = released or []
        self.minimum_spot = minimum_spot

    def desire_possession(self, instance, cur_instances=None, is_spot=None):
        """
        Returns priority of how much this instance once a newly booted instance
        Higher return values means more desired.
        
        Current mapping:
        0: Not desired
        1: Sort of desired to satisfy minimum_spot
        2: Desired to satisfy minimum_spot
        3: Desired to satisfy delta request        
        """

        delta = self.delta_request
        if delta > 0:
            other_az = delta - sum(self.az_minimums.values())
            if self.az_minimums.get(instance.placement, 0) > 0 or other_az > 0:
                return 3
        if self.minimum_spot and cur_instances and is_spot:
            # if we don't have enough instances, we definitely want this
            if len(cur_instances) < self.minimum_spot:
                return 2
            # somewhat want if instances >= self.minimum_spot, but less spots
            spot_instances = [inst for inst in cur_instances if is_spot(inst)]
            spot_instance_cnt = len(spot_instances)
            if spot_instance_cnt < self.minimum_spot:
                return 1
        return 0

    def desire_termination(self, instance):
        """Return true if request indicates that this instance should be terminated"""
        if self.delta_request < 0:
            delta = -self.delta_request
            other_az = delta - sum(self.az_minimums.values())
            if self.az_minimums.get(instance.placement, 0) > 0 or other_az > 0:
                return True
        return False

    def captured_server(self, instance):
        """Update this request to indicate that instance has been applied to this request"""
        if self.delta_request:
            sig = sign(self.delta_request)
            delta = abs(self.delta_request)
            other_az = delta - sum(self.az_minimums.values())
            my_az_req = self.az_minimums.get(instance.placement, 0)
            if my_az_req > 0:
                self.az_minimums[instance.placement] = my_az_req - 1
                self.delta_request = sig * (delta - 1)
            elif other_az > 0:
                self.delta_request = sig * (delta - 1)

    def __str__(self):
        return 'ManagerRequest (%s)' % ','.join(map(str, [self.delta_request, self.batches, self.az_minimums,
                                                          self.released, self.minimum_spot]))

    __repr__ = __str__

    """Basic arithmatical operations.  Additional operations will be added as needed"""

    def _check_type(self, y):
        """Validate that type y can be merged with this"""
        if not isinstance(y, ManagerRequest):
            raise TypeError('Got type %s when expecting ManagerRequest' % type(y))

    def __add__(self, y):
        """Returns a new object with the combined targets
        Purpose of combining objects is to determine server requests.
        Consequently, requests with delta_request <= 0 are ignored
            (other than released which are always merged)
        """

        self._check_type(y)

        released = self.released + y.released
        new_minimum_spot = self.minimum_spot + y.minimum_spot

        if y.delta_request <= 0:
            y = ManagerRequest()

        elif self.delta_request <= 0:
            self = ManagerRequest()

        new_delta_request = self.delta_request + y.delta_request
        new_batches = self.batches + y.batches  # merge typles w/ adding

        new_az_minimums = {}
        new_az_minimums.update(self.az_minimums)
        for az, num in y.az_minimums.items():
            new_az_minimums.setdefault(az, 0)
            new_az_minimums[az] += num

        return ManagerRequest(delta_request=new_delta_request, batches=new_batches,
                              az_minimums=new_az_minimums, released=released,
                              minimum_spot=new_minimum_spot)


class ProvisionThread(object):
    """Thread-like responsible for provisioning a server
    """

    instance = None
    manager = None
    jid = None  # identifier of threadpool object running
    role_inst = None  # The role instance handling provisioning

    running = False  # set to true when run called
    request_abort = False  # set to true if this should be aborted 

    # Either of below two variables being true indicates thread has terminated
    provisioned = False  # Set to true once thread has completed
    request_destroy = None  # Something went wrong with provisioning; destroy this instance at this time

    # configurable
    destory_delay = 8 * 60  # how many s to wait until destroying the instance

    def __init__(self, instance, manager):
        self.instance = instance
        self.manager = manager
        super(ProvisionThread, self).__init__()

    @property
    def executed(self):
        return self.provisioned or self.request_destroy

    def abort(self):
        self.request_abort = True
        if self.role_inst:
            self.manager.logger.info('Aborting instance %s deploying with role %s', self.instance, self.role_inst)
            self.role_inst.abort()

    def start(self):
        self.jid = self.manager.scaling_task.threadpool.add_task(target=self.run)

    def _provision_callback(self, role_inst, target_instance):
        assert target_instance.id == self.instance.id
        self.role_inst = role_inst
        if self.request_abort:
            # As we are in the same thread as the role, we could just raise an exception here
            # However, for safety, we rely on the role raising an exception
            self.abort()

    def set_request_destroy(self, delay=0):
        self.request_destroy = time.time() + delay

    def run(self):
        try:
            if self.request_abort:
                self.manager.logger.warning('Not deploying to instance %s due to abort', self.instance)
                self.set_request_destroy()
                return
            self.running = True
            self.manager.pool.server_control.auto_provision(self.instance,
                                                            role_creation_cb=self._provision_callback)
        except Exception:
            self.manager.log_email('Instance provisioning failed',
                                   'Instance %s (hostname %s) provisioning crashed:' % (
                                   self.instance, self.instance.public_dns_name),
                                   force=True, exc_info=True)
            self.set_request_destroy(self.destory_delay)
        else:
            assert self.role_inst
            try:
                self.role_inst.activate()
            except Exception:
                self.manager.log_email('Instance activation failed',
                                       'Instance %s (hostname %s) activation crashed:' % (
                                       self.instance, self.instance.public_dns_name),
                                       force=True, exc_info=True)
                self.set_request_destroy(self.destory_delay)
            else:
                self.provisioned = True

        # delete myself?
        if self.manager.auto_clear_provision:
            self.manager.clear_provision(self.instance.public_dns_name)


class ScalingManager(object):
    """Class that manages a specific EC2 instance type"""

    scaling_task = None  # parent scaling task
    pool = None
    enabled = True  # is scaler running

    settings_lock = None  # The lock on settings exists to prevent clashes between regular reads and another thread updating settings

    logger = None

    winding_down_hosts = None  # list of hostnames we are shutting down
    provision_threads = None  # map hostname --> threadpool ids; deleted once entity online
    thread_pool = None  # threadpool used for provisioning
    last_target = 0  # for debugging
    missing_start_time = None
    last_minimum_spot = 0  # minimum number of spot instances to connect to this manager; see ManagerRequest

    # static configuration    
    auto_clear_provision = True  # should prov thread clear itself? Set to False if you want to keep thread up until some other event occurs (e.g. server connects to a pool)
    missing_email_wait = 70  # number of seconds before emailing about missing instances
    # test is handled by moving instance from 'running' to 'extra'

    server_limit = 100  # maximum number of servers allowed
    server_minimum = 0  # minimum number of servers allowed

    # winddown timing
    # Define "window" in seconds needed for winding down to commence
    # top of wind down window is pool.max_teardown_time
    max_winddown_window = 2 * 60
    min_winddown_window = 20
    my_provision_thread = ProvisionThread

    def __init__(self, scaling_task, pool):
        """Manager constructor"""
        self.logger = logging.getLogger('Scaler.manager.%s' % self.name)
        self.scaling_task = scaling_task
        self.pool = pool

        self.winding_down_hosts = set()
        self.provision_threads = {}

        # times in terms of seconds since last charge
        self.winddown_max = self.pool.max_teardown_time - self.min_winddown_window
        self.winddown_min = self.pool.max_teardown_time - self.max_winddown_window

    def log_email(self, subj, msg, force=False, email_wait=None, logfunc=None, exc_info=False):
        """See ScalingTask.log_email"""
        subj = self.name + ': ' + subj
        if not logfunc:
            logfunc = self.logger.error
        self.scaling_task.log_email(subj, msg, force, email_wait, logfunc, exc_info)

    """below children need to be implement"""

    @property
    def name(self):
        raise NotImplementedError

    @property
    def entity(self):
        """Name of what this is scaling"""
        raise NotImplementedError

    def entity_str(self, instance):
        """Print entity running on instance as a string"""
        return 'instance (host=%s, inst_id=%s)' % (instance.public_dns_name, instance.id)

    def should_manage(self, instance):
        """Return true iff this manager is wishes to manage this instance"""
        return False

    def self_diagnostic(self):
        """Returns a dictionary of diagnostic information about this ScalingManager
        """

        my_dict = {'enabled': self.enabled,
                   'last_target': self.last_target,
                   'minimum_spot': self.last_minimum_spot,
                   'winding_down_hosts': self.winding_down_hosts
                   }
        prov_threads = {}
        for hostname, pthread in self.provision_threads.items():
            if not pthread:
                prov_threads[hostname] = None
            else:
                prov_threads[hostname] = {'provisioned': pthread.provisioned,
                                          'request_destroy': pthread.request_destroy,
                                          'instance': self.entity_str(pthread.instance),
                                          'jid': pthread.jid,
                                          'running': pthread.running
                                          }
        my_dict['provision_threads'] = prov_threads
        return my_dict

    def get_target(self):
        """Return details about number of servers wanted
        Called from get_request
        TODO: AZ infromation
        """
        raise NotImplementedError

    """Manager interface definitions"""

    def get_request(self, running_instances, extra_instances=[], **kwds):
        """Inspect running_instances and any additional information provided
        Instances are divided into 'running_instances' (online and running) 
        and 'extra_instances' (instances in process of starting up or shutting down)
        subclass determines what running_instances are 'extra', if any
       
        Return ManagerRequest object describing needed information
        
        TODO: Handle concept of 'running' better for different provisioning systems
        """

        # TODO(developer): This is the last place to refresh settings.

        if not self.enabled:
            self.logger.debug('Disabled -- aborting')
            return None

        all_instances = running_instances + extra_instances
        all_inst_hosts = {}  # map hostname to instances
        for inst in all_instances:
            all_inst_hosts[inst.public_dns_name] = inst

        request = ManagerRequest()

        # find instances that have wound down
        wounddown_hosts = set()
        for host in list(self.winding_down_hosts):
            inst = all_inst_hosts.get(host)
            if not inst:
                self.log_email('Instance disappeared',
                               'winding down host %s no longer exists!' % host,
                               logfunc=self.logger.warning)
                self.winding_down_hosts.remove(host)
                continue
            # TODO(developer): If you don't support graceful winddown (see notes in simple_scaler.get_request),
            #  you can add every instance in winding_down_hosts straight to wounddown_hosts
            # This will cause the instance to terminate on the next scaler iteration after the one that initiated windown.
            if inst not in running_instances:
                wounddown_hosts.add(host)

                # Count elements in system
        num_spawning = len(self.provision_threads)
        num_shutdown = len(wounddown_hosts)
        num_shutting_down = len(self.winding_down_hosts) - num_shutdown
        num_active = len(running_instances)

        num_servers = len(all_instances)

        expected_servers = num_active + num_shutdown + num_spawning

        # Targetting
        target, minimum_spot = self.get_target()
        self.logger.info('Scaler server target is %s. Number of %s are %s. spawning=%s, killing=%s',
                         target, self.entity, num_active, num_spawning, num_shutting_down)
        self.last_target = target
        self.last_minimum_spot = minimum_spot

        # Begin scaling logic -- Starting here we can error out by return None
        if num_servers != expected_servers:
            msg = 'Unexpected number of online running_instances (%d). num_%s=%d, num_shutting_down=%d, num_shutdown=%d num_spawning=%d. \
            connected=%s, extra instances = %s' % \
                  (num_servers, self.entity, num_active, num_shutting_down, num_shutdown, num_spawning,
                   [self.entity_str(inst) for inst in running_instances],
                   [inst.public_dns_name for inst in extra_instances])
            self.logger.warning(msg)

            if num_servers > expected_servers:  # Too many servers.. do not bring another on!
                subj = 'Scaler - Missing %s' % self.entity if expected_servers else 'Scaler - No %s connected' % self.entity

                # nothing is activated
                if not expected_servers:
                    self.logger.warning('no active seen. bypassing scaling')
                    return None

                if self.missing_start_time:
                    if (time.time() - self.missing_start_time) > self.missing_email_wait:
                        self.log_email(subj,
                                       msg + '\nMost likely due to dying %s. Or perhaps a bug in the system' % self.entity,
                                       logfunc=self.logger.warn)
                else:
                    self.missing_start_time = time.time()

        else:
            self.missing_start_time = None

        delta_request = 0  # The request

        num_online = num_active - num_shutting_down

        if target > num_online:
            # more servers desired (Compare against spawns later in code)
            if target > self.server_limit:
                self.log_email('Scaler Server Limit Hit',
                               'Server limit hit. %s servers online; max %s. target=%s num_%s=%s' % \
                               (num_servers, self.server_limit, target, self.entity, num_active))
                delta_request = self.server_limit - num_online
            elif num_servers > expected_servers:
                # error condition -- back off request by amount of errored servers
                delta_request = max(0, target - num_online - (num_servers - expected_servers))
            else:
                delta_request = target - num_online

        elif target < num_online:
            # want to shut down servers
            if num_online < self.server_minimum:
                self.logger.info('Will not shut down servers to satisfy target (%s) due to too few \
                active=%s. num_spawning=%s' % (target, num_online, num_spawning))
            else:
                delta_request = target - num_online

        shutting_down_insts = [all_inst_hosts[sh] for sh in self.winding_down_hosts]
        # calculate Shutting Down Info
        sdi = [(instance, self.pool.can_terminate(instance)) for instance in shutting_down_insts]
        # allocate instances that cannot be terminated to positive delta_request first 
        sdi.sort(key=lambda bi: bi[1])

        for instance, can_term in sdi:

            inst_alive = instance.public_dns_name not in wounddown_hosts

            if not inst_alive:
                self.logger.info('Detected that %s %s was shut down', self.entity, instance)

            if delta_request > 0:
                # Respawn
                if self.cancel_winddown(instance, inst_alive):
                    self.log_email('Aborting instance winddown',
                                   'Winddown of %s aborted as need servers. delta_request is %s!' % \
                                   (self.entity_str(instance), delta_request),
                                   logfunc=self.logger.info)
                    delta_request -= 1
                    self.winding_down_hosts.remove(instance.public_dns_name)
                    continue

            elif not can_term:
                if self.cancel_winddown(instance, inst_alive):
                    self.log_email('Aborting instance winddown',
                                   'Winddown of %s aborted -- too much time had passed!' % \
                                   self.entity_str(instance),
                                   logfunc=self.logger.info)
                    self.winding_down_hosts.remove(instance.public_dns_name)
                    continue

            if not inst_alive:
                # instance can be released once the activity has died 
                self.log_email('Terminating instance',
                               'Requesting termination of %s' % self.entity_str(instance),
                               logfunc=self.logger.info)
                request.released.append(instance)
                self.ready_release(instance)
                self.winding_down_hosts.remove(instance.public_dns_name)

        if delta_request > 0:  # remove servers spawning from request
            delta_request = max(0, delta_request - num_spawning)

        request.delta_request = delta_request
        request.minimum_spot = minimum_spot

        # Scan for provisioning that failed
        for hostname, thread in self.provision_threads.items():
            if thread.request_destroy and thread.request_destroy < time.time():
                self.logger.warn('Provisioner thread %s requested destruction of instance %s host %s',
                                 thread, thread.instance, hostname)
                if thread.instance not in request.released:
                    self.ready_release(thread.instance)
                    request.released.append(thread.instance)
                self.clear_provision(hostname)

        return request

    def ready_release(self, instance):
        """Ready the instance for release
        This will update any internal logic associated with this instance.
        """
        hostname = instance.public_dns_name
        # clear instance entry from spawning if it will be destroyed
        if hostname in self.provision_threads.keys():
            self.clear_provision(hostname)

    def get_min_winddown_time(self, instance):
        """Return the minimum winddown time for this instance (w/ units of second since last charge) 
            - based on the wind down windows 
        
        I still haven't determined optimal policy for tearing down normal instances when spot replacement is available.

        We clearly at least want to tear down normals early that are being spot replaced to prioritize them over
            another spot instance.
        However, it is unclear if that should apply to all normal instances (currently, does)
            Pro:
                If spot-replaced normal failed to wind down, most likely to kill that same normal first again (over a spot)
                Simplifies handling with minimum_spot logic
            Con:
                Will be terminating normal instances unncessarily earlier
                *If no spots online or spot price pricier, makes no sense to do this
                ** TODO: Have condition dealing this this rare case
        """

        # if instance.id in self.spot_replace:
        if self.pool.substitute_spot and not self.pool.server_control.is_spot(instance):
            return self.winddown_min - self.pool.spot_replace_time_earliest
        else:
            return self.winddown_min

    def is_windingdown(self, instance):
        """Return if this instance is presently being wound down"""
        return instance.public_dns_name in self.winding_down_hosts  # already tearing down

    def can_winddown(self, instance):
        """We need reasonable amount of time to wind down server
        Warning: Timing is AWS dependent        
        """
        # Abort if already being shut down
        if self.is_windingdown(instance):
            return False

        effective_uptime = self.pool.server_control.get_instance_uptime_since_charge(instance)
        return self.get_min_winddown_time(instance) < effective_uptime < self.winddown_max

    def start_winddown(self, instance):
        """Requests entity start winding down
        Superclass should override this if they do any special winding down
        """
        self.winding_down_hosts.add(instance.public_dns_name)
        return True

    def cancel_winddown(self, instance, is_alive=True):
        """Cancel winddown of instance
        is_alive indicates that entity appears to be currently running
        Return True if winddown could be cancelled; false otehrwise
        """
        return False

    def set_target(self, minimum_spot=None, **kwargs):
        """Define function to save target. Called by secretary
        Children typically will read a 'target' field from kwargs
        In general update only if kwargs.get(field) != None
        """
        pass

    def clear_provision(self, hostname):
        """Safely remove *hostname* and its thread from provision_threads"""
        pthread = self.provision_threads.get(hostname)
        if pthread:
            if not pthread.executed:
                pthread.abort()
            del self.provision_threads[hostname]

    def provision(self, instance):
        """Install code on this instance
        Subclasss responsible for settings tags on instance that will drive 
        auto_provision
        """
        prov_thread = self.my_provision_thread(instance, self)
        hostname = instance.public_dns_name
        self.provision_threads[hostname] = prov_thread
        prov_thread.start()

    def wait_for_thread_termination(self, wait_time=None):
        """Wait until all provisioning complete
        Do this by waiting half of wait_time and then aborting all threads
        """

        abort_time = time.time() + wait_time / 2 if wait_time else None

        # abort any provision thread that has not yet executed
        for p in self.provision_threads.values():
            if not p.running:
                p.request_abort = True

        while [p for p in self.provision_threads.values() if not p.executed]:
            if abort_time and time.time() > abort_time:
                break
            time.sleep(1.0)

        # abort any thread still running        
        for thread in self.provision_threads.values():
            if not thread.executed:
                thread.abort()

        # scaling task is now free to terminate threads
