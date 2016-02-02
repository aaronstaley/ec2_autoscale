'''
Managers Pool of servers at low level

Responsible for:
-deploying/cancelling servers
-optimizing server instance lifecycles (spot/normal)

Constraints:
Pools must always bring up the same type of server.  At the bare minimum, this means the same instance type

TODO: Extract pool out for generic class
TODO: Be really careful when we access reservations.
'''
import itertools
import logging
import random
import time

from collections import defaultdict, OrderedDict
from datetime import datetime as dtime
from threading import Thread

from .server_control.server_control import SimulatedInstance, ProvisionException
from .server_control.scaler_server_control import get_server_control
from .scalingmanager import ManagerRequest

from .server_control import service_settings

class Pool(object):
    """A pool is responsible managing instance resources on based on "targeting information"
        Targeting information describes:
            1) Number of instances to utilize
            2) Availability zone information (if relevant)
            3) Hints governing multiple spawning
        
        Pool furthermore has abilities to replace normal instances with spots
        
        Generally the pool is intended to be as stateless as possible
    """
    # Configuration
    boot_spot = False  # Not yet fully supported: If true, requests satisfied by booting spots. If false, always boot normal
    substitute_spot = True  # Should normal instances be substituted be replaced by spot instances when applicable? Implies boot_spot is False
    spot_extra_price_threshold = 0.28  # boot up extra spots if their price is this threshold * normal price
    spot_extra_price_term_threshold = 0.38  # terminate extra spots if their price exceeds  this threshold * normal price
    spot_replace_price_threshold = 0.8  # choose spot instance if it is no more than this threshold * normal price
    spot_price_maxbid_mult = 1.2  # bid no more than this number * ondemand price for spots
    instance_type = None  # instance type this pool manages
    server_role = None  # server role this pool manages (server roles defined in service_settings)
    myname = ""  # unique identifying name for this pool type. Instances are labeled with this

    # Server termination/spawning times
    # Note these numbers are specially tuned for spot replacemnet
    # We work with an assumption that spots will not be satisfied for at least 8 minutes
    # This way a spot replacement will be canceled if we elect to teardown a normal instance

    # TODO: messy.. needs to be part of manager?
    max_teardown_time = 59.5 * 60  # need about 30s to ready termination of instance
    max_spot_replace_launch_time = 55 * 60  # cancel spot if instance to be replaced hits this time

    # spot replacement times are in terms of number of seconds before earliest winddown can occur 
    spot_replace_time_latest = 8 * 60  # an instance being replaced winds down here as well
    spot_replace_time_earliest = 10 * 60

    """
    # Test timings
    min_spot_replacing_teardown_time = 5*60  #51 minutes in we start allowing shutdown for normal instances    
    min_teardown_time = 5*60  #52 minutes in we start allowing shutdown for normal instances
    max_teardown_time = 59.7*60  #need about 15s to ready termination of instance
    
    max_spot_replace_launch_time = 57*60  #cancel spot if instance to be replaced hits this time
    max_spot_replace_time = 46*60 # latest instance time spot replacement for instance can be initiated 
    min_spot_replace_time = 1*60 # earliest instance time spot replacement for instance can be initiated
    """

    managers = None  # managers that dictate pool parameters and analyze server state

    # stateful information:
    spot_replace = None  # table of spots that are being booted up to replace instances. map instance.id to spot objects
    termination_threads = None  # map instance ids to termination threads

    # aws api hackery
    # aws has a habit of showing state unchanged after performing an action
    # Hence we keep an in-memory map for some time; objects not found will be replaced by ones in map
    # Maps id to (object, expire time,force_state)
    # where object is instance/spot request, expire time is time.time() when this should be deleted
    #  and force_state will coerce object returned by aws to have same state as one in this mapping
    instance_override = None
    spotreq_override = None

    # Diagnostic information (new objects created every execute)
    instance_management = {}
    rev_instance_management = {}
    pending_instances = []
    pending_spot_replace_instances = []

    def self_diagnostic(self):
        """Returns a dictionary of diagnostic information about this Pool
        """
        my_dict = {}
        for manager in self.managers:
            my_dict[str(manager)] = manager.self_diagnostic()

        # instances and objects need to be strings
        spot_replace = {}
        for inst_id, spot_req in self.spot_replace.items():
            spot_replace[inst_id] = spot_req.id
        my_dict['spot_replace'] = spot_replace

        if not self.instance_management:
            inst_management = None
        else:
            inst_management = {}
            for manager, instances in self.instance_management.items():
                inst_management[str(manager)] = [str(i) for i in instances]
        my_dict['instance_management'] = inst_management
        my_dict['pending_instances'] = [inst.id for inst in self.pending_instances]
        my_dict['pending_spot_replace_instances'] = [inst.id for inst in self.pending_spot_replace_instances]
        my_dict['termination_threads'] = self.termination_threads.keys()
        my_dict['instance_override'] = self.instance_override.keys()
        my_dict['spotreq_override'] = self.spotreq_override.keys()

        return my_dict

    def __init__(self, scaling_task, instance_type, server_role, myname):
        # Each pool gets own server_control to allow future multithreading and parameterization
        self.server_control = get_server_control()
        self.scaling_task = scaling_task
        self.instance_type = instance_type
        self.server_role = server_role
        self.myname = myname
        self.managers = []
        self.spot_replace = {}
        self.termination_threads = {}
        self.on_demand_cost = self.server_control.get_instance_ondemand_cost(self.instance_type)
        self.max_extra_spot_cost = self.spot_extra_price_threshold * self.on_demand_cost
        self.max_extra_spot_cost_term = self.spot_extra_price_term_threshold * self.on_demand_cost
        self.logger = logging.getLogger('Scaler.pool.%s' % myname.replace('.', '_'))
        self.instance_override = {}
        self.spotreq_override = {}
        # Resolve instance type images here via settings

    def log_email(self, subj, msg, force=False, email_wait=None, logfunc=None, exc_info=False):
        """See ScalingTask.log_email"""
        subj = self.instance_type.upper() + ': ' + subj
        if not logfunc:
            logfunc = self.logger.error
        self.scaling_task.log_email(subj, msg, force, email_wait, logfunc, exc_info)

    def add_manager(self, manager):
        self.managers.append(manager)

    def __str__(self):
        return 'Pool(%s)' % self.myname

    def __repr__(self):
        return self.__str__()

    def find_manager(self, instance):
        """Find the manager that should handle *instance*
        Perform sanity checks as well
        Returns None if no manager desires this instance
        """
        found = None
        for sm in self.managers:
            if sm.should_manage(instance):
                if not found:
                    found = sm
                else:
                    # If 2+ managers want to manage instance, log error and abort
                    msg = 'Multiple managers trying to handle %s. %s requests management when already assigned to %s' % \
                          (instance, sm, found)
                    self.log_email('Critical scaler misconfiguration',
                                   msg=msg,
                                   logfunc=logging.critical)
                    raise Exception(msg)  # crash out
        return found

    def calc_spawn_delta(self, cur_delta, manager_req):
        """
        Determine effective differences between the current delta (servers being spawned)
        and a manager request (manager - current) for purposes of spawning
        
        *cur_delta* is a list of instance-like objects
        *manager_req* is a manager request object from a ScalingManager
        
        Returns dictionary mapping az to # of servers to be spawned
        None indicates don't care        
        """
        diff_delta = OrderedDict()
        if manager_req.delta_request <= 0:  # only worry about spawn requests
            return diff_delta
        diff_delta[None] = 0  # any az is assigned first

        cur_az = defaultdict(int)
        for inst in cur_delta:
            cur_az[inst.placement] += 1

        absolute_diff = manager_req.delta_request - len(cur_delta)
        for az, cnt in manager_req.az_minimums.items():
            dt = cnt - cur_az[az]
            if dt > 0:  # manager wants more of this type than pool is currently requesting
                diff_delta[az] = dt

        az_placed = sum(diff_delta.values())  # new spawns assigned to sepcific az
        diff_delta[None] = max(0, absolute_diff - az_placed)
        return diff_delta

    def generate_reservation_count(self, req_servers, rt_pattern_ptr):
        """A generator that returns the sizes of reservations that should be made
        for spawning *req_servers* under *rt_pattern_ptr['rt_pattern']*
        After generator is done, rt_pattern_ptr['rt_pattern'] will be set to the next rt_pattern
        for spawning additional servers beyohnd req_servers
        """

        req_left = req_servers
        i = -1
        num_svr = 0

        for i, num_svr in enumerate(rt_pattern_ptr['rt_pattern']):
            if not req_left:
                break
            if num_svr > req_left:
                yield req_left
                num_svr -= req_left
                req_left = 0
                break
            else:
                req_left -= num_svr
                yield num_svr

        new_pattern = list(rt_pattern_ptr['rt_pattern'][i + 1:])
        if num_svr:
            new_pattern.insert(0, num_svr)
        rt_pattern_ptr['rt_pattern'] = new_pattern
        rt_pattern_ptr['updated'] = True  # sentinel for early termination

        # Generate remainding serially
        for x in [1] * req_left:
            yield x

    def generate_normal_request(self, batch_size, availzone):
        """Request normal requests of *batch_size* in *avail_zone*
        This does not update pricingmgr; that is caller's responsibility
        """
        if batch_size < 1:
            raise ValueError('Invalid batch_size %s' % batch_size)

        instances = self.server_control.spawn_server(self.instance_type, self.server_role,
                                                     num_servers=batch_size, min_servers=1,
                                                     availzone=availzone)
        self.server_control.scaler.set_pool_managing(instances, self.myname)
        self.server_control.set_name(instances,
                                     '%s_%s_autospawn' % (self.myname.split('_')[0], self.server_control.effective_env))

        return instances

    def generate_spot_request(self, batch_size, availzone):
        """Request spot requests of *batch_size* in *avail_zone*
        Requests request object"""
        if batch_size < 1:
            raise ValueError('Invalid batch_size %s' % batch_size)

        # bid over ondemand to avoid shutdowns
        bid = self.spot_price_maxbid_mult * self.on_demand_cost

        availzone = None  # hack - let amazon place our spots to ensure lower pricing.. (fix up later!)

        spotreqs = self.server_control.spawn_spot_server(bid, self.instance_type, self.server_role,
                                                         num_servers=batch_size, availzone=availzone)
        self.server_control.scaler.set_pool_managing(spotreqs, self.myname)
        self.server_control.set_name(spotreqs,
                                     '%s_%s_autospawn' % (self.myname.split('_')[0], self.server_control.effective_env))

        return spotreqs

    def cancel_spot_requests(self, spot_reqs):
        if not hasattr(spot_reqs, '__iter__'):
            spot_reqs = [spot_reqs]

        for spot_req in spot_reqs:
            if spot_req.state == 'active':
                self.logger.error('Cannot terminate active spot_req %s' % spot_req)
                return  # raise exception?
            else:
                spot_req.state = 'cancelled'
                self.set_override(spot_req, True, 60, True)

        self.server_control.cancel_spot_requests(spot_reqs)

        spot_ids = [spot_req.id for spot_req in spot_reqs]
        for instance_id, spot in self.spot_replace.items():
            if spot.id in spot_ids:
                del self.spot_replace[instance_id]

    def cancel_spot_replacement(self, instance):
        """Cancel spot replacement for *instance* if it is currently happening"""
        spot_repl = self.spot_replace.get(instance.id)
        if spot_repl:
            if spot_repl.state == 'open':
                self.logger.info('Cancelling spot replacement %s for instance %s', spot_repl, instance)
                self.cancel_spot_requests(spot_repl)

    def terminate_instances(self, instances, instance_management):
        """Terminate these instances with correct teardown"""

        # TODO: This should run teardown in another thread
        # For now, we consider teardown process to be so fast that it is ok to be serial
        for instance in instances:
            if instance.id in self.termination_threads:
                self.logger.warn('Instance %s already terminating. ignoring termination request', instance)
                continue

            msg = 'Running teardown of %s (%s)' % (instance, instance.public_dns_name)
            self.log_email('Shutdown instance', msg, logfunc=self.logger.info, force=False)

            term_thread = TerminationThread(instance, self)
            self.termination_threads[instance.id] = term_thread
            term_thread.start()

            self.terminated_server(instance, instance_management)

    def terminated_server(self, instance, instance_management=None):
        """Notify that we have terminated this server"""

        # cancel out any spot replacement for this server:
        self.cancel_spot_replacement(instance)

        # mark spot instances as cleared
        spot_repl = self.spot_replace.get(instance.id)
        if spot_repl:
            self.logger.info('instance %s terminating; was successfully replaced by spot req %s', instance, spot_repl)
            del self.spot_replace[instance.id]

        # wipe from instance_management dict
        if instance_management:
            for running_instances in instance_management.values():
                if instance in running_instances:
                    running_instances.remove(instance)
                    break

    def pre_update_spot_replacements(self, running_instances, spot_requests):
        """
        Performs spot replacement updates before execute
        -Remap spot replacements
        -Clean out removed spot_replacements/instances
        """

        for inst_id, oldreq in self.spot_replace.items():
            for sreq in spot_requests:
                if oldreq.id == sreq.id:
                    self.spot_replace[inst_id] = sreq
                    break
            """
            # TODO: Amazon has consistency issues where a subsequent describe spot requests does not 
            #   return one earlier requested
            # Figure out a way to resolve this
            else: #spot was deleted
                # TODO: Email?
                self.logger.warn('pre_update_spot_replacements: Spot %s disappeared. purging from dict' % oldreq)
                del self.spot_replace[inst_id]
                continue
            """

            for instance in running_instances:
                if instance.id == inst_id:
                    break
            else:
                self.logger.warn(
                    'pre_update_spot_replacements: Instance %s to-be replaced by spot %s disappeared. purging from dict',
                    inst_id, oldreq)
                del self.spot_replace[inst_id]

    def post_update_spot_replacements(self, running_instances):
        """
        Performs spot replacement updates after execution
        -initiate replacement
        -removing completed replacement        
        """

        """
        TODO: Consider replacing expensive spots with normals?
        
        Internal notes: We don't currently check if an instance is in 'killing mode'.
            In fact, we don't even globally see if a manager is killing an instance
            Fortunately for workers this doesn't matter, as possible spot replacement times 
                do not intersect worker teardown times        
        """

        """
        Internal notes:
        AZ is not respected here.  Managers cannot currently communicate a 
        "minimum" number of servers required .  
        We can either use current az (when we care about distibution of workers)
        Or we can use any az (current policy as we don't care)
        """
        pricingmgr = self.scaling_task.pricing_manager

        cancel_spots = []
        for instance in running_instances:
            if self.can_initiate_spot_replacement(instance):
                spot_repl = self.spot_replace.get(instance.id)
                if spot_repl:
                    if spot_repl.state == 'open':
                        # see if if it is too expensive
                        spot_price = pricingmgr.get_cost(spot_repl, self)
                        if spot_price > self.spot_replace_price_threshold * self.on_demand_cost:
                            cancel_spots.append(spot_repl)
                        continue

                # hack: do not have more than 100 spot replace outstanding
                # as we could have a bug if this is occuring
                if len(self.spot_replace) > 100:
                    continue

                normal_cost = pricingmgr.get_cost(instance, self)
                # Allow any az but prioritize instance's current az
                az_list = service_settings.usable_zones[:]
                best_az = instance.placement
                az_list.remove(best_az)
                az_list.insert(0, best_az)

                # hack: we are controlling so much t1.micro we are exhausting available spots in a single az
                if self.instance_type == 't1.micro':
                    random.shuffle(az_list)

                spot_cost, spot_az = pricingmgr.new_spot_instance_cost(instance.instance_type,
                                                                       az_list=az_list)

                replace = spot_cost < (self.spot_replace_price_threshold * normal_cost)
                self.logger.debug(
                    'Considering spot replacement on %s. it costs %s. Spot costs %s in az %s replacing? %s',
                    instance, normal_cost, spot_cost, spot_az, replace)

                if replace:  # TODO: These need to be batched up. Build by az and mass execute
                    spot_reqs = self.generate_spot_request(1, spot_az)
                    msg = 'Replacing instance %s (az %s) with spot %s in az %s. \n norm cost=%s. spot cost=%s' % \
                          (instance, instance.placement, spot_reqs, spot_az, normal_cost, spot_cost)
                    self.log_email('Spot replacing', msg=msg, force=True, logfunc=self.logger.info)
                    self.spot_replace[instance.id] = spot_reqs[0]

            elif not self.can_fulfill_spot_replacement(instance):  # cancel spots
                spot_req = self.spot_replace.get(instance.id)
                if not spot_req:
                    continue
                if spot_req.state == 'open':
                    msg = '%s to replace %s was open too long.' % (spot_req, instance)
                    self.log_email('Cancelling spot replacement', msg,
                                   force=True, logfunc=self.logger.info)
                    self.cancel_spot_replacement(instance)
                else:  # too late for cancel. normal likely cannot wind down
                    self.logger.debug(
                        'Never shut down instance %s that was shut down by spot %s. This is expected behavior',
                        instance, spot_req)
                    del self.spot_replace[instance.id]

        if cancel_spots:
            msg = 'Aborting spot replacement. %s spots were too expensive. first spot was %s. max price %s' % \
                  (len(cancel_spots), pricingmgr.get_cost(cancel_spots[0], self),
                   self.spot_replace_price_threshold * self.on_demand_cost)
            self.log_email('Cancelling spot replacement', msg=msg, force=True, logfunc=self.logger.warning)
            self.cancel_spot_requests(cancel_spots)

            # Warning: timing functions are AWS dependent!

    def can_terminate(self, instance):
        """Return if it makes sense to start terminating a wound down instance"""
        effective_uptime = self.server_control.get_instance_uptime_since_charge(instance)
        floor = self.server_control.uptime_mod_factor / 4  # arbitrary floor; 15 minutes by default
        return floor < effective_uptime < self.max_teardown_time

    def can_initiate_spot_replacement(self, instance):
        """Can we start spot replacement process?"""
        if self.server_control.is_spot(instance):  # only replace normal instances
            return False
        effective_uptime = self.server_control.get_instance_uptime_since_charge(instance)
        mgr = self.rev_instance_management.get(instance.id)
        # calculate when winddown will start by:
        ear_wind = mgr.winddown_min if mgr else self.max_teardown_time
        return ear_wind - self.spot_replace_time_earliest < effective_uptime < \
               ear_wind - self.spot_replace_time_latest

    def can_fulfill_spot_replacement(self, instance):
        """Can we continue letting spot boot up to replace?
        Spot should be cancelled if this returns False"""
        if self.server_control.is_spot(instance):  # only replace normal instances
            return False
        effective_uptime = self.server_control.get_instance_uptime_since_charge(instance)
        mgr = self.rev_instance_management.get(instance.id)
        ear_wind = mgr.winddown_min if mgr else self.max_teardown_time  # when winddown needs to start by
        return ear_wind - self.spot_replace_time_earliest < effective_uptime < \
               self.max_spot_replace_launch_time

    instance_logging_period = 45  # number of seconds between successive instance log dumping
    # timestamp of last instance log (randomized so different pools log at different times)
    last_instance_log = time.time() - instance_logging_period * random.random()

    def handle_instance_logging(self, instances):
        safe_query = True  # enable for debugging

        if time.time() - self.last_instance_log < self.instance_logging_period:
            return

        self.last_instance_log = time.time()
        now = dtime.utcnow()

        for instance in instances:
            launch_time = dtime.strptime(instance.launch_time, '%Y-%m-%dT%H:%M:%S.000Z')
            is_spot = self.server_control.is_spot(instance)

            instance_dct = {'pool_name': self.myname,  # e.g. simple
                            'az': instance.placement,
                            'launch_time': launch_time,
                            'last_check': now,
                            'is_spot': is_spot,
                            'hostname': instance.public_dns_name,
                            'termination_time': None,
                            }

            if is_spot and hasattr(instance, 'spot_request'):
                instance_dct['spot_launch'] = dtime.strptime(instance.spot_request.create_time,
                                                             '%Y-%m-%dT%H:%M:%S.000Z')

        # mark all terminated that are not already marked terminated
        seen_ids = [instance.id for instance in instances]
        criteria = {'_id': {'$nin': seen_ids}, 'termination_time': None, 'pool_name': self.myname}
        self.logger.debug('handle_instance_logging took %s s', time.time() - self.last_instance_log)

    def clear_expired_overrides(self):
        now = time.time()

        for ov_dict in [self.instance_override, self.spotreq_override]:
            for iid, (obj, expire, force_state) in ov_dict.items():
                if now > expire:
                    del ov_dict[iid]

    def set_override(self, obj, is_spot, expire_time=60, force_state=False):
        ov_dict = self.spotreq_override if is_spot else self.instance_override
        ov_dict[obj.id] = (obj, expire_time + time.time(), force_state)

    def handle_overrides(self, obj_list, is_spot):
        """Returns a new list correctly overridden by relevant override dictionary"""
        ov_dict = self.spotreq_override if is_spot else self.instance_override
        aws_iids = set((obj.id for obj in obj_list))
        for obj in obj_list:
            if obj.id in ov_dict:
                repl_obj, expire, force_state = ov_dict[obj.id]
                if force_state and obj.state != repl_obj.state:
                    self.logger.debug('handle_overrides: Override %s state from %s to %s', obj, obj.state,
                                      repl_obj.state)
                    obj.state = repl_obj.state
        for iid, (repl_obj, expire, force_state) in ov_dict.items():
            if iid not in aws_iids:
                self.logger.debug('handle_overrides: obj %s not returned by aws. Injecting', repl_obj)
                obj_list.append(repl_obj)

    def execute(self, instances, spot_requests, **kwds):
        """Scale based on information provided by scaling managers"""

        self.clear_expired_overrides()
        self.handle_overrides(instances, False)
        self.handle_overrides(spot_requests, True)

        pricingmgr = self.scaling_task.pricing_manager

        instance_management = defaultdict(list)  # dict mapping manager to instances
        rev_instance_management = {}  # map instance ids to manager

        # clean out instances that are being torn down
        instances = [inst for inst in instances if inst.id not in self.termination_threads]
        # and all termianted
        instances = [inst for inst in instances if inst.state in ['running', 'pending']]

        self.handle_instance_logging(instances)

        running_instances = [inst for inst in instances if inst.state == 'running']
        pending_instances = [inst for inst in instances if inst.state == 'pending']
        open_spots = [spotreq for spotreq in spot_requests if spotreq.state == 'open']
        # spot requests that are booting up:
        pending_spots = [spotreq for spotreq in spot_requests if
                         spotreq.state == 'active' and spotreq.instance.state == 'pending']
        # instances that are booting up to replace running ones
        spot_replace_ids = [spotreq.id for spotreq in self.spot_replace.values()]
        pending_spot_replace_instances = [spotreq.instance for spotreq in pending_spots if
                                          spotreq.id in spot_replace_ids]

        # diag info
        self.pending_instances = pending_instances
        self.pending_spot_replace_instances = pending_spot_replace_instances
        self.logger.debug(
            'running_instances = %s, pending_instances = %s, all_spots = %s, open_spots = %s, pending_spots = %s, pending_spot_replace_instances = %s',
            running_instances, pending_instances, spot_requests, open_spots, pending_spots,
            pending_spot_replace_instances)

        if self.spot_replace:
            self.pre_update_spot_replacements(running_instances, spot_requests)

        for inst in running_instances:
            inst_manager = self.find_manager(inst)
            instance_management[inst_manager].append(inst)
            rev_instance_management[inst.id] = inst_manager

        self.instance_management = instance_management
        self.rev_instance_management = rev_instance_management
        self.logger.debug('instance management is %s', instance_management)

        # Instances that can be allocated (just transitioned from pending to running)
        unmanaged_instances = instance_management[None]

        # Get targetting information from managers
        manager_reqs = {}
        net_req = ManagerRequest()  # overall target

        for manager in self.managers:
            manager_instances = instance_management[manager]

            try:
                manager_req = manager.get_request(manager_instances, **kwds)
            except Exception:
                self.log_email('Manager crashed',
                               msg='manager %s raised exception' % manager,
                               exc_info=True)
                if self.server_control.effective_env != 'live':  # TODO(developer): You can add more complex logic here
                    raise
                else:
                    manager_req = None
            else:
                self.logger.debug('manager %s returns request %s', str(manager), str(manager_req))

            if not manager_req:  # signal for disable
                continue

            manager_reqs[manager] = manager_req
            net_req = net_req + manager_req

        self.logger.debug('net_req is %s', net_req)

        # Terminate instances that are releasable
        # TODO: Extra optimization: We could assign them to another manager
        releasable = net_req.released
        if releasable:
            self.terminate_instances(releasable, instance_management)
            running_instances = itertools.chain.from_iterable(
                instance_management.values())  # rebuild after modification

        """
        Assign unmanaged spawned instances
        """
        shuffled_managers = self.managers
        random.shuffle(shuffled_managers)  # allow random assignment if multiple managers need instances

        # process normal instances before spots (to provide normal by desirability and spot by spot_replace dict)
        unmanaged_instances.sort(key=lambda ui: self.server_control.is_spot(ui))

        assigned_management = defaultdict(list)  # instances assigned in below loop
        for inst in unmanaged_instances:
            # First give instance to manager desiring it
            self.logger.debug('Considering unmanaged instance %s', inst)
            max_score, target_mgrs = -1, None
            for mgr in shuffled_managers:
                req = manager_reqs.get(mgr)
                if req:
                    score = req.desire_possession(inst, instance_management.get(mgr) + assigned_management[mgr],
                                                  self.server_control.is_spot)
                    self.logger.debug('manager %s req %s wants new inst %s with score %s',
                                      mgr, req, inst, score)
                    if score > max_score:
                        max_score, target_mgrs = score, [mgr]
                    elif score == max_score:
                        target_mgrs.append(mgr)

            assert (target_mgrs)
            target_mgr = target_mgrs[0] if len(target_mgrs) < 2 else None
            if not target_mgr:  # tie breaker
                # 1st tie breaker: find instance being replaced and send spot to that manager
                repl_id_it = (repl_id for repl_id, spot_req in self.spot_replace.items() \
                              if getattr(spot_req, 'instance', None) == inst)
                repl_id = next(repl_id_it, None)
                if repl_id:  # resolve which manager instance being replaced is assigned to
                    self.logger.debug('%s from spot %s replacing %s',
                                      inst, self.spot_replace[repl_id], repl_id)
                    mgr = rev_instance_management.get(repl_id)
                    if mgr and mgr in target_mgrs:
                        target_mgr = mgr
                        self.logger.info('Successfully replaced %s with spot_req %s (inst %s). manager %s',
                                         repl_id, self.spot_replace[repl_id],
                                         self.spot_replace[repl_id].instance, mgr)
                        # We intentionally keep spot replace active until normal shuts down
                        #   to ensure that replaced server is wound down with highest priority  

                # secondary tie breaker: random manager
                if not target_mgr:
                    target_mgr = target_mgrs[0]  # assign to first manager if no one wants it
                    self.logger.debug('fallback to random manager %s for new inst %s',
                                      target_mgr, inst)

            target_mgr.provision(inst)
            req = manager_reqs.get(target_mgr)
            if req:
                req.captured_server(inst)
            assigned_management[target_mgr].append(inst)

        """
        Server spawn/teardown
        """
        # Calcuate current delta - it will be a list of instances and spots
        curdelta = unmanaged_instances + pending_instances

        # note if no substituion and boot_spot, curdelta will likely include open spots

        """Handle spawn requests"""
        spawned_this_round = 0
        if net_req.delta_request > 0:
            spawn_delta = self.calc_spawn_delta(curdelta, net_req)  # spawn_delta is az-># spawn
            total_spawning_servers = sum(spawn_delta.values())
            if total_spawning_servers:
                self.log_email('Spawning servers',
                               msg='spawn_delta is %s for delta_request %s and curdelta_inst %s' % \
                                   (spawn_delta, net_req.delta_request, curdelta),
                               force=True,
                               logfunc=self.logger.info)

            # slide past rt_pattern acounted for by current pending instances
            rt_pattern_ptr = {'rt_pattern': net_req.batches, 'updated': False}
            skip_count = len(curdelta)

            for _ in self.generate_reservation_count(skip_count, rt_pattern_ptr):
                if rt_pattern_ptr['updated']:
                    break
            self.logger.debug('Skipped %s batches within %s. Pattern now %s',
                              skip_count, net_req.batches, rt_pattern_ptr['rt_pattern'])

            # walk over different zones we need to spawn servers in
            for az, spawn_cnt in spawn_delta.items():
                rt_pattern_ptr['updated'] = False
                az_list = [az] if az else None

                # walk over the different batches that are to be spawned
                for num_instances in self.generate_reservation_count(spawn_cnt, rt_pattern_ptr):
                    self.logger.debug('Will attempt to spawn %s instances under spawn_count %s in az %s on pattern %s',
                                      num_instances, spawn_cnt, az, rt_pattern_ptr['rt_pattern'])
                    use_spot = False
                    desired_az = None
                    spawn_size = 0
                    force_no_az = False

                    while num_instances:  # loop until every instance is spawned

                        init = False
                        # inner loop splits batch as pricingmgr dictates different az/spot
                        while num_instances and spawn_size < num_instances:
                            # If there is a change, we break out of loop and spawn previously determined batch                     
                            normal_cost, normal_az = pricingmgr.new_normal_instance_cost(self.instance_type,
                                                                                         az_list)
                            if self.boot_spot:
                                spot_cost, spot_az = pricingmgr.new_spot_instance_cost(self.instance_type,
                                                                                       az_list)
                                new_use_spot = spot_cost < (self.spot_replace_price_threshold * normal_cost)
                            else:
                                new_use_spot = False
                                spot_az = None
                            new_az = spot_az if use_spot else normal_az

                            # TODO: We could have a flag for "any az allowed" to allow batching to go further
                            if init and (desired_az != new_az or new_use_spot != use_spot):
                                break
                            init = True
                            desired_az = new_az
                            use_spot = new_use_spot
                            spawn_size += 1

                            # Add a "virtual server" to pricingmgr so cost of next instance can be calculated
                            if not use_spot:
                                virtinst = SimulatedInstance(instance_type=self.instance_type,
                                                             availzone=desired_az)
                                pricingmgr.notify_spawned(virtinst, self)

                        if force_no_az:
                            desired_az = None

                        self.logger.info('Requesting spawn of %s servers in az %s. spot? %s. normal cost is %s',
                                         spawn_size, desired_az, use_spot, normal_cost)
                        try:
                            if use_spot:
                                insts = self.generate_spot_request(batch_size=spawn_size, availzone=desired_az)
                            else:
                                insts = self.generate_normal_request(batch_size=spawn_size, availzone=desired_az)
                            spawned_this_round += len(insts)
                        except ProvisionException, p:
                            if p.err_code == ProvisionException.NO_CAPACITY:
                                if force_no_az:
                                    self.log_email('Cannot spawn instances!', 'Something went wrong',
                                                   exc_info=True)
                                    break

                                if az_list and desired_az in az_list:
                                    az_list.remove(desired_az)  # recalculate
                                if not az_list:
                                    force_no_az = True
                        else:
                            num_instances -= spawn_size

        """
        Termination
        After teardown of *inst* is initiated, manager is responsible for returning False on can_winddown(inst)
        """
        for manager, req in manager_reqs.items():
            if req.delta_request < 0:
                # TODO: Add spot canceling if boot spot

                managed_instances = instance_management[manager]
                terminatable_instances = [inst for inst in managed_instances if \
                                          manager.can_winddown(inst)]

                cur_spots = [inst for inst in managed_instances if self.server_control.is_spot(inst)]
                num_cur_spots = len(cur_spots)

                self.logger.debug(
                    'For manager %s, want to terminate %s instances (but keep %s spots - %s are online). Terminatable %s',
                    manager, -req.delta_request, req.minimum_spot, num_cur_spots, terminatable_instances)

                cost_dict = {}
                for inst in terminatable_instances:
                    cost_dict[inst.id] = pricingmgr.get_cost(inst, self)

                # sort from most expensive to least
                # ties broken by most uptime since charge (soonest termination)
                now = time.time() + time.timezone
                terminatable_instances.sort(key=lambda inst: (cost_dict[inst.id],
                                                              self.server_control.get_instance_uptime_since_charge(inst,
                                                                                                                   now)),
                                            reverse=True)

                for inst in terminatable_instances:
                    if req.desire_termination(inst):

                        # spot check
                        inst_is_spot = self.server_control.is_spot(inst)

                        # preserve minimum spots
                        if inst_is_spot and cost_dict[inst.id] <= self.max_extra_spot_cost_term and \
                                        num_cur_spots <= req.minimum_spot:
                            continue

                        if manager.start_winddown(inst):
                            req.captured_server(inst)
                            self.cancel_spot_replacement(inst)
                            if inst_is_spot:
                                num_cur_spots -= 1

                    if not req.delta_request:  # stop when satisfied
                        break

        """
        Boot up extra spot to satisfy request,minimum spot
        """
        minimum_spot = net_req.minimum_spot
        all_requests = instances + open_spots
        all_requests_cnt = len(all_requests) + spawned_this_round

        self.logger.debug('Minimum spots requested is %s. all req cnt %s num spot replace %s. spawn this round %s',
                          minimum_spot, all_requests_cnt, len(self.spot_replace), spawned_this_round)

        # boot up only when minimum spot is larger than all requests on system
        if minimum_spot > all_requests_cnt:
            spawn_delta = minimum_spot - all_requests_cnt
            normal_cost = self.server_control.get_instance_ondemand_cost(self.instance_type)
            # az is constant for all spots as their pricing not affected by reservations      
            az_list = None  # TODO: Allow manager to constrain this
            spot_cost, spot_az = pricingmgr.new_spot_instance_cost(self.instance_type, az_list)
            if spot_cost <= self.max_extra_spot_cost:
                spot_reqs = self.generate_spot_request(spawn_delta, spot_az)
                for spot_req in spot_reqs:
                    self.set_override(spot_req, True, 60, False)

                msg = 'Launching %s excess spot %s in az %s. \n norm cost=%s. spot cost=%s' % \
                      (spawn_delta, spot_reqs, spot_az, normal_cost, spot_cost)
                self.log_email('Adding extra spot', msg=msg, force=True, logfunc=self.logger.info)

        elif minimum_spot < all_requests_cnt - len(self.spot_replace):
            # Cancellation: Ignore spot requests that are part of spot replacement.

            max_cancel = all_requests_cnt - len(self.spot_replace) - minimum_spot

            # only cancel ones that aren't spot replacements
            cancellable_spots = [spotreq for spotreq in open_spots if spotreq.id not in spot_replace_ids]
            cost_dict = {}
            for spot in cancellable_spots:
                cost_dict[spot.id] = pricingmgr.get_cost(spot, self)
            cancellable_spots.sort(key=lambda spot: (cost_dict[spot.id], spot.create_time),
                                   reverse=True)
            cancel_spots = cancellable_spots[:max_cancel]
            if cancel_spots:
                msg = 'Aborting %s spots. cancellable_spots: %s, minimum_spot: %s. max_cancel: %s, spots cancelling: %s' % \
                      (len(cancel_spots), len(cancellable_spots), minimum_spot, max_cancel, cancel_spots)
                self.log_email('Cancelling extra spot', msg=msg, force=True, logfunc=self.logger.info)
                self.cancel_spot_requests(cancel_spots)

        # clear out overpriced min_spot (as request)
        open_nonspotrepl = [spotreq for spotreq in open_spots if
                            spotreq.id not in spot_replace_ids and spotreq.state == 'open']
        cancel_spots = [spotreq for spotreq in open_nonspotrepl if
                        pricingmgr.get_cost(spotreq, self) > self.max_extra_spot_cost]
        if cancel_spots:
            msg = 'Aborting %s spots. Were too expensive. first spot was %s. max price %s' % \
                  (len(cancel_spots), pricingmgr.get_cost(cancel_spots[0], self), self.max_extra_spot_cost)
            self.log_email('Cancelling extra spot', msg=msg, force=True, logfunc=self.logger.warning)
            self.cancel_spot_requests(cancel_spots)

        """
        Spot replacement
        """
        if self.substitute_spot:
            # ignore all winding down instances
            # Also filter out instances that are likely to be shut down
            possible_instances = []
            for mgr, instances in instance_management.items():
                req = manager_reqs.get(mgr)
                if not req:  # dont do spot replacement on disabled managers
                    continue

                    # only factor in normals not winding down
                replacable_inst = [inst for inst in instances \
                                   if not mgr.is_windingdown(inst) and not self.server_control.is_spot(inst)]

                """
                skip past the next normals that are likely to be terminated
                Note: This weakens minimum spot. We could have len(servers) == minimum_spot; 
                    termination of normal (that won't be replaced) will cause len(servers) < minimum_spot;
                eventually spot request will come in and raise back number.
                This is an edge case though.. and minimum_spot isn't meant to be a strong guarentee regardless
                """

                now = time.time()
                replacable_inst.sort(key=lambda i: self.server_control.get_instance_uptime_since_charge(i, now),
                                     reverse=True)
                if req.delta_request < 0:
                    for ri in replacable_inst[:]:
                        if req.desire_termination(ri):
                            req.captured_server(ri)
                            replacable_inst.remove(ri)
                possible_instances.extend(replacable_inst)

            self.post_update_spot_replacements(possible_instances)

    def wait_for_thread_termination(self, wait_time=None):
        """Wait until all server termination is complete"""
        self.logger.debug('Waiting on terminations to complete')
        abort_time = time.time() + wait_time if wait_time else None
        while self.termination_threads:
            if abort_time and time.time() > abort_time:
                break
            time.sleep(1.0)
        # terminate everything
        for inst_id, tthread in self.termination_threads.items():
            del self.termination_threads[inst_id]
            tthread.terminate()


class TerminationThread(Thread):
    """Thread to handle termination"""
    instance = None
    pool = None

    def __init__(self, instance, pool):
        self.instance = instance
        self.pool = pool
        super(TerminationThread, self).__init__()

    def error_emailer(self, msg, exc_info=False):
        self.pool.log_email(subj='Teardown error', msg=msg, exc_info=exc_info)

    def run(self):
        try:
            self.pool.server_control.auto_teardown(self.instance, do_termination=True,
                                                   error_logger=self.error_emailer)
        except Exception:
            self.error_emailer('something went wrong with %s' % self.instance, exc_info=True)
        finally:
            self.pool.logger.info('Finished tearing down %s', self.instance)
            self.instance.state = 'terminated'
            self.pool.set_override(self.instance, False, 300, True)
            try:
                del self.pool.termination_threads[self.instance.id]
            except KeyError:  # Not sure why this occurs in rare cases
                self.pool.logger.warning('termination thread for %s was not in pool.termination threads?',
                                         self.instance.id)


"""
Future notes

    meta data gives uptime info:
    http://shlomoswidler.com/2011/02/play-chicken-with-spot-instances.html
    
    programatic pricing at:
    http://aws.amazon.com/ec2/pricing/pricing-on-demand-instances.json
"""
