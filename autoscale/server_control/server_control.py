'''
Low-level object to bring up and tear down servers 
Abstraction note: Instance objects are expected to conform to boto standard

Note that this method is not intended to be auto-scaler specific; it is a generic server booting system.
'''
import __builtin__
from datetime import datetime as dtime
import errno
import socket
import sys
import time

import logging

logger = logging.getLogger('ServerControl')


class ServerControlException(Exception):
    """Generic exception class for internals of server_control going wrong
    Note: Not every lower level exception is intercepted; in rare cases a non ServerControlException may be raised.
    """


class ConnectionException(ServerControlException):
    """Indicates that an ssh connection to a server failed
    """

    # error codes
    TIMEOUT = 1  # Connection timed out
    DNS_NOTFOUND = 10  # DNS could not be resolved
    HOSTUNREACH = 11  # Host unreachable / No route to host
    HOSTDOWN = 50  # Host down
    CONNREFUSED = 51  # sshd not running on server
    CONNRESET = 52  # connection was reset
    AUTHINVALID = 100  # authentication failure
    SSHERROR = 101  # some other ssh error
    SFTPERROR = 102  # error
    EOFERROR = 103  # end of file error (connection cut?)
    ABORT = 1000  # user aborted provisioning
    OTHER = 10000  # unknown errpr

    hrm = {TIMEOUT: 'Connection timed out',
           DNS_NOTFOUND: 'DNS could not be resolved',
           HOSTUNREACH: 'Could not reach host by ip address',
           HOSTDOWN: 'Host is down',
           CONNREFUSED: 'SSH connection refused',
           CONNRESET: 'SSH Connection reset',
           AUTHINVALID: 'SSH Authentication failed',
           SSHERROR: 'Other SSH Error',
           SFTPERROR: 'Other SFTP Error',
           EOFERROR: 'End of File error',
           ABORT: 'User initiated connection abort',
           OTHER: 'Other error'
           }

    err_code = None

    # Convert error to policy
    def server_maybe_booting(self):
        """Returns if this error could occur due to booting up process
        In other words, we should NOT retry"""
        return self.err_code not in [self.DNS_NOTFOUND, self.HOSTDOWN, self.ABORT, self.OTHER]

    def boot_finished_soon(self):
        """Returns true if this error could be a very short-lived one as part of boot up process.
        Within seconds the server should be online        
        Retuning true implies server_maybe_booting() is true
        """

        # Could occur if sshd is slow to boot. Should resolve itself very fast
        return self.err_code in [self.TIMEOUT]

    @classmethod
    def from_exception(cls, excp):
        """Alternate constructor to build this exception from lower-level exceptions"""
        import paramiko

        msg = str(excp)
        if isinstance(excp, socket.timeout):
            return cls(msg, cls.TIMEOUT)
        elif isinstance(excp, socket.gaierror):
            if excp.errno == socket.EAI_NONAME:
                return cls(msg, cls.DNS_NOTFOUND)
        elif isinstance(excp, socket.error):  # general socket errors have errno set to entry in errno module
            if excp.errno == errno.ECONNREFUSED:
                return cls(msg, cls.CONNREFUSED)
            elif excp.errno == errno.EHOSTDOWN:
                return cls(msg, cls.HOSTDOWN)
            elif excp.errno == errno.EHOSTUNREACH:
                return cls(msg, cls.HOSTUNREACH)
            elif excp.errno == errno.ECONNRESET:
                return cls(msg, cls.CONNRESET)
            elif excp.errno == errno.ETIMEDOUT:  # sometimes timeout is not wrapped as socket.timeout
                return cls(msg, cls.TIMEOUT)
        elif isinstance(excp, paramiko.AuthenticationException):  # subclass of SSHException
            return cls(msg, cls.AUTHINVALID)
        elif isinstance(excp, paramiko.SSHException):
            return cls(msg, cls.SSHERROR)
        elif isinstance(excp, paramiko.SFTPError):
            return cls(msg, cls.SFTPERROR)
        elif isinstance(excp, EOFError):
            return cls(msg, cls.EOFERROR)
        # TODO(developer): Add more exceptions here.

        return cls(msg, cls.OTHER)  # unknown error

    def __str__(self):
        if self.err_code not in [self.OTHER, self.SSHERROR, self.SFTPERROR]:
            return self.hrm[self.err_code]
        else:
            return str((self.hrm[self.err_code], super(ConnectionException, self).__str__()))

    def __init__(self, message, err_code):
        self.err_code = err_code
        super(ConnectionException, self).__init__(message)


class ProvisionException(ServerControlException):
    """Indicates that server provisioning somehow failed
    (Currently only exists on the waiting on server/spot functions)"""

    # Error codes
    TIMEOUT = 1  # Some form of timeout
    TERMINATED = 2  # Instance terminated while booting up
    NO_INSTANCE = 3  # Active spot request instance could not be resolved
    NO_CAPACITY = 4  # amazon reports no capactiy for this type of instance

    hrm = {TIMEOUT: 'Waiting timed out',
           TERMINATED: 'Unexpected termination',
           NO_INSTANCE: 'Spot instance unresolved',
           NO_CAPACITY: 'No capacity for this instance/az'
           }

    err_code = None

    def __init__(self, err_code, message):
        self.err_code = err_code
        super(ProvisionException, self).__init__(message)

    def __str__(self):
        return str((self.hrm[self.err_code], super(ProvisionException, self).__str__()))


class SimulatedInstance(object):
    """Simulated Instance used for internal wrapping and infrastructure simulation"""
    launch_time = None
    public_dns_name = None
    instanceLifecycle = 'normal'
    instance_type = None
    placement = None
    state = 'pending'
    id = None

    def __init__(self, instance_type, id=None, availzone=None, state='pending', is_spot=False,
                 public_dns_name=None, launch_time=None):
        self.instance_type = instance_type
        self.placement = availzone
        if launch_time:
            self.launch_time = launch_time
        else:
            self.launch_time = time.strftime('%Y-%m-%dT%H:%M:%S.000Z', time.gmtime())
        if not id:
            id = __builtin__.id(self)
        self._id = id

        self.public_dns_name = public_dns_name
        if is_spot:
            self.instanceLifecycle = 'spot'
        self.state = state

    @property
    def id(self):
        return self._id

    def __str__(self):
        return 'SimulatedInstance(id=%s, type=%s, az=%s, lifecycle=%s)' % (self.id, self.instance_type,
                                                                           self.placement, self.instanceLifecycle)

    __repr__ = __str__


class ServerControl(object):
    """Top level controller object to manage underlying infrastructure"""

    upTimeModFactor = None  # set to billable unit (for non-prorating infrastructure)
    _plugins = {}  # dictionary mapping namespace to plugin class
    effective_env = None  # environment this is monitoring (it will be an explicit namespace tag)

    # tag names
    tag_env = 'env'
    tag_role = 'role'
    tag_name = 'Name'

    # define in subclass
    uptime_mod_factor = None  # period of billing

    def __init__(self, effective_env):
        # Construct plugins
        for namespace, cls in self.__class__._plugins.items():
            setattr(self, namespace, cls(self))
        self.effective_env = effective_env

    """Subclasses must implement below"""

    def get_instance_uptime(self, instance, now=None):
        """Returns length of time instance has been up in seconds
        *now* can be provided for time calculations, overriding time.time()
        """
        raise NotImplementedError

    def get_instance_ondemand_cost(self, inst_type):
        """Get maximum (on-demand) cost of inst_type
        Returns None if cannot be determined
        """
        raise NotImplementedError

    def get_spot_prices(self, instance_type=None, availability_zone=None):
        """Return list of spot prices.  Can filter by a specific instance type or availability zone
        Returns list of bito SpotPriceHistoryObjects."""
        raise NotImplementedError

    def get_reserved_instances(self):
        """Get all reserved instances applying to this system"""
        raise NotImplementedError

    def get_instances(self, instance_ids=None, filters=None):
        """Return instances that match designated filters that are a subset of instance_ids
        Both fields are optional; instance_ids = None means get all system instances matching filters; 
        no filters matches all instances that are a subset of instance_ids
        """
        raise NotImplementedError

    def get_spot_requests(self, request_ids, filters=None):
        """Return spot requests that match designated filters that are a subset of request_ids"""
        raise NotImplementedError

    def merge_tags(self, spot_request, instance):
        """Copy spot_instance tags to instance (as EC2 doesn't do this automatically)
        Doesn't overwrite tags instance has that spot lacks"""
        raise NotImplementedError

    def get_tag(self, inst_or_spotreq, tag):
        """Retrieve an inst_or_spotreq tag"""
        raise NotImplementedError

    def set_tags(self, inst_or_spotreqs, tag, value):
        """Set the tag of an instance or spot request (or list thereof)"""
        raise NotImplementedError

    def run_instances(self, ami, ebs_boot=False, num_servers=1, min_servers=None, groups=None,
                      availzone=None, instance_type=None):
        """Low level command to boot instances"""
        raise NotImplementedError

    def request_spot_instances(self, max_price, ami, ebs_boot=False, num_servers=1,
                               groups=None, availzone=None, instance_type=None):
        """Low level commands to request spot instances"""
        raise NotImplementedError

    def terminate_servers(self, instances):
        """Terminate *instances*"""
        raise NotImplementedError

    def cancel_spot_requests(self, spot_requests):
        """Cancel open spot requests"""
        raise NotImplementedError

    """Base definitons.
    Unclear how well these will extend to different infrastucture"""

    def is_imaged(self, instance):
        """Return true if this instance was booted from an imaged ami"""
        raise NotImplementedError

    def get_local_instance_type(self):
        """Return instance type of local machine"""
        raise NotImplementedError

    def get_local_hostname(self):
        """Return the hostname of the current machine"""
        return socket.getfqdn()

    def get_instance_uptime_since_charge(self, instance, now=None):
        """Return length of time instance has been up since we have last been charged
        On AWS, a partial hour counts as an hour, so we only terminate when this is relatively large
        """
        return self.get_instance_uptime(instance, now) % self.uptime_mod_factor

    def get_instance_from_spec(self, instance_or_spot):
        """Return instance if instance
        Find instance-like object if spot request"""
        if hasattr(instance_or_spot, 'instance'):
            return instance_or_spot.instance
        elif hasattr(instance_or_spot, 'launch_specification'):
            return instance_or_spot.launch_specification
        else:
            return instance_or_spot

    """High level functions"""

    def get_spots_and_instances(self, filters=None, only_scaler_spots=False):
        """Return a tuple of instances running/pending, spot_requests 
        Search using aws *filters*1
        If *only_scaler_spots*, only return spot requests managed by the scaler        
        
        If a spot request has an instance_id link it to the instance by adding an instance field
        
        Note: Only pending/running instances are returned!
        
        FIXME: May need to be abstracted further if we ever use a different cloud provider
        """

        # we must get regular instances first, as we don't want 
        # to have a spot instance unlinked to its request

        if filters is None:
            filters = {}

        tag_env = 'tag:%s' % self.tag_env
        if tag_env not in filters:
            filters[tag_env] = self.effective_env

        inst_filters = {'instance-state-name': ['pending', 'running']}
        inst_filters.update(filters)
        instances = self.get_instances(filters=inst_filters)

        # must include cancelled as may be connected to instance
        spot_filters = {'state': ['open', 'active', 'cancelled']}
        # spot filter names are just instance filter names with 'launch.' prepended
        for name, val in filters.items():
            if name[:3] != 'tag':
                name = 'launch.' + name
            spot_filters[name] = val

        if only_scaler_spots:  # hack: this violates abstraction
            spot_filters.setdefault('tag-key', []).append(self.scaler.tag_scaler_pool)

        spot_requests = self.get_spot_requests(filters=spot_filters)

        # bind active spot requests to their instance id
        instance_dict = {}
        for instance in instances:
            instance_dict[instance.id] = instance

        for sr in spot_requests:
            if sr.state not in ['active', 'cancelled']:
                continue

            tst_id = getattr(sr, 'instance_id', None)
            if not tst_id:  # cancelled before turned on
                continue

            inst = instance_dict.get(tst_id)
            if not inst:

                # race condition -- instance came online between instance requests and spot requests
                logger.info(
                    'instance id %s associated with %s spot request %s not in batch instance query result. Explicitly requesting information',
                    tst_id, sr.state, str(sr))
                inst_list = self.get_instances(instance_ids=tst_id)
                if not inst_list:  # appears this can be a valid error. (zombie spot)
                    logger.warning('Instance id %s for active spot request %s not found!' % (tst_id, str(sr)))
                    sr.state = 'closed'
                    sr.instance = None
                    continue

                if len(inst_list) > 1:
                    raise ProvisionException(ProvisionException.NO_INSTANCE,
                                             'Instance id %s multiply defined in query result %s' % (tst_id, inst_list))
                inst = inst_list[0]

                logger.debug('instance %s is %s', inst, inst.state)
                # AWS API glitch where 'active' spots may link to 'terminated' instances
                if instance.state in ['pending', 'running']:
                    instance_dict[inst.id] = inst  # inject into dictionary
                else:  # mark spot request as closed if instance terminated
                    sr.state = 'closed'
            sr.instance = inst
            inst.spot_request = sr

            if sr.tags and not inst.tags:
                self.merge_tags(sr, inst)

        # refilter out closed spot requests
        spot_requests = [spot_req for spot_req in spot_requests if \
                         spot_req.state in ['open', 'active', 'cancelled']]
        return instance_dict.values(), spot_requests

    def get_name(self, inst_or_spotreq):
        """Returns human readable 'Name'"""
        return self.get_tag(inst_or_spotreq, self.tag_name)

    def set_name(self, inst_or_spotreqs, name):
        return self.set_tags(inst_or_spotreqs, self.tag_name, name)

    def get_role(self, inst_or_spotreq):
        """Returns role this server takes"""
        return self.get_tag(inst_or_spotreq, self.tag_role)

    def set_role(self, inst_or_spotreqs, role):
        """Set role server(s) take"""
        return self.set_tags(inst_or_spotreqs, self.tag_role, role)

    def get_env(self, inst_or_spotreq):
        """Return environment this server is a member of"""
        return self.get_tag(inst_or_spotreq, self.tag_env)

    def set_env(self, inst_or_spotreqs, env):
        """Set environment server(s) take"""
        return self.set_tags(inst_or_spotreqs, self.tag_env, env)

    """Utility functions"""

    def wait_for_instances(self, instances, poll_period=3, poll_limit=1000):
        """Wait on instances to be ready
        poll_period is number of seconds between polls
        poll_limit is total amount of time to wait in seconds
        
        Return updated instances (with state set to 'running')
        """
        if not instances:
            return instances

        if not hasattr(instances, '__iter__'):
            instances = [instances]

        num_polls = max(1, int(poll_limit / poll_period if poll_limit else sys.maxint))

        iids = [instance.id for instance in instances]

        for _ in xrange(num_polls):
            time.sleep(poll_period)

            instances = self.get_instances(instance_ids=iids)

            finished = True
            for instance in instances:
                if instance.state in ['terminated', 'stopped']:
                    raise ProvisionException(ProvisionException.TERMINATED,
                                             'instance %s was %s with code %s' % (
                                                 instance, instance.state, instance.state_code))

                if instance.state != 'running':
                    finished = False

            if finished:
                break

        else:
            raise (ProvisionException.TIMEOUT,
                   'EC2 did not ready servers %s within %s seconds' % (instances, poll_limit))
        return instances

    def wait_for_spot_reservations(self, spot_requests, poll_period=3, poll_limit=1000):
        """Wait for all spot_requests to be ready
        poll_period is number of seconds between polls
        poll_limit is total amount of time to wait in seconds
        
        Returns updated spot_requests with state set to 'active' and 
        instance_id set to id of their running instance  
        
        One can then wait on that those spots with:
        instances = get_instances([req.instance_id for req in returnedList])
        wait_for_instances(instances)    
        """
        if not spot_requests:
            return

        if not hasattr(spot_requests, '__iter__'):
            spot_requests = [spot_requests]

        num_polls = max(1, int(poll_limit / poll_period if poll_limit else sys.maxint))
        request_ids = [spot_request.id for spot_request in spot_requests]

        num_attr_check = 5  # sometimes attributes missing when they should be present

        for _ in xrange(num_polls):
            time.sleep(poll_period)

            server_req_list = self.get_spot_requests(request_ids=request_ids)

            finished = True
            for req in server_req_list:
                if req.state == 'open':
                    finished = False
                    continue

                if req.state != 'active':  # terminated!
                    raise ProvisionException(ProvisionException.TERMINATED,
                                             'spot request %s was %s' % (req, req.state))

            if finished:  # all active
                # double check attribute set (TODO: This needed?)
                for server_req in server_req_list:
                    if not hasattr(server_req, 'instance_id'):
                        if num_attr_check > 0:
                            num_attr_check -= 1
                            break  # and continue outer loop
                        else:
                            raise ProvisionException(ProvisionException.NO_INSTANCE,
                                                     (
                                                     'EC2 claimed spot %s was ready, but no instance_id was set!' % server_req))
                else:
                    break  # exit outer loop w/out executing else

        else:
            raise ProvisionException(ProvisionException.TIMEOUT,
                                     'EC2 did not satisfy spot requests %s within %s seconds' % (
                                         spot_requests, poll_limit))
        return server_req_list

    def _call_until_connected(self, func, poll_limit=450, poll_period=3):
        """        
        Call func(), which connections to a server over ssh, until it succeeds.
        Returns result of func() 
        
        Useful when server is booting
        *poll_limit* is maximum to poll in seconds before timing out
        """
        final_wait_time = 60  # number of seconds to wait when boot_finished_soon is active
        max_polls = poll_limit / poll_period
        cnt = 0

        while True:
            try:
                return func()
            except ConnectionException, ce:
                if cnt == max_polls:  # time out -- raise this exception
                    logging.debug('func %s timed out with exception %s. cnt was %s limit was %s',
                                  str(func), str(ce), cnt, poll_limit)
                    raise
                cnt += 1

                if ce.boot_finished_soon():  # Server should finish soon on errors; otherwise something failed
                    old_polls = max_polls
                    max_polls = min(max_polls, cnt + (final_wait_time + poll_period) / poll_period)
                    if max_polls < old_polls:
                        logging.debug(
                            'for func %s: %s indicates boot finished soon. reverting max polls to %s from init %s. cur cnt %s',
                            str(func), str(ce), max_polls, old_polls, cnt)

                if ce.server_maybe_booting():
                    time.sleep(poll_period)
                    continue
                elif ce.err_code == ce.ABORT:
                    logging.info('func %s was aborted', str(func))
                    raise
                else:  # non-boot error; something is wrong!
                    logging.warn('func %s resulted in unexpected excpetion %s', str(func), str(ce))
                    raise

    def deploy_role(self, role_cls, target_instance, mode, connection_kwargs=None, settings=None,
                    whitelist=None, poll_limit=0, role_creation_cb=None):
        """Deploy a role class and execute it under a certain mode
        mode should be one of install, update, post_ami, teardown

        What is a role_cls?  A role_cls is expected to:
            - be constructable with a target_server, connection_kwargs, and settings
            - Have a method for every single mode (see above), which is called.
                can optionally accept a whitelist.
            - offer an initialize(mod) method to run actual event.
        This was designed in theory where deploying would use paramiko, but you
            may have your own deployment system in place.
            Developer: See service_settings.SimpleRole for an example.
        
        A ConnectionException if something goes wrong connecting to server
                 
        If server could be booting, set poll_limit to the number of seconds to wait for the server to come
        online (typical values 450)
        
        An optional *role_creation_cb* callback may be provided. 
            role_creation_cb(role_inst,target_instance) is invoked every time role is initialized
            May be called multiple times if role errors and is retried
        """

        hostname = target_instance.public_dns_name
        logger.debug('Deploying role %s (mode %s) to %s. Settings are %s', role_cls, mode,
                     hostname, settings)
        role_name = role_cls.__name__.lower().replace('role', '')
        if self.get_role(target_instance) != role_name:
            self.set_role(target_instance, role_name)

        def wrapped_deploy():
            """Wrap deployment with exception handling"""
            try:
                role_inst = role_cls(target_server=hostname, connection_kwargs=connection_kwargs,
                                     settings=settings)
                meth = getattr(role_inst, mode, None)
                if not meth:
                    raise ValueError('%s is not a valid whiteprint mode' % mode)

                role_inst.initialize(mode)  # initiate connection before callback

                if role_creation_cb:
                    role_creation_cb(role_inst, target_instance)

                return meth(whitelist=whitelist)
            except Exception, e:
                ce = ConnectionException.from_exception(e)
                if ce.err_code == ce.EOFERROR:
                    logger.exception('Unexpected end of file error on connecting to %s' % hostname)
                    raise ce
                elif ce.err_code == ce.SFTPERROR:
                    logger.exception('Unexpected SFTP Error on connecting to %s' % hostname)
                    raise ce
                elif ce.err_code != ce.OTHER:
                    raise ce
                else:  # preserve original exception if unexpected
                    logging.error('unexpected exception %s encountered on deploying to %s',
                                  e, hostname)
                    raise

        if not poll_limit:
            return wrapped_deploy()
        else:
            return self._call_until_connected(wrapped_deploy, poll_limit)

    def self_diagnostic(self):
        """Return a dictionary of diagnostic information about this ServerControl"""
        return {}


class ServerControlPlugin(object):
    """
    Plugins exist to minimize the number of functions in the bottom level of ServerControl
    
    A plugin with functions a, b and namespace n will be available 
    in server control instance under instance.n.
    
    e.g. to call a:
    servercontrol_inst.n.a
    
    After defining a PluginClass invoke PluginClass.register to bind it to servercontrol
    
    When ServerControl is instanitated, it will also instantiate its plugins and bind them to itself
    """

    # class attribute:
    namespace = None  # attribute of servercontrol instance this plugin should be found as

    # instance attribute:
    server_control = None  # all instances have access to server control

    @classmethod
    def register(cls):
        ServerControl._plugins[cls.namespace] = cls

    def __init__(self, server_control):
        self.server_control = server_control


"""Boto debugging injection
Boto lacks detailed str/repr on its objects, so add some here
"""
from boto.ec2.reservedinstance import ReservedInstancesOffering


# reservations
def reservation_str_(self):
    return 'Reservation(%s, %s, %s)' % (self.instance_type, self.availability_zone, self.usage_price)


# patches
ReservedInstancesOffering.__str__ = reservation_str_
ReservedInstancesOffering.__repr__ = reservation_str_

"""Implementation for EC2"""
import boto
from . import aws


class EC2ServerControl(ServerControl):
    """Server control implementation for EC2"""
    platform = 'Linux/UNIX'  # AWS platform

    # Filter names
    filter_name_instance_type = 'instance_type'  # aws instance type filter key
    filter_name_dns = 'dns-name'  # aws (public) dns filter key
    filter_name_private_dns = 'private-dns-name'  # aws (private) dns filter key

    # some variables to be defined in subclass
    uptime_mod_factor = 60 * 60  # Amazon bills by the hour

    # settings set in subclass
    aws_region = None  # region running in
    boto_settings = {}  # Settings dictionary defining arguments to boto.connect_ec2
    key_name = None  # keyname that provisioned servers should have

    def get_instance_uptime(self, instance, now=None):
        """Returns length of time instance has been up in seconds
        *now* can be provided for time calculations, overriding time.time()
        """
        # TODO: We assume launch_time string's timezone is always GMT. We should read timezone

        server_created = time.mktime(time.strptime(instance.launch_time, '%Y-%m-%dT%H:%M:%S.000Z'))

        if not now:
            now = time.time() + time.timezone
        uptime = now - server_created  # uptime in seconds
        return uptime

    def get_instance_ondemand_cost(self, inst_type):
        """Get maximum (Amazon on-demand) cost of inst_type
        Returns None if cannot be determined
        """
        region_costs = aws.od_instance_costs.get(self.aws_region, {})
        return region_costs.get(inst_type)

    def get_spot_prices(self, instance_type=None, availability_zone=None):
        """Return list of spot prices.  Can filter by a specific instance type(s) or availability zone
        Returns list of boto SpotPriceHistoryObjects.
        """
        return aws.get_spot_price_history(self.ec2con, start_time=dtime.utcnow(),
                                          instance_type=instance_type,
                                          product_description=self.platform,
                                          availability_zone=availability_zone)

    @classmethod
    def make_connection(cls):
        """Returns ec2 connection object that can be passed through aws module
        Typically these connection objects are not thread safe
        
        This will operate in the default region (us-east-1).         
        """
        boto_version = getattr(boto, '__version__', None)
        api_version = None

        if boto_version < '2.1':
            logger.error(
                'Unsupported boto version %s. Please update boto to latest version. Unexpected behavior may occur!' % boto_version)
        elif boto_version <= '2.1.1':
            api_version = '2011-11-01'  # hack fix for boto bug https://github.com/boto/boto/issues/390

        boto_settings = {}
        boto_settings.update(cls.boto_settings)
        if 'api_version' not in boto_settings:
            boto_settings['api_version'] = api_version

        # The Region parameter can be used to pass a Boto regioninfo if other regions are desired
        # Currently service_settings.aws_region not obeyed
        ec2con = boto.connect_ec2(**boto_settings)
        # hack for boto SAX Parse exceptions - raise limit to 200 simultaneous connections
        ec2con._pool.connections_per_host = 200
        return ec2con

    def __init__(self, effective_env=None):
        super(EC2ServerControl, self).__init__(effective_env)
        self.ec2con = self.make_connection()

    def get_reserved_instances(self):
        """Return all reserved instances applying to this system
        Just include recurring_charges in usage_price
        """

        filters = {'state': 'active', 'product-description': self.platform}
        reservations = aws.get_reserved_instances(self.ec2con, filters=filters)
        for reserv in reservations:
            reserv.usage_price = float(reserv.usage_price)
            if getattr(reserv, 'recurring_charges', None):
                reserv.usage_price += sum((float(rc.amount) for rc in reserv.recurring_charges))
            reserv.fixed_price = float(reserv.fixed_price)
            reserv.duration = int(reserv.duration)
        return reservations

    def get_instances(self, instance_ids=None, filters=None):
        """Return instances that match designated filters that are a subset of instance_ids
        Both fields are optional; instance_ids = None means get all system instances matching filters; 
        no filters matches all instances that are a subset of instance_ids
        EC2 Filters are described at
        http://docs.amazonwebservices.com/AWSEC2/latest/APIReference/index.html?ApiReference-query-DescribeInstances.html
        """
        if instance_ids and not hasattr(instance_ids, '__iter__'):
            instance_ids = [instance_ids]

        return aws.get_instances(self.ec2con, instance_ids, filters=filters)

    def get_spot_requests(self, request_ids=None, filters=None):
        """Return spot requests that match designated filters that are a subset of request_ids
        Filters are described at
        http://docs.amazonwebservices.com/AWSEC2/latest/APIReference/index.html?ApiReference-query-DescribeSpotInstanceRequests.html
        (Very similar to instance filters)
        """

        if not filters:
            filters = {}
        if 'product-description' not in filters:  # only access linux/unix spots
            filters['product-description'] = self.platform
        if request_ids and not hasattr(request_ids, '__iter__'):
            request_ids = [request_ids]

        return aws.get_spot_requests(self.ec2con, request_ids=request_ids, filters=filters)

    def merge_tags(self, spot_request, instance):
        """Copy spot_instance tags to instance
        Doesn't overwrite tags instance has that spot lacks"""
        logger.debug('merge_tags spot request %s tags %s to instance %s',
                     spot_request, spot_request.tags, instance)
        aws.set_instance_or_req_tags(self.ec2con, instance.id, spot_request.tags)

        # Modify in-memory instance object
        if not instance.tags:
            instance.tags = {}
        for key, val in spot_request.tags.items():
            instance.tags[key] = val

    def is_spot(self, instance):
        """Returns true if this instance is managed by a spot request"""
        return getattr(instance, 'instanceLifecycle', 'normal') == 'spot'

    def get_tag(self, inst_or_spotreq, tag):
        """Return the value of a tag on an instance or spot_request"""
        return inst_or_spotreq.tags.get(tag)

    def set_tags(self, inst_or_spotreqs, tag, value):
        """Set the tag of an instance or spot request (or list thereof)"""
        logger.debug('set tag %s to %s on %s', tag, value, inst_or_spotreqs)
        if not hasattr(inst_or_spotreqs, '__iter__'):
            inst_or_spotreqs = [inst_or_spotreqs]
        ids = [ios.id for ios in inst_or_spotreqs]
        retval = aws.set_instance_or_req_tags(self.ec2con, ids, {tag: value})

        for ios in inst_or_spotreqs:
            ios.tags[tag] = value
        return retval

    # server spawning:
    def run_instances(self, ami, ebs_boot=False, num_servers=1, min_servers=None,
                      groups=['default'], availzone=None, instance_type='m1.small'):
        """Low level command to boot instances
        If key_name is none, will be set to default key
        
        Returns list of pending instances
        """

        logger.info('run_instances. Launch %s %s under ami %s in zone %s with secgroups %s',
                    num_servers, instance_type, ami, availzone, groups)

        try:
            reservation = aws.run_instances(connection=self.ec2con, ami=ami,
                                            ebs_boot=ebs_boot, num=num_servers,
                                            min_count=min_servers,
                                            groups=groups, key_name=self.key_name,
                                            zone=availzone, type=instance_type)
        except boto.exception.BotoServerError, be:
            logger.exception('Received boto exception on spawning type %s az %s' % (instance_type, availzone))
            if 'capacity' in be.body:  # insufficient capacity
                raise ProvisionException(ProvisionException.NO_CAPACITY,
                                         be.body)
            else:
                raise
        logger.info('successfully made instance request. reservation %s. requestid %s', reservation,
                    reservation.requestId)

        instances = aws.extract_instances([reservation])
        self.set_env(instances, self.effective_env)

        return instances

    def request_spot_instances(self, max_price, ami, ebs_boot=False, num_servers=1,
                               groups=['default'], availzone=None, instance_type='m1.small'):
        """Low level commands to request spot instances
        Returns list of open spot requests
        """
        logger.info('request_spot_instances. Launch %s %s under ami %s in zone %s with secgroups %s with bid %s',
                    num_servers, instance_type, ami, availzone, groups, max_price)
        reqs = aws.request_spot_instances(self.ec2con, max_price=max_price, ami=ami,
                                          ebs_boot=ebs_boot, num=num_servers,
                                          groups=groups, key_name=self.key_name,
                                          zone=availzone, type=instance_type)
        self.set_env(reqs, self.effective_env)
        return reqs

    def terminate_servers(self, instances):
        """Terminate *instances*
        Beware that this kills instances without any factory.roles teardown
        """
        if not hasattr(instances, '__iter__'):
            instances = [instances]
        logger.info('Terminating instances %s (%s)', instances, [inst.public_dns_name for inst in instances])
        terminated = aws.terminate_instances(self.ec2con, [inst.id for inst in instances])
        return terminated

    def cancel_spot_requests(self, spot_requests):
        """Cancel open spot requests"""
        if not hasattr(spot_requests, '__iter__'):
            spot_requests = [spot_requests]

        logger.debug('Cancelling spot requests %s', spot_requests)

        return aws.cancel_spot_requests(self.ec2con, [spotreq.id for spotreq in spot_requests])

    def is_imaged(self, instance):
        """Return true if this instance was booted from an imaged ami"""
        return aws.is_custom_image(instance.image_id)


"""Testing systems
"""


class ReadOnlyServerControl(EC2ServerControl):
    """All AWS writing (requests, tags) blocked
    Useful for read-only sanity checks
       
    Plugins are not blocked (EnableRoot) 
    """

    def deploy_role(self, role_cls, target_hostname, mode, connection_kwargs=None, settings=None,
                    whitelist=None, poll_limit=0):

        logger.debug('READ-ONLY BLOCKED: deploying role %s (mode %s) to %s. Settings are %s', role_cls, mode,
                     target_hostname, settings)
        pass

    def merge_tags(self, spot_request, instance):
        logger.debug('READ-ONLY BLOCKED: merge_tags spot tags %s to instance %s', spot_request.tags, instance)
        pass

    def set_tags(self, inst_or_spotreqs, tag, value):
        logger.debug('READ-ONLY BLOCKED: set tag %s to %s on %s', tag, value, inst_or_spotreqs)
        pass

    def run_instances(self, ami, ebs_boot=False, num_servers=1, min_servers=None,
                      groups=['default'], availzone=None, instance_type='m1.small'):
        """Low level command to boot instances
        If key_name is none, will be set to default key"""

        logger.debug('READ-ONLY BLOCKED: _low_level_run. Launch %s %s under ami %s in zone %s with secgroups %s',
                     num_servers, instance_type, ami, availzone, groups)
        return []

    def request_spot_instances(self, max_price, ami, ebs_boot=False, num_servers=1,
                               groups=['default'], availzone=None, instance_type='m1.small'):
        """Low level commands to request spot instances"""
        logger.info(
            'READ-ONLY BLOCKED: request_spot_instances. Launch %s %s under ami %s in zone %s with secgroups %s with bid %s',
            num_servers, instance_type, ami, availzone, groups, max_price)
        return []

    def terminate_servers(self, instances):
        """Terminate *instances*
        Beware that this kills instances without any factory.roles teardown"""
        if not hasattr(instances, '__iter__'):
            instances = [instances]
        logger.info('READ-ONLY BLOCKED: Terminating instances %s (%s)', instances,
                    [inst.public_dns_name for inst in instances])
        pass

    def cancel_spot_requests(self, spot_requests):
        """Cancel open spot requests"""
        if not hasattr(spot_requests, '__iter__'):
            spot_requests = [spot_requests]

        logger.debug('READ-ONLY BLOCKED: Cancelling spot requests %s', spot_requests)
        pass
