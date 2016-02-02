'''
Plugin for ServerControl to help drive AutoScaling.
'''
import logging

from .server_control import ServerControlPlugin
from . import service_settings

logger = logging.getLogger('ScalerServerControl')


def get_server_control(env=None):
    """ServerControl factory
    Returns instance of ServerControl intended for this environment
    
    * env: specify environment for this server_control to manage
        (only affects what env servers will take when deployed)
    If None use settings.environment
    """
    if not env:
        env = service_settings.environment
    return ScalerServerControl(effective_env=env)


servercontrol_parent = service_settings.get_infrastructure_server_control()


class ScalerServerControl(servercontrol_parent):
    """Scaler-specific server control object"""
    aws_region = service_settings.aws_region
    boto_settings = {'aws_access_key_id': service_settings.AWSAccessKeyId,
                     'aws_secret_access_key': service_settings.AWSSecretAccessKey}

    # separate plugins from parent
    _plugins = {}

    def __init__(self, effective_env=None):
        # hacky: ensure parent's plugins are in local dictionary
        for namespace, cls in servercontrol_parent._plugins.items():
            self.__class__._plugins.setdefault(namespace, cls)
        super(ScalerServerControl, self).__init__(effective_env)

    def _decode_server_role(self, server_role):
        """Return groups, ami for a server_role"""

        try:
            server_role_def = service_settings.server_role_def(server_role)
        except KeyError:
            raise KeyError('%s is not a defined server_role' % server_role)

        return server_role_def['groups'], server_role_def['ami']

    def _get_settings(self, target_instance):
        """Get appropriate role settings to use to deploy to *target_instance*"""

        settings = {'env': self.effective_env,
                    'instance_type': target_instance.instance_type,
                    'instance_id': target_instance.id}

        for namespace in self._plugins.keys():  # dynamic settings update
            plugin = getattr(self, namespace, None)
            meth = getattr(plugin, 'update_role_settings', None)
            if meth:
                meth(target_instance, settings)

        return settings

    """High level server booting"""

    def spawn_server(self, instance_type, server_role, num_servers=1,
                     min_servers=None, availzone=None,
                     force_ebs=False, override_ami=None):
        """High level spawn server functionality
        Request a server running on *instance_type*
        Server is to fulfill role *server_role* of which data is defined in service_settings.server_role_def
        Boot up server in *availzone*. uses default if None
        Multiple instances can be guaranteed to boot at same time by setting *num_servers*;
            Set min_servers to an integer to ensure that at least that many servers get provisioned
            if backend cannot satisfy large request
        Can force usage of ebs boot with *force_ebs*; otherwise will use instance storage if possible        
        Can force an ami (rather than using service_Settings) with override_ami
        
        Returns list of booted (pending) instances
        """

        groups, ami = self._decode_server_role(server_role)
        if override_ami:
            ami = override_ami

        instances = self.run_instances(ami, ebs_boot=force_ebs, num_servers=num_servers,
                                       min_servers=min_servers, groups=groups,
                                       availzone=availzone, instance_type=instance_type)
        self.set_role(instances, server_role)

        ids = [inst.id for inst in instances]
        logger.info('spawned servers of type %s w/ ids = %s for role %s',
                    instance_type, ids, server_role)

        return instances

    def spawn_spot_server(self, max_price, instance_type, server_role, num_servers=1, availzone=None,
                          force_ebs=False, override_ami=None):
        """Request spot instances
        Interface similar to spawn_server
        max_price: Maximum bid for the spot
        
        Returns list of open spot requests
        """

        groups, ami = self._decode_server_role(server_role)
        if override_ami:
            ami = override_ami

        spot_requests = self.request_spot_instances(ami=ami, max_price=max_price,
                                                    ebs_boot=force_ebs, num_servers=num_servers,
                                                    groups=groups, availzone=availzone,
                                                    instance_type=instance_type)
        self.set_role(spot_requests, server_role)

        logger.info('made spot requests for type %s w/ ids = %s for role %s',
                    instance_type, spot_requests, server_role)

        return spot_requests

    def auto_provision(self, target_instance, mode=None, role_creation_cb=None, whitelist=None):
        """Provision code to newly instantiated *target_instance* correctly based on internal tags and ami information"
        If mode provided, run scripts with provided mode; else autodetect mode based on ami        
        Note if mode is post_ami, update mode is run afterward
        
        An optional *role_creation_cb* callback may be provided; see server_control.deploy_role for usage
        """
        server_role = self.get_role(target_instance)
        svr_role_def = service_settings.server_role_def(server_role)
        role_cls = svr_role_def['role']
        if not mode:
            mode = 'post_ami' if self.is_imaged(target_instance) else 'install'
        settings = self._get_settings(target_instance)

        retval = self.deploy_role(role_cls=role_cls,
                                  target_instance=target_instance,
                                  mode=mode, settings=settings,
                                  whitelist=whitelist,
                                  poll_limit=320,
                                  role_creation_cb=role_creation_cb)

        if mode == 'post_ami':
            # finish an update
            logger.debug('Successfully deployed post_ami to %s. Running updates', target_instance)
            return self.deploy_role(role_cls=role_cls,
                                    target_instance=target_instance,
                                    whitelist=whitelist,
                                    mode='update', settings=settings, poll_limit=20)
        else:
            return retval

    def auto_teardown(self, target_instance, do_termination=True, error_logger=None,
                      role_creation_cb=None):
        """Teardown *target_instance* correctly based on internal tags
        if *do_termination*, terminate server as soon as possible 
            (currently, after settings downloaded from server, but before role executed)
        Due to importance of errors, special error_logger can be passed into this function
        
        An optional *role_creation_cb* callback may be provided; see server_control.deploy_role for usage
            callback will be called before termination occurs if do_termination
        """
        if not error_logger:
            error_logger = logger.error
        server_role = self.get_role(target_instance)
        svr_role_def = service_settings.server_role_def(server_role)
        role_cls = svr_role_def['role']
        mode = 'teardown'
        settings = self._get_settings(target_instance)

        retval = [None]  # 1st element holds result. needed due to nonlocal not in python 2.x

        def do_logging():
            # TODO(developer); Add whatever logging you want here
            pass


        def term_callback(role_inst, instance):
            if role_creation_cb:
                role_creation_cb(role_inst, instance)
            logger.info('role %s constructed. Now terminating %s' % (role_inst, instance))
            retval[0] = self.terminate_servers(instance)
            do_logging()

        try:
            self.deploy_role(role_cls=role_cls,
                             target_instance=target_instance,
                             mode=mode, settings=settings,
                             poll_limit=0,
                             role_creation_cb=term_callback if do_termination else role_creation_cb)
        except Exception:
            if not do_termination:
                raise
            msg = 'Instance %s teardown errored. Will hard terminate' % str(target_instance)
            error_logger(msg, exc_info=True)
            if not retval[0]:  # not yet terminated
                retval[0] = self.terminate_servers(target_instance)
                do_logging()
        return retval[0]

class ScalerPlugin(ServerControlPlugin):
    namespace = 'scaler'
    TAG_SCALER_POOL = 'scaler_pool'  # our tag to track what scaler pool an instance belonds to

    def get_pool_managing(self, inst_or_spotreq):
        """Return the scaler pool that manages this instance/spot (based on a tag)"""
        return self.server_control.get_tag(inst_or_spotreq, self.TAG_SCALER_POOL)

    def set_pool_managing(self, inst_or_spotreqs, pool_name):
        return self.server_control.set_tags(inst_or_spotreqs, self.TAG_SCALER_POOL, pool_name)


ScalerPlugin.register()
