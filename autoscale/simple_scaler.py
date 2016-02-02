'''
Created on Feb 8, 2012

Scale syncers
'''

from .scalingmanager import ScalingManager


class SimpleScaler(ScalingManager):
    """Simplified example scaler that use a static target"""
    # TODO(developer): You will likely make this much more complex

    # The number of servers we want
    # TODO(developer): You may want to wire this to read some sort of configuration file
    target = 1

    # static configuration
    instance_type = 'm1.xlarge'  # what instance this uses
    server_role = 'simple'  # what server_role (defined in service_settings) syncer uses

    auto_clear_provision = True

    # winddown timing
    max_winddown_window = 7 * 60
    min_winddown_window = 0.4 * 60

    def __init__(self, scaling_task, pool):
        # TODO(developer): Any initialization goes here
        super(SimpleScaler, self).__init__(scaling_task, pool)
        self.target = 1

    def __str__(self):
        return 'SimpleScaler'

    @property
    def name(self):
        return 'simpleScaler'

    @property
    def entity(self):
        """Name of what this is scaling"""
        # TODO(developer): Give this something sane
        return 'simple_job_servers'

    def self_diagnostic(self):
        """Returns a dictionary of diagnostic information about this ScalingManager
        """
        base_dict = super(SimpleScaler, self).self_diagnostic()
        my_dict = {
            'target': self.target
        }

        my_dict.update(base_dict)
        return my_dict

    def should_manage(self, instance):
        # TODO(developer): Check a tag on the instance to see if you should manage it.
        # Example silly example: Note how provision sets name in this form.
        # In practice you should use a custom tag. (implemented as a server control plugin)
        return self.pool.server_control.get_name(instance).begins('simple_auto')


    def get_target(self):
        """
        Returns tuple of target, minimum_spot
        """
        return self.target, 0

    def ready_release(self, instance):
        """Ready the instance for termination (as in killing instance)
        This will update any internal logic associated with this instance
        """

        super(SimpleScaler, self).ready_release(instance)
        # TODO(developer): Add anything you want here.

    def set_target(self, target=None, **kwargs):
        if target != None:
            self.target = target
        super(SimpleScaler, self).set_target(**kwargs)

    def provision(self, instance):
        """Install code on this instance
        This sets tags which will drive provisioning
        """

        self.logger.debug('Provisioning %s', instance)
        self.pool.server_control.set_name(instance, 'simple_auto_server')
        # additional provisioning
        super(SimpleScaler, self).provision(instance)

    def start_winddown(self, instance):
        """Requests entity start winding down
        Superclass should override this if they do any special winding down
        """
        return super(SimpleScaler, self).start_winddown(instance)

    def cancel_winddown(self, instance, is_alive=True):
        """Cancel winddown of instance
        is_alive indicates that entity appears to be currently running
        Return True if winddown could be cancelled; false otehrwise
        """
        return True  # assuming this is possible

    def get_request(self, running_instances, extra_instances=[], **kwds):
        """Inspect running_instances and any additional information provided
        Instances are divided into 'running_instances' (online and running)
        and 'extra_instances' (instances in process of starting up or shutting down)
        subclass determines what running_instances are 'extra', if any

        Return ManagerRequest object describing needed information

        TODO: Handle concept of 'running' better for different provisioning systems
        """

        # TODO(developer): You must remove any "dead" instance you wish to termiante (in self.winding_down hosts)
        #  from running_instances and add it to extra_instances if you wish for the instance to be gracefully
        #  shut down after a process running on it has terminated. (PiCloud pulled this off by waiting for winding
        #    down worker to update db state that this method pulled)
        #  If you just want to hard delete the instance, see note in scalingmanager.
        return super(SimpleScaler, self).get_request(running_instances, extra_instances, **kwds)
