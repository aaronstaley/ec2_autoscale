"""
This is where all settings describing your AWS cluster go
"""
from . import aws

# TODO(developer): Fill this out


# Example settings

# Your AWS ACCESS KEYS
AWSAccessKeyId = ''
AWSSecretAccessKey = ''

# Region our system is running in
aws_region = aws.REGION_US_EAST_1
# zones we can use in order of preference. default is usable_zones[0]
# us-east-1c is no longer available.
# ping times for connecting to us-east-1a:
# a: 0.4ms
# e: 0.6ms
# b: 0.95ms
# d: 1.7 ms
usable_zones = [aws_region + 'a', aws_region + 'e', aws_region + 'b', aws_region + 'd']

# Special label attached to instances we scale
# Developer: live refers to the live production system; please search code base for this string; it
#  is used in several special ways to allow for multiple systems (live, stage, etc.) to be used under
#  a single EC2 account.
environment = 'live'


# Infrastructure
def get_infrastructure_server_control():
    """Return ServerControl that should be the superclass of ScalerServerControl
    This is the ServerControl that manages infrastructure.
    """
    # TODO(developer): Set to write mode when you ready!
    read_only = True
    if read_only:  #no spawning or deployment allowed
        from .server_control import ReadOnlyServerControl
        return ReadOnlyServerControl
    else:
        from .server_control import EC2ServerControl
        return EC2ServerControl

def server_role_def(role):
    """Defines various role objects"""
    server_roles = {'simple' : {'groups' : ['default', 'web'],
                             'ami' : 'precise64',
                             'role' : SimpleRole}
                    }

    return server_roles[role]

class SimpleRole(object):
    """Define a basic role to deploy servers"""
    # TODO(developer): Fill this out

    def install(self, whitelist=None):
        """Fresh installation on base image"""
        pass

    def update(self, whitelist=None):
        """Update an already made server"""
        pass

    def post_ami(self, whitelist=None):
        """Any special command to execute after booting from an AMI

        Typically any call to this is followed by an update.

        PiCloud used this to enable root access (override AMZN AMI properties)"""
        pass


    def teardown(self, whitelist=None):
        """Called to teardown a server.
        This will be executed right before the server is torn down.
        """
        pass
