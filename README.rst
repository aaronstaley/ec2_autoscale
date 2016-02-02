#############
ec2 autoscale
#############

First released: 15-Feb-2016


*************
Introduction
*************

This is a fork of the autoscaler that PiCloud used to scale worker nodes.  Couplings to PiCloud's core infrastructure
have been removed, which will allow other developers to connect it to their system.  While this system is designed
to scale worker nodes (which can tolerate spot instance termination), in practice, can be used for any replicated instance
type (e.g. webservers)

This is not a drop-in module.  Various settings will need to be configured and logic plugged in.  However, it provides
a significant jump-start toward building an EC2 autoscaler that handles all sorts of corner cases.  It is this
autoscaling logic that won EC2's first `Spotathon <https://aws.amazon.com/blogs/aws/picloud-and-princeton-consultants-win-the-first-amazon-ec2-spotathon/>`__

************
Features
************

The autoscaler supports a variety of features:

* Enforce that N servers exist.  As N changes, servers will be automatically deployed or removed
* Intelligent use of spot instances

    * Spot instances may be deployed at start
    * Regular instances may be deployed for faster boot up and replaced with spot.
    * Checks active bid prices and purchased reservations to determine whether spot or on-demand instances should be utilized.

* Can control multiple instance types and multiple consumers of the instance types
* Mostly stateless. Uses AWS tags to label instances and spot requests, so can generally survive reboots.
* Intelligent shutdown:

    * Capable of dispatching winddown event to server and waiting until it is ready to shut down. (e.g. current jobs cleared)
    * Or if server fails to shut down before another hour is charged, capable of restarting (e.g. reconnect server to job pool)


*************
Architecture
*************

The server_control sub-package provides methods to interact with AWS.  In addition your AWS settings go in server_control.service_settings.

* scaler.py is a worker thread that runs scalingtask.py
* scalingtask.py periodically queries information from AWS (instances, spot requests, reservations) and dispatches them to various places
* pool manages an individual pool of instances of a given type.  When it finds unmanaged instances in its pool (controlled by AWS tags), it sends them to managers.  It is responsible for requesting and releasing instances to AWS.
* managers is the highest level abstraction. A pool has 1 or more managers that make requests to grant and release servers.  The pool assigns resources to managers as appropriate.
* Once a manager claims a resource (generally by setting tags), it holds the resource until it releases it to the pool.
* Roles define how to provision and tear down an instance.  See the example in service_settings.

***********
Deploying
***********

I advise that you examine the source code carefully to understand how this works.  There are many places with TODO(developer) notes that should be filled in by the user of this code.
Be advised that this code is not fully tested.  You'll likely need to change a few things to get it working correctly.

Pull requests welcome.