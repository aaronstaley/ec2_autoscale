"""
The aws module is intended to provide an interface to aws
    It tightly interfaces with boto. Indeed, many functions require a boto connection object parameter
While it exposes boto objects (espcially instances) to callees, it provides the following ease of use ability:
* Unpacking reservations into reservations
* Functionality to wait until servers are booted
* Retrying aws commands on errors
* Defining aws parameters
* Other aws utility functions
"""

import datetime
import operator
import random
import socket
import time

import boto

import logging
logger = logging.getLogger('aws')

meta_url = 'http://instance-data/latest/meta-data/'
meta_data = ['ami-id', 'hostname', 'instance-id', 'instance-type', 'kernel-id',
             'local-hostname', 'local-ipv4', 'public-hostname', 'public-ipv4']

# TODO(DEVELOPER): THIS DATA IS REALLY OUTDATED!!

# AWS API doesn't provide an easy way to access on-demand instance costs
# AWS definitions

REGION_US_EAST_1 = 'us-east-1'
REGION_US_WEST_1 = 'us-west-1'
REGION_US_WEST_2 = 'us-west-2'
REGION_AP_NORTHEAST_1 = 'ap-northeast-1'
REGION_AP_SOUTHEAST_1 = 'ap-southeast-1'
REGION_EU_WEST_1 = 'eu-east-1'

# information incomplete for regions other than us_east_1 and instances we don't use
od_instance_costs = {REGION_US_EAST_1: {'m1.small' : 0.06,
                                        'm1.medium' : 0.12,
                                        'm1.large' : 0.24,
                                        'm1.xlarge' : 0.48,                                        
                                        't1.micro' : 0.02,
                                        'm2.xlarge' : 0.41,
                                        'm2.2xlarge' : .820,
                                        'm2.4xlarge' : 1.640,                                                                                
                                        'c1.medium' : 0.145,
                                        'c1.xlarge' : 0.58,
                                        'cc1.4xlarge' : 1.3,                                   
                                        'cc2.8xlarge' : 2.4,
                                        'cg1.4xlarge' : 2.1,
                                        'hi1.4xlarge' : 3.1,
                                        'cr1.8xlarge' : 3.5
                                },
                    REGION_US_WEST_1: {
                                 }, 
                     }                  

#Definiton of instance boot AMIs we have on EC2
AMIs = {REGION_US_EAST_1: {'karmic32': 'ami-bb709dd2',
                      'karmic64': 'ami-55739e3c',
                      'lucid32': 'ami-4fd00726',
                      'lucid64': 'ami-35de095c',
                      'oneiric32' : 'ami-d1a671b8',
                      'oneiric64' : 'ami-4fa37426',                     
                      'precise64' : 'ami-cf5e2ba6',  
                      'raring64'  : 'ami-9597e1fc',
                      'setup_server': 'ami-2eff6047',   # precise with aufs and wsshd, created 03/08/13
                      
                     },
        REGION_US_WEST_1: {
                     },
        }

#Definition of EBS boot AMIs we have on EC2
AMIs_ebs = {REGION_US_EAST_1: {
                      'karmic32': 'ami-6743ae0e',
                      'karmic64': 'ami-7d43ae14',
                      'lucid32': 'ami-71dc0b18',
                      'lucid64': 'ami-55dc0b3c',
                      'oneiric32' : 'ami-6ba27502',
                      'oneiric64' : 'ami-6fa27506',
                      'precise64' : 'ami-e7582d8e',
                      'raring64'  : 'ami-e995e380',                           
                     },
        REGION_US_WEST_1: {
                     },
        }

#Definition of HVM AMIs we have on EC2
# (All of these are also EBS-boot)
AMIs_hvm = {REGION_US_EAST_1: {
                      'natty64': 'ami-f1589598',
                      'oneiric64' : 'ami-beba68d7',    
                      'precise64' : 'ami-f9582d90', 
                      'raring64'  : 'ami-eb95e382',                                   
                     },
        REGION_US_WEST_1: {
                     },
        }


def get_ami(ami, zone, ebs_boot=False, instance_type = None):
    """Get AMI from our AMI list if it exists
    Else return None"""
    
    if not zone:
        zone = REGION_US_EAST_1 + 'a'
        
    if instance_type == 't1.micro':  
        # t1.micro lack instance storage
        ebs_boot = True
        
    region = zone[:-1]  #dropping last letter should be region
    
    if instance_type in ['cc1.4xlarge', 'cc2.8xlarge', 'cg1.4xlarge']:
        # Cluster compute instances use ebs backed hvm
        dct = AMIs_hvm
    elif ebs_boot:
        dct = AMIs_ebs
    else:
        dct = AMIs
    
    return dct[region].get(ami,None)    

imaged_amis = []
def gen_custom_image_table():
    global imaged_amis    
    ami_dcts = [AMIs, AMIs_ebs, AMIs_hvm]
    region_defs = [REGION_US_EAST_1]
    # DEVELOPER: PUT YOUR CUSTOM AMI KEYS BELOW (e.g. ami-..)
    ami_keys = ['']
    
    for ami_dct in ami_dcts:
        for region_def in region_defs:
            region_dct = ami_dct.get(region_def)
            if not region_dct:
                continue
            for ami_key in ami_keys:
                ami = region_dct.get(ami_key)
                if ami:
                    imaged_amis.append(ami)

gen_custom_image_table() #needed for below function
def is_custom_image(ami_id):
    """Determinte if an ami-id, e.g. ami-63be790a is imaged.
    """ 
    global imaged_amis
    return ami_id in imaged_amis

def retry_n_times(func, n, caller, *args, **kwargs):
    """Run function func(*args, **kawargs) n times until no EC2ResponseError or n is reached
    caller is a string specifying who called this (for logging)"""
    
    i= -1
    while True:
        try:
            return func(*args,**kwargs)        
        except boto.exception.EC2ResponseError, e:  #aws hickups sometimes
            n-=1
            i+=1
            logger.error('%s: EC2ResponseError: %s', caller, e)
            if n <= 0:
                raise
            else:
                time.sleep(min(10,0.2 + (1<<i) * 0.5 * random.random())) # expodential backoff
                continue            

"""
Generic server spawning with boto
"""

def get_instances(connection, instance_ids = None, filters = None):
    """Get instances by instance_ids
    A dictionary of filters can be provided as well      
    See http://docs.amazonwebservices.com/AWSEC2/latest/APIReference/
    """
    #reservations = connection.get_all_instances(instance_ids = instance_ids)
    reservations = retry_n_times(connection.get_all_instances, 3, 'get_instances', 
                                 instance_ids = instance_ids, filters = filters)
    return extract_instances(reservations)
    

def extract_instances(reservations):
    """Extract instances from a list of reservations"""
    instances = []
    for reservation in reservations:        
        try:
            groups = [group.groupName for group in reservation.groups]
        except AttributeError:  #boto version < 2.0rc1
            try:
                groups = [group.name for group in reservation.groups]
            except AttributeError:
                groups = [group.id for group in reservation.groups]
        for instance in reservation.instances:
            instance.groups = groups 
            instances.append(instance)
    return instances        

"""Below need a boto EC2Connection object, connection, to work"""

def run_instances(connection, ami, ebs_boot = False, num=1, min_count=None, groups=['default'], 
                  key_name='team', zone='us-east-1a', type='m1.small'):     
    """
    Returns reservation
    reservation.instances accesses the actual instances
    """
       
    my_ami = get_ami(ami, zone, ebs_boot, type)
    if not my_ami:
        my_ami = ami
        
    if min_count == None:
        min_count = num
        
    reservation = connection.run_instances(image_id=my_ami, security_groups=groups, max_count=num, 
                                        min_count=min_count, instance_type=type, placement=zone,
                                        key_name=key_name)
    return reservation

rin = run_instances


def get_spot_requests(connection, request_ids = None, filters= None):
    """Get spot requests by request_ids
    A dictionary of filters can be provided as well      
    http://docs.amazonwebservices.com/AWSEC2/latest/APIReference/index.html?ApiReference-query-DescribeSpotInstanceRequests.html
    """
    #reservations = connection.get_all_instances(instance_ids = instance_ids)
    reservations = retry_n_times(connection.get_all_spot_instance_requests, 3, 'get_spot_requests', 
                                 request_ids = request_ids, filters = filters)
    return reservations


def get_spot_price_history(connection,start_time=None, end_time=None,
                               instance_type=None, product_description=None,
                               availability_zone=None):
    """Get spot price history.
    start_time and end_time should be datetime objects in UTC or None
    See boto's get_spot_price_history
    encode timestamp as a datetime.datetime object
    """
    
    """
    internally has a loop to handle boto returning 1,000 results max (no idea why)
    """
    
    
    start_times = {} # dictionary maps instance_type + az to times
    result_set = []
    extra_loops = 0
    while True:
        start_time_str = start_time.isoformat() if start_time else None
        end_time_str = end_time.isoformat() if end_time else None    
        
        price_hist = retry_n_times(connection.get_spot_price_history, 3, 'get_spot_price_history',
                                   start_time=start_time_str, end_time=end_time_str,
                                   instance_type=instance_type, product_description=product_description,
                                   availability_zone=availability_zone)
        for ph in price_hist:
            ph.timestamp = ts = datetime.datetime.strptime(ph.timestamp, '%Y-%m-%dT%H:%M:%S.000Z')
            key = '^'.join([ph.instance_type,ph.availability_zone])
            if key not in start_times or ts < start_times[key]:
                start_times[key] = ts 
            
        price_hist.sort(key=lambda ph: ph.timestamp )
        result_set = price_hist + result_set
        if not price_hist:
            #print 'epricehist term %s records' % len(start_times)
            break
        if not start_time and not end_time: # just show 1000..
            break
        if end_time and price_hist[0].timestamp >= end_time: # can't go earlier!
            break
        if start_time: # verify that all az have been found
            if price_hist[0].timestamp <= start_time: # at least one instance time has been resolved
                extra_loops += 1
                #print 'extra loop %s' % extra_loops
            if extra_loops > 20:
                # sanity check - don't go too far back
                break
            for record_start_time in start_times.values():
                if record_start_time > start_time: # fail case
                    break
            else: # all resolved successfully
                #print 'rc term %s records' % len(start_times)
                break
        
        end_time = price_hist[0].timestamp      
        
    return result_set

def request_spot_instances(connection, max_price, ami, ebs_boot = False, num=1, 
                           groups=['default'], key_name='team', zone='us-east-1a', type='m1.small'):
    """
    Returns List of Spot requests
    Price is a string, e.g. '0.08' for $0.08
    """

    my_ami = get_ami(ami, zone, ebs_boot, type)
    if not my_ami:
        my_ami = ami
                
    spot_requests = connection.request_spot_instances(image_id = my_ami, price=max_price,
                                     security_groups = groups, count=num, instance_type=type,
                                     placement = zone,key_name=key_name)
    return spot_requests    

def cancel_spot_requests(connection, spot_request_ids):
    """Terminate spot requests"""
    if not spot_request_ids:
        return
    
    if not hasattr(spot_request_ids, '__iter__'):
        spot_request_ids = [spot_request_ids]
        
    return retry_n_times(connection.cancel_spot_instance_requests, 3, 'cancel_spot_requests', 
                         request_ids = spot_request_ids)    

def get_reserved_instances(connection, filters = None):
    """Get reserved instances"""
    res_instances = retry_n_times(connection.get_all_reserved_instances, 3, 'get_reserved_instances', filters = filters)    
    return res_instances

def terminate_instances(connection, instance_ids):
    if not hasattr(instance_ids, '__iter__'):
        instance_ids = [instance_ids]
    
    return retry_n_times(connection.terminate_instances, 3, 'terminate_instances', 
                         instance_ids = instance_ids)

def set_instance_or_req_tags(connection, ids, tag_dict):
    """Set the tag of an instance(s) or spot request(s) with given ids
    tag_dict - maps tags to values
    """
    if not hasattr(ids, '__iter__'):
        ids = [ids]
    return retry_n_times(connection.create_tags, 9, 'set_instance_or_req_tags', 
                         resource_ids = ids, tags = tag_dict)

tin = terminate_instances
describe_instances = get_instances
din = describe_instances


"""
Generic ebs volume management with boto
"""

def wait_for_status(ec2obj, status, num_polls=10, sleep_time=0.5,
                    do_raise=True):
    """Waits until ec2obj.status (or ec2obj.state) becomes *status*. Expects a
    boto ec2 object with a status (or state) attribute and an update() method.
    """
    field = None
    if hasattr(ec2obj, 'status'):
        field = 'status'
    elif hasattr(ec2obj, 'state'):
        field = 'state'
    else:
        raise Exception('ec2obj has no status or state attribute')
    get_status = operator.attrgetter(field)

    tries = 0
    while True:
        if get_status(ec2obj) == status or tries > num_polls:
            break
        time.sleep(sleep_time)
        ec2obj.update()
        tries += 1
    if do_raise and get_status(ec2obj) != status:
        raise Exception('ec2obj status %s != %s' % (get_status(ec2obj), status))

def get_volumes(connection, volume_ids=None, filters=None):
    """Get all volumes satisfying criteria.
    * connection: An ec2 connection instance.
    * volume_ids: IDs of volumes to retrieve.
    * filters: Additional filtering criteria
    
    Returns a list of volume objects.
    """
    volumes = retry_n_times(connection.get_all_volumes, 3, 'get_volumes',
                            volume_ids=volume_ids, filters=filters)
    return volumes

def get_volume(connection, volume_id):
    """Returns an ebs volume with *volume_id*."""
    res_set = get_volumes(connection, [volume_id])
    if not res_set or len(res_set) != 1:
        raise Exception('unexpected result from get_volumes')
    volume = res_set.pop()
    return volume

def create_volume(connection, size, zone, snapshot=None, block=False,
                  num_polls=120, sleep_time=1, do_raise=True):
    """Creates an ebs volume.
    * connection: An ec2 connection instance.
    * size: Size of volume to create in GiB.
    * zone: Availability zone in which the volume should be created.
    * snapshot: Optional snapshot (or id) from which to create the volume.
    * block: If True, waits until the volume has been attached successfully.
    * num_polls: Max number of polls to perform while blocking.
    * sleep_time: Seconds to wait between polls while blocking.
    * do_raise: Raises exception if creation is not successful after block.
    
    Returns the volume object that was created.
    """
    volume = connection.create_volume(size, zone, snapshot)
    if block:
        wait_for_status(volume, 'available', num_polls, sleep_time, do_raise)
    return volume

def delete_volume(connection, volume_id=None):
    """Deletes an ebs volume.
    * connection: An ec2 connection instance.
    * volume_id: ID of volume to delete.
    
    Returns True if deletion is successful.
    """
    return connection.delete_volume(volume_id)

def attach_volume(connection, volume_id, instance_id, device, block=False,
                  num_polls=60, sleep_time=0.5, do_raise=True):
    """Attaches an ebs volume to an ec2 instance.
    * connection: An ec2 connection instance.
    * volume_id: ID of volume to attach.
    * instance_id: ID of instance where volume will be attached.
    * device: Device file where volume will be accessible.
    * block: If True, waits until the volume has been attached successfully.
    * num_polls: Max number of polls to perform while blocking.
    * sleep_time: Seconds to wait between polls while blocking.
    * do_raise: Raises exception if attachment is not successful after block.
    
    Returns True if successful.
    """
    result = connection.attach_volume(volume_id, instance_id, device)
    if result and block:
        volume = get_volume(connection, volume_id)
        wait_for_status(volume, 'in-use', num_polls, sleep_time, do_raise)
    return result

def detach_volume(connection, volume_id, instance_id, device, force=False,
                  block=False, num_polls=120, sleep_time=0.5, do_raise=True):
    """Detaches an ebs volume from an instance.
    * connection: An ec2 connection instance.
    * volume_id: ID of volume to detach.
    * instance_id: ID of instance from which volume will be detached.
    * device: Device file where volume is accessible.
    * block: If True, waits until the volume has been detached successfully.
    * num_polls: Max number of polls to perform while blocking.
    * sleep_time: Seconds to wait between polls while blocking.
    * do_raise: Raises exception if detachment is not successful after block.

    Returns True if successful.
    """
    result = connection.detach_volume(volume_id, instance_id, device, force)
    if result and block:
        volume = get_volume(connection, volume_id)
        wait_for_status(volume, 'available', num_polls, sleep_time, do_raise)
    return result

def get_volume_tags(connection, volume_id):
    """Returns the tags of an ebs volume."""
    volume = get_volume(connection, volume_id)
    return volume.tags

def add_volume_tag(connection, volume_id, key, value=''):
    """Adds key/value as a tag to an ebs volume."""
    volume = get_volume(connection, volume_id)
    return volume.add_tag(key, value)

def remove_volume_tag(connection, volume_id, key, value=None):
    """Removes a tag from an ebs volume."""
    volume = get_volume(connection, volume_id)
    return volume.remove_tag(key, value)


"""
Generic snapshot management with boto
"""

def get_snapshots(connection, snapshot_ids=None, filters=None):
    """Get all snapshots satisfying criteria.
    * connection: An ec2 connection instance.
    * snapshot_ids: IDs of snapshots to retrieve.
    * filters: Additional filtering criteria
    
    Returns a list of snapshot objects.
    """
    snapshots = retry_n_times(connection.get_all_snapshots, 3, 'get_snapshots',
                              snapshot_ids=snapshot_ids, filters=filters)
    return snapshots

def get_snapshot(connection, snapshot_id):
    """Returns a snapshot with *snapshot_id*."""
    res_set = get_snapshots(connection, [snapshot_id])
    if not res_set or len(res_set) != 1:
        raise Exception('unexpected result from get_snapshots')
    snapshot = res_set.pop()
    return snapshot

def create_snapshot(connection, volume_id, description=None, block=False,
                  num_polls=720, sleep_time=5, do_raise=True):
    """Creates a snapshot.
    * connection: An ec2 connection instance.
    * volume_id: ID of the ebs volume which should be snapshotted.
    * description: Optional description for the snapshot.
    * block: If True, waits until the snapshot creation has finished.
    * num_polls: Max number of polls to perform while blocking.
    * sleep_time: Seconds to wait between polls while blocking.
    * do_raise: Raises exception if creation is not successful after block.
    
    Returns the snapshot object that was created.
    """
    snapshot = connection.create_snapshot(volume_id, description)
    if block:
        wait_for_status(snapshot, 'completed', num_polls, sleep_time, do_raise)
    return snapshot

def delete_snapshot(connection, snapshot_id=None):
    """Deletes a snapshot.
    * connection: An ec2 connection instance.
    * volume_id: ID of snapshot to delete.
    
    Returns True if deletion is successful.
    """
    return connection.delete_snapshot(snapshot_id)

def get_snapshot_tags(connection, snapshot_id):
    """Returns the tags of a snapshot."""
    snapshot = get_snapshot(connection, snapshot_id)
    return snapshot.tags

def add_snapshot_tag(connection, snapshot_id, key, value=''):
    """Adds key/value as a tag to a snapshot."""
    snapshot = get_snapshot(connection, snapshot_id)
    return snapshot.add_tag(key, value)

def remove_snapshot_tag(connection, snapshot_id, key, value=None):
    """Removes a tag from a snapshot."""
    snapshot = get_snapshot(connection, snapshot_id)
    return snapshot.remove_tag(key, value)


"""
Useful utilities
"""
def is_ec2_instance(hostname=None):
    """Checks if *hostname* refers to an ec2 instance.  If hostname is not
    given, assumes the check is for the local machine.
    """
    if hostname is None:
        hostname = socket.getfqdn()
    domain = (hostname.split('.'))[-1]
    return domain == 'internal'
