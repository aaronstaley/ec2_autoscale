"""
This module governs calculating reserved and spot pricing

TODO:
Add Availability Zone State - Can it be trusted? from describe
boto.get_all_zones -- see state
"""

import itertools
import logging
import sys
import time

from collections import defaultdict, OrderedDict
from .server_control import service_settings

logger = logging.getLogger('Scaler.PricingManager')

class AZMap(defaultdict):
    """A dictionary designed to map availability zones to instances
    Offers many utility functions on top of regular defaultdictionary
    
    Note: None is a valid entry for spot requests with no az requested
    """

    def __init__(self):
        super(AZMap, self).__init__(list)

    def add_instance(self, instance_or_spot):
        """Add this instance_or_spot to the dictionary
        Also can be reserved instance"""
        if hasattr(instance_or_spot, 'launch_specification'):
            instance = instance_or_spot.launch_specification
        else:
            instance = instance_or_spot
        az = instance.placement if hasattr(instance, 'placement') else instance.availability_zone
        # implementation note: defaultdict.get() will not create object while __getitem__ will
        self.__getitem__(az).append(instance_or_spot)

    def all_instances(self):
        """Returns an iterator that walks over all instances"""
        return itertools.chain.from_iterable(self.values())

    def count(self, az):
        """Returns number of instances in a given az"""
        return len(self.get(az, {}))

    def total_count(self):
        """Returns total number of instances"""
        return len(list(self.all_instances()))


class PricingManager(object):
    """All pricing logic is here
    Internal information is updated via recalculate*
      
      How things work:
      Reservations are done by assigning matching (az, type, normal life cycle) 
          instances to a given reservations from lowest usage price to greatest
        Order of reservation assignment is:
          -unmanaged (by scaler) instances
          -pools in order provided
        Any additional reservations will be considered "free"
        
      Spot: We access spot price history. Most recent element is current price
          Spot price history varies with availability zone
          
      Internal notes to amazon pricing:
      -Charge is hourly rounded up.  e.g. 4 minutes of usage = 1 hour
      -Charge stops once instance enters "terminating" state
      -Reservation discounting is prorated
      
      See amazon ec2 pricing http://aws.amazon.com/ec2/pricing/
      
      TODO: Cost of a heavy reservation should be considered 0, not reported usage_price
      
      TODO: Consider making this thread safe by locking every function?
      """

    # config

    # Can reservations be considered when determining normal instance pricing?
    # If multiple scalers are running on single AWS account, only one can utilize reservations
    # due to current limitations
    # For debugging reasons, we intentionally still do reservation calculation even if this is False
    # can_use_reservations = True
    can_use_reservations = True

    # variables
    reservations = None  # list of all (boto) reservations
    normal_instance_map = None  # dictionary mapping pool --> availability zone --> list of normal instances
    spot_pricing_map = None  # dictionary mapping instance type --> availability zone --> spot price

    # calculated

    # For reservation assignment argorithm
    # dictionary maps pool --> availability zone --> sorted list of reservations by price
    # None key maps to reservations assigned to unmanaged instances
    # "free" maps to reservations that are free to be assigned
    pool_reservations = None

    def __init__(self, server_control, coerce_allow_reservations=False):
        """server_control is ServerControl instance that manages infrastructure"""
        self.server_control = server_control
        if self.server_control.effective_env != 'live' and not coerce_allow_reservations:
            # only the live scaler can consider reservations
            self.can_use_reservations = False
            # pass

    @staticmethod
    def _get_pool_match(dct, instance, pool):
        """For *dct* structured like pool->az->value
        return value from *instance*.az and *pool*
        """
        az = instance.placement if hasattr(instance, 'placement') else instance.availability_zone
        return dct[pool][az]

    @staticmethod
    def _get_inst_match(dct, instance):
        """For *dct* structured like inst_type->az->value
        return value from *instance*
        """
        if getattr(instance, 'instance', None):  # spots retrieved by ServerControl.get_spots_and_intances
            instance = instance.instance
        elif hasattr(instance, 'launch_specification'):  # open spot request
            instance = instance.launch_specification
        az = instance.placement if hasattr(instance, 'placement') else instance.availability_zone
        if az:
            return dct[instance.instance_type][az]
        else:  # argmin over az
            return min(dct[instance.instance_type].itervalues())

    def get_cost(self, instance, pool=None, use_effective=False):
        """Return how much *instance* governed by *pool* is currently costing us
        Note that an on-demand (normal) instance has a fixed mapping to a reservation. 
            Sorting is done in the recalculate function
            
        if use_Effective reservations include ammortized upfront cost; otherwise just usage price
        
        instance may be a spot request
        """

        # first handle spot requests and spot instances
        if hasattr(instance, 'launch_specification') or self.server_control.is_spot(instance):
            return self._get_inst_match(self.spot_pricing_map, instance)
        else:  # on-demand may be reserved
            reservations = self._get_pool_match(self.pool_reservations, instance, pool)
            normals = self._get_pool_match(self.normal_instance_map, instance, pool)
            # in case of None pool, we have different instance types stored; narrow it down
            if pool == None:
                reservations = [reserv for reserv in reservations if reserv.instance_type == instance.instance_type]
                normals = [norm for norm in normals if norm.instance_type == instance.instance_type]

            my_idx = normals.index(instance)

            if my_idx >= len(reservations) or not self.can_use_reservations:
                # not covered by a reservation
                return self.server_control.get_instance_ondemand_cost(instance.instance_type)
            else:
                r = reservations[my_idx]
                # print 'is reserved!', r, instance.instance_type
                if use_effective:
                    return (r.fixed_price) / (r.duration / 3600) + r.usage_price
                else:
                    return r.usage_price

    def notify_terminated(self, instance, pool):
        """Update internal information after an instance has been terminated"""
        if self.server_control.is_spot(instance):
            return

        # handle normal instance
        normals = self._get_pool_match(self.normal_instance_map, instance, pool)
        normals.remove(instance)

        self.recalculate_reservations()

    def notify_spawned(self, instance, pool):
        """Update internal information after an instance is spawned.
        """
        if self.server_control.is_spot(instance):
            return

        # handle normal reservations
        normals = self._get_pool_match(self.normal_instance_map, instance, pool)
        normals.append(instance)

        self.recalculate_reservations()

    def new_normal_instance_cost(self, instance_type, az_list=None):
        """Determine what a new *instance_type* will cost of normal lifecycle
        Checks reservations to see if discount possible
        If *az_list* provided, only consider normals in given availability zone. Else consider all
        On ties, use earlier items in az_list
        
        Returns price, az that minimizes price        
        """

        # free reservations with this instance type by az
        free_reservations_map = self.pool_reservations['free_%s' % instance_type]

        if not free_reservations_map.total_count():  # No reservations free -- fall back to ondemand
            return (self.server_control.get_instance_ondemand_cost(instance_type),
                    az_list[0] if az_list else service_settings.usable_zones[0])

        if not az_list:
            az_list = service_settings.usable_zones

        # Scan for minimum
        min_item = None
        for az in az_list:
            res_list = free_reservations_map[az]
            if not res_list:
                continue
            reserv = res_list[0]
            if not min_item or reserv.usage_price < min_item.usage_price:
                min_item = reserv

        if min_item and self.can_use_reservations:
            return min_item.usage_price, min_item.availability_zone
        else:
            return (self.server_control.get_instance_ondemand_cost(instance_type),
                    az_list[0] if az_list else service_settings.usable_zones[0])

    def new_spot_instance_cost(self, instance_type, az_list=None):
        """Determine what a new *instance_type* will cost for a spot
        If *az_list* provided only consider spots in given availability zone. Else consider all
        On ties, use earlier items in az_list
        
        Returns price, az that minimizes price.        
        """
        instance_prices = self.spot_pricing_map.get(instance_type)
        if not instance_prices:  # spot not available for this instance type
            logger.warning('new_spot_instance_cost: Spot price for instance_type %s not found', instance_type)
            return sys.maxint, None

            # Scan for minimum
        min_az = None
        min_price = sys.maxint

        if not az_list:
            az_list = service_settings.usable_zones

        for az in az_list:
            price = instance_prices.get(az)
            if not price:  # az not found
                continue
            if price < min_price:
                min_price = price
                min_az = az

        if min_az:
            return min_price, min_az
        else:
            return sys.maxint, None  # no spot available            

    def update(self, instances_governing, pools, reservations=None):
        """Allocate reservations to different pools.
        instances_governing maps Pool objects to a list of instances they are allocated to.
            None object maps to instances not being managed by scaler
        pools is a list of Pool objects that defines the order for instances_governing
        
        Reservations may be provided if calculated externally; else use server_control 
        """

        logger.debug('running update with ig = %s and pools = %s', instances_governing, pools)

        # all reserved instances
        if reservations is None:
            reservations = self.server_control.get_reserved_instances()
        # We assign cheapest reservations first
        reservations.sort(key=lambda reservation: reservation.usage_price)
        self.reservations = reservations

        logger.debug('AWS reservations are %s', reservations)

        # Assign normals to the pool they are in
        # Mapping is ordered by priority reservations should be assigned in
        pool_ordering = [None] + pools

        normal_instance_map = OrderedDict()  # pool->az->list of normal instances
        now = time.time() + time.timezone

        for pool in pool_ordering:
            az_map = AZMap()

            # Instances are sorted by uptime since last charge
            # Thus instances most recently charged are assigned reservations
            instances = instances_governing[pool][:]
            instances.sort(key=lambda instance: self.server_control.get_instance_uptime_since_charge(instance, now))

            for instance in instances:
                if not self.server_control.is_spot(instance):
                    az_map.add_instance(instance)

            normal_instance_map[pool] = az_map
        self.normal_instance_map = normal_instance_map
        logger.debug('system instance mapping is %s', normal_instance_map)

        # Base calculation of spot
        base_spot_pricing = self.server_control.get_spot_prices()
        spot_pricing_map = defaultdict(dict)  # instance_type -> az -> price

        for price_item in base_spot_pricing:
            spot_pricing_map[price_item.instance_type][price_item.availability_zone] = price_item.price

        self.spot_pricing_map = spot_pricing_map
        logger.debug('spot_pricing is %s', spot_pricing_map)

        self.recalculate_reservations()

    def simple_update(self, instances):
        """An update that is used for use-cases that autogenerates pools"""
        # pool_map = {instance.instance_type : instance for instance in instances}
        # pool_ordering = pool_map.keys()
        # pool_ordering.sort()

        # return self.update(pool_map, pool_ordering)

        return self.update({None: instances}, [])

    def recalculate_reservations(self):
        """Calculate how normal instances are assigned to reservations
        See description of this class for methodology
        """

        pool_reservations = defaultdict(AZMap)  # pool --> az --> reservations sorted by cost

        # duplicate multiple reservations
        reservations = []
        for reserv in self.reservations:
            add_iterations = 1
            if reserv.instance_count > 1:
                add_iterations = reserv.instance_count
                reserv.instance_count = 1
            for _ in xrange(add_iterations):
                reservations.append(reserv)

        for pool, az_dct in self.normal_instance_map.items():
            for az, instance_list in az_dct.items():
                # resort

                for instance in instance_list:

                    for reserv in reservations:
                        if reserv.instance_type == instance.instance_type and reserv.availability_zone == az:
                            break
                    else:
                        continue

                    for _ in xrange(add_iterations):
                        pool_reservations[pool].add_instance(reserv)
                    reservations.remove(reserv)

        # Save free reservations. e.g. free_c1.xlarge
        for reserv in reservations:
            pool_reservations['free_%s' % reserv.instance_type].add_instance(reserv)

        self.pool_reservations = pool_reservations

        logger.debug('pool_reservations re-calculated as %s' % self.pool_reservations)
