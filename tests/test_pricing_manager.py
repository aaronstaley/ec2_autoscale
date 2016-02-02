# NOTE(developer): These tests will fail because of outdated AWS pricing data

import logging
from decimal import Decimal

from boto.ec2.reservedinstance import ReservedInstance

from autoscale.server_control.server_control import SimulatedInstance
from autoscale.server_control.scaler_server_control import get_server_control
from autoscale.pool import Pool
from autoscale.pricingmanager import PricingManager

logging.basicConfig(level=logging.DEBUG,
                    format="[%(asctime)s] - [%(levelname)s] - %(name)s: %(message)s")

# Create some normal instances
t1 = 'c1.xlarge'
t2 = 'm2.xlarge'
z1 = 'us-east-1a'
z2 = 'us-east-1b'

s1 = SimulatedInstance(instance_type=t1,
                       availzone=z1,
                       state='running')
s2 = SimulatedInstance(instance_type=t1,
                       availzone=z1,
                       state='running')
s3 = SimulatedInstance(instance_type=t1,
                       availzone=z2,
                       state='running')

s4 = SimulatedInstance(instance_type=t2,
                       availzone=z1,
                       state='running')

# Reservations (match simulated instances)
r1 = ReservedInstance(instance_type=t1, availability_zone=z1,
                               usage_price=Decimal('0.17'))

r2 = ReservedInstance(instance_type=t1, availability_zone=z1,
                               usage_price=Decimal('0.26'))

r3 = ReservedInstance(instance_type=t1, availability_zone=z2,
                               usage_price=Decimal('0.17'))

r4 = ReservedInstance(instance_type=t2, availability_zone=z1,
                               usage_price=Decimal('0.30'))

# Pools (Used only for name mappings) -- map to instance types
p1 = Pool(scaling_task=None, instance_type=t1, server_role=None, myname='p_%s' % t1)
p2 = Pool(scaling_task=None, instance_type=t2, server_role=None, myname='p_%s' % t2)

svr_control = get_server_control(False)  # our system independent; needed for constants and spot price history
pmgr = PricingManager(svr_control)

# Test cases
pool_order = [p1, p2]

# null case
ig = {None: [], p1: [], p2: []}
pmgr.update(ig, pool_order, [])

# spot tests only done once as rather irrelevant
spot_tst = pmgr.new_spot_instance_cost(t1)
print 'spot %s Expect something like 0.24 to 1 == %s' % (t1, str(spot_tst))

spot_tst = pmgr.new_spot_instance_cost(t2)
print 'spot %s Expect something like 0.2 to 1 == %s' % (t2, str(spot_tst))

tst = pmgr.new_normal_instance_cost(t1)
print 'Expect 0.68 == %s' % str(tst)
tst = pmgr.new_normal_instance_cost(t2)
print 'Expect 0.5 == %s' % str(tst)

# No instaances, some reservations
ig = {None: [], p1: [], p2: []}
pmgr.update(ig, pool_order, [r2, r1, r3])

tst = pmgr.new_normal_instance_cost(t1)
print 'Expect 0.17 == %s' % str(tst)
tst = pmgr.new_normal_instance_cost(t2)
print 'Expect 0.5 == %s' % str(tst)

# 1 managed instance
ig = {None: [s1, s4], p1: [], p2: []}
pmgr.update(ig, pool_order, [r2, r1, r3])

tst = pmgr.new_normal_instance_cost(t1)
print 'Expect 0.17 (b)== %s' % str(tst)
tst = pmgr.new_normal_instance_cost(t2)
print 'Expect 0.5 == %s' % str(tst)

# use pricier reservation
ig = {None: [s1, s3], p1: [], p2: []}
pmgr.update(ig, pool_order, [r2, r1, r3, r4])

tst = pmgr.new_normal_instance_cost(t1)
print 'Expect 0.26 == %s' % str(tst)
tst = pmgr.new_normal_instance_cost(t2)
print 'Expect 0.30== %s' % str(tst)

# Pools controlling instances
ig = {None: [], p1: [s1], p2: []}
print 'for ig = %s' % str(ig)
pmgr.update(ig, pool_order, [r1])

tst = pmgr.new_normal_instance_cost(t1)
print 'Expect 0.68 == %s' % str(tst)

tst = pmgr.get_cost(s1, p1)
print 'Expect 0.17c == %s' % str(tst)

# kill instnace
pmgr.notify_terminated(s1, p1)
tst = pmgr.new_normal_instance_cost(t1)
print 'Expect 0.17 == %s' % str(tst)

ig = {None: [], p1: [s1], p2: []}
print 'for ig = %s - 2 res' % str(ig)
pmgr.update(ig, pool_order, [r1, r2])

tst = pmgr.new_normal_instance_cost(t1)
print 'Expect 0.26 == %s' % str(tst)

tst = pmgr.get_cost(s1, p1)
print 'Expect 0.17c == %s' % str(tst)

# new instance
pmgr.notify_spawned(s2, p2)
tst = pmgr.new_normal_instance_cost(t1)
print 'Expect 0.68 == %s' % str(tst)

tst = pmgr.get_cost(s2, p2)
print 'Expect 0.26c == %s' % str(tst)

# that should be good enough..
