#!/usr/bin/python
# Author: Trevor Nogues, Mania Abdi

#!/usr/bin/python
import utils.scheduler as sched
import utils.requester as req
import utils.pig as pig
import time

# write a class to build schedule dag in pig
NOCACHE = 0
KARIZ = 1
MRD = 2
CP=3
RCP=4
LRU=5
INFINITE=6


def start_pig_simulator(v):
    cache = KARIZ
    pig.build_stages(v);
    # build cache plans
    req.submit_new_dag(v)
    req.notify_stage_start(v, -1)
    if cache == KARIZ:
        v.plans_container = pig.build_kariz_priorities(v)
    elif cache == MRD:
        v.plans_container = pig.build_mrd_priorities(v)
    elif cache == CP:
        v.plans_container = pig.build_cp_priorities(v)
    elif cache == RCP:
        v.plans_container = pig.build_rcp_priorities(v)
    elif cache == INFINITE:
        v.plans_container = pig.build_infinite_priorities(v)
    elif cache == LRU:
        v.plans_container = pig.build_lru_priorities(v)
    else: 
        v.plans_container = None

    time.sleep(v.queue_time)
    return sched.gang_scheduler(v)

'''
# submit one dag to pig
v = tests.graphs[1]
v.inputs = {0:['a'], 1:['d'], 2:['c'], 3:['b'], 4:['b'], 5:['e'], 6:['a'] }
v.inputSize = {0 : [8], 1:[10], 2:[9], 3:[12], 4:[12], 5:[14], 6:[8] }
v.outputSize = {0 : [1], 1:[1], 2:[1], 3:[1], 4:[1], 5:[1], 6:[1]}
'''
