#!/usr/bin/python
# Author: Trevor Nogues, Mania Abdi

import sched, threading, time
import utils.requester as req
import colorama
from colorama import Fore, Style
import utils.pig as pig
import time
from multiprocessing.dummy import Pool as ThreadPool
import subprocess

# write a class to build schedule dag in pig
NOCACHE = 0
KARIZ = 1
MRD = 2
CP=3
RCP=4
LRU=5
INFINITE=6

start_t = 0
debug_sleep = 60 # second

def execute_job(job):
    jname = job.op # get job name
    iname = 's3a://data/Kariz/' + list(job.inputs.keys())[0]
    oname = 's3a://data/Kariz/wc' + str(job.runtime_remote)
    local_p = subprocess.Popen(["/local0/HiBench/bin/workloads/micro/%s/hadoop/run.sh %s %s" %(job.op, iname, oname)], shell=True)
    local_p.wait()
    return 0



# B-level Gang scheduler (PIG)
def execute_dag(g):
    global start_t

    if not g.schedule:
        pig.build_stages(g)

    
    for sid in g.stages:
        time.sleep(60)
        s = g.stages[sid]
        n_jobs = len(s.jobs)
        req.notify_stage_start(g, sid)
        pool = ThreadPool(n_jobs)
        print('jobs', s.jobs)
        pool.map(execute_job, s.jobs)
        pool.close()
        pool.join()
        
    
    



def start_pig_emulator(v):
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

    time.sleep(5)
    return execute_dag(v)
