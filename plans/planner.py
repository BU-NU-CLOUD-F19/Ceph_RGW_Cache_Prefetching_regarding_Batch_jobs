#!/usr/bin/python
import json

from utils.graph import *
import time
import utils.requester as requester
import utils.status as status
import copy
import colorama
from colorama import Fore, Style

class BWPlanner:
    def __init__(self, bandwidth=1200):
#        self.blocks = [0]*bandwidth
        self.index = 0;
        self.bandwidth = bandwidth
        
    def get_room(self, n_blocks):
        if self.index + n_blocks <= self.bandwidth:
            self.index += n_blocks
            return n_blocks
        allocated_blocks = self.bandwidth - self.index
        self.index = self.bandwidth
        return allocated_blocks 

class Planner:
    def __init__(self, bandwidth=1200):
        self.est_runtime = 0
        self.start_time = int(round(time.time() * 1000))
        self.dag_planners = {}
        self.available_bandwidth = bandwidth # 10Gbps = 1.2 GBps = 1200 MBps
        self.cache_block_size = 1 # 1MBype
        self.bw_planner = [] 
        self.bw_snapshot = []
        
    def add_dag(self, g):
        t_extend = (g.total_runtime + g.queue_time) - self.est_runtime
        if t_extend > 0:
            for t in range(0, t_extend):
                self.bw_planner.append(BWPlanner(self.available_bandwidth))

    def pack_bandwidth(self, plan, now):
        self.bw_snapshot = copy.deepcopy(self.bw_planner)
        rem_blocks = plan.size
        t_pdeadline = self.dag_planners[plan.dag_id].g.stages[plan.stage_id].start_time \
                    + self.dag_planners[plan.dag_id].g.submit_time \
                    + self.dag_planners[plan.dag_id].g.queue_time
        for t in range(now, t_pdeadline):
            allocated_block = self.bw_planner[t].get_room(rem_blocks)
            rem_blocks -= allocated_block
            if rem_blocks == 0: break;
        if rem_blocks > 0:
            self.bw_planner = self.bw_snapshot
            return False
        return True
    
    def roll_back(self):
        self.bw_planner = self.bw_snapshot

       
    def markas_pinned_datasets(self, dag_id, plan):
        if dag_id not in self.dag_planners: return None
        self.dag_planners[dag_id].markas_pinned_datasets(plan)
            
    def unpinned_completed_stage(self, dag_id, stage_id):
        if dag_id not in self.dag_planners: return None
        self.dag_planners[dag_id].unpinned_completed_stage(stage_id)
        
    def delete_dag(self, dag_id):
        self.dag_planners[dag_id].dump_stats()
        del self.dag_planners[dag_id]

    def online_planner(self, dag_id, stage_id):
        # uppin files from previous stage 
        self.unpinned_completed_stage(dag_id, stage_id)


    def get_stage_plans(self, dag_id, stage):
        return self.dags[dag_id].plans_container.get_stage_plans(stage)
    
    def get_plans_bystage(self, dag_id, stage):
        return None if dag_id not in self.dags else self.dags[dag_id].plans_container.get_stage_plans_bypriority(stage) 
    
    
    def get_plans(self, dag_id, stage):
        plans = [] 
        if len(self.dag_planners) <= 0: return plans
        available_bw = self.available_bandwidth/len(self.dag_planners) # give every Job a fair share of DAG
        for gid in self.dag_planners:
                plans.extend(self.dag_planners[gid].get_next_plans(available_bw))
        if dag_id in self.dag_planners: self.dag_planners[dag_id].current_running_stage = stage
        return plans
