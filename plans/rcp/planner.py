#!/usr/bin/python
import json

from utils.graph import *
import utils.pig as pig
import utils.requester as requester
import utils.status as status
import math
import copy

class Planner:
    def __init__(self):
        self.dags = {}
        self.pinned_plans = {}
        self.alpha = 0.5
        
    def update_pinned_datasets(self, plan):
        if plan.dag_id not in self.pinned_plans:
            self.pinned_plans[plan.dag_id] = {}
        if plan.original_stage_id not in self.pinned_plans[plan.dag_id]:
            self.pinned_plans[plan.dag_id][plan.original_stage_id] = []
        self.pinned_plans[plan.dag_id][plan.original_stage_id].append(plan)
    
    def unpinned_completed_stage(self, dag_id, stage_id):
        if dag_id not in self.pinned_plans: return
        if stage_id -1 not in self.pinned_plans[dag_id]: return
        for p in self.pinned_plans[dag_id][stage_id -1]:
            print('unpinned_completed_stage: ', dag_id, stage_id -1, p.data) 
            requester.uppined_datasets(p.data)
        
    
    def add_dag(self, g):
        g.plans_container = pig.build_rcp_priorities(g)
        self.dags[g.dag_id] = g
        
    def delete_dag(self, dag_id):
        del self.pinned_plans[dag_id]
        del self.dags[dag_id]
        
    def online_planner(self, dag_id, stage_id):        
        self.unpinned_completed_stage(dag_id, stage_id)
        plans = self.get_plans(dag_id, stage_id)
        if plans is None: return
        
        for p in plans:
            print("online planner: ", stage_id, p.data, p.type)
        while len(plans) > 0:
            print('len plans', len(plans))
            plan = plans.pop()
            if not plan.is_feasible(): continue
            
            _status = requester.cache_plan(plan) if plan.type == 0 else requester.prefetch_plan(plan) 
            if  _status != status.SUCCESS: continue
            if plan.type == 0: self.update_pinned_datasets(plan)
    
    def get_plans(self, dag_id, stage, bandwidth=1200):
        plans = []
        if dag_id not in self.dags: return None
        plans.extend(self.dags[dag_id].plans_container.get_cache_plans(stage))
        plans.extend(self.get_prefetch_plans(dag_id, stage, bandwidth))
        plans.sort(reverse=True)
        return plans
    
    ''' O(n), max n = # of plans in the DAG, is called per stage '''
    def get_prefetch_plan_unlimitedbw(self, dag_id, cur_stg_index = 0):
        prefetch_plans = []
        g = self.dags[dag_id]
        f_stg_index = len(g.stages) - 1
        
        if cur_stg_index != f_stg_index:
            if cur_stg_index + 1 in g.plans_container.cp_by_stage:
                plans_in_stage = g.plans_container.cp_by_stage[cur_stg_index + 1]
                for pp in plans_in_stage: # pp stands for priority plan
                    pplan = copy.deepcopy(plans_in_stage[pp])
                    pplan.type = 1
                    prefetch_plans.append(pplan)
        return prefetch_plans
    
    ''' O(n), max n = # of plans in the DAG, is called per stage '''
    def get_prefetch_plans(self, dag_id, cur_stg_index = 0, bandwidth = 1200):
        prefetch_plans = []
        g = self.dags[dag_id]
        f_stg_index = len(g.stages) - 1 # get furthest stage
        
        if bandwidth == -1 or cur_stg_index == -1:
            return self.get_prefetch_plan_unlimitedbw(dag_id, cur_stg_index)
        
        current_stage = g.plans_container.stages[cur_stg_index]    
        
        if cur_stg_index != f_stg_index:
            for stg in range(cur_stg_index + 1, f_stg_index): # loop over future stages plans
                if stg not in g.plans_container.cp_by_stage: continue
                plans_in_stage = g.plans_container.cp_by_stage[stg]
                feasible = 1
                for pp in plans_in_stage: # pp stands for priority plan
                    plan = plans_in_stage[pp] 
                    plan_est_ft = plan.stage.start_time - math.ceil(plan.size/bandwidth) # Estimated fetch time of the plan
                    
                    if plan_est_ft < current_stage.start_time or not feasible:
                        plan.feasible = 0
                        feasible = 0
                        continue
                     
                    if plan_est_ft < g.plans_container.stages[cur_stg_index+1].start_time:
                        pplan = copy.deepcopy(plan)
                        pplan.type = 1 # prefetch
                        prefetch_plans.append(pplan)
        return prefetch_plans