'''
Created on Oct 5, 2019

@author: mania
'''

import math
import copy
import utils.pig as pig
import utils.requester as requester
import pandas as pd
import colorama
from colorama import Fore, Style

class DAGPlanner:
    def __init__(self, g):
        self.g = g;
        self.pinned_plans = {}
        self.current_running_stage = -2
        self.current_processed_stage = -2
        self.cached_plans = []
    
    def markas_pinned_datasets(self, plan):
        if plan.stage_id not in self.pinned_plans:
            self.pinned_plans[plan.stage_id] = []
        self.pinned_plans[plan.stage_id].append(plan)
    
    def unpinned_completed_stage(self, stage_id):
        if stage_id -1 not in self.pinned_plans: return
        for p in self.pinned_plans[stage_id -1]:
            print(Fore.LIGHTMAGENTA_EX, 'Mirab unpin completed stage --> DAG: ', self.g.dag_id, 
                  'stage: ' , stage_id  -1, 'data in plan:', p.data, Style.RESET_ALL) 
            requester.uppined_datasets(p.data)
        del self.pinned_plans[stage_id -1]
            
    
    def update_statistics(self, stage_id, data):
        self.cached_plans.append({'dag_id': self.g.dag_id, 'name': self.g.name, 
                                  'mse_factor': self.g.mse_factor, 'data': data, 'stage_id' : stage_id})
        
    def dump_stats(self):
        fname = 'cache_stats.csv'
        fd = open(fname, 'a+')
        df = pd.DataFrame(self.cached_plans)
        df.to_csv(fd, index=False)
    
    def update_iscore(self, plan, s):
        last_plan_imprv = 0
        priority = plan.priority
        sid = s.stage_id
        if priority -1 in self.cp_by_stage[sid]:
            last_plan_imprv = self.g.plans_container.cp_by_stage[sid][priority -1].iscore
        plan.iscore += last_plan_imprv
        if plan.size > 0:
            plan.pscore = plan.iscore/plan.size
        else:
            plan.pscore = -1

    ''' O(n), max n = # of plans in the DAG, is called per stage '''
    def get_prefetch_plan_unlimitedbw(self, cur_stg_index = 0):
        prefetch_plans = []
        f_stg_index = len(self.g.stages) - 1
        
        if cur_stg_index != f_stg_index:
            if cur_stg_index + 1 in self.g.plans_container.cp_by_stage:
                plans_in_stage = self.g.plans_container.cp_by_stage[cur_stg_index + 1]
                for pp in plans_in_stage: # pp stands for priority plan
                    pplan = copy.deepcopy(plans_in_stage[pp])
                    pplan.type = 1
                    prefetch_plans.append(pplan)
        return prefetch_plans

    ''' O(n), max n = # of plans in the DAG, is called per stage '''
    def get_prefetch_plans(self, bandwidth = 1200, cur_stg_index = 0):
        prefetch_plans = []
        f_stg_index = len(self.g.stages) - 1 # get furthest stage
        
        if bandwidth == -1 or cur_stg_index == -1:
            return self.get_prefetch_plan_unlimitedbw(cur_stg_index)
        
        current_stage = self.g.plans_container.stages[cur_stg_index]    
        
        if cur_stg_index != f_stg_index:
            for stg in range(cur_stg_index + 1, len(self.g.stages)): # loop over future stages plans
                if stg not in self.g.plans_container.cp_by_stage: continue
                plans_in_stage = self.g.plans_container.cp_by_stage[stg]
                feasible = 1
                for pp in plans_in_stage: # pp stands for priority plan
                    plan = plans_in_stage[pp] 
                    plan_est_ft = plan.stage.start_time - math.ceil(plan.size/bandwidth) # Estimated fetch time of the plan
                    
                    if plan_est_ft < current_stage.start_time or not feasible:
                        plan.feasible = 0
                        feasible = 0
                        continue
                     
                    if plan_est_ft < self.g.plans_container.stages[cur_stg_index+1].start_time:
                        pplan = copy.deepcopy(plan)
                        pplan.type = 1 # prefetch
                        prefetch_plans.append(pplan)
    
        return prefetch_plans
                         
    def get_next_plans(self, bandwidth=1200):
        if self.current_running_stage + 1 <= len(self.g.stages) - 1:
            return self.get_plans(self.current_running_stage   + 1, bandwidth)
        return []
        
    def get_plans(self, stage, bandwidth=1200):
        ''' O(nlogn), max n = # of plans in the DAG, is called per stage '''
        plans = []
        plans.extend(self.g.plans_container.get_cache_plans(stage))
        plans.extend(self.get_prefetch_plans(bandwidth, stage))
        self.compute_share_plans(plans)
        plans.sort(reverse=True)
        return plans
    
    def compute_share_plans(self, plans):
        ''' O(n), max n = # of plans in the DAG, is called per stage '''
        stage_footprint = {}
        for p in plans:
            for f in p.data:
                if f not in stage_footprint:
                    stage_footprint[f]= {'size': 0, 'access': 0, 'stages' : []}
                ''' the reason is that the share data is the smallest
                 amount data that is shared with everybody'''
                if p.data[f]['size'] < stage_footprint[f]['size']: 
                    stage_footprint[f]['size'] = p.data[f]['size']
                if p.stage_id not in stage_footprint[f]['stages']:
                    stage_footprint[f]['stages'].append(p.stage_id)
                stage_footprint[f]['access']+=1
        
        for p in plans:
            for f in p.data:
                p.sscore += stage_footprint[f]['access']
            p.sscore = p.sscore/len(p.data) if len(p.data) > 0 else 0
