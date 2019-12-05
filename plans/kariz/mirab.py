#!/usr/bin/python
import json

from utils.graph import *
import dagplanner as dp
import utils.requester as requester
import utils.status as status
import colorama
from colorama import Fore, Style

class Mirab:
    def __init__(self):
        self.fairness_factor = 0.2
        self.fairness_scores = {}
        self.share_scores = {}
        self.dag_planners = {}
        self.alpha = 0.5
        self.available_bandwidth = 600 # 10Gbps = 1.2 GBps = 1200 MBps
        self.cache_block_size = 1 # 1MBype 
                
    def add_dag(self, g):
        self.dag_planners[g.dag_id] = dp.DAGPlanner(g)      
        self.fairness_scores[g.dag_id] = 1
        
    def markas_pinned_datasets(self, dag_id, plan):
        if dag_id not in self.dag_planners: return None
        self.dag_planners[dag_id].markas_pinned_datasets(plan)
            
    def unpinned_completed_stage(self, dag_id, stage_id):
        if dag_id not in self.dag_planners: return None
        self.dag_planners[dag_id].unpinned_completed_stage(stage_id)
        
    def delete_dag(self, dag_id):
        self.dag_planners[dag_id].dump_stats()
        del self.fairness_scores[dag_id]
        del self.dag_planners[dag_id]

    def online_planner(self, dag_id, stage_id):

        # uppin files from previous stage 
        self.unpinned_completed_stage(dag_id, stage_id)

        plans = self.get_plans(dag_id, stage_id)
        if plans is None:
            return
        
        # while there is no cache space avaialbe or no space left in bw
        while len(plans) > 0:
            plan = plans.pop(0)
            print(Fore.LIGHTYELLOW_EX, "Mirab, process plans of DAG", plan.dag_id, ', stage' , plan.stage_id, Style.RESET_ALL)    
            if not plan.is_feasible():
                continue
            if plan.type == 0:
                if requester.cache_plan(plan) != status.SUCCESS:
                    # for all priority plans larger than this priority on this stage mark them as infeasible
                    self.update_infeasible(plan)
                    continue
                self.markas_pinned_datasets(plan.dag_id, plan)
                if dag_id in self.dag_planners: 
                    self.dag_planners[dag_id].update_statistics(plan.stage_id, plan.data) 
            else:
                if requester.prefetch_plan(plan) != status.SUCCESS:
                    # for all priority plans larger than this priority on this stage mark them as infeasible
                    self.update_infeasible(plan)
                    continue
                self.update_planned_bandwidth(plan)
                #self.mirab.update_pinned_datasets(plan)
            # Here Kariz was able to cache the requested plan
            #print('cached/prefetch plan: ', plan)
            print(Fore.LIGHTGREEN_EX, "\t plan ", plan.data, ' is cached/prefetched: ', plan.type, Style.RESET_ALL)
            self.update_fairness_score(plan)
            self.compute_weighted_scores(plans)
            
    def update_planned_bandwidth(self, plan):
        return 0 

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
        
    def compute_share_scores(self, plans):
        files = {}
        #for p in plans:
        #    print(p)
    
    def compute_weighted_scores(self, plans):
        for p in plans:
            p.wscore = self.alpha*self.fairness_scores[p.dag_id] + (1 - self.alpha)*p.sscore 
        plans.sort(reverse=True)
    
    def updateby_share_score(self, plans):
        share_score = 0
        input_count = 1
        return share_score/input_count
    
    def updateby_pscore(self, plans):
        share_score = 0
        input_count = 1
        return share_score/input_count
    
    def update_fairness_score(self, plan):
        for gid in self.dag_planners:
            sign = -1 if gid == plan.dag_id else +1
            self.fairness_scores[gid] = self.fairness_scores[gid] + sign*self.fairness_scores[gid]*self.fairness_factor + sum(self.fairness_scores.values())/len(self.fairness_scores)
            
    def update_infeasible(self, plan):
        plan.feasible = 0
        # FIXME this should the cache         
