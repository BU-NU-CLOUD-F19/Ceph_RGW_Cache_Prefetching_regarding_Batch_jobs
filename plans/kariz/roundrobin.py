#!/usr/bin/python
import json

from utils.graph import *
import dagplanner as dp
from plans.planner import *
import utils.requester as requester
import utils.status as status
import colorama
from colorama import Fore, Style

class RoundRobin(Planner):
    def __init__(self, bandwidth=1200):
        super().__init__(bandwidth)
        self.dags = []
        self.cur_dag_index = 0
                 
    def add_dag(self, g):
        super().add_dag(g)
        self.dags.append(g)
    
    def delete_dag(self, dag_id):
        self.dags.remove(self.dag_planners[dag_id].g)
        super().delete_dag(dag_id)
        
    def get_plans(self, dag_id):
        plans = [] 
        if len(self.dag_planners) <= 0: return plans
        available_bw = self.available_bandwidth/len(self.dag_planners) # give every Job a fair share of DAG
        for gid in self.dag_planners:
                plans.extend(self.dag_planners[gid].get_next_plans(available_bw))
        return plans
    
    def online_planner(self, dag_id, stage_id):        
        super().online_planner(dag_id, stage_id)
        
        n_dags = len(self.dags) 
        if  not n_dags : return        
        next_dag_index = (self.cur_dag_index + 1)%n_dags
        g = self.dags[next_dag_index]
        if stage_id == 4 or stage_id == 3:
            print('stage4')
        plans = self.get_plans(g.dag_id)
        while len(plans) > 0:
            plan = plans.pop(0)
            if plan.type == 0:
                if requester.cache_plan(plan) != status.SUCCESS:
                    # for all priority plans larger than this priority on this stage mark them as infeasible
                    continue
                self.markas_pinned_datasets(plan.dag_id, plan)
                if dag_id in self.dag_planners: 
                    self.dag_planners[dag_id].update_statistics(plan.stage_id, plan.data) 
            else:
                now = 0 if stage_id == -1 else self.dag_planners[dag_id].g.stages[stage_id].start_time \
                    + self.dag_planners[dag_id].g.queue_time 
                now = now + self.dag_planners[dag_id].g.submit_time 
                if not self.pack_bandwidth(plan, now): continue 
                if requester.prefetch_plan(plan) != status.SUCCESS:
                    # for all priority plans larger than this priority on this stage mark them as infeasible
                    self.roll_back()
                    continue
                    
                self.update_planned_bandwidth(plan)
                #self.mirab.update_pinned_datasets(plan)
            print(Fore.LIGHTGREEN_EX, "\t plan ", plan.data, ', dag: ', plan.dag_id, ', stage', stage_id, 
                  ' is cached/prefetched: ', plan.type, Style.RESET_ALL)
            self.cur_dag_index = next_dag_index
        self.dag_planners[dag_id].current_running_stage = stage_id 
