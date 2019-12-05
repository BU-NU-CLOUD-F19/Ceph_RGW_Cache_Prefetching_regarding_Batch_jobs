#!/usr/bin/python
import json

from utils.graph import *
import utils.pig as solopig
import utils.requester as requester

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
        g.plans_container = solopig.build_cp_priorities(g)
        self.dags[g.dag_id] = g
        
    def delete_dag(self, dag_id):
        del self.pinned_plans[dag_id]
        del self.dags[dag_id]

        
    def get_plans(self, dag_id, stage):
        return self.dags[dag_id].plans_container.get_plans(stage)