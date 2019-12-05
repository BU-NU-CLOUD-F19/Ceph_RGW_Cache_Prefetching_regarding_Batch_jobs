#!/usr/bin/python
import json

from utils.graph import *
import plans.mrd.dagplanner as dp
from plans.planner import *
import utils.requester as requester
import utils.status as status
import csv
import colorama
from colorama import Fore, Style

class MinPlanner(Planner):
    def __init__(self, bandwidth=1200):
        super().__init__(bandwidth)
        #fpath = '/home/mania/Northeastern/MoC/Kariz/code/plans/mrd/'
        fpath = '/root/Kariz/code/plans/mrd/'
        self.min_table = self.read_min_table(fpath)
        self.cur_dag_index = 0
        self.dags = {}
                 
    def add_dag(self, g):
        self.dag_planners[g.dag_id] = dp.MRDagPlanner(g)
        self.dags[g.name] = g.dag_id
        super().add_dag(g)
    
    def delete_dag(self, dag_id):
        del self.dags[self.dag_planners[dag_id].g.name]
        super().delete_dag(dag_id)
        
        
    def online_planner(self, dag_id, stage_id):        
        super().online_planner(dag_id, stage_id)
        plans = []
        #if len(self.min_table) > 0:
        #    dag_id = self.dags[self.min_table.pop(0)['dag_name']]
        if dag_id not in self.dag_planners: return 
        plans.extend(self.dag_planners[dag_id].get_next_plans(stage_id))
    
        while len(plans) > 0:
            plan = plans.pop(0)
            if plan.type == 0:
                if requester.cache_mrd_plan(plan) != status.SUCCESS:
                    # for all priority plans larger than this priority on this stage mark them as infeasible
                    continue
                if plan.dag_id in self.dag_planners:
                    self.dag_planners[dag_id].markas_pinned_datasets(plan)
                    self.dag_planners[dag_id].update_statistics(plan.stage_id, plan.data) 
            else:
                now = 0 if stage_id == -1 else self.dag_planners[dag_id].g.stages[stage_id].start_time \
                    + self.dag_planners[dag_id].g.queue_time 
                now = now + self.dag_planners[dag_id].g.submit_time 
                if not self.pack_bandwidth(plan, now): continue 
                if requester.prefetch_mrd_plan(plan) != status.SUCCESS:
                    # for all priority plans larger than this priority on this stage mark them as infeasible
                    self.roll_back()
                    continue
                #self.mirab.update_pinned_datasets(plan)
            print(Fore.LIGHTGREEN_EX, "\t plan ", plan.data, ', dag: ', plan.dag_id, ', stage', stage_id, 
                  ' is cached/prefetched: ', plan.type, Style.RESET_ALL)
        self.dag_planners[dag_id].current_running_stage = stage_id 
        
        
    def read_min_table(self, fpath):
        print(Fore.LIGHTGREEN_EX, "Initialize MIN Table", Style.RESET_ALL)
        min_data = []
        with open(fpath + "min.csv", 'r') as minfd:
            min_csv = csv.reader(minfd)
            distance = 0
            for row in min_csv:
                fname = row[0]
                dag_name = row[1]
                min_data.append({'fname': fname , 'dag_name': dag_name})
        return min_data
