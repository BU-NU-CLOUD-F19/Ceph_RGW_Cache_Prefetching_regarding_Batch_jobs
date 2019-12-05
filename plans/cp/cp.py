#!/usr/bin/python3
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

import queue
from io import StringIO
import csv
import threading
import os
import ast

import utils.graph as graph
import plans.cp.planner as pl
import utils.requester as requester
import utils.status as status


import hdfs
import boto

_cp = None

class CP:    
    def online_planner(self, stage_metastr):
        stage_meta = ast.literal_eval(stage_metastr)
        self.planner.unpinned_completed_stage(stage_meta['id'], stage_meta['stage'])
        plans = self.planner.get_plans(stage_meta['id'], stage_meta['stage'])
        if plans is None: return
        
        for p in plans:
            print("online planner: ", stage_meta['stage'], p.data, p.type)
        while len(plans) > 0:
            print('len plans', len(plans))
            plan = plans.pop()
            if not plan.is_feasible(): continue
            
            _status = requester.cache_plan(plan) if plan.type == 0 else requester.prefetch_plan(plan) 
            if _status != status.SUCCESS: continue
            if plan.type == 0: self.planner.update_pinned_datasets(plan)
                                            

    def gq_worker(self):
        while True:
            graphstr = self.gq.get()
            if graphstr:
                g = graph.str_to_graph(graphstr, self.objectstore)
                self.planner.add_dag(g)
                self.gq.task_done()


    def pq_worker(self):
        while True:
            stage_meta = self.pq.get()
            if stage_meta:
                self.online_planner(stage_meta)
                self.pq.task_done()
                
    def dq_worker(self):
        while True:
            dagstr = self.dq.get()
            if dagstr:
                dag_meta = ast.literal_eval(dagstr) 
                self.planner.delete_dag(dag_meta['id'])
                self.dq.task_done()

    def __init__(self):
        global _cp
        # a thread to process the incoming dags 
        self.gq = queue.Queue();
        self.gt = threading.Thread(target=self.gq_worker)
        self.gt.start()
        # a thread to process the incoming stage 
        self.pq = queue.Queue();
        self.pt = threading.Thread(target=self.pq_worker)
        self.pt.start()
        
        self.dq = queue.Queue();
        self.dt = threading.Thread(target=self.dq_worker)
        self.dt.start()
        
        self.objectstore = None
        self.planner = pl.Planner() # Mirab logic 
        _cp = self # mirab daemon instance 

    def new_dag_from_string(self, dag_string):
        self.gq.put(dag_string)

    def notify_new_stage_from_string(self, stage_metastr):
        self.pq.put(stage_metastr)
        
    def remove_dag(self, dagstr):
        self.dq.put(dagstr)

