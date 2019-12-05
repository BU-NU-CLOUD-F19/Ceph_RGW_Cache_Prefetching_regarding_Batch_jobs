#!/usr/bin/python3
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

import queue
from io import StringIO
import csv
import threading
import os
import ast
import json
import utils.graph as graph
import mirab as mq




import hdfs
import boto

_kariz = None

class Kariz:
    def gq_worker(self):
        while True:
            graphstr = self.gq.get()
            if graphstr:
                g = graph.str_to_graph(graphstr, self.objectstore)
                #self.dag_id = g.dag_id
                self.mirab.add_dag(g)
                self.gq.task_done()


    def pq_worker(self):
        while True:
            stage_metastr = self.pq.get()
            if stage_metastr:
                stage_meta = ast.literal_eval(stage_metastr)
                #self.mirab.online_planner(self.dag_id, stage_meta['stage'])
                self.mirab.online_planner(stage_meta['id'], stage_meta['stage'])
                self.pq.task_done()
                
    def dq_worker(self):
        while True:
            dagstr = self.dq.get()
            if dagstr:
                dag_meta = ast.literal_eval(dagstr) 
                self.mirab.delete_dag(dag_meta['id'])
                self.dq.task_done()

    def __init__(self):
        global _kariz
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
        self.mirab = mq.Mirab() # Mirab logic
        self.dag_id = 0 
        _kariz = self # mirab daemon instance
         
   
    def new_dag_from_id(self, dag_string):
        print(dag_string)
        self.gq.put('ID:' + dag_string)

    def new_dag_from_string(self, dag_string):
        print('Lets comment it for now')
        self.gq.put(dag_string)

    def notify_new_stage_from_string(self, stage_metastr):
        self.pq.put(stage_metastr)
        
    def remove_dag(self, dagstr):
        self.dq.put(dagstr)

