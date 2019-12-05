#!/usr/bin/python3
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

import queue
from io import StringIO
import csv
import threading
import os
import ast

import utils.graph as graph
import utils.requester as requester
import utils.status as status

import hdfs
import boto

_kariz = None

class NoCache:
    def online_planner(self, stage_metastr):
        stage_meta = ast.literal_eval(stage_metastr)


    def pq_worker(self):
        while True:
            stage_meta = self.pq.get()
            if stage_meta:
                self.online_planner(stage_meta)
                self.pq.task_done()


    def __init__(self):
        global _nocache
        # a thread to process the incoming stage 
        self.pq = queue.Queue();
        self.pt = threading.Thread(target=self.pq_worker)
        self.pt.start()
        self.objectstore = None
        _nocache = self # mirab daemon instance 

    def notify_new_stage_from_string(self, stage_metastr):
        self.pq.put(stage_metastr)
