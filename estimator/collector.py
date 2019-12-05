#!/usr/bin/python3
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

import queue
from io import StringIO
import csv
import threading
import os
import pandas as pd
import ast
import utils.graph as graph
import hdfs
import utils.objectstore as objs
import utils.hadoop as hadoop
import math 

_collector = None

class Collector: 
    def update_statistic_from_string(self, stats_string):
        qdata = {'type': 'Stat',
                'data': stats_string}
        self.q.put(qdata)

    def new_dag_from_string(self, raw_execplan):
        qdata = {'type': 'DAG',
                'data': raw_execplan}
        self.q.put(qdata)
    
    def stats_to_json(self, raw_stats, dag_id):
        jobInfo = raw_stats.split(',')
        jobId = jobInfo[0]
        runtime, queuetime, maptime, name = hadoop.getJobStats(jobId)
        inputs = hadoop.getInputs(jobId)
        outputs = hadoop.getOutputs(jobId)
        type = jobInfo[12]
        print(jobInfo)
        if not type:
            type = 'UDF'
        stats = {'job_id': jobInfo[0],
                'n_maps':  int(jobInfo[1]),
                'Reduces': int(jobInfo[2]),
                'MaxMapTime':  math.ceil(int(jobInfo[3])),
                'MinMapTime': math.ceil(int(jobInfo[4])),
                'AvgMapTime':  math.ceil(int(jobInfo[5])),
                'MedianMapTime': math.ceil(int(jobInfo[6])),
                'MaxReduceTime':  math.ceil(int(jobInfo[7])),
                'MinReduceTime': math.ceil(int(jobInfo[8])),
                'AvgReduceTime':  math.ceil(int(jobInfo[9])),
                'MedianReducetime': math.ceil(int(jobInfo[10])),
                'Alias':  jobInfo[11],
                'Type': type,
                'Outputs':  outputs,
                'Inputs' : inputs,
                'DagId': dag_id,
                'runtime': math.ceil(int(runtime)//1000),
                'queuetime':  math.ceil(int(queuetime)//1000),
                'maptime': math.ceil(int(maptime)//1000),
                'name':  name}
        return stats


    def process_statstistics(self, raw_stats):
        stat_lines = raw_stats.replace('\n\n', '\n').split('\n')
        for s in stat_lines: # remove empty lines
            if not s:
                stat_lines.remove(s)                
        dag_id=stat_lines[0].split(':')[1]     
        pig_headers = stat_lines[1:2];
        data = []
        dagsids = []
        for s in stat_lines[4:]:
            sj = self.stats_to_json(s, dag_id)
            stats_header = sj.keys()
            data.append(sj)
            dagsids.append(dag_id)

        stats_df = pd.DataFrame(data)
        stats_fn = 'job_runtime_stats.csv';
        if os.path.isfile(stats_fn) and (os.stat(stats_fn).st_size > 0):
            stats_header = False
        runtime_stats_f = open(stats_fn,'a+');
        stats_df.to_csv(path_or_buf=runtime_stats_f,index=False,header=stats_header)
        
    def submit_new_dag(self, strdata):
        g = graph.str_to_graph(strdata, self.objectstore)
        dags_fn = 'dags_pool.csv';
        dags_f = open(dags_fn, 'a+');
        data_to_dump = str({'DAGid': g.dag_id,
            'dagdata': str(g)})
        dags_f.write(data_to_dump)
        dags_f.close()
        
    def worker(self):
        while True:
            qdata = self.q.get()
            if qdata:
                if qdata['type'] == 'DAG':
                    g = graph.str_to_graph(qdata['data'], self.objectstore)
                    dags_fn = 'dags_pool.csv';
                    dags_f = open(dags_fn, 'a+');
                    data_to_dump = str({'DAGid': g.dag_id,
                        'dagdata': str(g)})
                    dags_f.write(data_to_dump)
                    dags_f.close()
                elif qdata['type'] == 'Stat':
                    self.process_statstistics(qdata['data'])
                self.q.task_done()

    def __init__(self):
        global _collector
        self.q = queue.Queue();
        self.t = threading.Thread(target=self.worker)
        self.t.start()
        self.objectstore = None
        _collector = self


