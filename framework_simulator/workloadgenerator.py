#!/usr/bin/python
'''
Created on Sep 7, 2019

@author: mania
'''
import pigsimulator
import colorama
from colorama import Fore, Style
import json

'''
from alibaba traces we figure it out that average # of DAGs
submitted per 30 is L. The maxumum is Y and the minimum is Z

we generate DAGs for 1 hour every 30 seconds using poisson distribution 
with average time interval of L

For alibaba traces should I randomly 
lets independently apply size and identity 

generate set of N filenames 
Usibg exponential propablity to assign sizes for filenames use zipf distribution 
I use zipf to select from a list of file names

for ali baba traces just randomly assign inputs to nodes. 


'''
import scipy.stats as sp
import numpy as np
import sched, threading, time
import numpy as np
import tpc
import utils.graph as graph
import utils.hadoop as hadoop
import ast
import pandas as pd
#import estimator.predictor as pred
import random 
import pigsimulator as pigsim
import pigemulator as pigemu
import math
import threading
import time
import utils.inputs as inputs
import tpc
import colorama
from colorama import Fore, Style


class Workload: 
    def __init__(self):
        self.avg_ndags_p30s = 5
        self.interval = 30 # The resolution is seconds
        self.overall_sim_time = 3600 # 1hours, the resolution is in seconds
        self.n_intervals = self.overall_sim_time//self.interval
        self.n_dags_p30s = sp.poisson.rvs(mu=self.avg_ndags_p30s, size=self.n_intervals)        
        self.priority = 1
        self.elapsed_time = 0
        self.fname = '../plans/dags_pool.csv'
        self.n_datasets = 50
        self.dags = tpc.graphs_dict
        self.dags_byid = {}
        stats_fname = '../plans/job_runtime_stats.csv'
        #self.datasets = self.generate_datasets_pool()
        self.statistics = {}
        self.statistic_df = None
        #self.load_dag_pool(self.fname)
        #self.load_statistics(stats_fname)
    
    def generate_datasets_pool(self):
        datasets = ['file_'+str(i) for i in range(0, self.n_datasets)]
        #datasets_sizes = sp.expon.rvs(scale=1, loc=0, size=self.n_datasets)
        #datasets_frequency = np.random.zipf(5, size=120)
        return datasets
    
    def load_dag_pool(self, fname):
        with open(fname) as fd: 
            dags_str = fd.read()
            dagstr_ls = dags_str.split('}{')
            dagstr_ls[0] = dagstr_ls[0] + '}'
            dagstr_ls[-1] = '{' + dagstr_ls[-1]
            for i in range(1, len(dagstr_ls) - 1):
                dagstr_ls[i] = '{' + dagstr_ls[i] + '}'
            for gstr in dagstr_ls:
                raw_dag = ast.literal_eval(gstr)
                g = graph.jsonstr_to_graph(raw_dag['dagdata'])
                g.dag_id = raw_dag['DAGid']
                self.dags.append(g)
                self.dags_byid[g.dag_id] = g
    
    def load_statistics(self,fname):
        self.statistic_df = pd.read_csv(fname)
         
    def load_statistics2(self, fname):
        with open(fname) as fd:
            data1 = fd.read().split('\n')
            del data1[-1]
            df_header = data1[0].split(',')
            data = data1[1:]
            df_data = []
            for x in data:
                jobInfo = x.split(',')
                df_data.append(jobInfo)
                dag_id = jobInfo[14]
                jobId = jobInfo[0]
                if dag_id not in self.statistics:
                    self.statistics[dag_id] = {}
                type = jobInfo[12]
                if not type:
                    type = 'UDF'
                self.statistics[dag_id][jobId] = {'job_id': jobInfo[0],
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
                                               'Outputs':  jobInfo[13],
                                               'DagId': jobInfo[14],
                                               'runtime': math.ceil(int(jobInfo[15])//1000),
                                               'queuetime':  math.ceil(int(jobInfo[16])//1000),
                                               'maptime': math.ceil(int(jobInfo[17])//1000),
                                               'name':  jobInfo[18]}
                
        self.statistic_df = pd.DataFrame(data=df_data,columns=df_header)

    def select_dags(self):
        index = self.elapsed_time//self.interval
        n_dags_inpool = len(self.dags)
        n_submitted_dags = self.n_dags_p30s[index]
        submitted_dags_indexes = np.random.randint(n_dags_inpool, size=n_submitted_dags)
        print('timestamp: ', index, 
              ', \# of submitted DAGs:', n_submitted_dags, 
              'submitted DAGs:', submitted_dags_indexes)
        submitted_dags = [self.dags[i] for i in submitted_dags_indexes]
        return submitted_dags
            
    def schedule_timer(self):
        s = sched.scheduler(time.time, time.sleep)
        self.select_dags()
        self.elapsed_time+=self.interval
        if self.elapsed_time < self.overall_sim_time:
            s.enter(self.interval, self.priority, self.schedule_timer, argument=())
            s.run()
        
    def start_workload(self):
        global start_t
        s = sched.scheduler(time.time, time.sleep)
        start = time.time()
        start_t = start
        self.schedule_timer() 
        end = time.time()
        print("Total simulation time", int(end - start))
        
    def start_single_dag_coldcache_poolworkload(self):
        for dag in self.dags:
            if dag.dag_id in self.statistics: 
                g_stats = self.statistics[dag.dag_id]
                i = 0;
                for j in g_stats:
                    dag.static_runtime(i, int(g_stats[j]['runtime']),
                                       random.randint(int(g_stats[j]['runtime'])//10, 
                                                      int(g_stats[j]['runtime'])//3))
                    i = i + 1
                runtime = pigsim.start_pig_simulator(dag)
    
    def start_single_dag_coldcache_misestimation(self):
        stats = {"Kariz": {}, "RCP": {}, "PC": {}}
        try:
            with open('misestimation_result.json', 'r') as dumpf:
                stats = json.load(dumpf)
        except FileNotFoundError:
                print(Fore.YELLOW, "No stats available", Style.RESET_ALL)
        
        mse_factors= [-0.5, -0.4, -0.3, -0.2, -0.1, -0.05, 0, 0.05, 0.1, 0.2, 0.3, 0.4, 0.5]    
        runtimes = stats["Kariz"]
        print('Start misestimation experiment with Kariz, number of DAGs in the workload: ', len(self.dags))
        for dag_id in self.dags:
            if not dag_id.startswith('AQ'): continue 
            dag = self.dags[dag_id]
            dag.name = dag_id
            if dag_id not in runtimes:
                runtimes[dag_id] = {}
            
            for msef in mse_factors:
                print(Fore.GREEN, '\nProcess:', dag_id, ', with mse_factor:', msef, Style.RESET_ALL)
                dag.reset()
                dag.set_misestimation_error(msef)
                runtime, rtl, dataset_inputs = pigsim.start_pig_simulator(dag)
                
                runtimes[dag_id][msef] = {'Cache': 'Kariz', 'DAG_id': dag_id, 'Runtime': runtime, 
                                'runtime list': rtl, 'datasets': dataset_inputs, 'type': dag.category,
                                'lamda': msef}
            #print(Fore.GREEN, runtimes[dag_id], Style.RESET_ALL)
            print(Fore.GREEN, "We are done successfullyyyyy! We could get in!!!! Yoooohooooo", Style.RESET_ALL)
        
        with open('misestimation_result.json', 'w') as dumpf:    
            json.dump(stats, dumpf)

    def start_1seqworkload_vshistory(self):
        stats = {"Kariz": {}, "LRU": {}, "LRU+PG": {}, "MIN+MRD": {}, "MIN+CP": {}}
        try:
            with open('historybasedapproaches.json', 'r') as dumpf:
                stats = json.load(dumpf)
        except FileNotFoundError:
                print(Fore.YELLOW, "historybasedapproaches.json is not available", Style.RESET_ALL)
        
        runtimes = stats["MIN+MRD"]
        print('Experiment: Compare with history-based approaches, \n\t Replacement: LRU, Prefetch: PG, \n\t\t number of DAGs in the workload: ', len(self.dags))
        for dag_id in self.dags:
            if not dag_id.startswith('AQ'): continue 
            dag = self.dags[dag_id]
            dag.name = dag_id
            if dag_id not in runtimes:
                runtimes[dag_id] = {}
            
            print(Fore.GREEN, '\nProcess:', dag_id, Style.RESET_ALL)
            dag.reset()
            runtime, rtl, dataset_inputs = pigsim.start_pig_simulator(dag)    
            runtimes[dag_id] = {'Cache': 'MIN+MRD', 'DAG_id': dag_id, 'Runtime': runtime,
                                      'runtime list': rtl, 'datasets': dataset_inputs, 'type': dag.category}


        print(Fore.GREEN, "Lets parse the LRU results", Style.RESET_ALL)
        
        with open('historybasedapproaches.json', 'w') as dumpf:
            json.dump(stats, dumpf)


    
    def start_single_dag_coldcache_seqworkload(self):
        stats = {"Kariz": {}, "RCP": {}, "PC": {}, "MRD": {}}
        try:
            with open('simulation_result.json', 'r') as dumpf:
                stats = json.load(dumpf)
        except FileNotFoundError:
                print("No stats available")
            
        runtimes = stats["Kariz"]
        for dag_id in self.dags:
            if not dag_id.startswith('AQ'): continue 
            print('Process ', dag_id)
            dag = self.dags[dag_id]
            runtime, rtl, dataset_inputs = pigsim.start_pig_simulator(dag)
            runtimes[dag_id] = {'Cache': 'Kariz', 'DAG_id': dag_id, 'Runtime': runtime, 'runtime list': rtl, 'datasets': dataset_inputs}
            print(Fore.GREEN, runtimes[dag_id], Style.RESET_ALL)
        
        with open('simulation_result.json', 'w') as dumpf:    
            json.dump(stats, dumpf)
                
    
    def start_multiple_dags_workload(self):
        threads = []
        #tdags = self.dags
        tdags = ['AQ20', 'AQ21']
        for i in range(len(tdags)):
            x = threading.Thread(target=pigsim.start_pig_simulator, args=(tpc.graphs_dict[tdags[i]],))
            threads.append(x)
            x.start()
            
        for th in threads:
            th.join()
            
        return 0

    def start_multiple_dags_static_sample_workload(self):
        threads = []
        tdags = [['AQ6', 'AQ8', 'AQ7', 'AQ3', 'AQ15'], ['AQ1', 'AQ2', 'AQ7'], ['AQ2', 'AQ12', 'AQ13', 'AQ16']]
        tdags = [['AQ6', 'AQ8', 'AQ7', 'AQ3', 'AQ15'], ['AQ1', 'AQ2', 'AQ7'], ['AQ2', 'AQ12', 'AQ13', 'AQ16']]
        
        for i in range(len(tdags)):
            x = threading.Thread(target=pigsim.start_pig_simulator, args=(tdags[i],))
            threads.append(x)
            x.start()
            
        for th in threads:
            th.join()
            
        return 0

    def synthetic_static_sample_workload(self):
        poisson_output = '''5  3  4  5  5  4  1  2  6  6  8  5  2  4  6  3  9  7  4  8  3  5  9  5  6  9  4  5  5  2  3  5  5  4  9  1  3  2  4  2  2  7  3  4  6  1  4  8  2  3  5  7  3  7  7  5  4  7  4  7  3  7  3  5  1  2  8  1  6  6  2  7  8  5  6  6  3  2  6  6  6  10  3  4  6  5  6  9  4  4  5  4  5  6  2  8  4  4  4  9  5  2  6  8  4  2  4  11  4  9  4  5  8  8  5  2  6  5  5  6'''
        
        self.n_dags_p30s = list(map(int, poisson_output.split('  ')))        
        
        graphs = [0]*len(self.n_dags_p30s)
        for i, ng in enumerate(self.n_dags_p30s):
            graphs[i] = [] 
            for x in range(0, ng):
                gid = random.choice(list(tpc.graphs_dict))
                graphs[i].append(gid)
                
        return graphs

    
    def d3n_sequential_workload(self):
        g = tpc.graphs_dict['AQ19']
        runtime, rtl, dataset_inputs = pigemu.start_pig_emulator(g)
        
    
    def bw_allocation_workload(self):
        stats = {"Kariz": {}, "MRD": {}, "CP": {}, "NoCache": {}}
        try:
            with open('bandwidthsensitivity.json', 'r') as dumpf:
                stats = json.load(dumpf)
        except FileNotFoundError:
                print(Fore.YELLOW, "bandwidthsensitivity.json is not available", Style.RESET_ALL)

        runtimes = stats["Kariz"]
        #dags = ['AQ26', 'AQ27', 'AQ25']
        dags = tpc.graphs_dict
        prev_runtime = 0
        #bandwidth = [30, 150, 300, 600, 1200]
        bandwidth = 300
        for dag_id in dags:
            if dag_id not in runtimes:
                runtimes[dag_id] = {}

            if not dag_id.startswith('AQ'): continue 
            print('Process ', dag_id)
            dag = tpc.graphs_dict[dag_id]
            dag.submit_time = prev_runtime
            runtime, rtl, dataset_inputs = pigsim.start_pig_simulator(dag)
            prev_runtime = runtime
            runtimes[dag_id][bandwidth] = {'Cache': 'Kariz', 'DAG_id': dag_id, 'Runtime': runtime, 'runtime list': rtl, 'datasets': dataset_inputs, 'bandwidth':bandwidth}
            print(runtimes[dag_id][bandwidth])

        
        print(Fore.GREEN, runtimes, Style.RESET_ALL)    
        with open('bandwidthsensitivity.json', 'w') as dumpf:
            json.dump(stats, dumpf)

def own_test(self):
        dag = self.test_spark_collector()
        runtime, rtl, dataset_inputs = pigsim.start_pig_simulator(dag)

    def test_spark_collector(self):
        # my_collector = collector.Collector()
        #     raw_execplan = '''
        # (8) ShuffledRDD[5] at reduceByKey at ScalaWordCount.scala:48 []
        #  +-(8) MapPartitionsRDD[4] at map at ScalaWordCount.scala:46 []
        #     |  MapPartitionsRDD[3] at flatMap at ScalaWordCount.scala:44 []
        #     |  MapPartitionsRDD[2] at map at IOCommon.scala:44 []
        #     |  MapPartitionsRDD[1] at sequenceFile at IOCommon.scala:44 []
        #     |  hdfs://sandbox.hortonworks.com:8020/HiBench/Wordcount/Input HadoopRDD[0] at sequenceFile at IOCommon.scala:44 []
        #     '''
        raw_execplan = '''
    (8) PythonRDD[12] at collect at /Users/joe/Desktop/Spark_Source/spark/bin/wordcount.py:39 []
     |  MapPartitionsRDD[11] at mapPartitions at PythonRDD.scala:133 []
     |  ShuffledRDD[10] at partitionBy at NativeMethodAccessorImpl.java:0 []
     +-(8) PairwiseRDD[9] at reduceByKey at /Users/joe/Desktop/Spark_Source/spark/bin/wordcount.py:31 []
        |  PythonRDD[8] at reduceByKey at /Users/joe/Desktop/Spark_Source/spark/bin/wordcount.py:31 []
        |  UnionRDD[7] at union at NativeMethodAccessorImpl.java:0 []
        |  UnionRDD[4] at union at NativeMethodAccessorImpl.java:0 []
        |  input1/*.txt MapPartitionsRDD[1] at textFile at NativeMethodAccessorImpl.java:0 []
        |  input1/*.txt HadoopRDD[0] at textFile at NativeMethodAccessorImpl.java:0 []
        |  input2/*.txt MapPartitionsRDD[3] at textFile at NativeMethodAccessorImpl.java:0 []
        |  input2/*.txt HadoopRDD[2] at textFile at NativeMethodAccessorImpl.java:0 []
        |  input3/*.txt MapPartitionsRDD[6] at textFile at NativeMethodAccessorImpl.java:0 []
        |  input3/*.txt HadoopRDD[5] at textFile at NativeMethodAccessorImpl.java:0 []
        '''

        # objstore = objs.ObjectStore()
        # my_collector.objectstore = objstore
        g = Graph.sparkstr_to_graph(raw_execplan)
        # spark_longest_path.Graph(g).findAllPaths()
        # print(str(g))
        return g

