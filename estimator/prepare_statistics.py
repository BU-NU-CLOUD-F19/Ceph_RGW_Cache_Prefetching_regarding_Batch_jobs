#!/usr/bin/python
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LinearRegression
import utils.hadoop as hadoop
import json
import random

class Predictor:
    def __init__(self):
        self.name = 'Predictor class'
        
    def predit_runtime(self, conf):
        print(conf)
        
    def load_statistics(self, fpath):
        self.stats = pd.read_csv(fpath)
        statistics = []
        for index, e in self.stats.iterrows():
            print('Process job', index, ' of ', len(self.stats))
            counters = hadoop.getJobStats2(e['job_id'])
            fscntr = hadoop.getJobFSCounters(e['job_id'])
            
            if 'S3A_BYTES_READ' in fscntr:
                fscntr['datasize'] = fscntr['S3A_BYTES_READ'] + fscntr['HDFS_BYTES_READ']
                fscntr['bandwidth'] = 10#random.randint(5, 15)
            else:
                fscntr['datasize'] = fscntr['HDFS_BYTES_READ']
                fscntr['bandwidth'] = 40#random.randint(20, 50)
            counters.update(fscntr)
            counters.update(hadoop.getJobResourceRequest(e['job_id']))
            
            outputs = []
            for out in hadoop.getOutputs(e['job_id']):
                if 'hdfs' in out: 
                    outputs.append(out.rsplit('/')[-1])
                elif 's3a' in out:
                    outputs.append(out.replace('s3a://data/', '').replace('/', '_').replace('-', '_'))
            counters['output'] = outputs
            
            inputs = []
            e['Inputs'] = e['Inputs'].replace('[', '').replace(']', '').replace('\'', '')
            e['Type']= e['Type'].replace('[', '').replace(']', '').replace('\'', '')
            for i in e['Inputs'].split(','):
                if 'hdfs' in i: 
                    inputs.append(i.rsplit('/')[-1])
                elif 's3a' in i:
                    inputs.append(i.replace('s3a://data/', '').replace('/', '_').replace('-', '_'))
            counters['inputs'] = inputs
            #counters['type'] = e['Type'].split('|')
            for t in e['Type'].split('|'):
                counters['type'] = t
                statistics.append(counters)
        return statistics
            
fpath = './job_runtime_stats.csv'
pred = Predictor()
statistics = pd.DataFrame(pred.load_statistics(fpath))
statistics.to_csv('statistics2.csv', index=False)
print(statistics)

