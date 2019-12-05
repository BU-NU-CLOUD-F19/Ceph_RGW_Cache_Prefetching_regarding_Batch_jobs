'''
Created on Oct 7, 2019

@author: mania
'''
import utils.hadoop as hadoop
import pandas as pd


def get_runtime_info():
    jobs = pd.read_csv('hibenchtests.csv')
    
    stats = []
    for index, job in jobs.iterrows():
        jobId = job['job'].replace('application_', 'job_')
        host = job['master']
        print(jobId)
        js = hadoop.getJobStats2(jobId, host)
        js['cachestatus'] = job['cachestatus']
        js['app'] = job['app']
        js['experiment'] = job['experiment']
        stats.append(js)
        
    jobs_stats = pd.DataFrame(data=stats)
    return jobs_stats
    