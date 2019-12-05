#!/usr/bin/python

import json
import requests
import random

def getJobStats2(jobId, host=''):
    """ GETTING DIRECTORIES """
    end_point_address = 'http://192.168.35.2:19888' if not host else 'http://' + host + ':19888'
        
    command = end_point_address + "/ws/v1/history/mapreduce/jobs/" + jobId  + "/"
    #command = 'http://neu-3-2:19888/jobhistory/job/'+ jobId + '/'
    resp = requests.get(command)
   
    ResJson= json.loads(resp.content.decode('utf-8'))
    job = ResJson['job']
    job['runtime'] = job['finishTime'] - job['startTime']
    job['queue time'] =  job['startTime'] - job['submitTime']
    return job

def getJobFSCounters(jobId, host=''):
    """ GETTING DIRECTORIES """
    end_point_address = 'http://192.168.35.2:19888' if not host else 'http://' + host + ':19888'    
    command = end_point_address + "/ws/v1/history/mapreduce/jobs/" + jobId  + "/counters"
    #command = 'http://neu-3-2:19888/jobhistory/job/'+ jobId + '/'
    resp = requests.get(command)
   
    ResJson= json.loads(resp.content.decode('utf-8'))
    counters = ResJson['jobCounters']['counterGroup']
    for cntr in counters:
        if cntr['counterGroupName'] == 'org.apache.hadoop.mapreduce.FileSystemCounter':
            fscntr = {}
            for c in cntr['counter']:
                fscntr[c['name']] = c['totalCounterValue']
                
            return fscntr 



def getJobStats(jobId, host=''):
    """ GETTING DIRECTORIES """
    end_point_address = 'http://192.168.35.2:19888' if not host else 'http://' + host + ':19888'
        
    command = end_point_address + "/ws/v1/history/mapreduce/jobs/" + jobId  + "/"
    #command = 'http://neu-3-2:19888/jobhistory/job/'+ jobId + '/'
    resp = requests.get(command)
   
    ResJson= json.loads(resp.content.decode('utf-8'))
    job = ResJson['job']
    Elapsed = job['finishTime'] - job['startTime']
    waitTime =  job['startTime'] - job['submitTime']
    n_mappers = job['mapsCompleted']
    mapTime = job['avgMapTime'] 
    reduceTime = job['avgReduceTime'] 
    shuffleTime = job['avgShuffleTime']
    name = job['name'] 
    return Elapsed, waitTime, name, mapTime, reduceTime, shuffleTime 

def getInputs(jobId):
    command = "http://192.168.35.2:19888/ws/v1/history/mapreduce/jobs/" + jobId  + "/conf"
    resp = requests.get(command)

    ResJson= json.loads(resp.content.decode('utf-8'))
    properties = ResJson['conf']['property']

    inputs = next((item for item in properties if item["name"] == "pig.input.dirs"), None)
    if inputs is None:
        return []
    return inputs['value'].split(',')

def getOutputs(jobId):
    command = "http://192.168.35.2:19888/ws/v1/history/mapreduce/jobs/" + jobId  + "/conf"
    resp = requests.get(command)

    ResJson= json.loads(resp.content.decode('utf-8'))
    properties = ResJson['conf']['property']

    outputs = next((item for item in properties if item["name"] == "pig.reduce.output.dirs"), None)
    if outputs:
        return outputs['value'].split(',')
    
    outputs = next((item for item in properties if item["name"] == "pig.map.output.dirs"), None)
    if outputs:
        return outputs['value'].split(',')

def getJobName(jobId):
    """ GETTING DIRECTORIES """
    command = "http://192.168.35.2:19888/ws/v1/history/mapreduce/jobs/" + jobId  + "/conf"
    resp = requests.get(command)

    ResJson= json.loads(resp.content.decode('utf-8'))
    properties = ResJson['conf']['property']


    """ Job Name """
    job_runtime= next(item for item in properties if item["name"] == "mapreduce.job.name")
    print("job name is: " + job_name['value'])

    return job_runtime


def getJobResourceRequest(jobId):
   """ GETTING DIRECTORIES """
   command = "http://192.168.35.2:19888/ws/v1/history/mapreduce/jobs/" + jobId  + "/conf"
   resp = requests.get(command)
   resources = {}
   ResJson= json.loads(resp.content.decode('utf-8'))
   properties = ResJson['conf']['property']
   resources['reduce.cpu'] = next((item['value'] for item in properties if item["name"] == "mapreduce.reduce.cpu.vcores"), 0)
   resources['map.cpu'] = next((item['value'] for item in properties if item["name"] == "mapreduce.map.cpu.vcores"), 0)
   resources['map.memory'] = next((item['value'] for item in properties if item["name"] == "mapreduce.map.memory.mb"), 0)
   resources['reduce.memory'] = next((item['value'] for item in properties if item["name"] == "mapreduce.reduce.memory.mb"), 0)    
   return resources


def getJobInfo(jobId):
   """ GETTING DIRECTORIES """
   command = "http://192.168.35.2:19888/ws/v1/history/mapreduce/jobs/" + jobId  + "/conf"
   resp = requests.get(command)

   ResJson= json.loads(resp.content.decode('utf-8'))
   properties = ResJson['conf']['property']


   """ Job Name """
   job_name= next(item for item in properties if item["name"] == "mapreduce.job.name")
   print("job name is: " + job_name['value'])



   """ INPUT Directory """
   input_dir= next(item for item in properties if item["name"] == "pig.input.dirs")
   print("input directory is: " + input_dir['value'])


   """ OUTPUT Directory """
   output_dir= next(item for item in properties if item["name"] == "pig.reduce.output.dirs")
   print("output directory is: " + output_dir['value'])


   """ GETTING SIZES """
   command = "http://192.168.35.2:19888/ws/v1/history/mapreduce/jobs/" + jobId  + "/counters"
   resp = requests.get(command)
   ResJson= json.loads(resp.content.decode('utf-8'))
   properties = ResJson['jobCounters']['counterGroup']


   """ INPUT Size """
   counter = next(item for item in properties if item["counterGroupName"] == "org.apache.hadoop.mapreduce.FileSystemCounter")
   counter = counter['counter']

   input_size = next(item for item in counter if item["name"] == "HDFS_BYTES_READ")
   print("input size is: %d"  % input_size['totalCounterValue'])


   """ OUTPUT Size """
   output_size= next(item for item in counter if item["name"] == "HDFS_BYTES_WRITTEN")
   print("output size is: %d" % output_size['totalCounterValue'])

