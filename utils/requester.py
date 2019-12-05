#!/usr/bin/python

import requests
import random
import json

base_url = "http://0.0.0.0:3188/api"
#base_url = "http://0.0.0.0:5000/api"
cache_url = "http://0.0.0.0:3187/cache"

'''
Submit to KARIZ API
'''
def submit_new_dag(g):
    path = "/newdag"
    url = base_url + path
    reply = requests.post(url, data=str(g)).text

def notify_stage_start(g, stage_id):
    path = "/newstage"
    url = base_url + path
    data = '{"id": "'+ str(g.dag_id) + '", "stage": ' + str(stage_id) + '}'
    reply = requests.post(url, data=data).text

def complete(g):
    path = "/completed"
    url = base_url + path
    data='{"id": "' + str(g.dag_id) + '"}'
    reply = requests.post(url, data=data).text


'''
Submit to Cache API
'''
def uppined_datasets(data):
    if data is None or not len(data):
        return None
    path = "/unpinfiles"
    url = cache_url + path
    headers = {'Content-type': 'application/json'}
    _data = {'data': data}
    reply = json.loads(requests.post(url, headers=headers, json=_data).text)
    return reply['cached']
    
def cache_mrd_plan(plan):
    if not len(plan.data): return None
    path = "/cachemrd"
    url = cache_url + path
    headers = {'Content-type': 'application/json'}
    _data = {'data': plan.data, 'distance': plan.distance}
    reply = json.loads(requests.post(url, headers=headers, json=_data).text)
    return reply['cached']

def prefetch_mrd_plan(plan):
    if not len(plan.data): return None
    path = "/prefetchmrd"
    url = cache_url + path
    headers = {'Content-type': 'application/json'}
    _data = {'data': plan.data, 'distance': plan.distance}
    reply = json.loads(requests.post(url, headers=headers, json=_data).text)
    return reply['cached']


'''
Submit to Cache API
'''
def issue_mrd_plan(plan):
    path = "/cachedmrd"
    url = cache_url + path
    headers = {'Content-type': 'application/json'}
    data = plan.data
    reply = json.loads(requests.post(url, headers=headers, json=data).text)
    return reply['cached']

def is_plan_cached(plan):
    if not len(plan.data):
        return 0
      
    path = "/iscached"
    url = cache_url + path
    headers = {'Content-type': 'application/json'}
    data = {'data':plan.data}
    output = requests.post(url, headers=headers, json=data).text
    reply = json.loads(output)
    return reply['cached']


def cache_plan(plan): #FIXME: later get list of strides as well
    path = "/cacheplan"
    url = cache_url + path
    headers = {'Content-type': 'application/json'}
    data = {'data': plan.data, 'score': plan.pscore}
    reply = json.loads(requests.post(url, headers=headers, json=data).text)
    return reply['cached']

def prefetch_plan(plan): #FIXME: later get list of strides as well
    path = "/prefetchplan"
    url = cache_url + path
    headers = {'Content-type': 'application/json'}
    data = {'data': plan.data, 'score': plan.pscore}
    reply = json.loads(requests.post(url, headers=headers, json=data).text)
    return reply['cached']

def clear_cache():
    path = "/clearcache"
    url = cache_url + path
    headers = {'Content-type': 'application/json'}
    reply = json.loads(requests.post(url, headers=headers).text)
    

def cache_input(fname, size): #FIXME: later get list of strides as well
    # pin file name in the cache if it is already there and prefetch it if it is not there 
    return random.randint(0,1)
