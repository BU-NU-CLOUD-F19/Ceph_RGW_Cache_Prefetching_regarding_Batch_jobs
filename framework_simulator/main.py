#!/usr/bin/python
'''
Created on Sep 16, 2019

@author: mania
'''

import workloadgenerator as wkl

def test_misestimation():
    workload = wkl.Workload()
    workload.start_single_dag_coldcache_misestimation()


def test_sequential():
    workload = wkl.Workload()
    workload.start_single_dag_coldcache_seqworkload()

def test_concurrent():
    workload = wkl.Workload()
    workload.start_multiple_dags_workload()

def test_vshistory():
    workload = wkl.Workload()
    workload.start_1seqworkload_vshistory()

def test_d3n():
    workload = wkl.Workload()
    workload.d3n_sequential_workload()

def test_bw_allocation():
    workload = wkl.Workload()
    workload.bw_allocation_workload()
def test_spark():
    workload = wkl.Workload()
    workload.own_test()    
#test_d3n()
#test_sequential()
#test_concurrent()
#test_misestimation()
#test_bw_allocation()
#test_vshistory()
test_spark
