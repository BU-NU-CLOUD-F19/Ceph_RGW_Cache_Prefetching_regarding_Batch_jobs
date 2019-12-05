#!/usr/bin/python3
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4
"""
This is the daemon module and supports all the ReST actions for the
KARIZ cache management project
"""
from datetime import datetime

import plans.mrd.mrd as mrd

g_collector = None
g_mrd = None

def start_estimator():
#    global g_collector
#    collector = estimator.collector() 
#    g_collector = collector;
#    return collector
    return 0

def start_mrd():
    global g_mrd
    _mrd = mrd.MRD()
    g_mrd = _mrd
    return g_mrd

def get_timestamp():
    return datetime.now().strftime(("%Y-%m-%d %H:%M:%S"))

def notify_collector(stats):
    g_collector.update_statistic_from_string(stats.decode("utf-8"))

def notify_planner(new_stage):
    g_mrd.notify_new_stage_from_string(new_stage.decode("utf-8"))

def notify_mrd(new_dag):
    g_mrd.new_dag_from_string(new_dag.decode("utf-8"))
