#!/usr/bin/python3
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

"""
This is the daemon module and supports all the ReST actions for the
KARIZ cache management project
"""

# System modules
from datetime import datetime

# 3rd party modules
from flask import make_response, abort


import plans.cp.cp as cp
import estimator.collector as col
import utils.objectstore as objs

g_collector = None
g_cp = None
g_objectstore = None

def start_objectstore():
    global g_objectstore;
    objectstore = objs.ObjectStore()
    g_objectstore = objectstore
    return g_objectstore

def start_estimator():
    global g_collector
    collector = col.Collector() 
    g_collector = collector;
    g_collector.objectstore = g_objectstore
    return collector

def start_kariz():
    global g_cp
    _cp = cp.CP()
    g_cp = _cp
    g_cp.objectstore = g_objectstore
    return _cp

def get_timestamp():
    return datetime.now().strftime(("%Y-%m-%d %H:%M:%S"))

def notify_collector(stats):
    g_collector.update_statistic_from_string(stats.decode("utf-8"))

def notify_newstage(new_stage):
    g_cp.notify_new_stage_from_string(new_stage.decode("utf-8"))

def submit_newdag(new_dag):
    g_cp.new_dag_from_string(new_dag.decode("utf-8"))
    g_collector.new_dag_from_string(new_dag.decode("utf-8"))

def notify_completed(dagstr):
    g_cp.remove_dag(dagstr.decode("utf-8"))
