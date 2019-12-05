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


import plans.rcp.rcp as rcp
import estimator.collector as col
import utils.objectstore as objs

g_collector = None
g_rcp = None
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
    global g_rcp
    _rcp = rcp.RCP()
    g_rcp = _rcp
    g_rcp.objectstore = g_objectstore
    return _rcp

def get_timestamp():
    return datetime.now().strftime(("%Y-%m-%d %H:%M:%S"))

def notify_collector(stats):
    g_collector.update_statistic_from_string(stats.decode("utf-8"))

def notify_newstage(new_stage):
    g_rcp.notify_new_stage_from_string(new_stage.decode("utf-8"))

def submit_newdag(new_dag):
    g_rcp.new_dag_from_string(new_dag.decode("utf-8"))
    g_collector.new_dag_from_string(new_dag.decode("utf-8"))

def notify_completed(dagstr):
    g_rcp.remove_dag(dagstr.decode("utf-8"))
