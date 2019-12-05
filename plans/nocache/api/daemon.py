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


import plans.nocache.nocache as nc
import estimator.collector as col
import utils.objectstore as objs

g_collector = None
g_nocache = None
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

def start_nocache():
    global g_nocache
    nocache = nc.NoCache()
    g_nocache = nocache;
    g_nocache.objectstore = g_objectstore
    return nocache


def get_timestamp():
    return datetime.now().strftime(("%Y-%m-%d %H:%M:%S"))

def update_statistics(stats):
    g_collector.update_statistic_from_string(stats.decode("utf-8"))

def notify_planner(new_stage):
    g_nocache.notify_new_stage_from_string(new_stage.decode("utf-8"))

def notify_collector(new_dag):
    print("new_dag is submitted")
    #g_collector.new_dag_from_string(new_dag.decode("utf-8"))
