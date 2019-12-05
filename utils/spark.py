#!/usr/bin/python


import queue
from io import StringIO
import csv
import threading
import os
import json
from utils.graph import *
import pandas as pd
import estimator.spark_longest_path as spark_longest_path
import utils.pig as pig
from colorama import Fore, Style


stage_preplan = {}


def start_spark(g):
    longest_path_graph = spark_longest_path.Graph(g).findAllPaths()
    print(Fore.BLUE, longest_path_graph, Style.RESET_ALL)
    pig.build_stages(longest_path_graph)
    return pig.build_cp_priorities(longest_path_graph)


def preplan_stage(g, s):

    priority = 1;
    preplan = []

    print("Stage:", s)
    t_imprv = -1
    while t_imprv:
        cp_job = {'job': -1, 'uncr_time' : 0, 'cr_time' : 0}
        cp2_job = {'job': -1, 'uncr_time' : 0, 'cr_time' : 0}

        cp_jobs = []
        cp2_jobs = []
        for vx in s:
            if vx['uncr_time'] > cp_job['uncr_time']:
                cp2_job = cp_job
                cp_job = vx
            elif vx['uncr_time'] > cp2_job['uncr_time'] and vx['uncr_time'] != cp_job['uncr_time']:
                cp2_jobs.append(vx)
                cp2_job = vx

        for vx in s:
            if vx['uncr_time'] == cp_job['uncr_time']:
                cp_jobs.append(vx)
                if vx['cr_time'] > cp_job['cr_time']:
                    cp_job = vx
            elif vx['uncr_time'] == cp2_job['uncr_time']:
                cp2_jobs.append(vx)
                if vx['cr_time'] > cp2_job['cr_time']:
                    cp2_job = vx


        if cp_job['cr_time'] <= cp2_job['uncr_time']:
            t_imprv = cp_job['uncr_time'] - cp2_job['uncr_time']
            for e in cp_jobs:
                e['uncr_time'] = cp2_job['uncr_time']
                if t_imprv > 0:
                    preplan.append({'job': e['job'], 'priority': priority, 'ctime' : t_imprv  })
        else:
            t_imprv = cp_job['uncr_time'] - cp_job['cr_time']
            for e in cp_jobs:
                e['uncr_time'] = cp_job['cr_time']
                if t_imprv > 0:
                    preplan.append({'job': e['job'], 'priority': priority, 'ctime' : t_imprv  })

        priority = priority + 1
        if t_imprv == 0:
            imprv = False

    print("Stage preplan:", preplan, "\n")
    return preplan;


def preplan_dag(g):
    blevels, orderednodes = g.bLevel()
    cur_blevel = max(blevels)
    cur_stage = []
    ci = 0 # current index
    dag_preplan = []
    while ci < len(blevels):
        if blevels[ci] != cur_blevel:
            sp = preplan_stage(g, cur_stage) #sp: stage_preplan
            dag_preplan.append({'stage' :  max(blevels) - cur_blevel, 'preplan' : sp})
            cur_blevel = cur_blevel - 1
            cur_stage = []

        cur_stage.append({'job': orderednodes[ci], 'uncr_time' : g.timeValue[orderednodes[ci]], 'cr_time' : g.cachedtimeValue[orderednodes[ci]]})
        ci = ci + 1

    sp = preplan_stage(g, cur_stage)
    dag_preplan.append({'stage' :  max(blevels) - cur_blevel, 'preplan' : sp})
    return dag_preplan;
