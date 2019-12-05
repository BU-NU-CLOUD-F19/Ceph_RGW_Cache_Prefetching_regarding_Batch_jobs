#!/usr/bin/python

# Author: Mania Abdi
import utils.plan as plan
from plans.dagplanner import *
import json

class MRDagPlanner(DAGPlanner):
    def __init__(self, g):
        super().__init__(g)
        self.cached_plans = []
        self.mrd_tables = {}
        g.plans_container = pig.build_mrd_priorities(g)         
        self.initialize_mrdtable(g)
    
    def add_dag(self, g):
        g.plans_container = self.build_pig_mrd_plans(g)
        self.dags[g.dag_id] = g

    def get_stage_plans(self, dag_id, stage):
        return self.dags[dag_id].plans_container.get_stage_plans(stage)
    
    def initialize_mrdtable(self, g):
        ''' compute the MRD table for every stage in the graph'''
        mrd_table_by_distance = {}
        for stg in g.stages:
            stg_plans = g.plans_container.get_cache_plans(stg)
            if len(stg_plans) > 0:
                mrd_table_by_distance[stg] = stg_plans
        self.mrd_tables = mrd_table_by_distance
        
    def update_mrdtable(self):
        mrd_table_by_distance =  self.mrd_tables
        mrd_table_by_distance_tmp = {}
    
        distance=0         
        if distance in mrd_table_by_distance:
            del mrd_table_by_distance[distance]
            
        for k in mrd_table_by_distance:
            mrd_table_by_distance_tmp[k-1] = mrd_table_by_distance[k]
        
        self.mrd_tables = mrd_table_by_distance_tmp

    def get_cache_mrd_plan(self, dag_id):
        if 0 in self.mrd_tables:          
            return self.mrd_tables[0]
        return None
    
    def get_prefetch_mrd_plan(self, dag_id):
        return self.mrd_tables

    def get_pinned_for_stage(self, stage_id):
        return self.constant_mrd_tables[dag_id][stage_id]
    
    
    def get_next_plans(self, stage_id):
        plans = []
        keys = self.mrd_tables.keys()
        if not len(keys): return plans;
        distance = min(keys)
        if stage_id == -1 and distance in self.mrd_tables:
            ps = copy.deepcopy(self.mrd_tables[distance])
            for p in ps:
                p.type = 1
                p.distance = distance 
            plans.extend(ps)
            return plans
        
        # cache plans 
        keys = self.mrd_tables.keys()
        if not len(keys): return plans;
        distance = min(keys)
        if self.mrd_tables[distance][0].stage_id == stage_id:
            ps = self.mrd_tables[distance]
            for p in ps:
                p.distance = distance
            plans.extend(ps)
            self.update_mrdtable()
        
        # prefetch plans
        keys = self.mrd_tables.keys()
        if not len(keys): return plans;
        distance = min(keys)
        if distance in self.mrd_tables:
            ps = copy.deepcopy(self.mrd_tables[distance])
            for p in ps:
                p.type = 1
                p.distance = distance
            plans.extend(ps)
        return plans