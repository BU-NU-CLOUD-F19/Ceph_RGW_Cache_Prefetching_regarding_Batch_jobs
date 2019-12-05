#!/usr/bin/python
import uuid
import copy
import math

import utils.job as job

class Stage:
    def __init__(self, stage_id):
        self.stage_id = stage_id
        self.dag_id = -1
        self.jobs = [] 
        self.longest_jobs = []
        self.second_jobs = []
        self.longest_job = job.Job()
        self.second_job = job.Job();
        self.stage_inputs = {}
        self.start_time = 0
    
    def get_runtime(self):
        toyjob = job.Job()
        for j in self.jobs:
            if j.final_runtime > self.longest_job.final_runtime:
                self.longest_job = j
        return self.longest_job.final_runtime
    
    def update_inputs(self, j):
        for f in j.inputs:
            if f in self.stage_inputs:
                self.stage_inputs[f] = self.stage_inputs[f] if f in self.stage_inputs and self.stage_inputs[f] > j.inputs[f] else j.inputs[f]
    
    def add_job(self, j):      
        self.jobs.append(j)
        self.update_inputs(j)
        
        #FIXME: # update the longest job and second logest job
        if j.longer_than(self.longest_job):
            self.second_job = self.longest_job
            self.longest_job = j
        elif j.longer_than(self.second_job) and not j.concurrent_with(self.longest_job):
            self.second_job = j

    def update_longest_jobs(self):
        toyjob = job.Job()
        for j in self.jobs:
            if j.longer_than(self.longest_job):
                self.longest_job = j
            elif j.longer_than(self.second_job) and not j.concurrent_with(self.longest_job):
                self.second_job = j
        if self.longest_job.concurrent_with(self.second_job):
            self.second_job = toyjob
            self.second_jobs = []
            
        self.finish_add_jobs()
            
    def finish_add_jobs(self):
        for j in self.jobs:
            if j.concurrent_with(self.longest_job):
                if j in self.longest_jobs:
                    continue 
                self.longest_jobs.append(j)
                if j.longer_than_wcache(self.longest_job):
                    self.longest_job = j
            elif j.concurrent_with(self.second_job):
                if j in self.second_jobs:
                    continue 
                self.second_jobs.append(j)
                if j.longer_than_wcache(self.second_job):
                    self.second_job = j

    def end_time(self):
        return self.start_time + self.longest_job.est_runtime_remote

    def get_improvement(self): 
        if self.longest_job.est_runtime_cache > self.second_job.est_runtime_remote:
            return (self.longest_job.runtime_partial_cached - self.longest_job.est_runtime_cache), self.longest_job.est_runtime_cache
        return (self.longest_job.runtime_partial_cached - self.second_job.runtime_partial_cached), self.second_job.runtime_partial_cached
        
    def get_next_plan(self, priority):
        plan = Plan()
        plan.original_stage_id = self.stage_id
        plan.priority = priority
        plan.dag_id = self.dag_id
        plan.stage = self
        t_imprv, new_runtime = self.get_improvement()

        if t_imprv <= 0:
            return None, 0
        
        plan_inputs = {}
        plan_size = 1
        for e in self.longest_jobs:
            t_imprv_old = e.est_runtime_remote - e.runtime_partial_cached
            e.runtime_partial_cached = new_runtime
            if e.est_runtime_remote == e.est_runtime_cache:
                continue
            
            scale_factor =  (t_imprv_old + t_imprv)/(e.est_runtime_remote - e.est_runtime_cache)

            # FIXME: prepare inputs, this should represent the actual strides 
            for i in e.inputs:
                cached_size = math.ceil(e.inputs[i]*scale_factor)
                #score = t_imprv + t_imprv_old
                if i in plan_inputs:
                    cached_size = max(plan_inputs[i]['size'], cached_size)
                    #score = max(plan_inputs[i]['score'], score)
                    plan_size -= plan_inputs[i]['size']
                plan_size += cached_size
                plan_inputs[i] = {'size': cached_size}
            plan.jobs.append({'job': e, 'improvement': t_imprv_old + t_imprv})
        self.update_longest_jobs()
        plan.size = plan_size
        plan.data = plan_inputs
        plan.iscore = t_imprv 
        return plan, t_imprv
    
    def get_rcp_next_plan(self, priority):
        plan = Plan()
        plan.original_stage_id = self.stage_id
        plan.priority = priority
        plan.dag_id = self.dag_id
        plan.stage = self
        t_imprv, new_runtime = self.get_improvement()

        if t_imprv <= 0:
            return None, 0
        
        plan_inputs = {}
        plan_size = 1
        for e in self.longest_jobs:
            t_imprv_old = e.est_runtime_remote - e.runtime_partial_cached
            e.runtime_partial_cached = new_runtime
            if e.est_runtime_remote == e.est_runtime_cache:
                continue
            
            # FIXME: prepare inputs, this should represent the actual strides 
            for i in e.inputs:
                cached_size = math.ceil(e.inputs[i])
                if i in plan_inputs:
                    cached_size = max(plan_inputs[i]['size'], cached_size)
                    plan_size -= plan_inputs[i]['size']
                plan_size += cached_size
                plan_inputs[i] = {'size': cached_size}
            plan.jobs.append({'job': e, 'improvement': t_imprv_old + t_imprv})
        self.update_longest_jobs()
        plan.size = plan_size
        plan.data = plan_inputs
        plan.iscore = t_imprv 
        return plan, t_imprv

    
    def get_criticalpath_plan(self):
        plan = Plan()
        plan.original_stage_id = self.stage_id
        plan.dag_id = self.dag_id
        plan.stage = self
        t_imprv, new_runtime = self.get_improvement()

        if t_imprv <= 0:
            return None, 0
        
        plan_inputs = {}
        plan_size = 1
        for e in self.longest_jobs:
            e.improved_runtime = new_runtime
            for i in e.inputs:
                cached_size = math.ceil(e.inputs[i])
                if i in plan_inputs:
                    cached_size = max(plan_inputs[i]['size'], cached_size)
                    plan_size -= plan_inputs[i]['size']
                plan_size += cached_size
                plan_inputs[i] = {'size': cached_size}
            plan.jobs.append({'job': e, 'improvement': t_imprv})
        
        plan.size = plan_size
        plan.data = plan_inputs
        plan.iscore = t_imprv
        return plan, t_imprv


class Plan:
    def __init__(self, stg_id=-1):
        self.priority = -1 # -1 is the least priority, and 0 means highest priority
        self.orig_stage_id = stg_id # the stage_id of the plan 
        self.stage_id = stg_id
        self.assigned_stage_id = -1 # the stage_id that the prefetching of the plan should be started within that time frame.
        self.dag_id = -1 # dag_id 
        self.plan_id = uuid.uuid1()
        self.name = self.plan_id #FIXME: for now set the name to plan id, I may change it later
        self.size = 0
        self.iscore = 0 # improvement score
        self.pscore = 0 # should be iscore/size
        self.sscore = 0 # share score
        self.type = 0 # 0 means this is a cache plan and 1 means it is a prefetch plan
        self.wscore = 0 # weighted score
        self.status = 0 # status could be 0, and 1 where zero means unsatisfied and 1 means satisfied
        self.data = {}
        self.jobs = []
        self.stage = -1
        self.feasible = 1
        self.prefetchable = 0
        self.mrd_distance = 0

    def __str__(self):
        return 'DAG id: ' + str(self.dag_id) + ', stage id: ' + str(self.stage_id) + ', data' +  str(self.data)
    
    def __lt__(self, other):
        if self.wscore != other.wscore:
            return self.wscore < other.wscore
        if self.pscore != other.pscore:
            return self.pscore <  other.pscore
        if self.sscore != other.sscore:
            return self.sscore < other.sscore
        if self.iscore != other.iscore:
            return self.iscore < other.iscore
        return self.type > other.type
    
    def to_json(self):
        return str(self.data)
    
    def is_feasible(self):
        return self.feasible


class PlansContainer:
    def __init__(self, g):
        self.dag = g;
        self.id = 0;
        self.cp_by_stage = {} # cache plan by ...
        self.cp_by_priority = {}
        self.plans = []
        self.stages = {}
        self.cache_blocksize = 1

    def add_stage(self, s):
        ps_id = s.stage_id - 1 # previous stage id
        if ps_id in self.stages:
            s.start_time = self.stages[ps_id].end_time()
        self.stages[s.stage_id] = s

    def add_cache_plan(self, plan, s):
        sid = s.stage_id
        priority = plan.priority
        self.add_cache_plan_by_stage(plan, s)
        self.add_cache_plan_by_priority(plan, s)
        self.update_iscore(plan, s)
        self.plans.append(plan)

    def update_iscore(self, plan, s):
        last_plan_imprv = 0
        priority = plan.priority
        sid = s.stage_id
        if priority -1 in self.cp_by_stage[sid]:
            last_plan_imprv = self.cp_by_stage[sid][priority -1].iscore
        plan.iscore += last_plan_imprv
        if plan.size > 0:
            plan.pscore = plan.iscore/plan.size
        else:
            plan.pscore = -1

    def add_cache_plan_by_stage(self, plan, s):
        sid = s.stage_id
        priority = plan.priority 
        
        if sid not in self.cp_by_stage:
            self.cp_by_stage[sid] = {}
        if priority not in self.cp_by_stage[sid]:
            self.cp_by_stage[sid][priority] = {}
        self.cp_by_stage[sid][priority] = plan

    def add_cache_plan_by_priority(self, plan, s):
        sid = s.stage_id
        priority = plan.priority

        if priority not in self.cp_by_priority:
            self.cp_by_priority[priority] = {}
        if sid not in self.cp_by_priority[priority]:
            self.cp_by_priority[priority][sid] = {}
        self.cp_by_priority[priority][sid] = plan

        
    def get_cache_plans(self, stage):
        cache_plan = []
        if stage in self.cp_by_stage:
            for pp in self.cp_by_stage[stage]:
                cache_plan.append(self.cp_by_stage[stage][pp])
        return cache_plan
    
    def get_stage_cache_plans(self, stage):
        cache_plan = {}
        if stage in self.cp_by_stage:
            cache_plan = self.cp_by_stage[stage] 
        return cache_plan
    
    def get_stage_plans(self, stage):
        prefetch_plan = {} 
        cache_plan = {}
        if stage in self.cp_by_stage:
            cache_plan = self.cp_by_stage[stage] 
        if stage in self.pp_by_priority:
            prefetch_plan = self.pp_by_stage[stage]
        return cache_plan, prefetch_plan
    
    def get_plans(self, stage):
        cache_plan, prefetch_plan = self.get_stage_plans(stage)
        plans = []
        for stg in prefetch_plan:
            for pp in prefetch_plan[stg]:    
                plans.append(prefetch_plan[stg][pp])
        
        for cp in cache_plan: 
            plans.append(cache_plan[cp])
            
        for p in plans:
            plans.sort(reverse=True)
        return plans

