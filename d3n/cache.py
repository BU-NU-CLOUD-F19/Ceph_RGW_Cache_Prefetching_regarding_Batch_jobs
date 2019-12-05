#!/usr/bin/python
# Author: Mania Abdi, Trevor Nogues
import random
import math
import entry
import worker as wr
import utils.status as status
import d3n_utils as d3n
import subprocess
from entry import Entry

_cache = None

num_workers=1
placement_policy=0 # replacement policy for block of a file: 0 -> roundrobin, 1 -> deterministic hash, 2 -> local aware  
ROUND_ROBIN=0
DETERMINISTIC_HASH=1
LOCALITY_AWARE=2
PREFETCH = 0
CACHE = 1

class Cache:
    def __init__(self, size=100):
        global _cache
        self.num_workers = 1
        self.placement_policy = 0
        self.size = size
        self.global_status = {}
        self.free_space = size
        self.used_space = 0
        self.lrr = 0
        self.mrd_table_by_name = {}
        self.mrd_table_bydistance = []
        self.block_size = 1#1048576 # 1024*1024 or 1M
        
        # create workers
        self.workers = {}
        self.workers_index = []
        worker_size = size//num_workers
        for i in range(0, num_workers):
            w = wr.Worker(worker_size)
            self.workers_index.append(w.id)
            self.workers[w.id] = w    
        _cache = self
        

    def __str__(self):
        printstr = 'global status: { '
        for f in self.global_status:
            e = self.global_status[f]
            printstr = printstr + ' ' + e.name + ": " + str(e.size) + ','
        printstr = printstr[:-1]
        printstr += '} \n Worker: ' 
        wid = self.get_worker()
        printstr += str(self.workers[wid])
        return printstr
    
    def unpin_files(self, pdata):
        for f in pdata['data']:
            if f in self.global_status:
                e = self.global_status[f]
                wid = self.get_worker() #e.parent_id
                self.workers[wid].unpin_file(e.name)
        return status.SUCCESS
                
    def prefetch_mrd(self, pdata):
        for distance in pdata:
            sdistance = pdata[distance]
            for filesmeta in sdistance:
                for f in filesmeta:
                    if f in self.global_status:
                        e = self.global_status[f]
                        wid = e.parent_id
                        e.mrd_distance = distance
                        self.global_status[f] = e     
                    else:        
                        data_sz = filesmeta[f]
                        wid = self.get_mrd_worker(data_sz)
                        e, evf, estatus = self.workers[wid].mrd_cache_file(f, filesmeta[f], distance)
                        if estatus == status.SUCCESS:
                            e.mrd_distance = distance
                            self.global_status[f] = e
                            if evf:
                                for ev in evf:
                                    self.mrd_table_bydistance.remove(ev)
                                    del self.global_status[ev.name]
                    
        self.update_mrd_table()
        print('MRD Prefetch plan:', pdata, ', global status:', self.global_status)
        return status.SUCCESS
             
             
    def cache_mrd(self, data):
        cached = {}
        for filesmeta in data:
            for f in filesmeta:
                if f in self.global_status:
                    e = self.global_status[f]
                    wid = self.get_worker() #e.parent_id
                    self.workers[wid].pin_file(f, e.size)
                    cached[e.name] = e.size
                #else:        
                #    data_sz = filesmeta[f]
                #    wid = self.get_mrd_worker(data_sz)
                #    e, evf, estatus = self.workers[wid].cache_file_mrd(f, filesmeta[f], 0)
                #    if estatus == status.SUCCESS:
                #        self.workers[wid].pin_file(f, e.size)
                #        self.global_status[f] = e 
                #        cached[e.name] = e.size
                #        if evf:
                #            for ev in evf:
                #                self.mrd_table_bydistance.remove(ev)
                #                del self.global_status[ev.name]
                    
        self.update_mrd_table()
        #print('MRD cache plan:', data, ', global status:', self.global_status)
        return cached
    
    
    def update_mrd_table(self):
        self.mrd_table_bydistance.sort(reverse=True)
    
    
    def get_mrd_worker(self, size):
        # FIXME
        # find the worker that has of available space 
        # or has largest mrd distance or has -1 for mrd distance
        return self.workers_index[0]


    def cache_file(self, fname, size, score):
        if fname in self.global_status:
            e = self.global_status[fname]
            wid = e.parent_id
            if e.size < size:
                return status.UNABLE_TO_CACHE
            e.touch()
            e.update_pscore(score)
            self.workers[wid].pin_file(fname, e.size)
            return status.SUCCESS
        return status.FILE_NOT_FOUND
                        
                        
    def cache_plan(self, data, score):
        revertables = {}
        for f in data:
            files = data[f]
            if f in self.global_status:
                e = self.global_status[f]
                wid = self.get_worker() # e.parent_id
                if e.size < files['size']:
                    for f2 in data:
                        self.workers[wid].unpin_file(f2, data[f2]['size'])
                    return status.UNABLE_TO_CACHE
                self.workers[wid].pin_file(f, e.size)
            else:
                wid = self.get_worker()
                for f2 in data:
                    self.workers[wid].unpin_file(f2)
                return status.UNABLE_TO_CACHE
        
        for f in data:
            e = self.global_status[f]
            e.touch()
            e.increment_freq()
            e.update_pscore(score)
        
        return status.SUCCESS
        
    def clean_up(self, revertible):
        wid = self.get_worker() #e.parent_id
        print('revertibles are', revertible)
        for r in revertible:
            self.workers[wid].kariz_revert_status(r, revertible[r]['osize'])
            if not revertible[r]['osize']:
                del self.global_status[r]
            else:
                self.global_status[r].size = revertible[r]['osize']    
        
    def prefetch_plan(self, data, score):
        evicted = []
        revertible = {}
        token = d3n.get_token()
        for f in data:
            fd = data[f]
            if f in self.global_status:
                e = self.global_status[f]
                wid = self.get_worker() #e.parent_id
                if e.size < fd['size']:
                    d3n.prefetch_object(token, 'test', f, 0, fd['size'])
            else:
                if 's3a' in f: 
                   bucket_name = f.split('/')[-2]
                   url = f.replace(f.split('/')[-1], '').replace('s3a', 's3')
                   p = subprocess.Popen(["s3cmd", "ls", url], stdout=subprocess.PIPE)
                   ls_output = p.communicate()[0].decode("utf-8").split('\n')
                   for o in ls_output:
                      ol = o.split(' ')
                      if len(ol) <= 3:
                         break
                      ol.remove('')
                      print(ol)
                      object_name = ol[5].split('/')[-1]
                      d3n.prefetch_object(token, bucket_name, object_name, 0, int(ol[2]))
                      e = entry.Entry(name = bucket_name + '/' + object_name,  size=int(ol[2])/(1024*1024))
                      self.global_status[bucket_name + '/' + object_name] = e
            
        return status.SUCCESS

    def evict(self, fname):
        if fname in self.global_status:
            e = self.global_status[fname]
            size = e.size
            wid = self.get_worker() # FIXME e.parent_id
            self.workers[wid].evict_file(fname)
            del self.global_status[fname]
            self.free_space -= size
            self.used_space = self.size - self.free_space
            
    def clear_cache(self):
        wid = self.get_worker() # FIXME e.parent_id
        self.workers[wid].clear_cache()
        self.global_status.clear()
            
    def status(self):   
        return self.cache, self.free_space, self.size


    def sort(self):
        self.cache.sort(reverse = True)

    def get_item(self, i):
        return self.cache[i]
    
    def get_worker(self):
        if self.placement_policy == ROUND_ROBIN:
            return self.workers_index[0]
        else:
            return self.workers_index[0]
        
    def lru_cache_file(self, fname, size, worker=0):
        wid = self.get_worker()
        e, evicted, pstatus = self.workers[wid].lru_cache_file(fname, size)
        if not e:
            return
        if pstatus in [status.SUCCESS, status.LRU_UPDATED]:
            if evicted:
                [self.evict(evt.name) for evt in evicted] 
            self.global_status[fname] = e
            self.free_space -= e.size
            self.used_space = self.size - self.free_space 
    
    def get_blocks_count(self, size):
        if size < self.block_size:
            return 1
        return int(math.ceil(size/self.block_size))
    
    def is_plancached(self, data):
        for fname in data:
            size = self.get_blocks_count(data[fname]['size'])
            if fname not in self.global_status:
                return 0
                self.lru_cache_file(fname, size)
                return 0    
                        
            e = self.global_status[fname]
            if e.size < size:
                return 0
                self.evict(fname)
                self.lru_cache_file(fname, size)
                return 0
        return 1
        
