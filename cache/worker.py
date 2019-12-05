#!/usr/bin/python
# Author: Mania Abdi, Trevor Nogues

import entry
import uuid
import utils.status as status
from entry import Entry
'''
NOTES: 
    - On eviction we cannot evict partially, because we dont know future strides
    - On pin/unpin we cannot pin partially. but we should support it in the future 
'''

#FIXME: You made a mistake here. When you are caching you should only potentially report the files 
# that should be cache and if you can cache the entire plan you should apply your modifications
# just remmenber to do checkpointing or someting or do something that is revertable 

PREFETCH = 0
CACHE = 1


class Worker:
    def __init__(self, size):
        self.id = uuid.uuid1()
        self.size = size
        self.status = {}
        self.pinned_files = {}
        
        self.used_space = 0 # the amount of cache that is currently used by both pinned and unpinned files
        self.free_space = size # size - self.used_space 

        self.pinned_space = 0 # the amount of cache that is currently used by both pinned files
        self.unpinned_space = size # available space: size - pinned_file_size
        self.mrd_table = {}
        self.mrd_table_bydistance = []
        
        self.kariz_pscore_table = []
        
        # lru related variables
        self.lru_head = None
        self.lru_end = None
        self.lru_list = {}
    
    def __str__(self):
        worker_str = 'free space: ' + str(self.free_space) + ', unpinned space: ' +  str(self.unpinned_space) + ', { '  
        for e in self.status:
            worker_str += e + ': ' + str(self.status[e].size) + ','
        worker_str = worker_str[:-1]
        worker_str += '}'
        return worker_str

    def get_lru_head_fname(self):
        return self.lru_head.name if self.lru_head else '' 


    ###### General functionality #################################    
    def addto_free_space(self, size):
        self.used_space -= size
        self.free_space = self.size - self.used_space 

    def addto_used_space(self, size):
        self.used_space += size
        self.free_space = self.size - self.used_space         

    
    def pin_file(self, fname, size):
        ''' if you already pinned fname you don't need to take any action'''
        revertible = {}
        if fname not in self.status:
            return status.FILE_NOT_FOUND 

        e = self.status[fname]
        if e.size < size:
            return status.NOT_ENOUGH_TO_PIN 
        
        cur_pinned_size = self.pinned_files[fname] if fname in self.pinned_files else 0
        if cur_pinned_size < size:
            revertible = {'osize': cur_pinned_size, 'newsize': size}
            self.pinned_files[fname] = size;
            e.pin = 1
            
        self.pinned_space = sum(self.pinned_files.values())  
        self.unpinned_space = self.size - self.pinned_space
                    
        return status.SUCCESS, revertible
        
        
    def unpin_file(self, fname, size):
        if fname not in self.status or fname not in self.pinned_files:
            self.pinned_space = sum(self.pinned_files.values())  
            self.unpinned_space = self.size - self.pinned_space
            return status.FILE_NOT_FOUND
        
        if size < self.pinned_files[fname]:
            self.pinned_space = sum(self.pinned_files.values())  
            self.unpinned_space = self.size - self.pinned_space 
            return status.FILE_IS_BUSY
        
        e = self.status[fname]
        e.pin = 0
        e.pscore = 0
        
        del self.pinned_files[fname]
        self.pinned_space = sum(self.pinned_files.values())  
        self.unpinned_space = self.size - self.pinned_space
            
    
    def evict_file(self, fname):
        if fname not in self.status:
            return status.FILE_NOT_FOUND
        e = self.status[fname]
        if e.pin:
            return status.EVICT_PINNED_FILE # we cannot evict a pinned file 
        del self.status[fname]
        self.addto_free_space(e.size)
        if e in self.mrd_table_bydistance:
            self.mrd_table_bydistance.remove(e)
        self.lru_remove(e)
        return e
    
        
    ###### MRDs #################################
    '''FIXME: this function should be improved'''
    def mrd_eviction_candidates(self, size, distance=0):
        candidates = []
        candidate_size = 0

        # FIXME: Do the search using red black tree
        self.mrd_table_bydistance.sort(reverse=True);
        for e in self.mrd_table_bydistance:
            if e.mrd_distance == -1:
                candidates.append(e)
                candidate_size += e.size
            elif int(distance) < int(e.mrd_distance):
                candidates.append(e)
                candidate_size += e.size
            if size <= candidate_size:
                break
        return candidates, candidate_size


    def mrd_free(self, size, distance=0):
        # FIXME: This function loops over candidates. It should first sort them by score before evicting them 
        evicted_size = 0
        evicted = {}

        if size <= 0:
            return evicted_size, evicted, status.SUCCESS

        # FIXME sort cache by score and remove those with smallest one
        candidates, candidate_size = self.mrd_eviction_candidates(size, distance)
        if candidate_size < size:
            return 0, None, status.NO_SPACE_LEFT

        for e in candidates:
            evicted_size += e.size
            self.evict(e)
            evicted[e.name] = e
            if size <= evicted_size:
                self.free_space -= evicted_size
                return evicted_size, evicted, status.SUCCESS;
            
    def mrd_update_rf_table(self):
        self.mrd_table_bydistance.sort(reverse=True)
        
    #return codes: 0 successful, -2 no space left 
    def mrd_cache_file(self, fname, size, distance):
        evicted = {}
        if fname in self.status:
            e = self.status[fname]
        else:
            e = entry.Entry(fname)
            e.parent_id = self.id

        if self.unpinned_space < size: # cannot cache the file /w size on this worker
            return 0, None, status.NO_SPACE_LEFT
        elif self.free_space < size:
            evicted_size, evicted, estatus = self.mrd_free(size - e.size, distance)
            if  evicted_size < size:
                return None, None, estatus
        #update sizes here before update the cache
        e.size = size
        e.mrd_distance = distance
        e.touch()
        self.status[fname] = e 
        self.used_space += e.size 
        self.free_space = self.size - self.used_space
        self.mrd_table_bydistance.append(e)
        self.mrd_update_rf_table()
        return e, evicted, status.SUCCESS
        
    ###### KARIZs #################################
    '''FIXME: this function should be improved'''
    def kariz_eviction_candidates(self, size, score=0):
        candidates = {}
        candidate_size = 0
        sorted_cache = sorted((e.pscore, k) for (k,e) in self.status.items())
        while len(sorted_cache) > 0:
            f = sorted_cache.pop(0)[1]
            e = self.status[f]
            if score < e.pscore: break
            if e.pin == 0:
                candidates[e.name] = e
                candidate_size += e.size
            if size <= (self.free_space + candidate_size) : break
        return candidates, candidate_size + self.free_space 
    
    
    def kariz_free(self, size, score=0):
        # FIXME: This function loops over candidates. It should first sort them by score before evicting them 
        evicted_size = self.free_space
        evicted = []
        
        if size <= 0:
            return evicted_size, evicted, status.NOTHIN_TO_CACHE
        
        # FIXME sort cache by pscore and remove those with smallest one
        candidates, candidate_size = self.kariz_eviction_candidates(size, score)
        if candidate_size < size:
            return evicted_size, evicted, status.NO_SPACE_LEFT
        
        for c in candidates:
            e = candidates[c]
            evicted_size += e.size
            self.evict_file(c)
            evicted.append(c)
            if size <= evicted_size:
                return evicted_size, evicted, status.SUCCESS; 
        
    #return codes: 0 successful, -2 no space left 
    def kariz_cache_file(self, fname, size, score=0):
        evicted = []
        if fname in self.status:
            e = self.status[fname]
        else:
            e = entry.Entry(fname)
            e.parent_id = self.id
        
        if e.size > size:
            return e, evicted, status.SUCCESS
        
        if self.unpinned_space < (size - e.size): # cannot cache the file /w size on this worker
            return None, evicted, status.NO_SPACE_LEFT
        
        if self.free_space < (size - e.size):
            evicted_size, evicted, estatus = self.kariz_free((size - e.size), score)
            if  estatus != status.SUCCESS:
                return None, evicted, estatus
        oldsize = e.size
        e.size = size
        self.addto_used_space(size - oldsize)
        self.status[fname] = e
#        self.pin_file(fname, size - oldsize)
        return e, evicted, status.SUCCESS
        
    ###### LRUs #################################
    def lru_set_head(self, entry):
        if not self.lru_head:
            self.lru_head = entry
            self.lru_end = entry
        else:
            entry.lru_prev = self.lru_head
            self.lru_head.lru_next = entry
            self.lru_head = entry        

        
    def lru_touch_file(self, fname):
        if fname not in self.status:
            return -1
        e = self.status[fname]
        if self.lru_head and self.lru_head == e:
            return 0
        self.lru_remove(e)
        self.lru_set_head(e)
        return 0
    
    def lru_remove(self, entry):
        if not self.lru_head: # if cache is empty
            return None
        
        if entry.lru_prev:
            entry.lru_prev.lru_next = entry.lru_next
        if entry.lru_next:
            entry.lru_next.lru_prev = entry.lru_prev

        # head = end = node
        if not entry.lru_next and not entry.lru_prev:
            self.lru_head = None
            self.lru_end = None
            return entry

        if self.lru_end == entry:
            self.lru_end = entry.lru_next
            self.lru_end.lru_prev = None
        return entry
    
    def lru_evict(self, size):
        evicted = []
        while self.free_space < size:
            if self.lru_end is None:
                break
            e = self.lru_end
            self.evict_file(e.name)
            evicted.append(e)
        return evicted
    
    def lru_cache_file(self, fname, size):
        if fname in self.status:
            self.lru_touch_file(fname)
            return None, None, status.LRU_UPDATED 
        
        if self.unpinned_space < size: # we cannot evict from this
            return None, None, status.NO_SPACE_LEFT
        
        # free up some space
        evicted = None 
        if self.free_space < size: # evict some to free up some space
            evicted = self.lru_evict(size - self.free_space)
            
        e = Entry(name = fname, size = size)
        self.status[fname] = e
        self.addto_used_space(size)
        self.lru_set_head(e)        
        return e, evicted, status.SUCCESS
    
    def kariz_revert_status(self, fname, oldsize):
        if fname not in self.status:
            return
        e = self.status[fname]        
        diff = e.size - oldsize
        if diff <= 0:
            return

        #self.unpin_file(fname, diff)
        if not oldsize:
            self.evict_file(fname)
            return
        e.size = oldsize
        self.addto_free_space(diff)
        
    
    def clear_cache(self):
        self.status.clear()
        
        self.used_space = 0 # the amount of cache that is currently used by both pinned and unpinned files
        self.free_space = self.size # size - self.used_space 

        self.pinned_space = 0 # the amount of cache that is currently used by both pinned files
        self.unpinned_space = self.size # available space: size - pinned_file_size
        self.mrd_table = {}
        self.mrd_table_bydistance = []
        
        # lru related variables
        self.lru_head = None
        self.lru_end = None
        self.lru_list = {}
