#!/usr/bin/python

import time
import operator

KARIZ = 0
MRD = 1
CP = 2


class PGEntry:
    def __init__(self):
        self.pg_nodes = {} # next: count
        self.total_access = 0
    
    def touch(self):
        self.total_access += 1
    
    def __str__(self):
        printstr = '{'
        for f in self.pg_nodes:
            printstr += (f + ':' + str(self.pg_nodes[f]) + ',')
        printstr += '}'
        return printstr
    
    def get_next_object(self):
        return max(self.pg_nodes.items(), key=operator.itemgetter(1))[0]
    
    def update_pg(self, f):
        if f not in self.pg_nodes:
            self.pg_nodes[f] = 0    
        self.pg_nodes[f]+=1

class Entry:
    """Cache Entry DataClass"""
    # this should support ranges
    def __init__(self, name, size=0, score=0):
        self.parent_id = 0
        self.name = name
        self.size = size
        self.score = score
        self.pscore = score/size if size else -1
        self.access_time = time.time()
        self.freq = 0
        self.refdags= set()
        self.sscore = 0 #share score
        self.pin = 0 # whether it is pin or not
        self.mrd_distance = -1
        self.policy = 0 # 0: KARIZ, 1: MRD, 2: CP
        self.lru_prev = None
        self.lru_next = None

    def __lt__(self, other):
        if self.policy == MRD:
            if self.mrd_distance < 0:
                return False; 
            if other.mrd_distance < 0:
                return True;
            return self.mrd_distance < self.mrd_distance
        return self.pscore < other.pscore

    def __eq__(self, other):
        if self.policy == MRD:
            return self.mrd_distance == self.mrd_distance
        return self.name == other.name

    def __ne__(self, other):
        if self.policy == MRD:
            return self.mrd_distance != self.mrd_distance
        return self.name != other.name

    def __str__(self):
        return self.name + ":" + str(self.size)
    
    def increment_freq(self):
        self.freq += 1
    
    def update_pscore(self, score):
        self.pscore = score if score > self.pscore else self.pscore
        
    def touch(self):
        self.access_time = time.time()
        
