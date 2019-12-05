#!/usr/bin/python3
# Trevor Nogues, Mania Abdi

from utils.graph import *

# 3, 8, 

graphs = []

#Example 1
g = Graph(15) 
g.add_edge(0, 1, 4) 
g.add_edge(0, 2, 3) 
g.add_edge(0, 4,11)
g.add_edge(0,11, 1)
g.add_edge(11,13,1) 
g.add_edge(13,14,1) 
g.add_edge(1, 3, 7) 
g.add_edge(1, 4, 1) 
g.add_edge(2, 5, 9) 
g.add_edge(3, 6, 2)
g.add_edge(4, 6, 2)
g.add_edge(5, 4, 1)
g.add_edge(5, 6, 5)    
g.add_edge(7, 5, 5)
g.add_edge(7, 8, 9) 
g.add_edge(9, 8, 4)
g.add_edge(10,6, 6)
g.add_edge(10,8,12)
g.add_edge(10,12,5)
g.add_edge(10,13,4)
g.random_runtime()
graphs.append(g)

# Example 2
v = Graph(7)
v.add_edge(0, 3, 0)
v.add_edge(3, 5, 0)
v.add_edge(2, 4, 0)
v.add_edge(1, 6, 0)
v.add_edge(4, 6, 0)
v.add_edge(5, 6, 0)
v.static_runtime(0, 30, 22)
v.static_runtime(1, 23, 15)
v.static_runtime(2, 18, 11)
v.static_runtime(3, 20, 15)
v.static_runtime(4, 14, 10)
v.static_runtime(5, 16, 11)
v.static_runtime(6, 8, 6)
graphs.append(v)


# Example 3
h = Graph(10)
h.add_edge(0, 1, 1)
h.add_edge(0, 2, 2)
h.add_edge(0, 5, 3)
h.add_edge(1, 2, 0)
h.add_edge(1, 3, 8)
h.add_edge(2, 3, 4)
h.add_edge(2, 7, 5)
h.add_edge(3, 4, 2)
h.add_edge(6, 5, 3)
h.add_edge(7, 9, 2)
h.add_edge(8, 1, 7)
h.static_runtime(0, 7, 3)
h.static_runtime(1, 5, 4)
h.static_runtime(2, 6, 3)
h.static_runtime(3, 8, 5)
h.static_runtime(4, 5, 3)
h.static_runtime(5, 5, 2)
h.static_runtime(6, 7, 5)
h.static_runtime(7, 6, 4)
h.static_runtime(8, 8, 5)
h.static_runtime(9, 7, 3)
graphs.append(h)

# Example 3
i = Graph(9)
i.add_edge(0, 2, 1)
i.add_edge(0, 4, 1)
i.add_edge(0, 6, 1)
i.add_edge(2, 1, 1)
i.add_edge(4, 3, 1)
i.add_edge(1, 7, 1)
i.add_edge(1, 5, 2)
i.add_edge(3, 5, 3)
i.add_edge(8, 5, 1)
i.random_runtime()
graphs.append(i)


# Example 4
j = Graph(6)
j.add_edge(0, 1, 1)
j.add_edge(1, 2, 1)
j.add_edge(2, 3, 1)
j.add_edge(3, 4, 1)
j.add_edge(4, 5, 1)
j.random_runtime()
graphs.append(j)


# Example 5
k = Graph(6) 
k.add_edge(5, 2, 1); 
k.add_edge(5, 0, 1); 
k.add_edge(4, 0, 1); 
k.add_edge(4, 1, 1); 
k.add_edge(2, 3, 1); 
k.add_edge(3, 1, 1); 
k.random_runtime()
graphs.append(k)

multi_dags_test = []
# Example 2
j1 = Graph(4)
j1.add_edge(0, 2, 0)
j1.add_edge(1, 2, 0)
j1.add_edge(2, 3, 0)
j1.static_runtime(0, 10, 7)
j1.static_runtime(1, 14, 6)
j1.static_runtime(2, 9, 2)
j1.static_runtime(3, 8, 6)
j1.inputs = {0:['a'], 1:['b'], 2:['c'], 3:['a']}
j1.inputSize = {0 : [6], 1:[24], 2:[21], 3:[6]}
j1.outputSize = {0 : [1], 1:[1], 2:[1], 3:[1]}
multi_dags_test.append(j1)

# Example 2
j2 = Graph(3)
j2.add_edge(0, 2, 0)
j2.add_edge(1, 2, 0)
j2.static_runtime(0, 8, 6, 4)
j2.static_runtime(1, 9, 2, 3)
j2.static_runtime(2, 14, 10, 4)
j2.inputs = {0:['a'], 1:['c'], 2:['d']}
j2.inputSize = {0 : [6], 1:[21], 2:[4]}
j2.outputSize = {0 : [1], 1:[1], 2:[1]}
#multi_dags_test.append(j2)




