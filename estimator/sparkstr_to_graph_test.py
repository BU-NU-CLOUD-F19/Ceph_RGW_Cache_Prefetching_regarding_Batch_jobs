import uuid


#Class to represent a graph
# class Graph:
#     def __init__(self, n_vertices = 0, type = Type.complex):
#         self.dag_id = uuid.uuid1()
#         self.n_vertices = n_vertices
#         self.jobs = {}
#         for i in range(0, n_vertices):
#             self.jobs[i] = jb.Job(i)
#
#         self.misestimated_jobs = np.zeros(2*n_vertices)
#
#         self.roots = set(range(0, n_vertices))
#         self.leaves = set(range(0, n_vertices))
#         self.blevels = {}
#
#         self.mse_factor = 0
#         self.plans_container = None
#         self.stages = {}
#         self.name = 'graph'
#         self.category = type
#
#     def reset(self):
#         for j in self.jobs:
#             job = self.jobs[j]
#             job.reset()
#         self.stages = {}
#         self.plans_container = None
#         self.blevels = {}
#
#     def __str__(self):
#         graph_str = '{ "jobs": ['
#         for j in self.jobs:
#             graph_str += str(self.jobs[j])
#             graph_str += ','
#         graph_str = graph_str[:-1]
#         graph_str = graph_str  + '], "uuid": "' + str(self.dag_id)
#         graph_str = graph_str  + '", "n_vertices" : ' + str(self.n_vertices) + ', "mse_factor" : ' + str(self.mse_factor) + ', "name" : "' + str(self.name) + '"}'
#         return graph_str
#
#     def add_new_job(self, value):
#         self.jobs[self.n_vertices] = jb.Job(self.n_vertices)
#         self.n_vertices+= 1
#
#     def set_misestimated_jobs(self, mse_jobs):
#         for i in range(0, self.n_vertices):
#             self.jobs[i].set_misestimation(mse_jobs[i], mse_jobs[i + self.n_vertices])
#
#     def config_misestimated_jobs(self): # mse_factor: miss estimation factor
#         for i in range(0, self.n_vertices):
#             self.jobs[i].config_misestimated_runtimes(self.mse_factor)
#
#     def set_misestimation_error(self, mse_factor):
#         self.mse_factor = mse_factor;
#
#     # Randomly assign time value to each node
#     def random_runtime(self):
#         for i in range(0, self.n_vertices):
#             self.jobs[i].random_runtime(1, 10)
#
#     def static_runtime(self, v, runtime_remote, runtime_cache):
#         self.jobs[v].static_runtime(runtime_remote, runtime_cache)
#
#     def config_ntasks(self, v, n_tasks):
#         self.jobs[v].config_ntasks(n_tasks)
#
#     def config_inputs(self, v, inputs):
#         self.jobs[v].config_inputs(inputs)
#
#     def add_edge(self, src, dest, distance = 0):
#         if src not in self.jobs:
#             self.add_new_job(src)
#         if dest not in self.jobs:
#             self.add_new_job(dest)
#         self.jobs[src].add_child(dest, distance)
#         if src in self.leaves:
#             self.leaves.remove(src)
#             self.jobs[src].blevel = -1
#
#         self.jobs[dest].add_parent(src, distance)
#         if dest in self.roots:
#             self.roots.remove(dest)
#             self.jobs[src].tlevel = -1
#
#     def bfs(self, s = 0):
#         visited = [False]*(self.n_vertices)
#         bfs_order = []
#         queue = list(self.roots)
#         for r in self.roots:
#             visited[r] = True
#
#         while queue:
#             s = queue.pop(0)
#             print (self.jobs[s].id, end = " ")
#             bfs_order.append(s)
#
#             for i in self.jobs[s].children:
#                 if visited[i] == False:
#                     queue.append(i)
#                     visited[i] = True
#
#     def blevel(self):
#         if self.blevels:
#             return self.blevels
#         cur_lvl = 0
#         visited = [False]*self.n_vertices
#         self.blevels[cur_lvl] = list(self.leaves)
#         queue = list(self.leaves)
#         for v in self.leaves:
#             visited[v] = True
#             queue.extend(self.jobs[v].parents.keys())
#
#         while queue:
#             s = queue.pop(0)
#             if visited[s] : continue
#
#             max_children_blvl = -1
#             for child in self.jobs[s].children:
#                 if self.jobs[child].blevel == -1:
#                     max_children_blvl = -1
#                     queue.append(s)
#                     break
#
#                 if self.jobs[child].blevel > max_children_blvl:
#                     max_children_blvl = self.jobs[child].blevel
#             if max_children_blvl != -1:
#                 self.jobs[s].blevel = max_children_blvl + 1
#                 visited[s] = True
#                 if self.jobs[s].blevel not in self.blevels: self.blevels[self.jobs[s].blevel] = []
#                 self.blevels[self.jobs[s].blevel].append(s)
#                 queue.extend(self.jobs[s].parents.keys())
#
#         return self.blevels
#
#
#     # A recursive function used by topologicalSort
#     def topologicalSortUtil(self,v,visited,stack):
#
#         # Mark the current node as visited.
#         visited[v] = True
#
#         # Recur for all the vertices adjacent to this vertex
#         for i in self.graph[v]:
#             if visited[i[0]] == False:
#                 self.topologicalSortUtil(i[0],visited,stack)
#
#         # Push current vertex to stack which stores result
#         stack.insert(0,v)
#
#     # The function to do Topological Sort. It uses recursive
#     # topologicalSortUtil()
#     def topologicalSort(self):
#         # Mark all the vertices as not visited
#         visited = [False]*self.V
#         stack =[]
#
#         # Call the recursive helper function to store Topological
#         # Sort starting from all vertices one by one
#         for i in range(self.V):
#             if visited[i] == False:
#                 self.topologicalSortUtil(i,visited,stack)
#
#         # Return contents of the stack
#         return stack
#
#
#         # Helper to update tLevel() contents
#     # Same as bLevelHelper()
#     def tLevelHelper(self, revGraphCopy, deleted, levels, count):
#         checked = [True]*self.V
#         for c in range(len(deleted)):
#             if deleted[c] == False and revGraphCopy[c] == []:
#                 checked[c] = False
#
#         for i in range(len(checked)):
#             if checked[i] == False:
#                 deleted[i] = True
#                 count -= 1
#                 for node in range(self.V):
#                     for subnode in revGraphCopy[node]:
#                         if subnode[0] == i:
#                             revGraphCopy[node].remove(subnode)
#
#         # print(count, revGraphCopy)
#         return count
#
#     # Find t-level of DAG
#     def tLevel(self):
#         # "Reverse" the graph, then use code for finding b-level
#         revGraphCopy = self.revGraph()
#         levels = [0]*self.V
#         deleted = [False]*self.V
#         count = self.V
#         while count > 0:
#             count = self.tLevelHelper(revGraphCopy,deleted,levels,count)
#             for i in range(len(deleted)):
#                 if deleted[i] == False:
#                     levels[i] += 1
#         return levels
#
#     def update_runtime(self, plan):
#         for j in plan.jobs:
#             t_imprv = 0
#             for f in plan.data:
#                 if f in j['job'].inputs:
#                     t_imprv_tmp = int(plan.data[f]['size']*(j['job'].runtime_remote - j['job'].runtime_cache)/j['job'].inputs[f])
#                     if t_imprv_tmp > t_imprv:
#                         t_imprv = t_imprv_tmp
#             j['job'].final_runtime = j['job'].runtime_remote - t_imprv #j['improvement']
#             #j['job'].est_runtime_remote = j['job'].runtime_remote - j['improvement']


def sparkstr_to_graph(raw_execplan, objectstore):
    ls = raw_execplan.split("\n")
    vertices= {}
    functions = []
    outputs = []
    inputs = []
    for i in reversed(range(len(ls))):
        print("--------RAW",ls[i])
        line = ls[i].split('at')
        if len(line) ==3:
            functions.append(line[1].strip())
            io = line[0].split(' ')
            outputs.append(io[-2])
            if not io[-3].startswith('(') and not io[-3].startswith("+") and not io[-3].startswith("|"):
                inputs.append(io[-3])
            else:
                inputs.append('')

    print(functions,outputs,inputs)
    graph = {}
    for i in range(len(functions)):
        if functions[i] not in graph.keys():
            graph[functions[i]] = {}
        graph[functions[i]][outputs[i]] = [inputs[i]]
        if i>0:
            graph[functions[i]][outputs[i]] += [outputs[i-1]]
    print(graph)


def pigstr_to_graph(raw_execplan, objectstore):
    print("Start")
    ls = raw_execplan.split("\n")
    start_new_job = False
    v_index = -1
    vertices= {}
    vertices_size = {}
    dag_id = ""
    for x in ls:

        if x.startswith('DAG'):
            dag_id = x.split(':')[1]
            print(dag_id)
            dag_id.replace('\'', '')

        if x.find("#"):
            continue

        if x.find("MapReduce node:"):

            v_index = v_index + 1
            start_new_job = True
            vertices[v_index] = {}

        if x.find("Store") != -1:

            result = x.split('(')[1].split(')')[0]
            extra = result.split(":")[-1]
            results = result.replace(":" + extra, "")
            if 'output' not in vertices[v_index]:
                vertices[v_index]['output'] = {}

            outputs = results.split(',')
            for o in outputs:
                print(outputs)
                dataset_size, obj_name = objectstore.get_datasetsize_from_url(o)
                vertices[v_index]['output'][obj_name] = dataset_size

        if x.find("Load") != -1:
            result = x.split('(')[1].split(')')[0]
            extra = result.split(":")[-1]
            inputs =  result.replace(":" + extra, "")
            inputs = inputs.split(',')
            if 'inputs' not in vertices[v_index]:
                vertices[v_index]['inputs'] = {}
            for i in inputs:
               dataset_size, obj_name = objectstore.get_datasetsize_from_url(i)
               vertices[v_index]['inputs'][obj_name] = dataset_size

        if x.find("Quantile file") != -1:
            result = x.split('{')[1].split('}')[0]
            if 'inputs' not in vertices[v_index]:
                vertices[v_index]['inputs'] = {}
            inputs = result.split(',')
            for i in inputs:
                dataset_size, obj_name = objectstore.get_datasetsize_from_url(i)
                vertices[v_index]['inputs'][obj_name] = dataset_size

    # g = Graph(len(vertices))
    # g.dag_id = dag_id
    # for v1 in vertices:
    #     for v2 in vertices:
    #         if v1 == v2: # and len(vertices) != 1:
    #             g.add_new_job(v1)
    #
    #         g.config_inputs(v1, vertices[v1]['inputs'])
    #
    #         for i in vertices[v1]['inputs']:
    #             if i in vertices[v2]['output']:
    #                 g.add_edge(v2, v1, 0)
    #print(str(g))
    return vertices


if __name__ == "__main__":
    raw_execplan = '''
    (2) ShuffledRDD[21] at reduceByKey at <console>:24 []
    +-(2) MapPartitionsRDD[20] at map at <console>:24 []
        |  MapPartitionsRDD[19] at flatMap at <console>:24 []
        |  README.md MapPartitionsRDD[18] at textFile at <console>:24 []
        |  README.md HadoopRDD[17] at textFile at <console>:24 []
    '''
    raw_execplan_pig = '''DAG:'eaf51b30-f457-4bc7-97a7-c3462698cd73'
#--------------------------------------------------
# Map Reduce Plan
#--------------------------------------------------
MapReduce node: {scope-107
Map Plan: {
b: Store(/pigmix1/pigmix_power_users_samples:PigStorage('')) - scope-106
{b: Filter[bag] - scope-102
{{Less Than[boolean] - scope-105
{{{POUserFunc(org.apache.pig.builtin.RANDOM)[double] - scope-103
{{{Constant(0.5) - scope-104
{{a: New For Each(false,false,false,false,false,false)[bag] - scope-101
{{{Project[bytearray][0] - scope-89
{{{Project[bytearray][1] - scope-91
{{{Project[bytearray][2] - scope-93
{{{Project[bytearray][3] - scope-95
{{{Project[bytearray][4] - scope-97
{{{Project[bytearray][5] - scope-99
{{{a: Load(/pigmix1/pigmix_power_users:PigStorage('')) - scope-88}
Global sort: {false}
}
'''
    objectstore = 0
    res = sparkstr_to_graph(raw_execplan,0)
    # print(pigstr_to_graph(raw_execplan_pig,0))
