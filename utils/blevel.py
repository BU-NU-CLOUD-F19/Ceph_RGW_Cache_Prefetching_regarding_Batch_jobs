#ssaw#Python program to print topological sorting of a DAG 
from collections import defaultdict 
  
#Class to represent a graph 
class Graph: 
    def __init__(self, vertices):
        self.nodes = set()
        self.edges = {}
        self.distances = {}
        self.V = vertices
        self.graph = defaultdict(list)
        self.inDegree = [0]*self.V
        self.outDegree = [0]*self.V

    def add_node(self, value):
        self.nodes.add(value)

    def add_edge(self, from_node, to_node, distance):
        if from_node not in self.nodes:
            self.add_node(from_node)
        if to_node not in self.nodes:
            self.add_node(to_node)
        self._add_edge(from_node, to_node, distance)
        # self._add_edge(to_node, from_node, distance)
        self.graph[from_node].append((to_node,distance)) 
        self.inDegree[to_node] += 1
        self.outDegree[from_node] += 1

    def _add_edge(self, from_node, to_node, distance):
        self.edges.setdefault(from_node, [])
        self.edges[from_node].append(to_node)
        self.distances[(from_node, to_node)] = distance

    # A recursive function used by topologicalSort 
    def topologicalSortUtil(self,v,visited,stack): 
  
        # Mark the current node as visited. 
        visited[v] = True
  
        # Recur for all the vertices adjacent to this vertex 
        for i in self.graph[v]: 
            if visited[i[0]] == False: 
                self.topologicalSortUtil(i[0],visited,stack) 
  
        # Push current vertex to stack which stores result 
        stack.insert(0,v) 
  
    # The function to do Topological Sort. It uses recursive  
    # topologicalSortUtil() 
    def topologicalSort(self): 
        # Mark all the vertices as not visited 
        visited = [False]*self.V 
        stack =[] 
  
        # Call the recursive helper function to store Topological 
        # Sort starting from all vertices one by one 
        for i in range(self.V): 
            if visited[i] == False: 
                self.topologicalSortUtil(i,visited,stack) 
  
        # Return contents of the stack 
        return stack

    def bLevel(self, vertex, checked):
        revSort = self.topologicalSort()
        revSort.reverse()
        checked[vertex] = True 
        # print(revSort)
        levels = []
        for node in revSort:
            maxVal = 0
            print("self.graph[node]", self.graph[node])
            for child in self.graph[node]:
                if checked[child[0]] == False:
                    checkVal = 1 + self.bLevel(child[0],checked)
                    if checkVal > maxVal:
                        maxVal = checkVal
                        levels[child[0]] = maxVal
            print("bLevel for ", vertex, "is ", maxVal)
        return levels
    
    # def deleteOutNodes(graph):
    #     for node in range(self.V):
    #         if self.outDegree[node] == 0

    def bLevel2Helper(self, graphCopy, deleted,levels):
        for i in range(len(deleted)):
            if deleted[i] == False:
                if i not in graphCopy:
                    deleted[i] = True
                    for j in range(len(graphCopy)):
                        for subnode in graphCopy[j]:
                            if subnode[0] == i:
                                graphCopy[j].remove(subnode)
                                print(len(graphCopy))

                        for k in range(self.V):                                                                       
                            if graphCopy[k] == []:
                                levels[k] += 1
                                deleted[k] = True
                                del graphCopy[k]

        # print(graphCopy)
        # print(graphCopy)

    def bLevel2(self):
        levels = [0]*self.V
        deleted = [False]*self.V
        graphCopy = self.graph
        # print(len(graphCopy))
        while len(graphCopy) != 0:
            self.bLevel2Helper(graphCopy,deleted,levels)
            # print(deleted)
            for i in range(len(deleted)):
                # if graphCopy[i] == []:
                #         levels[i] += 1
                #         del graphCopy[i]
                if deleted[i] == False:
                    levels[i] += 1
                    # print("add to ", i)
            # print(levels)
        print(levels)

                    


# Tests

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
print("\nResult for graph g: ")
# g.topologicalSort()

# Example 2
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
print("\nResult for graph h: ")
# h.topologicalSort()

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
print("\nResult for graph i: ")
# i.topologicalSort()
# i.bLevel(0)
# print(i.graph[0])
i.bLevel2()


# Example 4
j = Graph(6)
j.add_edge(0, 1, 1)
j.add_edge(1, 2, 1)
j.add_edge(2, 3, 1)
j.add_edge(3, 4, 1)
j.add_edge(4, 5, 1)
print("\nResult for graph j: ")
# j.topologicalSort()
# checked = [False]*j.V
# j.bLevel2()

# Example 5
k = Graph(6) 
k.add_edge(5, 2, 1); 
k.add_edge(5, 0, 1); 
k.add_edge(4, 0, 1); 
k.add_edge(4, 1, 1); 
k.add_edge(2, 3, 1); 
k.add_edge(3, 1, 1); 
print("\nResult for graph k: ")
k.topologicalSort()
