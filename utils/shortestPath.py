# Author: Trevor Nogues

# dijkstra's algorithm + shortest path 

from collections import defaultdict 
<<<<<<< HEAD
import time
import sys

# startTime = time.time()
=======
>>>>>>> 42ebd5d5fc08a257c18c9ca10dbfe4ad28051618

"""
Dijkstra Algorithm
1. Assign to every node a distance value. Set it to zero for our initial node
   and to infinity for all other nodes.
2. Mark all nodes as unvisited. Set initial node as current.
3. For current node, consider all its unvisited neighbors and calculate their
   tentative distance (from the initial node). For example, if current node
   (A) has distance of 6, and an edge connecting it with another node (B)
   is 2, the distance to B through A will be 6+2=8. If this distance is less
   than the previously recorded distance (infinity in the beginning, zero
   for the initial node), overwrite the distance.
4. When we are done considering all neighbors of the current node, mark it as
   visited. A visited node will not be checked ever again; its distance
   recorded now is final and minimal.
5. If all nodes have been visited, finish. Otherwise, set the unvisited node
   with the smallest distance (from the initial node) as the next "current
   node" and continue from step 3.
 - source: wikipedia http://en.wikipedia.org/wiki/Dijkstra%27s_algorithm
"""


class Graph:
    """
    A simple undirected, weighted graph
    """

    def __init__(self, vertices):
        self.nodes = set()
        self.edges = {}
        self.distances = {}
        self.V = vertices
        self.graph = defaultdict(list)
<<<<<<< HEAD
        self.inDegree = [0]*self.V
        self.outDegree = [0]*self.V
=======
>>>>>>> 42ebd5d5fc08a257c18c9ca10dbfe4ad28051618

    def add_node(self, value):
        self.nodes.add(value)

    def add_edge(self, from_node, to_node, distance):
        if from_node not in self.nodes:
            self.add_node(from_node)
        if to_node not in self.nodes:
            self.add_node(to_node)
        self._add_edge(from_node, to_node, distance)
<<<<<<< HEAD
        # self._add_edge(to_node, from_node, distance)
        self.graph[from_node].append((to_node,distance)) 
        self.inDegree[to_node] += 1
        self.outDegree[from_node] += 1
=======
        self._add_edge(to_node, from_node, distance)
        self.graph[from_node].append((to_node,distance)) 
>>>>>>> 42ebd5d5fc08a257c18c9ca10dbfe4ad28051618

    def _add_edge(self, from_node, to_node, distance):
        self.edges.setdefault(from_node, [])
        self.edges[from_node].append(to_node)
        self.distances[(from_node, to_node)] = distance

<<<<<<< HEAD
    # def criticalNodes(self): 
    #         start = []
    #         # end = []

    #         # Find all nodes with parent nodes
    #         children = []
    #         for node in self.graph:
    #             #print("nodes in graph: ", node)
    #             for subNode in self.graph[node]:
    #                 #print("subnodes: ", subNode[0], "weight: ", subNode[1])
    #                 if subNode[0] not in children:
    #                     children.append(subNode[0])
            
    #         # All non-children have no input (start)
    #         for node in self.graph:
    #             if node not in children:
    #                 start.append(node)

    #         # # Find nodes that have no output (end)
    #         # for i in range(self.V):
    #         #     if i not in self.graph:
    #         #         end.append(i)
    
    #         return start

    # def topologicalHelper(self, v, checked, stack):
    #         # Value has now been checked
    #         checked[v] == True
            
    #         if v in self.graph.keys():
    #             for node,weight in self.graph[v]:
    #                 if checked[node] == False:
    #                     self.topologicalHelper(node, checked, stack)
    #         stack.append(v)

    # def shortestPath(self, start, end):
    #     stack = []
    #     checked = [False]*self.V
    #     for i in range(self.V):
    #         if checked[i] == False:
    #             self.topologicalHelper(start, checked, stack)
    
    #     # Initialize all distances as 'infinite'
    #     dist = [float("Inf")]*(self.V)
    #     # Distance begins at 0 from start
    #     dist[start] = 0
    #     while stack:
    #         i = stack.pop()
    #         for node, weight in self.graph[i]:
    #             if dist[node] > dist[i] + weight:
    #                 dist[node] = dist[i] + weight
        
    #     # If end is reachable, return distance to end
    #     if dist[end] != float("Inf"):
    #         return dist[end]
    #     # Else, end is unreachable
    #     else:
    #         return "Undefined"
    # Give reachable end nodes from start
    # def findEnds(self, s, endL):
    #     if self.graph[s] == []:
    #         endL.append(s)
    #     else:   
    #         for subnode in self.graph[s]:
    #             self.findEnds(subnode[0], endL)
=======
    def criticalNodes(self): 
            start = []
            end = []
        
            # Find all nodes with parent nodes
            children = []
            for node in self.graph:
                #print("nodes in graph: ", node)
                for subNode in self.graph[node]:
                    #print("subnodes: ", subNode)
                    if subNode[0] not in children:
                        children.append(subNode[0])
            #print(children)
            # All non-children have no input (start)
            for node in self.graph:
                if node not in children:
                    start.append(node)

            # Find nodes that have no output (end)
            for i in range(self.V):
                if i not in self.graph:
                    end.append(i)
    
            return start, end

    def topologicalHelper(self, v, checked, stack):
            # Value has now been checked
            checked[v] == True
            # print("keys: ", self.graph.keys())
            if v in self.graph.keys():
                #print("v is ", v, " graph[v] is: ", self.graph[v])
                for node,weight in self.graph[v]:
                    #print("node and weight are ", node, weight)
                    if checked[node] == False:
                        self.topologicalHelper(node, checked, stack)
            stack.append(v)

    def shortestPath(self, start, end):
        stack = []
        checked = [False]*self.V
        for i in range(self.V):
            if checked[i] == False:
                #print("calling helper at ", i)
                self.topologicalHelper(start, checked, stack)
    
        # Initialize all distances as 'infinite'
        dist = [float("Inf")]*(self.V)
        # Distance begins at 0 from start
        dist[start] = 0
        while stack:
            i = stack.pop()
            for node, weight in self.graph[i]:
                if dist[node] > dist[i] + weight:
                    dist[node] = dist[i] + weight
        
        # If end is reachable, return distance to end
        if dist[end] != float("Inf"):
            return dist[end]
        # Else, end is unreachable
        else:
            return "Undefined"
>>>>>>> 42ebd5d5fc08a257c18c9ca10dbfe4ad28051618

    # Dijkstra Algorithm
    def dijkstra(self, initial_node):
        visited = {initial_node: 0}
        current_node = initial_node
        path = {}

        nodes = set(self.nodes)
<<<<<<< HEAD

=======
        # print("self.nodes: ", nodes, "end of self.nodes")
>>>>>>> 42ebd5d5fc08a257c18c9ca10dbfe4ad28051618
        while nodes:
            min_node = None
            for node in nodes:
                if node in visited:
                    if min_node is None:
                        min_node = node
                    elif visited[node] < visited[min_node]:
                        min_node = node

            if min_node is None:
                break

            nodes.remove(min_node)
            cur_wt = visited[min_node]

<<<<<<< HEAD
            if min_node in self.edges:
                for edge in self.edges[min_node]:
                    # print(edge)
                    wt = cur_wt + self.distances[(min_node, edge)]
                    if edge not in visited or wt < visited[edge]:
                        visited[edge] = wt
                        path[edge] = min_node
        
        return visited, path

    # Gives route for the shortest path
    def minRoute(self, initial_node, goal_node):
        distances, paths = self.dijkstra(initial_node)
        if goal_node in distances:
            # print("dist", distances)
            # print(goal_node)
            shortestDist = distances[goal_node]
            route = [goal_node]
            while goal_node != initial_node:
                route.append(paths[goal_node])
                goal_node = paths[goal_node]
                
            route.reverse()
            return shortestDist, route
        else:
            return int(sys.maxsize), []
=======
            for edge in self.edges[min_node]:
                wt = cur_wt + self.distances[(min_node, edge)]
                if edge not in visited or wt < visited[edge]:
                    visited[edge] = wt
                    path[edge] = min_node

        return visited, path

    # Gives route for the shortest path
    def shortestRoute(self, initial_node, goal_node):
        distances, paths = self.dijkstra(initial_node)
        shortestDist = distances[goal_node]
        #print(shortestDist, paths)
        route = [goal_node]
        # print(distances, paths)
        while goal_node != initial_node:
            #print(route, goal_node)
            route.append(paths[goal_node])
            goal_node = paths[goal_node]
            
        route.reverse()
        return route
>>>>>>> 42ebd5d5fc08a257c18c9ca10dbfe4ad28051618

    # Find shortest paths from every 0-in node
    # to its reachable 0-out nodes.
    def findAllPaths(self):
<<<<<<< HEAD
        # start = self.criticalNodes()
        startL = []
        for i in range(self.V):
            if self.inDegree[i] == 0:
                startL.append(i)
        for s in startL:
            endL = []
            # self.findEnds(s, endL)
            for j in range(self.V):
                if self.outDegree[j] == 0:
                    endL.append(j)
=======
        start, end = self.criticalNodes()
        #print("start and end nodes: ", start,end)
        for s in start:
>>>>>>> 42ebd5d5fc08a257c18c9ca10dbfe4ad28051618
            endNodes = []
            allRoutes = []
            pathLengths = []
            # Find path lengths to all reachable 0-out
            for e in endL:
                # pathResult = self.shortestPath(s,e)
                # if pathResult != "Undefined":
                endNodes.append(e)
                minDist, minRoute = self.minRoute(s,e)
                # pathLengths.append(pathResult)
                pathLengths.append(minDist)
                allRoutes.append(minRoute)
                        
            # if len(pathLengths) > 0:
                # For every 0-in, find the shortest path to any reachable 0-out
            shortestLen = min(pathLengths)
            shortestEnd = endNodes[pathLengths.index(shortestLen)]
            shortestRoute = allRoutes[pathLengths.index(shortestLen)]
            print("Shortest path starting at node ", s, " ends at node ", shortestEnd, " with length ", shortestLen)
            print("The shortest route is ", shortestRoute)
        # else:
            #     print("Error: Not a true DAG")


    # def nodeMatrix(self):
    #     nodeMatrix = []
    #     for i in range(self.V):
    #         row = []
    #         for j in range(self.V):
    #             row.append(0)
    #         nodeMatrix.append(row)
        
    #     for node in self.graph:
    #         for subNode in self.graph[node]:
    #             nodeMatrix[node][subNode[0]] = subNode[1]

    #     print(nodeMatrix)
            for e in end:
                pathResult = self.shortestPath(s,e)
                #print("for ", e, " path result: ", pathResult)
                if pathResult != "Undefined":
                    endNodes.append(e)
                    pathLengths.append(pathResult)
                    allRoutes.append(self.shortestRoute(s,e))
                        
            if len(pathLengths) > 0:
                #print("pathLen[]: ", pathLengths)
                # For every 0-in, find the shortest path to any reachable 0-out
                shortestLen = min(pathLengths)
                shortestEnd = endNodes[pathLengths.index(shortestLen)]
                shortestRoute = allRoutes[pathLengths.index(shortestLen)]
                print("Shortest path starting at node ", s, " ends at node ", shortestEnd, " with length ", shortestLen)
                print("The shortest route is ", shortestRoute)
            else:
                print("Error: Not a true DAG")

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
g.findAllPaths()

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
h.findAllPaths()

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
<<<<<<< HEAD
# print(i.inDegree)
# print(i.outDegree)
print(i.graph)
=======
>>>>>>> 42ebd5d5fc08a257c18c9ca10dbfe4ad28051618
i.findAllPaths()

# Example 4
j = Graph(6)
j.add_edge(0, 1, 1)
j.add_edge(1, 2, 1)
j.add_edge(2, 3, 1)
j.add_edge(3, 4, 1)
j.add_edge(4, 5, 1)
print("\nResult for graph j: ")
j.findAllPaths()
<<<<<<< HEAD

# Compute runtime
# endTime = time.time()
# print("Time: ", endTime-startTime)
# Output: 0.004065036773681641



def findEnds(self, s):
    if self.graph[s] == []:
        print(s)
    else:   
        for subnode in self.graph[node]:
            self.findEnds(subnode[0])

=======
>>>>>>> 42ebd5d5fc08a257c18c9ca10dbfe4ad28051618
