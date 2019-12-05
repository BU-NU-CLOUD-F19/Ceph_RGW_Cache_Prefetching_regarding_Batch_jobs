# Author: Trevor Nogues

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

    def _add_edge(self, from_node, to_node, distance):
        self.edges.setdefault(from_node, [])
        self.edges[from_node].append(to_node)
        self.distances[(from_node, to_node)] = distance

    def criticalNodes(self): 
            start = []
            end = []
        
            # Find all nodes with parent nodes
            children = []
            for node in self.graph:
                for subNode in self.graph[node]:
                    if subNode[0] not in children:
                        children.append(subNode[0])

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
            if v in self.graph.keys():
                for node,weight in self.graph[v]:
                    if checked[node] == False:
                        self.topologicalHelper(node, checked, stack)
            stack.append(v)

    def longestPath(self, start, end):
        stack = []
        checked = [False]*self.V
        for i in range(self.V):
            if checked[i] == False:
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

    # Dijkstra Algorithm
    def dijkstra(self, initial_node):
        visited = {initial_node: 0}
        current_node = initial_node
        path = {}

        nodes = set(self.nodes)

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
            
            if min_node in self.edges:
                for edge in self.edges[min_node]:
                    wt = cur_wt + self.distances[(min_node, edge)]
                    if edge not in visited or wt < visited[edge]:
                        visited[edge] = wt
                        path[edge] = min_node
                        
        return visited, path

    # Gives route for the longest path
    def longestRoute(self, initial_node, goal_node):
        distances, paths = self.dijkstra(initial_node)
        route = [goal_node]
        while goal_node != initial_node:
            route.append(paths[goal_node])
            goal_node = paths[goal_node]
            
        route.reverse()
        return route


    def findAllPaths(self):
        start, end = self.criticalNodes()
        for s in start:
            endNodes = []
            allRoutes = []
            pathLengths = []

            # Find path lengths to all reachable 0-out
            for e in end:
                pathResult = self.longestPath(s,e)
                if pathResult != "Undefined":
                    endNodes.append(e)
                    pathLengths.append(pathResult)
                    allRoutes.append(self.longestRoute(s,e))
                        
            if len(pathLengths) > 0:
                
                # For every 0-in, find the longest path to any reachable 0-out
                longestLen = min(pathLengths)
                longestEnd = endNodes[pathLengths.index(longestLen)]
                longestRoute = allRoutes[pathLengths.index(longestLen)]
                print("Longest path starting at node ", s, " ends at node ", longestEnd, " with length ", -1*longestLen)
                print("The longest route is ", longestRoute)
            else:
                print("Error: Not a true DAG")

# Tests

# Example 1
g = Graph(15) 
g.add_edge(0, 1, -4) 
g.add_edge(0, 2, -3) 
g.add_edge(0, 4,-11)
g.add_edge(0,11, -1)
g.add_edge(11,13,-1) 
g.add_edge(13,14,-1) 
g.add_edge(1, 3, -7) 
g.add_edge(1, 4, -1) 
g.add_edge(2, 5, -9) 
g.add_edge(3, 6, -2)
g.add_edge(4, 6, -2)
g.add_edge(5, 4, -1)
g.add_edge(5, 6, -5)    
g.add_edge(7, 5, -5)
g.add_edge(7, 8, -1) 
g.add_edge(9, 8, -4)
g.add_edge(10,6, -6)
g.add_edge(10,8,-12)
g.add_edge(10,12,-5)
g.add_edge(10,13,-4)
print("\nResult for graph g: ")
g.findAllPaths()

# Example 2
h = Graph(10)
h.add_edge(0, 1, -1)
h.add_edge(0, 2, -2)
h.add_edge(0, 5, -3)
h.add_edge(1, 2, -0)
h.add_edge(1, 3, -8)
h.add_edge(2, 3, -4)
h.add_edge(2, 7, -5)
h.add_edge(3, 4, -2)
h.add_edge(6, 5, -3)
h.add_edge(7, 9, -2)
h.add_edge(8, 1, -7)
print("\nResult for graph h: ")
h.findAllPaths()

# Example 3
i = Graph(9)
i.add_edge(0, 2, -1)
i.add_edge(0, 4, -1)
i.add_edge(0, 6, -1)
i.add_edge(2, 1, -1)
i.add_edge(4, 3, -1)
i.add_edge(1, 7, -1)
i.add_edge(1, 5, -2)
i.add_edge(3, 5, -3)
i.add_edge(8, 5, -1)
print("\nResult for graph i: ")
i.findAllPaths()

# Example 4
j = Graph(6)
j.add_edge(0, 1, -1)
j.add_edge(1, 2, -1)
j.add_edge(2, 3, -1)
j.add_edge(3, 4, -1)
j.add_edge(4, 5, -1)
print("\nResult for graph j: ")
j.findAllPaths()

<<<<<<< HEAD
# Compute runtime
endTime = time.time()
print("Time: ", endTime-startTime)
# Output: 0.0033698081970214844
=======
>>>>>>> 42ebd5d5fc08a257c18c9ca10dbfe4ad28051618
