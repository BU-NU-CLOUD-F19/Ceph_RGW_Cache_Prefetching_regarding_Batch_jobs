from collections import defaultdict
import estimator.collector as collector
import utils.job as jb
import utils.graph as graph

class Graph:
    """
    A simple undirected, weighted graph
    """

    def __init__(self, g):
        self.longestroute=[]
        self.longestlength = 0
        self.g = g
        self.V = g.n_vertices
        self.nodes = set()
        self.edges = {}
        self.distances = {}
        self.graph = defaultdict(list)
        for j in g.jobs:
            self.add_node(j)
            for e, d in g.jobs[j].children.items():
                self.add_edge(j, e, d)

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
                if longestLen > self.longestlength:
                    self.longestlength = longestLen
                    self.longestroute = longestRoute.copy()
            else:
                print("Error: Not a true DAG")

        path = graph.Graph(len(self.longestroute))
        for i in range(len(self.longestroute)):
            #path.add_new_job(self.longestroute[i], self.g.jobs.get(self.longestroute[i]).func_name)
            if i < len(self.longestroute) - 1:
                path.add_edge(self.longestroute[i], self.longestroute[i+1])
            path.static_runtime(self.longestroute[i], self.g.jobs.get(self.longestroute[i]).runtime_remote,self.g.jobs.get(self.longestroute[i]).runtime_cache)
            path.config_inputs(self.longestroute[i], self.g.jobs.get(self.longestroute[i]).inputs)
            runtime_ = self.g.jobs.get(self.longestroute[i]).runtime_remote+self.g.jobs.get(self.longestroute[i]).runtime_cache
        return path
