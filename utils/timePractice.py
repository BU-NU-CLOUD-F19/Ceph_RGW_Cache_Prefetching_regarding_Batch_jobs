from collections import defaultdict 
import sched, threading, time
import random


s = sched.scheduler(time.time, time.sleep)
# threadObj = threading.Thread(target=takeANap)

def print_time(currentCheck = 0, times = [0,0,0], checked = [0,0,0], priority = 0):
    print(currentCheck, checked, time.time())
    lower = currentCheck - 1
    higher = currentCheck + 1
    priority += 1
    if lower >= 0 and checked[lower] == False:
        checked[lower] = True
        s.enter(times[lower],priority,print_time,argument=(lower,times,checked,priority))
        s.run()
    if higher < len(checked) and checked[higher] == False:
        checked[higher] = True
        s.enter(times[higher],priority,print_time,argument=(higher,times,checked,priority))
        s.run()

def print_some_times():
    print("start",time.time())
    length = 10
    times = [0]*length
    checked = [False]*length
    for i in range(length):
        times[i] = random.randint(1,10)
    currentCheck = random.randint(0,9)
    checked[currentCheck] = True
    priority = 1
    print(currentCheck,times)
    s.enter(times[currentCheck], priority, print_time, argument=(currentCheck,times,checked,priority))
    s.run() 
    print("end",time.time())

# print_some_times()

# Multi-threading technique to be implemented on Spark scheduler:

def threadingHelper(delay, thread):
    print("start of", thread, time.time())
    for currentDelay in delay:
        time.sleep(currentDelay)
        print(thread, "slept for", currentDelay, "seconds", time.time())
    print("end of", thread, time.time())

def threadingExample():
    trev = [0]*10
    small = []
    large = []
    for i in range(10):
        trev[i] = random.randint(1,10)
        if trev[i] <= 5:
            small.append(trev[i])
        else:
            large.append(trev[i])
    # print(trev,small,large)

    threadA = threading.Thread(target=threadingHelper, args=(small,"thread a"))
    threadB = threading.Thread(target=threadingHelper, args=(large,"thread b"))
    threadA.start()
    threadB.start()

    # print("end:", time.time())

# threadingExample()

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
        self.timeValue = [0]*self.V

    def add_node(self, value):
        self.nodes.add(value)

    def add_time(self, node, time):
        self.timeValue[node] = time

    # Randomly assign time value to each node
    def add_all_times(self):
        for i in range(len(self.timeValue)):
            self.timeValue[i] = random.randint(1,5)
        print("times: ", self.timeValue)    

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

    # Helper to update tLevel() contents 
    # Same as bLevelHelper()
    def tLevelHelper(self, revGraphCopy, deleted, levels, count):
        # print(revGraphCopy)
        checked = [True]*self.V
        for c in range(len(deleted)):
            if deleted[c] == False and revGraphCopy[c] == []:
                checked[c] = False
        # print("checked: ", checked)

        for i in range(len(checked)):
            if checked[i] == False:
                # print("i is: ",i)
                deleted[i] = True
                count -= 1
                # print(count)
                for node in range(self.V):
                    for subnode in revGraphCopy[node]:
                        if subnode[0] == i:
                            revGraphCopy[node].remove(subnode)
    
        # print(count, revGraphCopy)
        return count
    
    # Find t-level of DAG
    def tLevel(self):
        # "Reverse" the graph, then use code for finding b-level
        revGraphCopy = self.revGraph()
        levels = [0]*self.V
        deleted = [False]*self.V
        count = self.V
        # print(len(graphCopy))
        while count > 0:
            count = self.tLevelHelper(revGraphCopy,deleted,levels,count)
            # print(levels)
            for i in range(len(deleted)):
                if deleted[i] == False:
                    levels[i] += 1

        # print(levels)
        return levels

    # Reverse the direction of every edge
    def revGraph(self):
        revGraph = defaultdict(list)
        for node in self.graph:
            for subnode in self.graph[node]:
                revGraph[subnode[0]].append((node,subnode[1]))
        # print(revGraph)
        return revGraph

    # Sort in ascending order according to tlevel
    def tLevelSort(self, levels):
        orderedLevels = []
        orderedNodes = []
        maxLevel = max(levels)
        for i in range(len(levels)):
            currentMin = min(levels)
            currentIndex = levels.index(currentMin)
            orderedLevels.append(currentMin)
            orderedNodes.append(currentIndex)
            levels[currentIndex] += (1+maxLevel)

        # Convert back to original level values
        for i in range(len(levels)):
            levels[i] -= (1+maxLevel)

        return orderedLevels, orderedNodes
    
    def scheduleHelper(self, ordL = [0], ordN = [0], checked = [True], currentIndex = 0, currentPriority = 0):
        # Might not need this if case 
        # (May always only be passed in when False)
        if checked[ordN[currentIndex]] == False:
            checked[ordN[currentIndex]] = True
            print("node", ordN[currentIndex], "priority", currentPriority, "time", self.timeValue[ordN[currentIndex]], time.time())
            currentPriority += 1
            # Check if node has children that need to be scheduled
            if self.outDegree[ordN[currentIndex]] > 0:
                # currentKeyIndex = list(self.graph.keys()).index(ordN[currentIndex])
                # print(currentKeyIndex)
                children = []
                # Make list of all current node's children
                for child in self.graph[ordN[currentIndex]]:
                    children.append(child[0])
                # print(children)
                # Check if children have had all of their parents scheduled
                # If not, remove from list
                for parent in self.graph:
                    for childNode in self.graph[parent]:
                        # print(parent, self.graph[parent],childNode)
                        # If node's parent still has not been checked, remove it
                        if childNode[0] in children:
                            if checked[parent] == False:
                                children.remove(childNode[0])
                print("\nchildren to look at:",children,"\n")
                # Only child nodes with all parents checked remain, schedule them
                for goodChild in children:
                    currentIndex = ordN.index(goodChild)
                    currentThread = threading.Thread(target=self.schedule, args=(ordL,ordN,checked,currentIndex,currentPriority))
                    currentThread.start()
        else:
            print("did nothing for", currentIndex)


    def schedule(self,ordL,ordN,checked,currentIndex,currentPriority):
        s = sched.scheduler(time.time, time.sleep)
        s.enter(self.timeValue[ordN[currentIndex]], currentPriority, self.scheduleHelper, argument=(ordL,ordN,checked,currentIndex,currentPriority))
        s.run()

    def scheduleStarter(self):
        tlevels = self.tLevel()
        ordL, ordN = self.tLevelSort(tlevels)
        currentPriority = 1
        checked = [False]*self.V
        currentLevel = ordL[0]
        currentIndex = 0
        print("start",time.time())
        # print("ordL",ordL,"ordN",ordN)
        while ordL[currentIndex] == currentLevel:
            currentThread = threading.Thread(target=self.schedule, args=(ordL,ordN,checked,currentIndex,currentPriority))
            currentThread.start()
            currentIndex += 1
        # print("end",time.time())


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
# print("\nResult for graph g: ")
# levelG = g.tLevel()
# print("t-level: ",levelG)
# print("t-level sorted: ",g.tLevelSort(levelG))
# defaultdict(<class 'list'>, {0: [(1, 4), (2, 3), (4, 11), (11, 1)], 11: [(13, 1)], 13: [(14, 1)], 1: [(3, 7), (4, 1)], 2: [(5, 9)], 3: [(6, 2)], 4: [(6, 2)], 5: [(4, 1), (6, 5)], 7: [(5, 5), (8, 9)], 9: [(8, 4)], 10: [(6, 6), (8, 12), (12, 5), (13, 4)]})
# g.add_all_times()
# g.schedule()

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
# print("\nResult for graph h: ")
# levelH = h.tLevel()
# print("t-level: ",levelH)
# print("t-level sorted: ",h.tLevelSort(levelH))

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
# levelI = i.tLevel()
# print("t-level: ",levelI)
# print("t-level sorted: ",i.tLevelSort(levelI))
# print(i.graph)
# print(i.graph[1])
# print(i.graph[2])
# print(i.graph[5])
i.add_all_times()
i.scheduleStarter()


# Example 4
j = Graph(6)
j.add_edge(0, 1, 1)
j.add_edge(1, 2, 1)
j.add_edge(2, 3, 1)
j.add_edge(3, 4, 1)
j.add_edge(4, 5, 1)
# print("\nResult for graph j: ")
# levelJ = j.tLevel()
# print("t-level: ",levelJ)
# print("t-level sorted: ",j.tLevelSort(levelJ))
# j.add_all_times()
# j.schedule()


# Example 5
k = Graph(6) 
k.add_edge(5, 2, 1); 
k.add_edge(5, 0, 1); 
k.add_edge(4, 0, 1); 
k.add_edge(4, 1, 1); 
k.add_edge(2, 3, 1); 
k.add_edge(3, 1, 1); 
# print("\nResult for graph k: ")
# levelK = k.tLevel()
# print("t-level: ",levelK)
# print("t-level sorted: ",k.tLevelSort(levelK))
# print(k.graph)
# k.add_all_times()
# k.schedule()


