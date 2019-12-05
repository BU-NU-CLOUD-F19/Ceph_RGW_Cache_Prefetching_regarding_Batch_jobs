# Compute b-level of DAG
from collections import defaultdict 
import random
import sched, time
  
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
            self.timeValue[i] = random.randint(1,8)
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
    
    # Helper to update bLevel() contents
    def bLevelHelper(self, graphCopy, deleted, levels, count):
        checked = [True]*self.V
        for c in range(len(deleted)):
            if deleted[c] == False and graphCopy[c] == []:
                checked[c] = False
        # print("checked: ", checked)

        for i in range(len(checked)):
            if checked[i] == False:
                # print("i is: ",i)
                deleted[i] = True
                count -= 1
                # print(count)
                for node in range(self.V):
                    for subnode in graphCopy[node]:
                        if subnode[0] == i:
                            graphCopy[node].remove(subnode)
    
        # print(count, graphCopy)
        return count

    # Find b-level of DAG
    def bLevel(self):
        levels = [0]*self.V
        deleted = [False]*self.V
        graphCopy = self.graph
        count = self.V
        # print(len(graphCopy))
        while count > 0:
            count = self.bLevelHelper(graphCopy,deleted,levels,count)
            # print(levels)
            for i in range(len(deleted)):
                if deleted[i] == False:
                    levels[i] += 1

        # print(levels)
        return levels

    # Sort in descending order according to blevel
    def bLevelSort(self, levels):
        orderedLevels = []
        orderedNodes = []
        maxLevel = max(levels)

        # Sort by finding current max, then make it negative
        for i in range(len(levels)):
            currentMax = max(levels)
            currentIndex = levels.index(currentMax)
            orderedLevels.append(currentMax)
            orderedNodes.append(currentIndex)
            if levels[currentIndex] == 0:
                levels[currentIndex] = -1*(1+maxLevel)
            else:
                levels[currentIndex] = -1*levels[currentIndex]

        # Convert back to original level values
        for i in range(len(levels)):
            if levels[i] == -1*(1+maxLevel):
                levels[i] = 0
            else:
                levels[i] = (-1)*levels[i]

        return orderedLevels, orderedNodes

    def eventBased(self,blevels):
        ordL, ordN = self.bLevelSort(blevels)
        maxPerLevel = []
        for j in range(len(ordL)):
            i = j
            if i == 0:
                timeL = [self.timeValue[ordN[i]]]
                while ordL[i] == ordL[i+1]:
                    timeL.append(self.timeValue[ordN[i+1]])
                    if i+1 == len(ordL)-1:
                        break
                    i += 1
                    # print(ordL[i],ordL[i+1])
                maxPerLevel.append(max(timeL))
            elif ordL[i] != ordL[i-1] and i == len(ordL) - 1:
                maxPerLevel.append(self.timeValue[ordN[i]])
            elif ordL[i] != ordL[i-1]:
                # print(maxPerLevel)
                timeL = [self.timeValue[ordN[i]]]
                if i != len(ordL):
                    while ordL[i] == ordL[i+1]:
                        timeL.append(self.timeValue[ordN[i+1]])
                        if i+1 == len(ordL)-1:
                            break
                        i += 1
                maxPerLevel.append(max(timeL))
        return maxPerLevel

    def scheduleHelper(self, ordL = [0], ordN = [0], currentLevel = -1, priority = 0):
        s = sched.scheduler(time.time, time.sleep)
        priority += 1
        print("From helper", time.time())
        currentIndex = 0
        if currentLevel != -1:
            currentMaxTime = self.timeValue[ordN[currentIndex]]
            if currentLevel == 0:
                while currentIndex < len(ordL) - 1:
                    if self.timeValue[ordN[currentIndex+1]] > currentMaxTime:
                        currentMaxTime = self.timeValue[ordN[currentIndex+1]]
                    currentIndex += 1
                    
            else:
                count = 1
                while ordL[currentIndex] == ordL[currentIndex+1]:
                    if self.timeValue[ordN[currentIndex+1]] > currentMaxTime:
                        currentMaxTime = self.timeValue[ordN[currentIndex+1]]
                    currentIndex += 1
                    count += 1
                for i in range(count):
                    del ordL[0]
                    del ordN[0]
            print(currentMaxTime, ordL, ordN)
            currentLevel -= 1
            s.enter(currentMaxTime, priority, self.scheduleHelper, argument=(ordL,ordN,currentLevel,priority))
            s.run()

    def schedule(self):
        s = sched.scheduler(time.time, time.sleep)
        blevels = self.bLevel()
        ordL, ordN = self.bLevelSort(blevels)
        currentIndex = 0
        priority = 1
        currentLevel = ordL[0]
        currentMaxTime = self.timeValue[ordN[currentIndex]]
        count = 1
        print("ordL",ordL,"ordN",ordN)
        print("start",time.time())
        # Assume not all nodes are same blevel. 
        # Will not index out of range.
        while ordL[currentIndex] == ordL[currentIndex+1]:
            if self.timeValue[ordN[currentIndex+1]] > currentMaxTime:
                currentMaxTime = self.timeValue[ordN[currentIndex+1]]
            currentIndex += 1
            count += 1
        for i in range(count):
            del ordL[0]
            del ordN[0]
        print(currentMaxTime, ordL, ordN)
        currentLevel -= 1
        s.enter(currentMaxTime, priority, self.scheduleHelper, argument=(ordL,ordN,currentLevel,priority))
        s.run()
        print("end",time.time())



                    


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
# levelG = g.bLevel()
# levelGcopy = levelG.copy()
# print(levelGcopy)
# print("b-level: ",levelG)
# sortedLevelG, sortedNodeG = g.bLevelSort(levelG)
# print("b-level sorted: ",sortedLevelG,sortedNodeG)
g.add_all_times()
# print("event based: ", g.eventBased(levelG))
g.schedule()


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
# levelH = h.bLevel()
# print("b-level: ",levelH)
# print("b-level sorted: ",h.bLevelSort(levelH))
h.add_all_times()
# print("event based: ", h.eventBased(levelH))
h.schedule()

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
# levelI = i.bLevel()
# print("b-level: ",levelI)
# print("b-level sorted: ",i.bLevelSort(levelI))
i.add_all_times()
# print("event based: ", i.eventBased(levelI))
i.schedule()

# Example 4
j = Graph(6)
j.add_edge(0, 1, 1)
j.add_edge(1, 2, 1)
j.add_edge(2, 3, 1)
j.add_edge(3, 4, 1)
j.add_edge(4, 5, 1)
print("\nResult for graph j: ")
# levelJ = j.bLevel()
# print("b-level: ",levelJ)
# print("b-level sorted: ",j.bLevelSort(levelJ))
j.add_all_times()
# print("event based: ", j.eventBased(levelJ))
j.schedule()

# Example 5
k = Graph(6) 
k.add_edge(5, 2, 1); 
k.add_edge(5, 0, 1); 
k.add_edge(4, 0, 1); 
k.add_edge(4, 1, 1); 
k.add_edge(2, 3, 1); 
k.add_edge(3, 1, 1); 
print("\nResult for graph k: ")
# levelK = k.bLevel()
# print("b-level: ",levelK)
# print("b-level sorted: ",k.bLevelSort(levelK))
k.add_all_times()
# print("event based: ", k.eventBased(levelK))
k.schedule()