#!/usr/bin/python
# Trevor Nogues, Mania Abdi

# Graph abstraction 
from collections import defaultdict 
import random
import sched, threading, time
import copy
import random
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
  
#Class to represent a graph 
class Graph: 
    def __init__(self, vertices):
        self.nodes = set()
        self.edges = {}
        self.distances = {}
        self.inputs = {}
        self.inputSize = {}
        self.outputSize = {}
        self.V = vertices
        self.graph = defaultdict(list)
        self.inDegree = [0]*self.V
        self.outDegree = [0]*self.V
        self.timeValue = [0]*self.V
        self.cachedtimeValue = [0]*self.V
        self.alphabet = {}
        self.cacheSize = 50
        self.cache = [""]*self.cacheSize
        self.cacheRefDist = [-1]*self.cacheSize
        self.mrdTable = {}

    def add_node(self, value):
        self.nodes.add(value)
    
    # Randomly assign time value to each node
    def random_runtime(self):
        for i in range(len(self.timeValue)):
            self.timeValue[i] = random.randint(1,10)
            self.cachedtimeValue[i] = random.randint(1, self.timeValue[i])
        print("uncached times: ", self.timeValue)
        print("cached times: ", self.cachedtimeValue)
    
    # Set size for each letter
    def createAlphabet(self):
        letter = "a"
        while letter <= "z":
            self.alphabet[letter] = random.randint(1,10)
            letter = chr(ord(letter)+1)
        print("alphabet: ", self.alphabet)

    def random_ios(self):
        for i in range(self.V):
            numInputs = random.randint(1,3)
            inputs = []
            inputSize = []
            while len(inputs) < numInputs:
                inputLetter = chr(random.randint(97,122))
                if inputLetter not in inputs:
                    inputs.append(inputLetter)
                    inputSize.append(self.alphabet[inputLetter])
            self.inputs[i] = inputs
            self.inputSize[i] = inputSize
            self.outputSize[i] = random.randint(1,100)
        print("inputs",self.inputs)
        print("input size",self.inputSize)
        print("output size",self.outputSize)

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

    # Helper to update bLevel() contents
    def bLevelHelper(self, graphCopy, deleted, levels, count):
        checked = [True]*self.V
        for c in range(len(deleted)):
            if deleted[c] == False and graphCopy[c] == []:
                checked[c] = False

        for i in range(len(checked)):
            if checked[i] == False:
                deleted[i] = True
                count -= 1
                for node in range(self.V):
                    for subnode in graphCopy[node]:
                        if subnode[0] == i:
                            graphCopy[node].remove(subnode)
    
        return count

    # Find b-level of DAG, then sort
    def bLevel(self):
        levels = [0]*self.V
        deleted = [False]*self.V
        graphCopy = copy.deepcopy(self.graph)
        count = self.V
        while count > 0:
            count = self.bLevelHelper(graphCopy,deleted,levels,count)
            for i in range(len(deleted)):
                if deleted[i] == False:
                    levels[i] += 1
        
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
        # print("blevels, sorted:", orderedLevels)
        # print("nodes, sorted:", orderedNodes)
        return orderedLevels, orderedNodes

    # Imitation of MRD algorithm 
    def mrd(self):
        levels = self.bLevel() # change it to the way you get these two
        ordL, ordN = self.bLevelSort(levels)
        count = 0
        currentRefDist = 0
        while count < len(ordL)-1:
            currentLevel = ordL[count]
            currentNodes = [ordN[count]]
            currentInputs = [self.inputSize[ordN[count]]]
            currentNames = [self.inputs[ordN[count]]]
            if currentLevel == 0:
                while count < len(ordL)-1:
                    count += 1
                    currentNodes.append(ordN[count])
                    currentInputs.append(self.inputSize[ordN[count]])
                    currentNames.append(self.inputs[ordN[count]])
            elif ordL[count] != ordL[count+1]:
                count += 1
            else:
                while ordL[count] == ordL[count+1]:
                    currentNodes.append(ordN[count+1])
                    currentInputs.append(self.inputSize[ordN[count+1]])
                    currentNames.append(self.inputs[ordN[count+1]])
                    count += 1
                count += 1
            print("\nFor b-level", currentLevel)
            print("nodes:", currentNodes)
            print("input sizes:", currentInputs)
            print("input names:", currentNames, "\n")

            # if currentLevel == ordL[0]:
            self.updateCache(currentRefDist, currentNames)
            currentRefDist += 1

        self.updateCacheAgain()

    # Initialize cache, to be overwritten later
    def createCache(self):
        numAdded = 0
        added = []
        # If still possible to fit max letter size
        # Currently max is 100, might be changed
        while self.cacheSize - numAdded > 10:
            randLetter = chr(random.randint(97,122))
            if randLetter not in added:
                added.append(randLetter)
                letterSpace = self.alphabet[randLetter]
                for i in range(letterSpace):
                    self.cache[numAdded+i] = randLetter
                numAdded += letterSpace
        print("cache init:", self.cache)

    # Put into cache
    def updateCache(self, currentRefDist, currentNames):
        # distance = 0
        # Check what this loops over, might not be good
        for i in range(len(currentNames)):
            for name in currentNames[i]:
                if name in self.cache:
                    # Get index name starts at
                    startIndex = self.cache.index(name)
                    # Update self.cacheRefDist
                    letterSize = self.alphabet[name]
                    for j in range(startIndex, startIndex + letterSize):
                        self.cacheRefDist[j] = currentRefDist
                    # Update ref dist 
                    self.mrdTable[name] = currentRefDist
                else:
                    # Store ref dist for later
                    self.mrdTable[name] = currentRefDist

                    # letterSize = self.alphabet[name]
                    # count = 0
                    # while count < letterSize:
                    #     index = self.cacheRefDist.index(-1)
                    # get next __ elements with ref dist of -1
                    # set self.cache at each to name, then update ref dist

        print(self.cacheRefDist)
        print(self.cache)
        # print(self.mrdTable)

    # Overwrite less important info in cache
    # Only call after updateCache has finished being called
    def updateCacheAgain(self):
        maxTableDist = max(self.mrdTable.values())
        for name in self.mrdTable:
            if name not in self.cache:
                letterSize = self.alphabet[name]
                letterRefDist = self.mrdTable[name]
                count = 0
                print("trying to add",name, "with ref dist", letterRefDist)
                while count < letterSize:
                    if -1 in self.cacheRefDist:
                        print("cache over -1:", name, letterSize, letterRefDist)
                        currentIndex = self.cacheRefDist.index(-1)
                        self.cache[currentIndex] = name
                        self.cacheRefDist[currentIndex] = letterRefDist
                        count += 1
                    else:
                        findDist = maxTableDist
                        while findDist not in self.cacheRefDist and findDist > letterRefDist+1:
                            findDist -= 1
                        if findDist == letterRefDist:
                            print("break")
                            break
                        elif findDist in self.cacheRefDist:
                            currentIndex = self.cacheRefDist.index(findDist)
                            print("cache over",findDist,self.cache[currentIndex],":", name, letterSize, letterRefDist)
                            self.cache[currentIndex] = name
                            self.cacheRefDist[currentIndex] = letterRefDist
                            count += 1
                        else:
                            break
        print("\n")
        print("alphabet",self.alphabet)
        print("mrd table",self.mrdTable)
        print("\n")
        print("updated cache",self.cache)
        print("updated ref dist",self.cacheRefDist)
    
    # B-level (PIG) schedule helper 
    def scheduleHelper(self, ordL = [0], ordN = [0], currentLevel = -1, priority = 0, timesUsed = []):
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
            timesUsed.append(currentMaxTime)
            s.enter(currentMaxTime, priority, self.scheduleHelper, argument=(ordL,ordN,currentLevel,priority,timesUsed))
            s.run()

    # B-level (PIG) scheduler
    def schedule(self):
        s = sched.scheduler(time.time, time.sleep)
        blevels = self.bLevel()
        print("levels",blevels)
        ordL, ordN = self.bLevelSort(blevels)
        currentIndex = 0
        priority = 1
        currentLevel = ordL[0]
        currentMaxTime = self.timeValue[ordN[currentIndex]]
        count = 1
        timesUsed = []
        print("ordL",ordL,"ordN",ordN)
        start = time.time()
        print("start", start)
        # Assume not all nodes are same blevel
        # Will not index out of range
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
        timesUsed.append(currentMaxTime)
        s.enter(currentMaxTime, priority, self.scheduleHelper, argument=(ordL,ordN,currentLevel,priority,timesUsed))
        s.run()
        end = time.time()
        print("end", end)
        print("total time", end - start)
        return end - start, timesUsed

    # Do uncached and cached schedulers
    # Note: currently overwrites self.time, need to fix
    def runSchedulers(self):
        print("\nrunning uncached scheduler")
        uncached, timesUncached = self.schedule()
        print("\nbefore", self.timeValue)
        for node in range(self.V):
            cacheIt = True
            for name in self.inputs[node]:
                if name not in self.cache:
                    cacheIt = False
                    break
            if cacheIt == True:
                self.timeValue[node] = self.cachedtimeValue[node]
        print("after", self.timeValue)
        print("\nrunning cached scheduler")
        cached, timesCached = self.schedule()
        print("\nuncached:",uncached, "cached:",cached)
        print("\nuncached:",timesUncached, "cached:",timesCached)
        return uncached, cached

    def prefetch(self, currentLevel, ordL, ordN, timeStamp):
        currentIndex = ordL.index(currentLevel)

        # Prefetch first index
        currentNode = ordN[currentIndex]
        numInputs = len(self.inputs[currentNode])
        for i in range(numInputs):
            name = self.inputs[currentNode][i]
            offset = 0
            size = self.inputSize[currentNode][i]
            while offset < size:
                print(name, offset, currentNode, 0, timeStamp, "prefetch", size)
                offset += 1
        
        # Prefetch rest 
        while currentIndex + 1 != len(ordL) and ordL[currentIndex] == ordL[currentIndex+1]:
            nextNode = ordN[currentIndex+1]
            numInputs = len(self.inputs[nextNode])
            for i in range(numInputs):
                name = self.inputs[nextNode][i]
                offset = 0
                size = self.inputSize[nextNode][i]
                while offset < size:
                    print(name, offset, nextNode, 0, timeStamp, "prefetch", size)
                    offset += 1
            currentIndex += 1

    def read(self, currentLevel, ordL, ordN, timeStamp):
        currentIndex = ordL.index(currentLevel)

        # Read first index
        currentNode = ordN[currentIndex]
        numInputs = len(self.inputs[currentNode])
        for i in range(numInputs):
            name = self.inputs[currentNode][i]
            offset = 0
            size = self.inputSize[currentNode][i]
            while offset < size:
                print(name, offset, currentNode, 0, timeStamp, "read", size)
                offset += 1

        # Read rest 
        while currentIndex + 1 != len(ordL) and ordL[currentIndex] == ordL[currentIndex+1]:
            nextNode = ordN[currentIndex+1]
            numInputs = len(self.inputs[nextNode])
            for i in range(numInputs):
                name = self.inputs[nextNode][i]
                offset = 0
                size = self.inputSize[nextNode][i]
                while offset < size:
                    print(name, offset, nextNode, 0, timeStamp, "read", size)
                    offset += 1
            currentIndex += 1

    def prefetch_and_read(self):
        blevels = self.bLevel()
        ordL, ordN = self.bLevelSort(blevels)
        currentLevel = ordL[0]
        timeStamp = 0
        
        # Prefetch first level
        self.prefetch(currentLevel, ordL, ordN, timeStamp)
        timeStamp += 1

        # Read current level and prefetch next
        while currentLevel > 0:
            self.read(currentLevel, ordL, ordN, timeStamp)
            currentLevel -= 1
            self.prefetch(currentLevel, ordL, ordN, timeStamp)
            timeStamp += 1
            
        # Read last level
        self.read(currentLevel, ordL, ordN, timeStamp)

    # Ignore this function for now... not useful
    def pigGraph(self, label):
        uncached, cached = self.runSchedulers()
        # Create matrix
        numRows = len(uncached)
        numCols = 2
        # Initialize table with all 0s
        timeTable = [[0 for x in range(numRows)] for y in range(numCols)]
        # Add time values
        gain = []
        for row in range(numRows):
            x = cached[row]
            y = uncached[row]
            gainValue = ((y-x)/y)*100
            timeTable[0][row] = x
            timeTable[1][row] = y
            # timeTable[2][row] = gain
            gain.append(gainValue)
        
        print(gain)

        ser = pd.Series(gain)
        ser = ser.sort_values()
        # ser[len(ser)] = ser.iloc[-1]
        totalDist = np.linspace(0.,1.,len(ser))
        ser_cdf = pd.Series(totalDist, index=ser)
        ser_cdf.plot(label=label,marker='.', linestyle='none')
        # ser_cdf.plot(drawstyle='steps',label=1)
        plt.title('CDF for MRD algorithm')
        plt.xlabel('gain')
        plt.ylabel('proportion of gain values of equal or lesser value')
        plt.legend()
        plt.axis((-2,100,0,1))



        

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

g.createAlphabet()
g.createCache()
g.random_ios()
g.random_runtime()
# g.schedule()
g.mrd()
# g.runSchedulers()
# g.pigGraph("G")
# g.prefetch_and_read()

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

h.createAlphabet()
h.createCache()
h.random_ios()
h.random_runtime()
h.mrd()
# h.runSchedulers()
# h.pigGraph("H")
# h.prefetch_and_read()

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

i.createAlphabet()
i.createCache()
i.random_ios()
i.random_runtime()
i.mrd()
# i.runSchedulers()
# i.pigGraph("I")
# i.prefetch_and_read()

# Example 4
j = Graph(6)
j.add_edge(0, 1, 1)
j.add_edge(1, 2, 1)
j.add_edge(2, 3, 1)
j.add_edge(3, 4, 1)
j.add_edge(4, 5, 1)

j.createAlphabet()
j.createCache()
j.random_ios()
j.random_runtime()
j.mrd()
# j.runSchedulers()
# j.pigGraph("J")
# j.prefetch_and_read()

# Example 5
k = Graph(6) 
k.add_edge(5, 2, 1); 
k.add_edge(5, 0, 1); 
k.add_edge(4, 0, 1); 
k.add_edge(4, 1, 1); 
k.add_edge(2, 3, 1); 
k.add_edge(3, 1, 1); 

k.createAlphabet()
k.createCache()
k.random_ios()
k.random_runtime()
k.mrd()
# k.runSchedulers()
# k.pigGraph("K")
# k.prefetch_and_read()




# Generates CDF, each graph is one data point
def pigMRD():
    gain = []

    uncachedG, cachedG = g.runSchedulers()
    gainG = ((uncachedG-cachedG)/uncachedG)*100
    gain.append(gainG)

    uncachedH, cachedH = h.runSchedulers()
    gainH = ((uncachedH-cachedH)/uncachedH)*100
    gain.append(gainH)

    uncachedI, cachedI = i.runSchedulers()
    gainI = ((uncachedI-cachedI)/uncachedI)*100
    gain.append(gainI)

    uncachedJ, cachedJ = j.runSchedulers()
    gainJ = ((uncachedJ-cachedJ)/uncachedJ)*100
    gain.append(gainJ)

    uncachedK, cachedK = k.runSchedulers()
    gainK = ((uncachedK-cachedK)/uncachedK)*100
    gain.append(gainK)

    ser = pd.Series(gain)
    ser = ser.sort_values()
    totalDist = np.linspace(0.,1.,len(ser))
    ser_cdf = pd.Series(totalDist, index=ser)
    ser_cdf.plot(marker='.', linestyle='none')
    plt.title('CDF for MRD algorithm')
    plt.xlabel('gain')
    plt.ylabel('proportion of gain values of equal or lesser value')
    plt.legend()
    plt.axis((-2,100,0,1))
    plt.show()

pigMRD()
