# Trevor Nogues
import random
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


# Randomly generate data for table
numRows = 100
numCols = 4

# Initialize table with all 0s
timeTable = [[0 for x in range(numRows)] for y in range(numCols)]

# Generate performance time for all but last column
minTime = 0
maxTime = 10
for col in range(numCols-1):
    for row in range(numRows):
        time = random.randint(minTime,maxTime)
        timeTable[col][row] = time

# Generate performance time for last column
# More likely to be greater value
minTime = 8
maxTime = 10
for row in range(numRows):
    time = random.randint(minTime,maxTime)
    timeTable[numCols-1][row] = time

# Initialize gain table
gainTable = [[0 for x in range(numRows)] for y in range(numCols-1)]

histogram_one = []
histogram_two = []
histogram_three = []

# Calculate gain values
for col in range(numCols-1):
    for row in range(numRows):
        x = timeTable[col][row] 
        y = timeTable[numCols-1][row]
        gain = ((y-x)/y) *100
        gainTable[col][row] = gain

        # Add values to histogram list
        if col == 0:
            histogram_one.append(gain)
        if col == 1:
            histogram_two.append(gain)
        if col == 2:
            histogram_three.append(gain)
        
# print(histogram_one)
# print(histogram_two)
# print(histogram_three)

df = pd.DataFrame({
    'a': histogram_one,
    'b': histogram_two,
    'c': histogram_three
    }, 
    columns=['a','b','c']
)

# Same plot
# df.plot.hist(stacked=True, bins = 10)

# Sub plots
# df.diff().hist(bins=[-10,0,10,20,30,40,50,60,70,80,90,100])

serOne = pd.Series(histogram_one)
serOne = serOne.sort_values()
serOne[len(serOne)] = serOne.iloc[-1]
totalDistOne = np.linspace(0.,1.,len(serOne))
serOne_cdf = pd.Series(totalDistOne, index=serOne)
serOne_cdf.plot(drawstyle='steps',label=1)

serTwo = pd.Series(histogram_two)
serTwo = serTwo.sort_values()
serTwo[len(serTwo)] = serTwo.iloc[-1]
totalDistOne = np.linspace(0.,1.,len(serTwo))
serTwo_cdf = pd.Series(totalDistOne, index=serTwo)
serTwo_cdf.plot(drawstyle='steps',label=2)

serThree = pd.Series(histogram_three)
serThree = serThree.sort_values()
serThree[len(serThree)] = serThree.iloc[-1]
totalDistOne = np.linspace(0.,1.,len(serThree))
serThree_cdf = pd.Series(totalDistOne, index=serThree)
serThree_cdf.plot(drawstyle='steps',label=3)

plt.title('CDF for various caching methods')
plt.xlabel('gain')
plt.ylabel('proportion of gain values of equal or lesser value')
plt.legend()
plt.show()