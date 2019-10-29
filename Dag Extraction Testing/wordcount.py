from __future__ import print_function

import sys
import os
from operator import add

#from pyspark.sql import SparkSession
from pyspark import SparkContext


def takeSecond(elem):
    return elem[1]

if __name__ == "__main__":
    #if len(sys.argv) != 2:
    #    print("Usage: wordcount <file>", file=sys.stderr)
    #    sys.exit(-1)

    #spark = SparkSession.builder.appName("PythonWordCount").getOrCreate()

    sc = SparkContext()
    filedirs = []
    filedirs.append("input1")
    filedirs.append("input2")
    filedirs.append("input3")
    
    rdd = sc.textFile(filedirs[0] + "/*.txt")
    for i in range(1,len(filedirs)):
        rdd = rdd.union(sc.textFile(filedirs[i] + "/*.txt"))

    counts = rdd.flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1)).reduceByKey(lambda a,b:a +b)


    
    #print("1----------------------------------------1")
    #print(counts.toDebugString())
    #print("1----------------------------------------1")

    output = counts.collect()


    output.sort(key=takeSecond,reverse=True)
    for (word, count) in output:

        print("%s: %i" % (word, count))

    #spark.stop()