
from __future__ import print_function

import sys
import os
from operator import add

#from pyspark.sql import SparkSession
from pyspark import SparkContext

if __name__ == "__main__":
    #if len(sys.argv) != 2:
    #    print("Usage: wordcount <file>", file=sys.stderr)
    #    sys.exit(-1)

    sc = SparkContext()
    sc._jsc.hadoopConfiguration().set("fs.s3.impl","org.apache.hadoop.fs.s3native.NativeS3FileSystem")
    sc._jsc.hadoopConfiguration().set("fs.s3a.awsAccessKeyId", "0555b35654ad1656d804")
    sc._jsc.hadoopConfiguration().set("fs.s3a.awsSecretAccessKey", "h7GhxuBLTrlhVUyxSPUKUV8r/2EI4ngqJxD7iBdBYLhwluN30JaT3Q==")
#sc._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")

    sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "http://10.0.0.13:8000")   
    rdd = sc.textFile(sys.argv[1])

    counts = rdd.flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1)).reduceByKey(lambda a,b:a +b)


    output = counts.collect()

    for (word, count) in output:

        print("%s: %i" % (word, count))
