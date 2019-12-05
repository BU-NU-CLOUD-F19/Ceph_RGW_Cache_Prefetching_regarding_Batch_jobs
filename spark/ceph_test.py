
from pyspark import SparkContext
import sys

sc = SparkContext()
sc._jsc.hadoopConfiguration().set("fs.s3.impl","org.apache.hadoop.fs.s3native.NativeS3FileSystem")
#sc._jsc.hadoopConfiguration().set("fs.s3a.awsAccessKeyId", "0555b35654ad1656d804")
#sc._jsc.hadoopConfiguration().set("fs.s3a.awsSecretAccessKey", "h7GhxuBLTrlhVUyxSPUKUV8r/2EI4ngqJxD7iBdBYLhwluN30JaT3Q==")

sc._jsc.hadoopConfiguration().set("fs.s3a.awsAccessKeyId", "123456")
sc._jsc.hadoopConfiguration().set("fs.s3a.awsSecretAccessKey", "abcdefg")
sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "http://10.0.0.13:8000")
#words = sc.textFile("s3a://test/wanzy.txt").flatMap(lambda line: line.split(" "))
words = sc.textFile(sys.argv[1]+"/*.txt").flatMap(lambda line: line.split(" "))
wordCounts = words.map(lambda word: (word, 1)).reduceByKey(lambda a,b:a +b)
for word in wordCounts.collect():
	print(word)
