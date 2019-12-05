from pyspark import SparkContext

sc = SparkContext()
words = sc.textFile("README.md").flatMap(lambda line: line.split(" "))
wordCounts = words.map(lambda word: (word, 1)).reduceByKey(lambda a,b:a+b)
for word in wordCounts.collect():
	print(word)
