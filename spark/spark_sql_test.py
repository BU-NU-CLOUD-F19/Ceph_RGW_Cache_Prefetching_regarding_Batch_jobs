from pyspark.sql import SQLContext
from pyspark import SparkContext
sc = SparkContext()
sqlContext = SQLContext(sc)    
df = sqlContext.read.json("/home/centos/spark/test.json")
df.printSchema()
