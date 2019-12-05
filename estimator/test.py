'''
Created on Sep 13, 2019
@author: mania
'''
import estimator.collector as collector
import utils.objectstore as objs
import pandas as pd
import utils.hadoop as hadoop
import utils.graph as Graph
import spark_longest_path


def test_collector_alluxio():
    raw_execplan = '''DAG:'92af9f31-1238-4e1d-b517-605c4f8f2730'
#--------------------------------------------------
# Map Reduce Plan
#--------------------------------------------------
MapReduce node: {scope-106
Map Plan: {
StatusGroup: Local Rearrange[tuple]{tuple}(false) - scope-61
{Project[bytearray][0] - scope-62
{Project[bytearray][1] - scope-63
{SubLine: New For Each(false,false,false,false,false,false,false)[bag] - scope-58
{{Project[bytearray][4] - scope-29
{{Project[bytearray][5] - scope-31
{{Project[double][0] - scope-33
{{Project[double][1] - scope-35
{{Multiply[double] - scope-41
{{{Project[double][1] - scope-37
{{{Subtract[double] - scope-40
{{{{Constant(1.0) - scope-38
{{{{Project[double][2] - scope-39
{{Multiply[double] - scope-52
{{{Multiply[double] - scope-48
{{{{Project[double][1] - scope-44
{{{{Subtract[double] - scope-47
{{{{{Constant(1.0) - scope-45
{{{{{Project[double][2] - scope-46
{{{Add[double] - scope-51
{{{{Constant(1.0) - scope-49
{{{{Project[double][3] - scope-50
{{Project[double][2] - scope-56
{{LineItems: New For Each(false,false,false,false,false,false)[bag] - scope-28
{{{Cast[double] - scope-13
{{{{Project[bytearray][0] - scope-12
{{{Cast[double] - scope-16
{{{{Project[bytearray][1] - scope-15
{{{Cast[double] - scope-19
{{{{Project[bytearray][2] - scope-18
{{{Cast[double] - scope-22
{{{{Project[bytearray][3] - scope-21
{{{Project[bytearray][4] - scope-24
{{{Project[bytearray][5] - scope-26
{{{SubLineItems: Filter[bag] - scope-7
{{{{Less Than or Equal[boolean] - scope-11
{{{{{Cast[chararray] - scope-9
{{{{{{Project[bytearray][6] - scope-8
{{{{{Constant(1998-09-02) - scope-10
{{{{LineItems: Load(alluxio://kariz-1:19998/tpch-4G/lineitem:PigStorage('|')) - scope-6}
Reduce Plan: {
Store(hdfs://kariz-1:9000/tmp/temp661924923/tmp-398745305:org.apache.pig.impl.io.InterStorage) - scope-107
{PriceSummary: New For Each(false,false,false,false,false,false,false,false,false,false)[bag] - scope-101
{{Project[bytearray][0] - scope-65
{{{Project[tuple][0] - scope-64
{{Project[bytearray][1] - scope-68
{{{Project[tuple][0] - scope-67
{{POUserFunc(org.apache.pig.builtin.DoubleSum)[double] - scope-72
{{{Project[bag][2] - scope-71
{{{{Project[bag][1] - scope-70
{{POUserFunc(org.apache.pig.builtin.DoubleSum)[double] - scope-76
{{{Project[bag][3] - scope-75
{{{{Project[bag][1] - scope-74
{{POUserFunc(org.apache.pig.builtin.DoubleSum)[double] - scope-80
{{{Project[bag][4] - scope-79
{{{{Project[bag][1] - scope-78
{{POUserFunc(org.apache.pig.builtin.DoubleSum)[double] - scope-84
{{{Project[bag][5] - scope-83
{{{{Project[bag][1] - scope-82
{{POUserFunc(org.apache.pig.builtin.DoubleAvg)[double] - scope-88
{{{Project[bag][2] - scope-87
{{{{Project[bag][1] - scope-86
{{POUserFunc(org.apache.pig.builtin.DoubleAvg)[double] - scope-92
{{{Project[bag][3] - scope-91
{{{{Project[bag][1] - scope-90
{{POUserFunc(org.apache.pig.builtin.DoubleAvg)[double] - scope-96
{{{Project[bag][6] - scope-95
{{{{Project[bag][1] - scope-94
{{POUserFunc(org.apache.pig.builtin.COUNT)[long] - scope-99
{{{Project[bag][1] - scope-98
{{StatusGroup: Package(Packager)[tuple]{tuple} - scope-60}
Global sort: {false}
}
MapReduce node: {scope-109
Map Plan: {
SortedSummary: Local Rearrange[tuple]{chararray}(false) - scope-114
{Constant(all) - scope-113
{New For Each(false,false)[tuple] - scope-112
{{Project[bytearray][0] - scope-110
{{Project[bytearray][1] - scope-111
{{Load(hdfs://kariz-1:9000/tmp/temp661924923/tmp-398745305:org.apache.pig.impl.builtin.RandomSampleLoader('org.apache.pig.impl.io.InterStorage','100')) - scope-108}
Reduce Plan: {
Store(hdfs://kariz-1:9000/tmp/temp661924923/tmp-174642847:org.apache.pig.impl.io.InterStorage) - scope-124
{New For Each(false)[tuple] - scope-123
{{POUserFunc(org.apache.pig.impl.builtin.FindQuantiles)[tuple] - scope-122
{{{Project[tuple][*] - scope-121
{{New For Each(false,false)[tuple] - scope-120
{{{Constant(-1) - scope-119
{{{SortedSummary: POSort[bag]() - scope-104
{{{{Project[bytearray][0] - scope-117
{{{{Project[bytearray][1] - scope-118
{{{{Project[bag][1] - scope-116
{{{Package(Packager)[tuple]{chararray} - scope-115}
Global sort: {false}
}
MapReduce node: {scope-126
Map Plan: {
SortedSummary: Local Rearrange[tuple]{tuple}(false) - scope-127
{Project[bytearray][0] - scope-102
{Project[bytearray][1] - scope-103
{Load(hdfs://kariz-1:9000/tmp/temp661924923/tmp-398745305:org.apache.pig.impl.io.InterStorage) - scope-125}
Reduce Plan: {
SortedSummary: Store(/tpch-4G/Q1out:org.apache.pig.builtin.PigStorage) - scope-105
{New For Each(true)[tuple] - scope-130
{{Project[bag][1] - scope-129
{{Package(LitePackager)[tuple]{tuple} - scope-128}
Global sort: {true}
Quantile file: {hdfs://kariz-1:9000/tmp/temp661924923/tmp-174642847}
}
'''
    col = collector.Collector()
    objstore = objs.ObjectStore()
    col.objectstore = objstore
    col.submit_new_dag(raw_execplan)




def test_collector():
    my_collector = collector.Collector()
    raw_execplan = '''
    DAG:'eaf51b30-f457-4bc7-97a7-c3462698cd73'
    #--------------------------------------------------
    # Map Reduce Plan
    #--------------------------------------------------
    MapReduce node: {scope-107
    Map Plan: {
    b: Store(/pigmix1/pigmix_power_users_samples:PigStorage('')) - scope-106
    {b: Filter[bag] - scope-102
    {{Less Than[boolean] - scope-105
    {{{POUserFunc(org.apache.pig.builtin.RANDOM)[double] - scope-103
    {{{Constant(0.5) - scope-104
    {{a: New For Each(false,false,false,false,false,false)[bag] - scope-101
    {{{Project[bytearray][0] - scope-89
    {{{Project[bytearray][1] - scope-91
    {{{Project[bytearray][2] - scope-93
    {{{Project[bytearray][3] - scope-95
    {{{Project[bytearray][4] - scope-97
    {{{Project[bytearray][5] - scope-99
    {{{a: Load(/pigmix1/pigmix_power_users:PigStorage('')) - scope-88}
    Global sort: {false}
    }
    '''
    objstore = objs.ObjectStore()
    my_collector.objectstore = objstore
    g = Graph.pigstr_to_graph(raw_execplan,objstore);
    print(str(g))


def test_spark_collector():
    my_collector = collector.Collector()
#     raw_execplan = '''
# (8) ShuffledRDD[5] at reduceByKey at ScalaWordCount.scala:48 []
#  +-(8) MapPartitionsRDD[4] at map at ScalaWordCount.scala:46 []
#     |  MapPartitionsRDD[3] at flatMap at ScalaWordCount.scala:44 []
#     |  MapPartitionsRDD[2] at map at IOCommon.scala:44 []
#     |  MapPartitionsRDD[1] at sequenceFile at IOCommon.scala:44 []
#     |  hdfs://sandbox.hortonworks.com:8020/HiBench/Wordcount/Input HadoopRDD[0] at sequenceFile at IOCommon.scala:44 []
#     '''
    raw_execplan = '''
(8) PythonRDD[12] at collect at /Users/joe/Desktop/Spark_Source/spark/bin/wordcount.py:39 []
 |  MapPartitionsRDD[11] at mapPartitions at PythonRDD.scala:133 []
 |  ShuffledRDD[10] at partitionBy at NativeMethodAccessorImpl.java:0 []
 +-(8) PairwiseRDD[9] at reduceByKey at /Users/joe/Desktop/Spark_Source/spark/bin/wordcount.py:31 []
    |  PythonRDD[8] at reduceByKey at /Users/joe/Desktop/Spark_Source/spark/bin/wordcount.py:31 []
    |  UnionRDD[7] at union at NativeMethodAccessorImpl.java:0 []
    |  UnionRDD[4] at union at NativeMethodAccessorImpl.java:0 []
    |  input1/*.txt MapPartitionsRDD[1] at textFile at NativeMethodAccessorImpl.java:0 []
    |  input1/*.txt HadoopRDD[0] at textFile at NativeMethodAccessorImpl.java:0 []
    |  input2/*.txt MapPartitionsRDD[3] at textFile at NativeMethodAccessorImpl.java:0 []
    |  input2/*.txt HadoopRDD[2] at textFile at NativeMethodAccessorImpl.java:0 []
    |  input3/*.txt MapPartitionsRDD[6] at textFile at NativeMethodAccessorImpl.java:0 []
    |  input3/*.txt HadoopRDD[5] at textFile at NativeMethodAccessorImpl.java:0 []
    '''


    objstore = objs.ObjectStore()
    my_collector.objectstore = objstore
    g = Graph.sparkstr_to_graph(raw_execplan, objstore)
    longest_g = spark_longest_path.Graph(g).findAllPaths()
    print(str(g))
    print("\n\n\n")
    print(str(longest_g))




def test_collector2():
    collector = collector.Collector()
    raw_execplan = '''DAG:'261c8b72-1aa1-4d05-b11c-9a04c6f2a58b'
#--------------------------------------------------
# Map Reduce Plan
#--------------------------------------------------
MapReduce node: {scope-192
Map Plan: {
Union[tuple] - scope-193
{FR_N: Local Rearrange[tuple]{int}(false) - scope-35
{{Project[int][0] - scope-36
{{FRegion: Filter[bag] - scope-13
{{{Equal To[boolean] - scope-16
{{{{Project[chararray][1] - scope-14
{{{{Constant(EUROPE) - scope-15
{{{Region: New For Each(false,false,false)[bag] - scope-12
{{{{Cast[int] - scope-4
{{{{{Project[bytearray][0] - scope-3
{{{{Cast[chararray] - scope-7
{{{{{Project[bytearray][1] - scope-6
{{{{Cast[chararray] - scope-10
{{{{{Project[bytearray][2] - scope-9
{{{{Region: Load(s3a://data/tpch-2G/region:PigStorage('|')) - scope-2
{FR_N: Local Rearrange[tuple]{int}(false) - scope-37
{{Project[int][2] - scope-38
{{Nation: New For Each(false,false,false,false)[bag] - scope-30
{{{Cast[int] - scope-19
{{{{Project[bytearray][0] - scope-18
{{{Cast[chararray] - scope-22
{{{{Project[bytearray][1] - scope-21
{{{Cast[int] - scope-25
{{{{Project[bytearray][2] - scope-24
{{{Cast[chararray] - scope-28
{{{{Project[bytearray][3] - scope-27
{{{Nation: Load(s3a://data/tpch-2G/nation:PigStorage('|')) - scope-17}
Reduce Plan: {
Store(hdfs://kariz-1:9000/tmp/temp-111918155/tmp-2090231785:org.apache.pig.impl.io.InterStorage) - scope-194
{FR_N: New For Each(true,true)[tuple] - scope-41
{{Project[bag][1] - scope-39
{{Project[bag][2] - scope-40
{{FR_N: Package(Packager)[tuple]{int} - scope-34}
Global sort: {false}
}
MapReduce node: {scope-198
Map Plan: {
Union[tuple] - scope-199
{FR_N_S: Local Rearrange[tuple]{int}(false) - scope-69
{{Project[int][3] - scope-70
{{Load(hdfs://kariz-1:9000/tmp/temp-111918155/tmp-2090231785:org.apache.pig.impl.io.InterStorage) - scope-195
{FR_N_S: Local Rearrange[tuple]{int}(false) - scope-71
{{Project[int][3] - scope-72
{{Supplier: New For Each(false,false,false,false,false,false,false)[bag] - scope-64
{{{Cast[long] - scope-44
{{{{Project[bytearray][0] - scope-43
{{{Cast[chararray] - scope-47
{{{{Project[bytearray][1] - scope-46
{{{Cast[chararray] - scope-50
{{{{Project[bytearray][2] - scope-49
{{{Cast[int] - scope-53
{{{{Project[bytearray][3] - scope-52
{{{Cast[chararray] - scope-56
{{{{Project[bytearray][4] - scope-55
{{{Cast[double] - scope-59
{{{{Project[bytearray][5] - scope-58
{{{Cast[chararray] - scope-62
{{{{Project[bytearray][6] - scope-61
{{{Supplier: Load(s3a://data/tpch-2G/supplier:PigStorage('|')) - scope-42}
Reduce Plan: {
Store(hdfs://kariz-1:9000/tmp/temp-111918155/tmp-1814521436:org.apache.pig.impl.io.InterStorage) - scope-200
{FR_N_S: New For Each(true,true)[tuple] - scope-75
{{Project[bag][1] - scope-73
{{Project[bag][2] - scope-74
{{FR_N_S: Package(Packager)[tuple]{int} - scope-68}
Global sort: {false}
}
MapReduce node: {scope-204
Map Plan: {
Union[tuple] - scope-205
{FR_N_S_PS: Local Rearrange[tuple]{long}(false) - scope-97
{{Project[long][7] - scope-98
{{Load(hdfs://kariz-1:9000/tmp/temp-111918155/tmp-1814521436:org.apache.pig.impl.io.InterStorage) - scope-201
{FR_N_S_PS: Local Rearrange[tuple]{long}(false) - scope-99
{{Project[long][1] - scope-100
{{Partsupp: New For Each(false,false,false,false,false)[bag] - scope-92
{{{Cast[long] - scope-78
{{{{Project[bytearray][0] - scope-77
{{{Cast[long] - scope-81
{{{{Project[bytearray][1] - scope-80
{{{Cast[long] - scope-84
{{{{Project[bytearray][2] - scope-83
{{{Cast[double] - scope-87
{{{{Project[bytearray][3] - scope-86
{{{Cast[chararray] - scope-90
{{{{Project[bytearray][4] - scope-89
{{{Partsupp: Load(s3a://data/tpch-2G/partsupp:PigStorage('|')) - scope-76}
Reduce Plan: {
Store(hdfs://kariz-1:9000/tmp/temp-111918155/tmp608016270:org.apache.pig.impl.io.InterStorage) - scope-206
{FR_N_S_PS: New For Each(true,true)[tuple] - scope-103
{{Project[bag][1] - scope-101
{{Project[bag][2] - scope-102
{{FR_N_S_PS: Package(Packager)[tuple]{long} - scope-96}
Global sort: {false}
}
MapReduce node: {scope-210
Map Plan: {
Union[tuple] - scope-211
{FR_N_S_PS_FP: Local Rearrange[tuple]{long}(false) - scope-145
{{Project[long][14] - scope-146
{{Load(hdfs://kariz-1:9000/tmp/temp-111918155/tmp608016270:org.apache.pig.impl.io.InterStorage) - scope-207
{FR_N_S_PS_FP: Local Rearrange[tuple]{long}(false) - scope-147
{{Project[long][0] - scope-148
{{FPart: Filter[bag] - scope-133
{{{And[boolean] - scope-140
{{{{Equal To[boolean] - scope-136
{{{{{Project[long][5] - scope-134
{{{{{Constant(15) - scope-135
{{{{Matches - scope-139
{{{{{Project[chararray][4] - scope-137
{{{{{Constant(.*BRASS) - scope-138
{{{Part: New For Each(false,false,false,false,false,false,false,false,false)[bag] - scope-132
{{{{Cast[long] - scope-106
{{{{{Project[bytearray][0] - scope-105
{{{{Cast[chararray] - scope-109
{{{{{Project[bytearray][1] - scope-108
{{{{Cast[chararray] - scope-112
{{{{{Project[bytearray][2] - scope-111
{{{{Cast[chararray] - scope-115
{{{{{Project[bytearray][3] - scope-114
{{{{Cast[chararray] - scope-118
{{{{{Project[bytearray][4] - scope-117
{{{{Cast[long] - scope-121
{{{{{Project[bytearray][5] - scope-120
{{{{Cast[chararray] - scope-124
{{{{{Project[bytearray][6] - scope-123
{{{{Cast[double] - scope-127
{{{{{Project[bytearray][7] - scope-126
{{{{Cast[chararray] - scope-130
{{{{{Project[bytearray][8] - scope-129
{{{{Part: Load(s3a://data/tpch-2G/part:PigStorage('|')) - scope-104}
Reduce Plan: {
Store(hdfs://kariz-1:9000/tmp/temp-111918155/tmp-465527780:org.apache.pig.impl.io.InterStorage) - scope-212
{FR_N_S_PS_FP: New For Each(true,true)[tuple] - scope-151
{{Project[bag][1] - scope-149
{{Project[bag][2] - scope-150
{{FR_N_S_PS_FP: Package(Packager)[tuple]{long} - scope-144}
Global sort: {false}
}
MapReduce node: {scope-214
Map Plan: {
G1: Local Rearrange[tuple]{long}(false) - scope-154
{Project[long][14] - scope-155
{Load(hdfs://kariz-1:9000/tmp/temp-111918155/tmp-465527780:org.apache.pig.impl.io.InterStorage) - scope-213}
Reduce Plan: {
Store(hdfs://kariz-1:9000/tmp/temp-111918155/tmp-136970874:org.apache.pig.impl.io.InterStorage) - scope-215
{RawResults: New For Each(false,false,false,false,false,false,false,false)[bag] - scope-183
{{Project[double][12] - scope-167
{{Project[chararray][8] - scope-169
{{Project[chararray][4] - scope-171
{{Project[long][19] - scope-173
{{Project[chararray][21] - scope-175
{{Project[chararray][9] - scope-177
{{Project[chararray][11] - scope-179
{{Project[chararray][13] - scope-181
{{MinCost: Filter[bag] - scope-163
{{{Equal To[boolean] - scope-166
{{{{Project[double][17] - scope-164
{{{{Project[double][28] - scope-165
{{{Min: New For Each(true,false)[bag] - scope-162
{{{{Project[bag][1] - scope-156
{{{{POUserFunc(org.apache.pig.builtin.DoubleMin)[double] - scope-160
{{{{{Project[bag][17] - scope-159
{{{{{{Project[bag][1] - scope-158
{{{{G1: Package(Packager)[tuple]{long} - scope-153}
Global sort: {false}
}
MapReduce node: {scope-217
Map Plan: {
SortedMinimumCostSupplier: Local Rearrange[tuple]{chararray}(false) - scope-224
{Constant(all) - scope-223
{New For Each(false,false,false,false)[tuple] - scope-222
{{Project[double][0] - scope-218
{{Project[chararray][2] - scope-219
{{Project[chararray][1] - scope-220
{{Project[long][3] - scope-221
{{Load(hdfs://kariz-1:9000/tmp/temp-111918155/tmp-136970874:org.apache.pig.impl.builtin.RandomSampleLoader('org.apache.pig.impl.io.InterStorage','100')) - scope-216}
Reduce Plan: {
Store(hdfs://kariz-1:9000/tmp/temp-111918155/tmp-1042434768:org.apache.pig.impl.io.InterStorage) - scope-236
{New For Each(false)[tuple] - scope-235
{{POUserFunc(org.apache.pig.impl.builtin.FindQuantiles)[tuple] - scope-234
{{{Project[tuple][*] - scope-233
{{New For Each(false,false)[tuple] - scope-232
{{{Constant(-1) - scope-231
{{{SortedMinimumCostSupplier: POSort[bag]() - scope-188
{{{{Project[double][0] - scope-227
{{{{Project[chararray][1] - scope-228
{{{{Project[chararray][2] - scope-229
{{{{Project[long][3] - scope-230
{{{{Project[bag][1] - scope-226
{{{Package(Packager)[tuple]{chararray} - scope-225}
Global sort: {false}
}
MapReduce node: {scope-238
Map Plan: {
SortedMinimumCostSupplier: Local Rearrange[tuple]{tuple}(false) - scope-239
{Project[double][0] - scope-184
{Project[chararray][2] - scope-185
{Project[chararray][1] - scope-186
{Project[long][3] - scope-187
{Load(hdfs://kariz-1:9000/tmp/temp-111918155/tmp-136970874:org.apache.pig.impl.io.InterStorage) - scope-237}
Combine Plan: {
Local Rearrange[tuple]{tuple}(false) - scope-244
{Project[double][0] - scope-184
{Project[chararray][2] - scope-185
{Project[chararray][1] - scope-186
{Project[long][3] - scope-187
{Limit - scope-243
{{New For Each(true)[tuple] - scope-242
{{{Project[bag][1] - scope-241
{{{Package(LitePackager)[tuple]{tuple} - scope-240}
Reduce Plan: {
HundredMinimumCostSupplier: Store(/tpch1g/Q2out:PigStorage('|')) - scope-189
{Limit - scope-248
{{New For Each(true)[tuple] - scope-247
{{{Project[bag][1] - scope-246
{{{Package(LitePackager)[tuple]{tuple} - scope-245}
Global sort: {true}
Quantile file: {hdfs://kariz-1:9000/tmp/temp-111918155/tmp-1042434768}
}
'''
    objstore = objs.ObjectStore()
    collector.objectstore = objstore
    collector.submit_new_dag(raw_execplan)

def correct_statistics():
    fname = 'job_runtime_stats.csv'
    with open(fname) as fd:
        data1 = fd.read().split('\n')
        del data1[-1]
        df_header = data1[0]
        data = data1[1:]
        df_data = []
        for x in data:
            df_data.append(x.split(','))
        df = pd.DataFrame(data=df_data)
        #df.columns = df_header
        more_stats = []
        for index, row in df.iterrows():
            jobId = row[0]
            runtime, queuetime, maptime, name = hadoop.getJobStats(jobId)
            more_stats.append([runtime, queuetime, maptime, name])
        df2 = pd.DataFrame(data=more_stats)
        df['runtime'] = df2[0]
        df['queuetime'] = df2[1]
        df['maptime'] = df2[2]
        df['name'] = df2[3]
        stats_fn = 'job_runtime_stats.csv'
        runtime_stats_f = open(stats_fn,'a+');
        df.to_csv(stats_fn, header = False, index=False)

    # open statistics file name
    # read it line by lien

def statistics_checker():
    fname = 'job_runtime_stats.csv'
    with open(fname) as fd:
        data1 = fd.read().split('\n')
        del data1[-1]
        df_header = data1[0].split(',')
        data = data1[1:]
        df_data = []
        for x in data:
            df_data.append(x.split(','))
            if len(x.split(',')) != len(df_header):
                print(x.split(',')[0],len(x.split(',')), len(df_header))
        #df = pd.DataFrame(data=df_data,columns=df_header)
        #for index, row in df.iterrows():
        #    print(row[0])

    # open statistics file name
    # read it line by lien


if __name__ == "__main__":
    test_spark_collector()
    #statistics_checker()
    #correct_statistics()
