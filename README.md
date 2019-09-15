# Ceph_RGW_Cache_Prefetching_regarding_Batch_jobs

## 1.Vision and Goals Of The Project:

MapReduce is a programming model which is widely used in tackling huge data sets by distributing processing across many nodes, and then reducing the results of those nodes. But each MapReduce operation is independent of each other. While in Spark, a DAG(Directed Acyclic Graph) of consecutive computation  stages is formed. This DAG could be used for future data usage prediction and prefetching to increase the operation efficiency. High level goals of this project include:

- Generate directed acyclic graph (DAG) by DAG Scheduler in Spark
- Create software to perform the prediction of which data will be
  accessed in the future based on DAG.
- Prefetch data by using prefetching commands (Ceph prefetching API) in
  Ceph Rados Gateway (RGW) to improve the computing speed of batch jobs in Spark.
- Performance test: Comparing efficiency (running time) of batch jobs between with/without prefetching the data. 

## 2.Users/Personas Of The Project:

Ceph RGW Cache prefetching will be used not only by RedHat staff, but also by developers of big-data enterprises as an open source solution for cloud speedup.

## 3.Scope and Features Of The Project:

- Obtain the DAG of jobs and files out of Hive apps.
- Prefetch the data into cache based on the DAG.
- Implement performance test application to analyze the result.

## 4.Solution Concept

Below is a description of the system components that are building blocks of the architectural design

- Hive application: application to generate DAG
- Ceph: Ceph stores data as objects within logical storage pools. Using the CRUSH algorithm, Ceph calculates which placement group should contain the object, and further calculates which Ceph OSD Daemon should store the placement group. The CRUSH algorithm enables the Ceph Storage Cluster to scale, rebalance, and recover dynamically.
- Prefetching mechanism (User-directed prefetching): User will send a special header in the normal GET request and upon receiving this request, RGW should prefetch the data into the cache and reply the user with success message
- Rados Gateway (RGW): The S3/Swift gateway component of Ceph.
- Object storage device (OSD): A physical or logical storage unit

### Design Implications and Discussion:

Key design decisions and motivation behind them.

- Spark:Apache Spark is a powerful open source engine that provides real-time stream processing, interactive processing, graph processing, in-memory processing as well as batch processing with very fast speed, ease of use and standard interface.
- Hive: Apache Hive saves developers from writing complex Hadoop MapReduce jobs for ad-hoc requirements. Hence, hive provides summarization, analysis, and query of data. Hive is very fast and scalable. It is highly extensible. Since Apache Hive is similar to SQL, hence it becomes very easy for the SQL developers to learn and implement Hive Queries.
- Why not using Hadoop and MapReduce:
  1. The MapReduce result need to be saved to disk in Hadoop. While Spark puts intermediate data into memory. The iteration operations of Spark are more efficient.
  2. Spark introduces Resilient Distributed Dataset(RDD), which ensures higher fault tolerance than Hadoop.
  3. Hive queries are converted to MapReduce programs in the background. This helps developers focus more on the business problem rather than focus on complex programming language logic.

## 5.Acceptance criteria

The MVP is having a certain degree of acceleration with prefetching mechanism running common benchmarks. (e.g. TPC-DS/TPC-H) as well as other common jobs.

## 6.Release Planning:

### Sprint 1: 9/16 - 9/30

- Have Spark, Hive and Ceph up and running
- Understand the prefetching interface.
- Write a Hive application.

### Sprint 2: 10/7 - 10/21

- Run TPC-DS/TPC-H benchmarks, get into Spark and Hive code to generate the DAG, extract DAG information out of results,

### Sprint 3: 10/28 - 11/11

- Do prefetching using the information gotten from the DAG and the Prefetching API of Ceph

### Sprint 4: 11/18 - 12/2

- Performance testsAnalyze resultsPresentation

