# Ceph RGW Cache Prefetching regarding Batch jobs

## 1. Vision and Goals Of The Project:

- **Spark** is a high-performance open source data processing engine that can perform batch processing. 

- **Hive** is is a data warehouse framework for querying and analysis of data that is stored in HDFS.

- **DAG** (Directed Acyclic Graph)[ ](http://data-flair.training/blogs/apache-spark-tutorial/)is a set of Vertices and Edges, where vertices represent the RDDs(Resilient Distributed Dataset, a fundamental data structure in Spark) and the edges represent the Operation to be applied on RDD.

  ![DAG](https://github.com/BU-NU-CLOUD-F19/Ceph_RGW_Cache_Prefetching_regarding_Batch_jobs/blob/master/doc/DAG.jpg)

- **Ceph** is an open source storage platform, which implements object storage on a single distributed computer cluster, and provides interfaces for object-, block- and file-level storage. 

### Goal of this project

When developer using Spark to deal with batch jobs, the jobs are done in sequence which means one job cannot start until all of its dependencies are done. We want to establish a mechanism to extract the dependencies and prefetch the data from Ceph RGW beforehand so that the overall runtime can be speed up.

## 2. Users/Personas Of The Project:

### For Spark developers:

Accelerate the computation speed of batch jobs by prefetching data from file systems to Ceph RGW high speed cache instead of directly fetching the data from low speed file systems.

## 3. Scope and Features Of The Project:

- Obtain the DAG of jobs and files out of Hive apps.

- Prefetch the data into cache based on the DAG.

- Implement performance test application to analyze the result.

## 4. Solution Concept

![design](https://github.com/BU-NU-CLOUD-F19/Ceph_RGW_Cache_Prefetching_regarding_Batch_jobs/blob/master/doc/Design.png)

### Global Architectural Structure Of the Project and a Walkthrough:

Below is a description of the system components that are building blocks of the architectural design

- Hive application: application to generate DAG

- Ceph: Ceph stores data as objects within logical storage pools. Using the CRUSH algorithm, Ceph calculates which placement group should contain the object, and further calculates which Ceph OSD Daemon should store the placement group. The CRUSH algorithm enables the Ceph Storage Cluster to scale, rebalance, and recover dynamically.

- Prefetching mechanism (User-directed prefetching): User will send a special header in the normal GET request and upon receiving this request, RGW should prefetch the data into the cache and reply the user with success message

- Rados Gateway (RGW): The S3/Swift gateway component of Ceph.

- Object storage device (OSD): A physical or logical storage unit

### Design Implications and Discussion:

- Generate directed acyclic graph (DAG) by DAG Scheduler in Spark
- Create software to perform the prediction of which data will be
   accessed in the future based on DAG.
- Prefetch data by using prefetching commands (Ceph prefetching API) in
     Ceph Rados Gateway (RGW) to improve the computing speed of batch jobs in Spark.
- Performance test: Comparing efficiency (running time) of batch jobs between with/without prefetching the data. 

## 5. Acceptance Criteria

- The MVP is having a certain degree of acceleration with prefetching mechanism running common benchmarks. (e.g. TPC-DS/TPC-H) as well as other common jobs.


## 6. Release Planning:

### Sprint 1: 9/16 - 10/6 

-   Have Spark, Hive and Ceph up and running。
  
-   Understand the prefetching interface.
  
-   Wite a Hive application.
  
### Sprint 2: 10/7 - 10/27

-   Run TPC-DS/TPC-H benchmarks, get into Spark and Hive code to generate the DAG, extract DAG information out of results,
  

### Sprint 3: 10/28 - 11/17

-   Do prefetching using the information gotten from the DAG and the Prefetching API of Ceph
  
### Sprint 4: 11/18 - 12/2

-   Performance tests
  
-   Analyze results
  
-   Presentation






