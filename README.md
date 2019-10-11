# Ceph RGW Cache Prefetching regarding Batch jobs
[Sprint 1 presentation](https://docs.google.com/presentation/d/1Wu4Z7c8MkjNF0_733TzDGl1x2EopEmEqpkTrnMzqN3k/edit#slide=id.p)

[Sprint 2 presentation](https://docs.google.com/presentation/d/1DTolgI3JfyM3HCCrTtMie6bbcx3SGHGu_B-67uDjoYU/edit#slide=id.p)
## 1. Vision and Goals Of The Project:

- **Spark** is a high-performance open source data processing engine that can perform batch processing. 

- **Hive** is is a data warehouse framework for querying and analysis of data that is stored in HDFS.

- **DAG** (Directed Acyclic Graph)[ ](http://data-flair.training/blogs/apache-spark-tutorial/)is a set of Vertices and Edges, where vertices represent the RDDs(Resilient Distributed Dataset, a fundamental data structure in Spark) and the edges represent the Operation to be applied on RDD.

  ![DAG](https://github.com/BU-NU-CLOUD-F19/Ceph_RGW_Cache_Prefetching_regarding_Batch_jobs/blob/master/doc/DAG.jpg)

- **Ceph** is an open source storage platform, which implements object storage on a single distributed computer cluster, and provides interfaces for object-, block- and file-level storage. 

- **RGW**(RADOS Gateway) is an object storage interface built on top of OSDs(Ceph Distributed Storage) to provide applications with the location of data in clusters.

### Goal of this project

When developer using Spark to deal with batch jobs, the jobs are done in sequence which means one job cannot start until all of its dependencies are done. We want to establish a mechanism to extract the dependencies and prefetch the data from Ceph RGW into cache beforehand so that the overall runtime can be speed up.

## 2. Users/Personas Of The Project:

### For Spark developers:

Accelerate the computation speed of batch jobs by prefetching data from file systems to Ceph RGW high speed cache instead of directly fetching the data from low speed file systems.

## 3. Scope and Features Of The Project:

This project provides an efficient mechanism to accelerate  Spark application running time by prefetching batch jobs data into cache. Below is an overview of the project features:

- Create the DAG of operations and data from user’s Spark applications.

- According to the DAG and the use of KARIZ, prefetch data that reduce the runtime the most. 

- Prefetch the data from OSDs into cache based on the DAG.



## 4. Solution Concept

### Global Architectural Structure Of the Project and a Walkthrough:

![design](https://github.com/BU-NU-CLOUD-F19/Ceph_RGW_Cache_Prefetching_regarding_Batch_jobs/blob/master/doc/Design.png)

Below is a description of the system components that are building blocks of the architectural design

- DAG generation mechanism: Application will generate DAG out of querys input by users
- KARIZ method: Contains of job output size estimator, job runtime estimator, prefetching per input estimator, and prefetch planner, making the decision of how to prepare cache.
- Prefetching mechanism (User-directed prefetching): User will send a special header in the normal GET request and upon receiving this request, RGW should prefetch the data into the cache and reply the user with success message

### Design Implications and Discussion:

- Generate directed acyclic graph (DAG) by DAG Scheduler in Spark
- Create software to perform the prediction of which data will be
   accessed in the future based on DAG.
- Use KARIZ that reducing the runtime the most by finding the longest path of the dag.
- The idea of KARIZ:
  An DAG planner that build prefered cache plans for a signle DAG at the submission time;
  A cache planner that make decisions for multiple DAG at runtime considering limited bandwidth and limited cache space.
  Prefetch the cache data partially.

- """Prefetch data by using prefetching commands (Ceph prefetching API) in
     Ceph Rados Gateway (RGW) to improve the computing speed of batch jobs in Spark."""
- Performance test: Comparing efficiency (running time) of batch jobs between with/without prefetching the data. 

## 5. Acceptance Criteria

- The MVP is demonstrating speed improvements with our prefetching mechanism using common benchmarks. (e.g. TPC-DS/TPC-H) as well as other common jobs.


## 6. Release Planning:

### Sprint 1: 9/16 - 10/6 

-   Set up infrastructures (make Spark, Hive and Ceph running)on VMs.
  
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

## 7. Open Questions & Risks

- How to find the DAG(dependencies) of batch jobs? This is the core issue of this project.





