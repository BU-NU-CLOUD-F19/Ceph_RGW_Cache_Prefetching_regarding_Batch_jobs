# Ceph RGW Cache Prefetching regarding Batch jobs
[Sprint 1 presentation](https://docs.google.com/presentation/d/1Wu4Z7c8MkjNF0_733TzDGl1x2EopEmEqpkTrnMzqN3k/edit#slide=id.p)

[Sprint 2 presentation](https://docs.google.com/presentation/d/1DTolgI3JfyM3HCCrTtMie6bbcx3SGHGu_B-67uDjoYU/edit#slide=id.p)

[Sprint 3 presentation](https://docs.google.com/presentation/d/1n7PmR-aY--I_aHW3KDkfpajbVTVvKDzvywhq51mLwe8/edit#slide=id.g705ee6c8d7_0_109)

[Sprint 4 presentation](https://docs.google.com/presentation/d/1Elgr6rXA-xoFwnOammK2Vb-8GB7G5y7CcH37rbB17dc/edit#slide=id.g6ad904b7d2_8_2)

[Sprint 5 presentation](https://drive.google.com/open?id=1iN-gZnn5MLgFggXmeUB45YmFmqarpcOAsKt2gkvPSZI)

## 1. Vision and Goals Of The Project:

Nowadays, due to the large amount of data, big data analysis is frequently used in industrial, spark is one of the most popular platforms. 
**Spark** is a distributed, data processing engine for batch and streaming modes featuring SQL queries, graph processing, and machine learning. A Spark task will be compiled into an execution plan with multiple parallel batch jobs. And a direct acyclic graph of job dependencies, which is called **DAG**, will be generated. Jobs are scheduled in parallel, with the constraint set by the DAG.

Data are stored in object store clusters, which is Ceph in this project. **Ceph** is a free-software storage platform, implements object storage on a single distributed computer cluster. **RGW**(RADOS Gateway) is a Ceph provided interface. Inside of Ceph, cache is a reusable storage area that provide high read/write speed.

### Goal of this project

When a job read the data in object storage, the data would be fetched into the cache so that data accessing time would be reduced if future jobs use the same data. If we can find information about which data/file will be accessed in the future, we can prefetch the data from low speed hard disk to the high speed cache in advance so that the whole work flow can be accelerated.

## 2. Users/Personas Of The Project:

### For Spark developers:

Accelerate the computation speed of batch jobs by prefetching data from file systems to Ceph RGW high speed cache instead of directly fetching the data from low speed file systems.

## 3. Scope and Features Of The Project:

This project provides an efficient mechanism to accelerate  Spark application running time by prefetching batch jobs data into cache. Below is an overview of the project features:

- Create the DAG of operations and data from user’s Spark applications.

- According to the DAG and the use of KARIZ, prefetch data that reduce the runtime the most. 

- Prefetch the data from OSDs into cache based on the DAG.



## 4. Solution Concept

  ![system](https://github.com/BU-NU-CLOUD-F19/Ceph_RGW_Cache_Prefetching_regarding_Batch_jobs/blob/master/doc/video-demo.jpg)

Below is a description of the system components that are building blocks of the architectural design

-Spark: In Spark, there is a fundamental data structure, resilient distributed datasets, is a fault-tolerant collection of elements that can be operated on in parallel. When Spark scheduler initiates the RDDs, we can get the DAG of RDDs by using Internal function of RDD class toDebugString. After we get the DAG String, we send it to Kariz through HTTP POST request to a specific endpoint.

-Kariz: After Kariz get the request with DAG, kariz will interpret the DAG String into a critical path and send it to prefetching planner. The planner will optimize caching for the critical path with longest runtime reduction. Ceph RGW gets the plan with functions in Kariz.

-Ceph: Ceph RGW will prefetch the data and files from ceph clusters into D3N Cache. How can we do that? RGW used prefetch commands based on the prefetch planner mentioned before. Spark will access the data in D3N through s3a commands. So what is s3a? S3 is the AWS object storage system. Ceph has s3 buckets in the Object Storage Devices. S3a is an interface which provides API for connecting Ceph.

### Design Implications and Discussion:

- Generate directed acyclic graph (DAG) by DAG Scheduler in Spark
- Create software to perform the prediction of which data will be
   accessed in the future based on DAG.
- Use KARIZ that reducing the runtime the most by finding the critical path of the dag.
- Two planners in KARIZ:
  
  A DAG planner that build prefered cache plans for a signle DAG at the submission time;
 
  A cache planner that make decisions for multiple DAG at runtime considering limited bandwidth and limited cache space. For each DAG, get the prefetching and caching plans for the their next stage considering their bandwidth share. 

- Prefetch data by using prefetching commands (Ceph prefetching API) in
     Ceph Rados Gateway (RGW) to improve the computing speed of batch jobs in Spark.
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





