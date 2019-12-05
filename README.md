# Ceph RGW Cache Prefetching regarding Batch jobs
[Sprint 1 presentation](https://docs.google.com/presentation/d/1Wu4Z7c8MkjNF0_733TzDGl1x2EopEmEqpkTrnMzqN3k/edit#slide=id.p)

[Sprint 2 presentation](https://docs.google.com/presentation/d/1DTolgI3JfyM3HCCrTtMie6bbcx3SGHGu_B-67uDjoYU/edit#slide=id.p)

[Sprint 3 presentation](https://docs.google.com/presentation/d/1n7PmR-aY--I_aHW3KDkfpajbVTVvKDzvywhq51mLwe8/edit#slide=id.g705ee6c8d7_0_109)

[Sprint 4 presentation](https://docs.google.com/presentation/d/1Elgr6rXA-xoFwnOammK2Vb-8GB7G5y7CcH37rbB17dc/edit#slide=id.g6ad904b7d2_8_2)

[Sprint 5 presentation](https://drive.google.com/open?id=1iN-gZnn5MLgFggXmeUB45YmFmqarpcOAsKt2gkvPSZI)

[Final Video presentation](https://drive.google.com/a/bu.edu/file/d/1xxrbolo_70cQ4eJZPNyoocUccrJN1FmX/view?usp=sharing)


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

- Spark: In Spark, there is a fundamental data structure, resilient distributed datasets, is a fault-tolerant collection of elements that can be operated on in parallel. When Spark scheduler initiates the RDDs, we can get the DAG of RDDs by using Internal function of RDD class toDebugString. After we get the DAG String, we send it to Kariz through HTTP POST request to a specific endpoint.

- Kariz: After Kariz get the request with DAG, kariz will interpret the DAG String into a critical path and send it to prefetching planner. The planner will optimize caching for the critical path with longest runtime reduction. Ceph RGW gets the plan with functions in Kariz.

- Ceph: Ceph RGW will prefetch the data and files from ceph clusters into D3N Cache. How can we do that? RGW used prefetch commands based on the prefetch planner mentioned before. Spark will access the data in D3N through s3a commands. So what is s3a? S3 is the AWS object storage system. Ceph has s3 buckets in the Object Storage Devices. S3a is an interface which provides API for connecting Ceph.

### Design Implications and Discussion:

- Modify the source codes of Spark to generate DAG before application actually starts running, 
- Use POST request to send the DAG string to Kariz.
- Transfer DAG string into Kariz graph class.
- Use KARIZ that reducing the runtime the most by finding the critical path of the dag.
- Find the longest path by Dijkstra and prefetch all the files along the path.
- Prefetch data by Kariz prefetching module.
- Performance test: Comparing efficiency (running time) of batch jobs between with/without prefetching the data. 

## 5. Acceptance Criteria

- The MVP is have a system end-to-end test: extract DAG out of Spark Applications; find the job dependency path; generate cache planner 
;prefetch Files/data while running batch jobs

## 6. Release Planning

### Sprint 1: 9/16 - 10/6 

-   Set up infrastructures (make Spark, Hive and Ceph running)on VMs.
  
-   Understand the prefetching interface.
  
  
### Sprint 2: 10/7 - 10/27

-   Run TPC-DS/TPC-H benchmarks, get into Spark code to generate the DAG, extract DAG information out of results,
  

### Sprint 3: 10/28 - 11/17

-   Do prefetching using the information gotten from the DAG and the Prefetching API of Ceph
  
### Sprint 4: 11/18 - 12/2

-   Performance tests
-   Analyze results
-   Presentation

## 7. Workflow

In this part we will describe how we implement the system and how to run the project.
### Ceph
- **S3a**
  - dnf(DNF Package Manager In CentOS) install
  ```shell
  yum install epel-release -y
  
  yum install dnf -y
  ```
  - s3cmd install
  ```shell
  dnf install s3cmd
  ```
  - s3a configuration
  ```shell
  s3cmd --configure
  ```
- **Run Ceph**
```shell
cd ceph/build/

sudo  MON=1 OSD=3 RGW=1 MGR=0 MDS=0 ../src/vstart.sh -n -d
```


### Spark

- **Download**

  - Download the Spark source code we have modified, you can see them in the Spark branch.

- **Compile the source code** 

  - Using maven build to compile the source code as in the following command

  ```
  ./build/mvn -DskipTests clean package
  ```

  You can see the whole tutorial from https://github.com/apache/spark

- **Spark configurations**
  - spark-defaults.conf in $SPARK_HOME/conf
  ```yaml
  spark.hadoop.fs.s3a.impl	org.apache.hadoop.fs.s3a.S3AFileSystem  

  spark.hadoop.fs.s3a.access.key	s3a access key

  spark.hadoop.fs.s3a.secret.key	s3a secret key
  ```

- **Upload input files to Ceph Machine**

  - cd to Spark folders and run the following cammand. You can change the name of input files by modifying *./input*

  ```shell
  python3 ceph_init.py ./input
  ```

- **Run Spark Application**

  - In the Spark folder, run the following command

  ```shell
  ./bin/spark-submit ceph_test.py s3a://test1
  ```

### Kariz

- **Download**

  - Download the Kariz code in Kariz branch. It is written with Python3
  
- **Dependencies**
  - pip install connextion flask pandas numpy matplotlib 
- **User configuration**
  - In ceph VM, go to directory /home/centos/ceph-prefetching/Ceph-RGW-Prefetching/build
  - run create user command: 
  ```shell
  sudo ./bin/radosgw-admin user create --uid=<user id, eg: jay> --display-name=<display name: eg: cloud-user> --access=full --access-key <access key> --secret-key <secret key>
  ```
  - run create subuser command: 
  ```shell
  sudo ./bin/radosgw-admin subuser create --uid=<user id, eg: jay> --subuser=jay:swift --access=full
  ```
  - After doing this, user can get a json format String in the terminal: 
  ```yaml
  {
    "user_id": "testuser3",
    "display_name": "Cloud",
    "email": "",
    "suspended": 0,
    "max_buckets": 1000,
    "subusers": [
        {
            "id": "testuser3:swift", 
            "permissions": "full-control"  
        } 
    ],
    "keys": [
        {
            "user": "testuser3",
            "access_key": "BAPLVLXYE067O2ZCTRT6",
            "secret_key": "MRt915piNShNY1bW6QtQNIjuTH9lMmK4R5DWU0PR"
        }
    ],
    "swift_keys": [
        {
            "user": "testuser3:swift",
            "secret_key": "27ZMRojp3DA20dHJlTHhimreJzC4FGq9kokfLYGj"
        }
    ],
    "caps": [],
    "op_mask": "read, write, delete",
    "default_placement": "",
    "placement_tags": [],
    "bucket_quota": {
        "enabled": false,
        "check_on_raw": false,
        "max_size": -1,
        "max_size_kb": 0,
        "max_objects": -1
    },
    "user_quota": {
        "enabled": false, 
        "check_on_raw": false,
        "max_size": -1,
        "max_size_kb": 0,
        "max_objects": -1
    },
    "temp_url_keys": [],
    "type": "rgw",
    "mfa_ids": []
  }
  ```
  - Modify corresponding config file /home/centos/ceph-prefetching/Kariz/code/d3n/d3n_cfg.py with right d3n port and d3n key (secret key in swift key)
  
- **Run Kariz server**
  - open file setup.sh and add the code path into your PYTOHN PATH
  - Make sure you would disable your firewall on port 3188 and 3187. Kariz deamon uses port 3188 and cache daemon uses port 3187.
  - Go to ${KARIZ_ROOT}/d3n/api and run ./server.py




