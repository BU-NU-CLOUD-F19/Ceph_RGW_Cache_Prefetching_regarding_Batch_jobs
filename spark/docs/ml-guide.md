---
layout: global
title: "MLlib: Main Guide"
displayTitle: "Machine Learning Library (MLlib) Guide"
license: |
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at
 
     http://www.apache.org/licenses/LICENSE-2.0
 
  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
---

MLlib is Spark's machine learning (ML) library.
Its goal is to make practical machine learning scalable and easy.
At a high level, it provides tools such as:

* ML Algorithms: common learning algorithms such as classification, regression, clustering, and collaborative filtering
* Featurization: feature extraction, transformation, dimensionality reduction, and selection
* Pipelines: tools for constructing, evaluating, and tuning ML Pipelines
* Persistence: saving and load algorithms, models, and Pipelines
* Utilities: linear algebra, statistics, data handling, etc.

# Announcement: DataFrame-based API is primary API

**The MLlib RDD-based API is now in maintenance mode.**

As of Spark 2.0, the [RDD](rdd-programming-guide.html#resilient-distributed-datasets-rdds)-based APIs in the `spark.mllib` package have entered maintenance mode.
The primary Machine Learning API for Spark is now the [DataFrame](sql-programming-guide.html)-based API in the `spark.ml` package.

*What are the implications?*

* MLlib will still support the RDD-based API in `spark.mllib` with bug fixes.
* MLlib will not add new features to the RDD-based API.
* In the Spark 2.x releases, MLlib will add features to the DataFrames-based API to reach feature parity with the RDD-based API.
* After reaching feature parity (roughly estimated for Spark 2.3), the RDD-based API will be deprecated.
* The RDD-based API is expected to be removed in Spark 3.0.

*Why is MLlib switching to the DataFrame-based API?*

* DataFrames provide a more user-friendly API than RDDs.  The many benefits of DataFrames include Spark Datasources, SQL/DataFrame queries, Tungsten and Catalyst optimizations, and uniform APIs across languages.
* The DataFrame-based API for MLlib provides a uniform API across ML algorithms and across multiple languages.
* DataFrames facilitate practical ML Pipelines, particularly feature transformations.  See the [Pipelines guide](ml-pipeline.html) for details.

*What is "Spark ML"?*

* "Spark ML" is not an official name but occasionally used to refer to the MLlib DataFrame-based API.
  This is majorly due to the `org.apache.spark.ml` Scala package name used by the DataFrame-based API, 
  and the "Spark ML Pipelines" term we used initially to emphasize the pipeline concept.
  
*Is MLlib deprecated?*

* No. MLlib includes both the RDD-based API and the DataFrame-based API.
  The RDD-based API is now in maintenance mode.
  But neither API is deprecated, nor MLlib as a whole.

# Dependencies

MLlib uses the linear algebra package [Breeze](http://www.scalanlp.org/), which depends on
[netlib-java](https://github.com/fommil/netlib-java) for optimised numerical processing.
If native libraries[^1] are not available at runtime, you will see a warning message and a pure JVM
implementation will be used instead.

Due to licensing issues with runtime proprietary binaries, we do not include `netlib-java`'s native
proxies by default.
To configure `netlib-java` / Breeze to use system optimised binaries, include
`com.github.fommil.netlib:all:1.1.2` (or build Spark with `-Pnetlib-lgpl`) as a dependency of your
project and read the [netlib-java](https://github.com/fommil/netlib-java) documentation for your
platform's additional installation instructions.

The most popular native BLAS such as [Intel MKL](https://software.intel.com/en-us/mkl), [OpenBLAS](http://www.openblas.net), can use multiple threads in a single operation, which can conflict with Spark's execution model.

Configuring these BLAS implementations to use a single thread for operations may actually improve performance (see [SPARK-21305](https://issues.apache.org/jira/browse/SPARK-21305)). It is usually optimal to match this to the number of cores each Spark task is configured to use, which is 1 by default and typically left at 1.

Please refer to resources like the following to understand how to configure the number of threads these BLAS implementations use: [Intel MKL](https://software.intel.com/en-us/articles/recommended-settings-for-calling-intel-mkl-routines-from-multi-threaded-applications) and [OpenBLAS](https://github.com/xianyi/OpenBLAS/wiki/faq#multi-threaded).

To use MLlib in Python, you will need [NumPy](http://www.numpy.org) version 1.4 or newer.

[^1]: To learn more about the benefits and background of system optimised natives, you may wish to
    watch Sam Halliday's ScalaX talk on [High Performance Linear Algebra in Scala](http://fommil.github.io/scalax14/#/).

# Highlights in 2.3

The list below highlights some of the new features and enhancements added to MLlib in the `2.3`
release of Spark:

* Built-in support for reading images into a `DataFrame` was added
([SPARK-21866](https://issues.apache.org/jira/browse/SPARK-21866)).
* [`OneHotEncoderEstimator`](ml-features.html#onehotencoderestimator) was added, and should be
used instead of the existing `OneHotEncoder` transformer. The new estimator supports
transforming multiple columns.
* Multiple column support was also added to `QuantileDiscretizer` and `Bucketizer`
([SPARK-22397](https://issues.apache.org/jira/browse/SPARK-22397) and
[SPARK-20542](https://issues.apache.org/jira/browse/SPARK-20542))
* A new [`FeatureHasher`](ml-features.html#featurehasher) transformer was added
 ([SPARK-13969](https://issues.apache.org/jira/browse/SPARK-13969)).
* Added support for evaluating multiple models in parallel when performing cross-validation using
[`TrainValidationSplit` or `CrossValidator`](ml-tuning.html)
([SPARK-19357](https://issues.apache.org/jira/browse/SPARK-19357)).
* Improved support for custom pipeline components in Python (see
[SPARK-21633](https://issues.apache.org/jira/browse/SPARK-21633) and 
[SPARK-21542](https://issues.apache.org/jira/browse/SPARK-21542)).
* `DataFrame` functions for descriptive summary statistics over vector columns
([SPARK-19634](https://issues.apache.org/jira/browse/SPARK-19634)).
* Robust linear regression with Huber loss
([SPARK-3181](https://issues.apache.org/jira/browse/SPARK-3181)).

# Migration Guide

The migration guide is now archived [on this page](ml-migration-guide.html).

