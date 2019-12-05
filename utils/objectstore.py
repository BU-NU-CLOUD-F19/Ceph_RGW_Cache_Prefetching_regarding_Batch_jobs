#!/usr/bin/python

#usage: test_boto.py put bucket_name object_name
# example: python run_boto.py put traces alibaba_clusterdata_v2018/machine_usage.csv
# example run_boto.py get traces  alibaba_clusterdata_v2018/batch_instance.csv


# contributors: Amin M. Zadeh, Mania Abdi, Peter Desnoyers, Orran Krieger

import boto3
import boto.s3.connection
from botocore.client import Config
import sys
import hdfs
import json
import random
import utils.inputs as inputs
import subprocess
import math

class ObjectStore:
    def __init__(self):
        #Kaizen
        access_key = "4c3da79d02bb4a2e8f04495bff5203b2"
        secret_key = "b7bd5b4abcd34ca8a94e93e8b76527f4"
        s3a_endpoint_url="https://127.0.0.1:8000"
        is_secure = False
        self.s3client = boto3.client(service_name='s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key,
                    endpoint_url=s3a_endpoint_url)
        self.s3conn = boto.connect_s3(aws_access_key_id=access_key, aws_secret_access_key=secret_key, host=s3a_endpoint_url,
                calling_format = boto.s3.connection.OrdinaryCallingFormat())
        hdfs_endpoint_url = 'http://10.0.0.9:50070'
        hdfs_user = 'ubuntu'
        fpath = '/home/centos/ceph-prefetching/Kariz/code/utils/'
        #fpath = '/home/xun/Kariz/code/utils/'
        #fpath = '/home/mania/Northeastern/MoC/Kariz/code/utils/'
        self.hdfsclient = hdfs.InsecureClient(hdfs_endpoint_url, user=hdfs_user)
        self.tpch_metadata, self.tpcds_metadata = inputs.prepare_tpc_metadata(fpath)
        self.tpch_runtime, self.tpcds_runtime = inputs.prepare_tpc_runtimes(fpath)



    def s3a_get_dataset_size(self, url):
        object_size = 0
        url = url.replace(url.split('/')[-1], '').replace('s3a', 's3')
        #print('s3a_get_dataset_size', url)
        p = subprocess.Popen(["s3cmd", "ls", url], stdout=subprocess.PIPE)
        ls_output = p.communicate()[0].decode("utf-8").split('\n')
        for o in ls_output:
           ol = o.split(' ')
           if len(ol) <= 3: 
              break
           ol.remove('')
           object_size += int(ol[2])
        #   print(ol)
        blocksize = 1024*1024 
        return math.ceil(object_size/blocksize)

    def get_datasetsize_tpc_url(self, url):
        dataset_size = 0
        url = url.replace('s3a://data/', '')  # remove s3a:// from the url
        dataset_name = url.split("/")[0].split('-')[0]
        dataset_size = url.split("/")[0].split('-')[1]
        obj_name= url.split("/")[1]
        if dataset_name == 'tpch':
            return self.tpch_metadata[dataset_size][obj_name], obj_name
        if dataset_name == 'tpcds':
            return self.tpcds_metadata[dataset_size][obj_name], obj_name
        return dataset_size, url.split('/')[-1]


    def get_datasetsize_from_url(self, url):
        dataset_size = 0
        if url.startswith("hdfs") or url.startswith('/'):
            return dataset_size, url.split('/')[-1]
        if 'tpc' in url:
           return self.get_datasetsize_tpc_url(url)

        if url.startswith("s3a"):
            dataset_size = self.get_datasetsize_from_s3a_url(url)
        elif url.startswith("alluxio"):
            dataset_size = self.get_datasetsize_from_alluxio_url(url)
        elif url.startswith("hdfs"):
            return dataset_size, url.split('/')[-1]
        else:
            dataset_size = self.get_datasetsize_from_hdfs_url(url)
        return dataset_size;

    def get_datasetsize_from_hdfs_url(self, url):
        dataset_size = 0
        status = self.hdfsclient.status(url)
        if status['type'] == 'DIRECTORY':
            files = self.hdfsclient.list(url, status=True)
            dataset_size = sum(f[1]['length'] for f in files)
        elif status['type'] == 'FILE':
            dataset_size = status['length']
        return dataset_size;

    def get_datasetsize_from_s3a_url(self, url):
        dataset_size = 0
        url = url.replace('s3a://', '')  # remove s3a:// from the url
        bucket_name = url.split("/")[0]
        obj_name=url.replace(bucket_name+'/','')
        bucket_meta = self.s3client.list_objects(Bucket=bucket_name, Prefix=obj_name)['Contents']
        dataset_size = sum(f['Size'] for f in bucket_meta)
        return dataset_size;

    def get_datasetsize_from_alluxio_url(self, url):
        dataset_size = 0
        return dataset_size;


def test_object_store_s3a():
    objs = ObjectStore()
    objs.get_datasetsize_from_s3a_url("s3a://data/tpch-1G/part/")
