#!/usr/bin/python
import pandas as pd
import math
import numpy as np
import csv
import json

blocksize = 1024*1024

inputs = {'a': 326, 'b': 250, 'c': 250, 'd' : 100
          , 'aa': 700, 'ab': 350, 'ac': 1400, 'ad': 120, 'f': 150, 'g':200, 'h':200
          , 'a1': 160, 'a2': 160, 'a3': 160, 'a4': 160
          , 'b1': 160, 'b2': 160, 'b3': 160, 'b4': 160
          , 'na': 2, 'li': 1449, 'p': 272, 'sup': 200, 'or': 326, 'cus': 46
          , 'c2': 50, 're': 120, 'b7': 2, 'd7': 446, 'c7': 328 
          , 'b23': 400, 'c23': 300, 'd23': 30, 'e23': 1300, 'e4': 262, 'f4': 326
          , 'a5': 46, 'b5': 200, 'b6': 50, 'c6': 226, 'd6': 300, 'li2': 1449
          , 'a13': 120, 'b13': 200, 'a14': 250, 'a16': 449, 'b16': 250, 'c16': 300
          , 'd16':185, 'e16': 250, 'a18': 400, 'b18': 146, 'a19': 4000, 'b19': 3000, 'c19':2500
          , 'wc8G':8000, 'wc3G':3000, 'wc4G': 4000, 'wc5G': 5000
          , 'a20': 80, 'a21': 120, 'b20': 120, 'b21': 160, 'c20': 160, 'c21': 160
          , 'd20': 200, 'd21': 240, 'e20': 100, 'e22': 100, 'd25': 200, 'c26': 240
          , 'a27': 240}  


def prepare_tpc_metadata(fpath):
   tpch_inputs = {'1G' : {}, '2G' : {}, '4G' : {}, '8G' : {}, '16G' : {},
               '32G' : {}, '48G' : {}, '64G' : {}, '80G' : {},
               '100G' : {}, '128G' : {}, '150G' : {}, '200G' : {},
               '256G' : {}, '300G' : {}}

   tpcds_inputs = {'1G' : {}, '2G' : {}, '4G' : {}, '8G' : {}, '16G' : {},
               '32G' : {}, '48G' : {}, '64G' : {}, '80G' : {},
               '100G' : {}, '128G' : {}, '150G' : {}, '200G' : {},
               '256G' : {}, '300G' : {}}

   df = pd.read_csv(fpath + 'inputs.csv')
   df['size'] = pd.to_numeric(df['size'])
   df['n_blocks'] = (df['size']/blocksize).apply(np.ceil).astype(int)
   working_set_size = df['n_blocks'].sum() # 393GB 
   for index, row in df.iterrows():
      dataset_meta = row['name'].split('_')
      query = dataset_meta[0]
      ds_sz = dataset_meta[1]
      input_name = row['name'].replace(query+'_'+ds_sz+'_', '')
    
      if query == 'tpcds':
         tpcds_inputs[ds_sz][input_name] = row['n_blocks']         
      elif query == 'tpch':
         tpch_inputs[ds_sz][input_name] = row['n_blocks']

   return tpch_inputs, tpcds_inputs;


def prepare_tpc_runtimes(fpath):
   tpch_runtimes = {}
   tpcds_runtimes = {}
   with open(fpath + 'tpchjobs.csv') as csvfile:
        readCSV = csv.reader(csvfile, delimiter=',')
        headers = next(readCSV, None)
        for row in readCSV:
           query = row[0]
           dataset = row[4]
           jobid = int(row[2])
           rorc = row[6]
           runtime = int(row[3])
           if query not in tpch_runtimes:
              tpch_runtimes[query]={}
           if dataset not in tpch_runtimes[query]:
              tpch_runtimes[query][dataset] = {}
           if jobid not in tpch_runtimes[query][dataset]:
              tpch_runtimes[query][dataset][jobid] = {}
           if rorc == 'R':
              tpch_runtimes[query][dataset][jobid]['remote'] = runtime
           elif rorc == 'C': 
              tpch_runtimes[query][dataset][jobid]['cached'] = runtime
   return tpch_runtimes, tpcds_runtimes
