import math

def select_tpch_dags7(self):
    tpch8 = self.statistic_df[self.statistic_df['name'] == 'PigLatin:Q9.pig']
    dag_id = tpch8.iloc[0]['DagId']
    runtime = tpch8['runtime'].tolist()
    tpch8_dag = self.dags_byid[dag_id]
    tpch8_dag.add_edge(0, 2, 0)
    for i in range(tpch8_dag.V):
        tpch8_dag.static_runtime(i, runtime[i], runtime[i]//3)
        
        for ins in range(len(tpch8_dag.inputSize[i])):
            tpch8_dag.inputSize[i][ins] = math.ceil(tpch8_dag.inputSize[i][ins]//(1024*1024)) 
    print(tpch8_dag.timeValue)
    print(tpch8_dag.cachedtimeValue)
    print(tpch8_dag.edges)
    print(tpch8_dag.inputSize)
    print(tpch8_dag.inputs)        
    return tpch8_dag

def select_tpchq8_dags(self):
    tpch8 = self.statistic_df[self.statistic_df['name'] == 'PigLatin:Q8.pig']
    dag_id = tpch8.iloc[0]['DagId']
    runtime = tpch8['runtime'].tolist()
    tpch8_dag = self.dags_byid[dag_id]
    tpch8_dag.add_edge(1, 7, 0)
    for i in range(tpch8_dag.V):
        tpch8_dag.static_runtime(i, runtime[i], runtime[i]//3)
        
        for ins in range(len(tpch8_dag.inputSize[i])):
            tpch8_dag.inputSize[i][ins] = math.ceil(tpch8_dag.inputSize[i][ins]//(1024*1024)) 
    print(tpch8_dag.timeValue)
    print(tpch8_dag.cachedtimeValue)
    print(tpch8_dag.edges)
    print(tpch8_dag.inputSize)
    print(tpch8_dag.inputs)        
    return tpch8_dag
