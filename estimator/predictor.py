#!/usr/bin/python
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import sklearn as sk
import utils.hadoop as hadoop
import json

class Predictor:
    def __init__(self):
        self.name = 'Predictor class'
        self.stats = None
        
    def predit_runtime(self, conf):
        print(conf)
        
    def load_statistics(self, fpath):
        self.stats = pd.read_csv(fpath)
        self.stats['runtime (sec)'] = self.stats['runtime']/1000
        self.stats['avgMapTime (sec)'] = self.stats['avgMapTime']/1000
        print(self.stats.columns)
        #print(self.stats[['type', 'inputs', 'runtime']])
        
    def visualize_statistics(self):
        if self.stats is None: return
        sns.set(style='whitegrid', context='notebook')
        cols = ['runtime (sec)', 'avgMapTime (sec)']
        sns.pairplot(self.stats[cols], size=2.5)
        plt.show()
        
    def visualize_corrolation(self):
        if self.stats is None: return
        jobs_grpby_type = self.stats.groupby(['type'])
        for index, group in jobs_grpby_type:
            print(index, len(jobs_grpby_type)) 
            fig, ax = plt.subplots(figsize=(6, 6))
            sns.set(style='whitegrid', context='notebook')
            cols = ['datasize', 'runtime', 'avgMapTime', 'avgReduceTime', 'avgShuffleTime', 'avgMergeTime']
            cm = np.corrcoef(group[cols].values.T)
            sns.set(font_scale=1)
            hm = sns.heatmap(cm,
                            cbar=True,
                            annot=True,
                            square=True,
                            fmt='.2f',
                            annot_kws={'size': 15},
                            yticklabels=cols,
                            xticklabels=cols, ax=ax)
            ax.xaxis.set_ticks_position('top')
            plt.yticks(rotation=0) 
            plt.xticks(rotation=-45) 
            plt.title(index, y=-0.1)
            plt.show()
            fig.savefig('fig_corrolation_' + index + '.pdf', format='pdf', dpi=200)
            fig.savefig('fig_corrolation_' + index + '.png', format='png', dpi=200)

        
        
            
fpath = './statistics2.csv'
inputs = './inputs'
pred = Predictor()
pred.load_statistics(fpath)
pred.visualize_statistics()
pred.visualize_corrolation()

