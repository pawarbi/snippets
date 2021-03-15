# Calculating strength of interaction between variables using sklearn_gbmi

#https://github.com/ralphhaygood/sklearn-gbmi/blob/master/example.ipynb

import numpy as np

import pandas as pd

from sklearn.ensemble import GradientBoostingRegressor

from sklearn_gbmi import *

DATA_COUNT = 10000

RANDOM_SEED = 137

TRAIN_FRACTION = 0.9

np.random.seed(RANDOM_SEED)



xs = pd.DataFrame(np.random.uniform(size = (DATA_COUNT, 3)))
xs.columns = ['x0', 'x1', 'x2']

y = pd.DataFrame(xs.x0*xs.x1 + xs.x2 + pd.Series(0.1*np.random.randn(DATA_COUNT)))
y.columns = ['y']

train_ilocs = range(int(TRAIN_FRACTION*DATA_COUNT))

test_ilocs = range(int(TRAIN_FRACTION*DATA_COUNT), DATA_COUNT)

gbr_1 = GradientBoostingRegressor(random_state = RANDOM_SEED)

gbr_1.fit(xs.iloc[train_ilocs], y.y.iloc[train_ilocs])

h_all_pairs(gbr_1, xs.iloc[train_ilocs])

#function to return dataframe with specified no of top interacting features
def get_interactions(model, df, top_N=10):
    d = h_all_pairs(model, df)
    df = pd.DataFrame(columns=['interaction','h'])
    for key in d.keys():
        cols = str(key[0])+"-"+str(key[1])
        df = (
            
            df.append({'interaction': cols, 
                       'h':d[key]}, 
                        ignore_index=True)
        
              )
         
    return df.sort_values(by='h', ascending=False).nlargest(top_N, 'h').set_index('interaction') 
  
  
get_interactions(gbr_1,xs.iloc[train_ilocs] )
