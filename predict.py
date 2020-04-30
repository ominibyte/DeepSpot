from __future__ import print_function
import pandas as pd
import numpy as np
from tqdm import tqdm
print("DATA PREPROCESSING...")
import pickle

#  libaray Loading and setting for multi-processing

from sklearn.metrics import classification_report
from sklearn.model_selection import train_test_split,KFold
from sklearn.metrics import r2_score,roc_curve, auc,confusion_matrix,f1_score,precision_score,recall_score,accuracy_score,mean_squared_error,roc_auc_score,mean_absolute_error
from xgboost import XGBClassifier,XGBRegressor
from sklearn.utils.class_weight import compute_class_weight
from scipy.stats import randint, expon, uniform
from sklearn.model_selection import RandomizedSearchCV
import xgboost

import datetime
import time
import os
import os.path
from joblib import Parallel, delayed
import multiprocessing
import time
import re
import shutil
from datetime import datetime
import socket
import pickle
from multiprocessing import Process
import math
import warnings
from itertools import combinations
import gc

# SELECT = 'discrete1/SelectedPatientConditionsL.xlsx'
if socket.gethostbyname(socket.gethostname()) == '169.48.97.149':
    SERVER = 1
elif socket.gethostbyname(socket.gethostname()) == '169.48.97.152':
    SERVER = 2
elif socket.gethostbyname(socket.gethostname()) == '169.61.92.249':
    SERVER = 3
elif socket.gethostbyname(socket.gethostname()) == '169.61.92.251':
    SERVER = 4
elif socket.gethostbyname(socket.gethostname()) == '132.206.55.112':
    SERVER = 5





# MULTIPROCESSING CONTROL

# deicide number of workers to be used for multiprocessing
if SERVER == 3:
    WORKERS = 94
elif SERVER == 0:
    WORKERS = 4
elif SERVER == 5:
    WORKERS = 30
else:
    WORKERS = -1


# Helper function for apply_by_multiprocessing
def _apply_df(args):
    df, func, num, kwargs = args
    return num, df.apply(func, **kwargs)


# multiprocessing function for each rows of pandas dataframe
def apply_by_multiprocessing(df, func, **kwargs):
    workers = kwargs.pop('workers')
    pool = multiprocessing.Pool(processes=workers)
    result = pool.map(_apply_df, [(d, func, i, kwargs) for i, d in enumerate(np.array_split(df, workers))])
    pool.close()
    result = sorted(result, key=lambda x: x[0])
    return pd.concat([i[1] for i in result])


# Helper function for applyParallel
def temp_func(args):
    func, name, group, kwargs = args
    return func(group, **kwargs), name


# multiprocessing function for groupby
def applyParallel(dfGrouped, func, **kwargs):
    retLst, top_index = zip(
        *Parallel(n_jobs=WORKERS)(delayed(temp_func)((func, name, group, kwargs)) for name, group in dfGrouped))
    return pd.concat(retLst, keys=top_index)


# END


def inputConverter(dataS, featureLimit = 7):
    dataS = dataS.sort_values('Timestamp').reset_index(drop = True)
    i = len(dataS)
    featureHeader = ['price_-' + str(featureLimit - ii) for ii in range(featureLimit)]
    features = pd.DataFrame(np.expand_dims(dataS['SpotPrice'].iloc[i-featureLimit: i].values.astype(np.float32),axis = 0),columns = featureHeader)
    D = features
    D['AvailabilityZone'] = dataS['AvailabilityZone'].iloc[0]
    D['ProductDescription'] = dataS['ProductDescription'].iloc[0]
    return D

def firstLevel(s,limitHeader,R):
    allRegion = s['AvailabilityZone'].unique()
    allOS = s['ProductDescription'].unique()
    allInstance = s['InstanceType'].unique()

    OS = []
    RE = []
    INS = []
    FLAG = []
    R = []
    for re in allRegion:
        for os in allOS:
            for ins in allInstance:
                ss = s[(s['AvailabilityZone'] == re) & (s['ProductDescription'] == os) & (s['InstanceType'] == ins)]
                OS.append(os)
                RE.append(re)
                INS.append(ins)
                if len(ss) == 0:
                    FLAG.append(-1)
                    r = {}
                    for lH in limitHeader:
                        r[lH] = None
                    R.append(r)
                else:
                    if ss['SpotPrice'].nunique() <= 1:
                        FLAG.append(0)
                        r = {}
                        for lH in limitHeader:
                            r[lH] = None
                        R.append(r)
                    else:
                        FLAG.append(1)
                        r = {}
                        for lH in limitHeader:
                            pR = MODELS[lH].predict(D[D[lH] > -1][featureHeader].iloc[0:1])
                            if len(pR) > 0:
                                r[lH] = str(pR[0])
                            else:
                                r[lH] = str(-1)
                                
                        R.append(r)
    return pd.DataFrame({'ProductDescription':OS, 'AvailabilityZone':RE, 'InstanceType':INS, 'FLAG':FLAG,'R':R})

with open('sample.json', 'r') as f:
    s = pd.DataFrame(json.load(f))    

s['Timestamp'] = pd.to_datetime(s['Timestamp'])
s = s.drop_duplicates().sort_values(['AvailabilityZone','ProductDescription','InstanceType', 'Timestamp'])
sD = applyParallel(s.groupby(['AvailabilityZone', 'ProductDescription', 'InstanceType']), inputConverter)
featureLimit = 7
checkPrices = [1 + i / 10 for i in range(1,11)]
featureHeader = ['price_-' + str(featureLimit - i) for i in range(featureLimit)]
labelHeader = ['Limit_' + str(cPrice) for cPrice in checkPrices] 
with open('MODELS.p', 'rb') as fp:
    MODELS = pickle.load(fp)
R = {}
for lH in labelHeader:
    R[lH] = list(MODELS[lH].predict(sD[featureHeader]))
r = firstLevel(s,labelHeader,pd.DataFrame(R))

r.to_json(r'expectTime.json',orient='records')