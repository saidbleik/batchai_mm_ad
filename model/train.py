import pyodbc
import numpy as np
from sklearn.svm import OneClassSVM
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline
from azure.storage.blob import BlockBlobService
import pickle
import sys
import json

# query params
device = sys.argv[1]
tag = sys.argv[2]
ts_from = sys.argv[3]
ts_to = sys.argv[4]

# input/output params
config_file = sys.argv[5]
with open(config_file) as f:
    j = json.loads(f.read())

sql_con_string = j['sql_con_string']
sql_query = j['sql_query']
blob_account = j['blob_account']
blob_key = j['blob_key']
blob_container = j['blob_container']

model_name = 'model_{0}_{1}'.format(device, tag)

# get data
cnxn = pyodbc.connect(sql_con_string)
query = sql_query.format(device, tag, ts_from, ts_to)

def get_vals(cursor, n=1000):
    while True:
        results = cursor.fetchmany(n)
        if not results:
            break
        for result in results:
            yield result

cursor = cnxn.cursor()
cursor.execute(query)
vals = [x[0] for x in get_vals(cursor, 1000)]
vals = np.array(vals)

# train model
sc = StandardScaler()
clf = OneClassSVM(nu=0.0001, kernel='rbf', gamma=0.01)
pipe = Pipeline(steps=[('scaler', sc), ('classifier', clf)])
pipe.fit(vals.reshape(-1, 1))

# save model
blob_service = BlockBlobService(
    account_name=blob_account, account_key=blob_key)
blob_service.create_blob_from_bytes(
    blob_container, model_name, pickle.dumps(pipe))
