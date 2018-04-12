from azure.common.credentials import ServicePrincipalCredentials
from azure.mgmt.compute import ComputeManagementClient
import azure.mgmt.batchai as batchai
import azure.mgmt.batchai.models as baimodels
import os
import json
import sys

config_file = sys.argv[1]
with open(config_file) as f:
    j = json.loads(f.read())

# Azure service principle login credentials
TENANT_ID = j['TENANT_ID']
CLIENT = j['CLIENT']
KEY = j['KEY']

# Batch AI cluster info
resource_group_name = j['resource_group_name']
subscription_id = str(j['subscription_id'])

# job parameters
job_name_prefix = j['job_name_prefix']

credentials = ServicePrincipalCredentials(
    client_id=CLIENT,
    secret=KEY,
    tenant=TENANT_ID
)

batchai_client = batchai.BatchAIManagementClient(
    credentials=credentials, subscription_id=subscription_id)



def deletejobs(prefix):
    for j in batchai_client.jobs.list():
        if j.name.startswith(prefix): 
            if j.execution_state.value=='succeeded' or j.execution_state.value=='failed':
                batchai_client.jobs.delete(resource_group_name, j.name)
        

            
deletejobs(job_name_prefix)    