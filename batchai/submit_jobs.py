from azure.common.credentials import ServicePrincipalCredentials
from azure.mgmt.compute import ComputeManagementClient
import azure.mgmt.batchai as batchai
import azure.mgmt.batchai.models as baimodels
import os
import json
import sys

config_file = sys.argv[3]
with open(config_file) as f:
    j = json.loads(f.read())

# Azure service principle login credentials
TENANT_ID = j['TENANT_ID']
CLIENT = j['CLIENT']
KEY = j['KEY']

# Batch AI cluster info
resource_group_name = j['resource_group_name']
subscription_id = str(j['subscription_id'])
cluster_name = j['cluster_name']
location = j['location']
command_line = j['command_line']
std_out_err_path_prefix = j['std_out_err_path_prefix']
config_file_path = j['config_file_path']
node_count = j['node_count']

# job parameters
ts_from = sys.argv[1]
ts_to = sys.argv[2]
device_ids = j['device_ids']
tags = j['tags']
job_name_template = j['job_name']

credentials = ServicePrincipalCredentials(
    client_id=CLIENT,
    secret=KEY,
    tenant=TENANT_ID
)

batchai_client = batchai.BatchAIManagementClient(
    credentials=credentials, subscription_id=subscription_id)
cluster = batchai_client.clusters.get(resource_group_name, cluster_name)

# run an async job for each sensor
for device_id in device_ids:
    for tag in tags:
        job_name = job_name_template.format(device_id, tag)
        custom_settings = baimodels.CustomToolkitSettings(command_line=command_line.format(device_id, tag, ts_from, ts_to, config_file_path))
        print('command line: ' + custom_settings.command_line)
        params = baimodels.job_create_parameters.JobCreateParameters(location=location,
                                                                     cluster=baimodels.ResourceId(
                                                                         cluster.id),
                                                                     node_count=node_count,
                                                                     std_out_err_path_prefix=std_out_err_path_prefix,
                                                                     custom_toolkit_settings=custom_settings
                                                                     )

        batchai_client.jobs.create(resource_group_name, job_name, params)


            
     