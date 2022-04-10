import pandas as pd
import boto3
import json
import configparser
from datetime import datetime
import warnings
import os
warnings.filterwarnings("ignore")

#In AWS cosole create new IAM user and then download keys and put into dcwh.cfg

#Read in AWS Acccount info and Redshift names to implement.
cwd = os.getcwd()
keys_file = os.path.join(cwd, 'keys.json')
keys = json.load(open(keys_file))

KEY                    = keys['AWS']['KEY']
SECRET                 = keys['AWS']['SECRET']

DWH_CLUSTER_TYPE       = keys["DWH"]["DWH_CLUSTER_TYPE"]
DWH_NUM_NODES          = keys["DWH"]["DWH_NUM_NODES"]
DWH_NODE_TYPE          = keys["DWH"]["DWH_NODE_TYPE"]

DWH_CLUSTER_IDENTIFIER = keys["DWH"]["DWH_CLUSTER_IDENTIFIER"]
DWH_DB                 = keys["DWH"]["DWH_DB"]
DWH_DB_USER            = keys["DWH"]["DWH_DB_USER"]
DWH_DB_PASSWORD        = keys["DWH"]["DWH_DB_PASSWORD"]
DWH_PORT               = keys["DWH"]["DWH_PORT"]

roleArn = keys["Redshift"]["RoleARN"]

redshift = boto3.client(service_name='redshift',
                       region_name="us-west-2",
                       verify = False,
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                       )


ec2 = boto3.resource(service_name='ec2',
                       region_name="us-west-2",
                       verify = False,
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                       )



# Create and Deploy a Redshift Cluster
# Create and Deploy a Redshift Cluster
response = redshift.create_cluster(       
    #Cluster Specs
    ClusterType=DWH_CLUSTER_TYPE,
    NodeType=DWH_NODE_TYPE,
    NumberOfNodes=int(DWH_NUM_NODES),

    #Identifiers & Credentials
    DBName=DWH_DB,
    ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
    MasterUsername=DWH_DB_USER,
    MasterUserPassword=DWH_DB_PASSWORD,
    PubliclyAccessible=True,
    ClusterSubnetGroupName ='dwh-us-west-2',
    #Roles (for s3 access)
    IamRoles=[roleArn]  
)

while redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]['ClusterAvailabilityStatus'] != 'Available':
    print(datetime.now(),end = '\r',flush = True)
    
print('Cluster Available:',str(datetime.now()))

# Get Clutser Properties and save for later use when connecting and doing ETL
myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
DWH_ENDPOINT = myClusterProps['Endpoint']['Address']
DWH_ROLE_ARN = myClusterProps['IamRoles'][0]['IamRoleArn']

print("Redhsift Cluster '{}' Created.".format(DWH_CLUSTER_IDENTIFIER))

with open('dwh_access_params.txt','w') as params:
    params.write('Endpoint:' + DWH_ENDPOINT + '\n')
    params.write('Role:' + DWH_ROLE_ARN + '\n')




