import json
import pymongo
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from bson.json_util import dumps, loads
import yaml
import boto3
from botocore.exceptions import ClientError

with open ('config.yaml','r') as yamlfile:
    try:
        cfg = yaml.safe_load(yamlfile)
    except yaml.YAMLError as error:
        print(error)

# global variable
connection_string = cfg['mongodb']['connect_string']
s3_bucket = cfg['s3']['Bucket']
s3_file_path = cfg['s3']['file_path']
s3_client = boto3.client(cfg['s3']['client'])

def get_connection():
    client = MongoClient(connection_string)
    try:
        client.admin.command('ismaster')
        print('connect successful')
    except ConnectionFailure:
        print('Server not available')
    return client

def get_collection(client, db_name, collection_name):
    if db_name in client.list_database_names():
        db = client[db_name]
        if collection_name in db.list_collection_names():
            table = db[collection_name]
            print(f'get {collection_name} collection in {db_name} database')
        else:
            raise ValueError("Can't find this collection")
    else:
        print(f'{db_name} have not been in client yet')
    return table

def get_data():

    mongodb_client = get_connection()

    # filter and get jobcategories data
    jobcategories = get_collection(mongodb_client, 'jr', 'jobcategories')
    jobcategories_filter = jobcategories.find(
        {},
        {'_id':1, 'key':1, 'name':1, '__v':1}
    )
    jobcategories_json = json.loads(dumps(jobcategories_filter))
    print('jobcategories json file created')

    # filter and get jobs data
    jobs = get_collection(mongodb_client,'jr','jobs')
    jobs_filter = jobs.find(
        {},
        {'_id':1, 'deadline':1, 'title':1, 'categories':1,'__v':1}
    )
    jobs_json = json.loads(dumps(jobs_filter))
    print('jobs json file created')

    # filter and get users data
    users = get_collection(mongodb_client, 'jr', 'users')
    users_filter = users.find(
        {},
        {'_id':1, 'student':1, 'interestedFields':1, 
        'jobStatus':1, 'degree':1, 'currentStatus':1,
        'gender':1,'city':1}
    )
    users_json = json.loads(dumps(users_filter))
    print('users json file created')

    mongodb_client.close()

    return jobcategories_json, jobs_json, users_json

def upload_s3(data, bucket, key):
    upload_data = json.dumps(data, ensure_ascii=False)
    key = s3_file_path + key
    try:
        s3_client.put_object(Bucket=bucket, Key=key, Body=upload_data)
    except ClientError as e:
        error_message = e.response['Error']['Message']
        print(error_message)

def lambda_handler(event=None, context=None):

    jobcategories_json, jobs_json, users_json = get_data()
    
    # upload jobcategories data
    upload_s3(jobcategories_json, s3_bucket, 'jobcategories.json')
    print('jobcategories data upload s3 successfully')

    #upload jobs data
    upload_s3(jobs_json, s3_bucket, 'jobs.json')
    print('jobs data upload s3 successfully')

    #upload users data
    upload_s3(users_json, s3_bucket, 'users.json')
    print('users data upload s3 successfully')

if __name__ == "__main__":
    lambda_handler()






