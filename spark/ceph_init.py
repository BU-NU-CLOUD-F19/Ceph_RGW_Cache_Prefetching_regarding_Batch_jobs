import boto3
from botocore.client import Config
import sys
import os

file_list = []
def file_name_listdir(file_dir):
	for file in os.listdir(file_dir):
		file_list.append(file)
		#print(file)

#access_key = "0555b35654ad1656d804"
#secret_key ="h7GhxuBLTrlhVUyxSPUKUV8r/2EI4ngqJxD7iBdBYLhwluN30JaT3Q=="
#access_key="BAPLVLXYE067O2ZCTRT6"
#asecret_key ="MRt915piNShNY1bW6QtQNIjuTH9lMmK4R5DWU0PR"
access_key="123456"
secret_key ="abcdefg"
endpoint_url = "http://10.0.0.13:8000"

client = boto3.client(service_name='s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key,
                    endpoint_url=endpoint_url,
                    use_ssl=False,
                    verify=False,
                    config=Config(signature_version='s3v4'))

bucket_name = sys.argv[1]
direct_name = sys.argv[2]

file_name_listdir(direct_name)

client.create_bucket(Bucket=bucket_name)

for file in file_list:
	print(file)
	f = open(direct_name + file, 'r')
	data = f.read()
	client.put_object(Bucket=bucket_name, Key=file, Body=data)
