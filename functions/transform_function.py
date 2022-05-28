from decouple import config
import pandas as pd
import boto3

ACCESS_KEY = config('ACCESS_KEY')
SECRET_ACCESS_KEY = config('SECRET_ACCESS_KEY')

def trasform_data():
     s3_client = boto3.client('s3', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_ACCESS_KEY,region_name='us-east-1')
     s3_object = s3_client.get_object(Bucket='data-bucket-etl-test',Key='public/data.json')
     body = s3_object['Body'].read()
     print(body)


trasform_data()