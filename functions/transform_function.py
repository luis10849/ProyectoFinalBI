from decouple import config
import pandas as pd
import boto3
import json

ACCESS_KEY = config('ACCESS_KEY')
SECRET_ACCESS_KEY = config('SECRET_ACCESS_KEY')

def transform_data():
     s3_client = boto3.client('s3', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_ACCESS_KEY,region_name='us-east-1')
     s3_object = s3_client.get_object(Bucket='data-bucket-etl-test',Key='public/extraction_data.json')
     body = s3_object['Body'].read()
     data_json = json.loads(body)

     goals_for = {
         'total': data_json['goals']['for']['total']['total'],
         'average': data_json['goals']['for']['average']['total'],
     }

     fixtures = {
         'played': data_json['fixtures']['played']['total'],
         'wins': data_json['fixtures']['wins']['total'],
         'draws': data_json['fixtures']['draws']['total'],
         'loses': data_json['fixtures']['loses']['total'],
     }

     transform_data = {
         'id': data_json['team']['id'],
         'name': data_json['team']['name'],
         'logo': data_json['team']['logo'],
         'goals': goals_for,
         'fixtures': fixtures 
     } 

     load_json(transform_data)


def load_json(data):
    try:
        # save to s3
        destination_s3_bucket = 'data-bucket-etl-test'
        upload_file_key = 'public/transform_data'
        filepath =  upload_file_key + ".json"
        #
        s3_client = boto3.client('s3', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_ACCESS_KEY,region_name='us-east-1')
        response = s3_client.put_object(
            Bucket=destination_s3_bucket, Key=filepath, Body=(bytes(json.dumps(data).encode('UTF-8')))
        )

        status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

        if status == 200:
            print(f"Successful S3 put_object response. Status - {status}")
        else:
            print(f"Unsuccessful S3 put_object response. Status - {status}")
        print("Data imported successful")
           
    except Exception as e:
        print("Data load error: " + str(e))

transform_data()