from decouple import config
import pandas as pd
import requests
import json
import io
import boto3
import time



# enviroment variables
API_HOST = config('API_HOST')
API_KEY = config('API_KEY')
API_URL = config('API_URL')
ACCESS_KEY = config('ACCESS_KEY')
SECRET_ACCESS_KEY = config('SECRET_ACCESS_KEY')

def extract_data():
    url = API_URL
    league_id = 140
    season = 2021
    team_id = 529

    querystring = {"league":league_id,"season":season,"team":team_id}

    headers = {
	    "X-RapidAPI-Host": API_HOST,
	    "X-RapidAPI-Key": API_KEY  
    }

    data = requests.request("GET", url, headers=headers, params=querystring).json()
    date_consult_api = int( time.time() )
    
    if len(data) == 0:
        print('data not found')
    else:
        if len(data['response']):
            league = data['response']['league']
            team = data['response']['team']
            goals = data['response']['goals']
            fixtures = data['response']['fixtures']
            data_dict = {
                'date_load': time.time(),
                'date_consult_api': date_consult_api,
                'fixtures': fixtures,
                'league': league,
                'team': team,
                'goals': goals
            }
            data_df = pd.DataFrame.from_dict(data=data_dict)
            load_json(data_dict)

def load_cvs(df):
    try:
        # save to s3
        destination_s3_bucket = 'data-bucket-etl-test'
        upload_file_key = 'public/extraction_data'
        filepath =  upload_file_key + ".csv"
        #
        s3_client = boto3.client('s3', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_ACCESS_KEY,region_name='us-east-1')
        with io.StringIO() as csv_buffer:
            df.to_csv(csv_buffer, index=False)

            response = s3_client.put_object(
                Bucket=destination_s3_bucket, Key=filepath, Body=csv_buffer.getvalue()
            )

            status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

            if status == 200:
                print(f"Successful S3 put_object response. Status - {status}")
            else:
                print(f"Unsuccessful S3 put_object response. Status - {status}")
            print("Data imported successful")
    except Exception as e:
        print("Data load error: " + str(e))

def load_json(data):
    try:
        # save to s3
        destination_s3_bucket = 'data-bucket-etl-test'
        upload_file_key = 'public/extraction_data'
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


extract_data()