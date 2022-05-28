from decouple import config
import psycopg2
import boto3
import json

ACCESS_KEY = config('ACCESS_KEY')
SECRET_ACCESS_KEY = config('SECRET_ACCESS_KEY')

def load_data():
    db_name = config('DB_NAME')
    db_user = config('DB_USER')
    db_pass = config('DB_PASS')
    db_host = config('DB_HOST')
    db_port = config('DB_PORT')
    conn = None
    cur = None
    try:
        conn = psycopg2.connect(
                    host= db_host, 
                    dbname = db_name, 
                    user = db_user,
                    password = db_pass,
                    port = db_port)

        cur = conn.cursor()

        cur.execute('DROP TABLE IF EXISTS Team')

        create_script = ''' CREATE TABLE IF NOT EXISTS Team (
                                id    SERIAL PRIMARY KEY,
                                name  varchar(40) NOT NULL,
                                logo  varchar(255) NOT NULL,
                                goals json,
                                fixtures json
                         )'''
        cur.execute(create_script)

        s3_client = boto3.client('s3', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_ACCESS_KEY,region_name='us-east-1')
        s3_object = s3_client.get_object(Bucket='data-bucket-etl-test',Key='public/transform_data.json')
        body = s3_object['Body'].read()
        data_json = json.loads(body)

        insert_script = 'INSERT INTO Team (name, logo, goals, fixtures) VALUES (%s, %s, %s, %s)'
        insert_value = (data_json['name'],data_json['logo'],json.dumps(data_json['goals']),json.dumps(data_json['fixtures']))
        cur.execute(insert_script, insert_value)

        conn.commit()

    except Exception as error:
        print(error)    
    finally:
        if cur is not None:
            cur.close()
        if conn is not None:
            conn.close()
    

load_data()