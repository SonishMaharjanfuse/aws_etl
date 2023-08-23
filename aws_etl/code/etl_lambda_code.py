import json
import urllib3
import pandas as pd
import boto3
import psycopg2
import os

s3 = boto3.client("s3")

def lambda_handler(event, context):
    # TODO implement
    
    http = urllib3.PoolManager()

    url = "https://ott-details.p.rapidapi.com/advancedsearch"
    
    headers = {
	"X-RapidAPI-Key": "a6e2bda748msh81f7e71a9b3a768p13cb68jsnbcce107d6ae0",
	"X-RapidAPI-Host": "ott-details.p.rapidapi.com"
    }
    
    # GET DATA
    response = http.request("GET", url, headers=headers)
    
    data = response.data.decode("utf-8")
    
    data_dict = json.loads(data)
    
    # print(data_dict)
    
    bucket_name = 'apprentice-training-ml-dev-sonish-raw-data'
    key = "/raw-data/ott_raw_data.json"
    
    
    # Save raw data to bucket
    # s3.put_object(Body=response.data, Bucket=bucket_name ,Key=key)
    # print(response)
        
        
        
    df = pd.DataFrame(data_dict)    
    results = df['results']
    results = dict(results)
    df = pd.DataFrame(results).transpose()
    df.drop(['imageurl','synopsis'], inplace = True, axis=1)
    
    cleaned_data = df.to_json(orient="records")
    
    bucket_name = "apprentice-training-ml-dev-sonish-cleaned-data"
    object_key = "cleaned_ott_data.json"
    
    # s3.put_object(
    #     Bucket=bucket_name,
    #     Key=object_key,
    #     Body=cleaned_data.encode("utf-8")
    # )
    
    
    ### RDS
    try:
      conn = psycopg2.connect(
      host = os.environ['host'],
      database = os.environ['database'],
      user = os.environ['user'],
      password = os.environ['password'])
      print('Connected to database')
    except Exception as e:
        print("Connection Failed")
        print(e)
        
    cursor = conn.cursor()
    table_name = "etl_sonish_ott_details"
    
    
    query =f"""
                   Create table {table_name}(
                      genre varchar(100),
                      imdbid varchar(100) primary key,
                      title varchar(100),
                      imdbrating float,
                      released int,
                      type varchar(100)
                   )
                   """
    cursor.execute(query)
    conn.commit()

    data_to_insert = [tuple(row) for row in df.values]

    insert_query = f"""
    INSERT INTO {table_name}
    (genre,imdbid,title, imdbrating,released,type)
    VALUES (%s, %s, %s, %s, %s, %s)
    """
    cursor.executemany(insert_query, data_to_insert)
    
        # Commit the transaction
    conn.commit()
    
    
    return {
        'statusCode': 200,
        'body': json.dumps('success')
    }

