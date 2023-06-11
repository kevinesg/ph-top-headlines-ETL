import os
from datetime import datetime
import requests
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket

from google.cloud import storage


parameters = {
    'country' : 'ph',
    'project_id' : 'kevinesg-ph-news',
    'bucket_name' : 'kevinesg-ph-news-bucket'
}

@flow(name='web to GCS ETL', log_prints=True)
def etl_web_to_gcs(
    country:str=parameters['country'],
    project_id:str=parameters['project_id'],
    bucket_name:str=parameters['bucket_name']
) -> None:

    latest_batch:str = 'latest_batch.csv'
    new_data:str = 'new_data.csv'
    latest_batch_path:os.path = os.path.join('data', latest_batch)
    new_data_path:os.path = os.path.join('data', new_data)


    df_raw:pd.DataFrame = extract_from_api(country=country)
    upload_to_gcs(
        project_id=project_id,
        bucket_name=bucket_name, 
        latest_batch_path=latest_batch_path, 
        new_data_path=new_data_path, 
        df=df_raw
    )

    return




@task(log_prints=True)
def extract_from_api(country:str) -> pd.DataFrame:
    with open('credentials/mediastack_apikey.txt', 'r') as f:
        api_key:str = f.readline()

    url:str = 'http://api.mediastack.com/v1/news'

    params = {
        'access_key': api_key,
        'countries': country,
        'language': 'en',
        'sort': 'published_desc',
        'limit': 100,
    }

    response = requests.get(url, params=params)
    data:list = response.json()['data']
    df:pd.DataFrame = pd.json_normalize(data)

    return df




@task(log_prints=True)
def upload_to_gcs(
    project_id:str, bucket_name:str, latest_batch_path:os.path, new_data_path:os.path, df:pd.DataFrame
) -> None:

    gcs_bucket = GcsBucket(bucket=bucket_name)

    # create GCS bucket if it does not exist yet
    client = storage.Client()
    if not client.bucket(bucket_name).exists():
        gcs_bucket.create_bucket(project=project_id)
        bucket = client.bucket(bucket_name)
        df_new:pd.DataFrame = df
    else:
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(latest_batch_path)
        if blob.exists():
            df_prev:pd.DataFrame = pd.read_csv(f'gs://{bucket_name}/{latest_batch_path}')
            df_new:pd.DataFrame = df[~df['url'].isin(df_prev['url'])]
        else:
            df_new:pd.DataFrame = df
    
    bucket.blob(latest_batch_path).upload_from_string(df.to_csv(index=False), 'text/csv')
    bucket.blob(new_data_path).upload_from_string(df_new.to_csv(index=False), 'text/csv')

    return




if __name__ == '__main__':
    api_to_local_etl()