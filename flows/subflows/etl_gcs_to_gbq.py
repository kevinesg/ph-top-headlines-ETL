import os
from datetime import datetime
import pandas as pd
from prefect import flow, task
from prefect_gcp import GcpCredentials
from prefect_gcp.cloud_storage import GcsBucket

from google.cloud import storage, bigquery




parameters = {
    'service_account_creds' : './credentials/ph-news-etl-creds.json',
    'min_date_str' : '2023-06-01 00:00:00+08:00',
    'gcs_bucket_name': 'kevinesg-ph-news-bucket',
    'gbq_project': 'kevinesg-ph-news',
    'gbq_dataset': 'ph_news',
    'gbq_table': 'news_data'
}

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = parameters['service_account_creds']


@flow(name='GCS to GBQ ETL', log_prints=True)
def etl_gcs_to_gbq(
    gcs_bucket_name:str=parameters['gcs_bucket_name'],
    min_date_str:str=parameters['min_date_str'],
    gbq_project:str=parameters['gbq_project'],
    gbq_dataset:str=parameters['gbq_dataset'],
    gbq_table:str=parameters['gbq_table']
) -> None:

    min_date:datetime = pd.to_datetime(min_date_str)

    df_raw:pd.DataFrame = extract_from_gcs(gcs_bucket_name=gcs_bucket_name)
    df_clean:pd.DataFrame = transform_raw_data(min_date=min_date, df=df_raw)
    ingest_to_gbq(gbq_project=gbq_project, gbq_dataset=gbq_dataset, gbq_table=gbq_table, df=df_clean)

    return




@task(log_prints=True)
def extract_from_gcs(gcs_bucket_name:str) -> pd.DataFrame:

    new_data:str = 'new_data.csv'
    new_data_path:os.path = os.path.join('data', new_data)

    storage_client = storage.Client()
    bucket = storage_client.bucket(gcs_bucket_name)
    df:pd.DataFrame = pd.read_csv(f'gs://{gcs_bucket_name}/{new_data_path}')

    return df




@task(log_prints=True)
def transform_raw_data(min_date:datetime, df:pd.DataFrame) -> pd.DataFrame:

    if df.shape[0] == 0:
        return df
        
    # data cleaning and filtering
    df.drop(columns=['language', 'country'], inplace=True)

    df['published_at'] = pd.to_datetime(df['published_at'])
    #df['published_at'] = df['published_at'].dt.tz_localize('UTC').dt.tz_convert('Asia/Manila')
    df['published_at'] = df['published_at'].apply(lambda x: convert_timezone(x, 'Asia/Manila'))
    df.dropna(subset=['published_at'], inplace=True)

    df = df[df['published_at'] >= min_date] # only relevant for the first couple of runs

    return df




@task(log_prints=True)
def ingest_to_gbq(gbq_project:str, gbq_dataset:str, gbq_table:str, df:pd.DataFrame) -> None:

    if df.shape[0] == 0:
        print('[INFO] No new rows to be ingested.')
        return

    bq_client = bigquery.Client(project=gbq_project)

    datasets = list(bq_client.list_datasets())
    dataset_ids = [dataset.dataset_id for dataset in datasets]
    
    if gbq_dataset not in dataset_ids:
        dataset = bq_client.create_dataset(bigquery.Dataset(bq_client.dataset(gbq_dataset)))

    tables = list(bq_client.list_tables(gbq_dataset))
    table_ids = [table.table_id for table in tables]

    schema = [
        bigquery.SchemaField('author', 'STRING', 'NULLABLE', None, None, (), None),
        bigquery.SchemaField('title', 'STRING', 'NULLABLE', None, None, (), None),
        bigquery.SchemaField('description', 'STRING', 'NULLABLE', None, None, (), None),
        bigquery.SchemaField('url', 'STRING', 'NULLABLE', None, None, (), None),
        bigquery.SchemaField('source', 'STRING', 'NULLABLE', None, None, (), None),
        bigquery.SchemaField('image', 'STRING', 'NULLABLE', None, None, (), None),
        bigquery.SchemaField('category', 'STRING', 'NULLABLE', None, None, (), None),
        bigquery.SchemaField('published_at', 'TIMESTAMP', 'NULLABLE', None, None, (), None)
 ]

    if gbq_table not in table_ids:
        bq_client.create_table(bigquery.Table(f'{gbq_project}.{gbq_dataset}.{gbq_table}', schema=schema))

    df.to_gbq(f'{gbq_project}.{gbq_dataset}.{gbq_table}', project_id=gbq_project, if_exists='append')

    return




def convert_timezone(ts, target_tz):
    if ts.tzinfo is None:
        return ts.tz_localize('UTC').tz_convert(target_tz)
    else:
        return ts.tz_convert(target_tz)




if __name__ == '__main__':
    local_to_gbq_etl()