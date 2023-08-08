from datetime import date, timedelta
import os

import pandas as pd
from sodapy import Socrata
import ee
import geemap
from google.cloud import storage

os.environ["no_proxy"]="*" # set this for airflow errors. https://github.com/apache/airflow/discussions/24463


geom = ee.Geometry.Polygon(
    [[[-121.80060839949489, 39.493571826452566],
      [-121.7914538287501, 39.49367847877613],
      [-121.79185069768432, 39.507314102557764],
      [-121.79210484603693, 39.507716637866956],
      [-121.7928807598091, 39.50784738301864],
      [-121.80058163080678, 39.50779575894922],
      [-121.80060839949489, 39.493571826452566]]],
    None, False
    )


def create_dir(parent_dir, directory):
    path = os.path.join(parent_dir, directory)
    os.makedirs(path, exist_ok=True)
    
def retrieve_gee_data(gee_token, start_string, end_string, geom):
    service_account = 'gee-auth@tnc-birdreturn-test.iam.gserviceaccount.com'
    credentials = ee.ServiceAccountCredentials(service_account, gee_token)
    ee.Initialize(credentials)
    image = ee.ImageCollection('COPERNICUS/S2_HARMONIZED').select(['B11'])
    start = ee.Date(start_string)
    end = ee.Date(end_string)
    image = image.filterDate(start,end).filterBounds(geom)
    data = geemap.ee_to_numpy(image.first(), region=geom)
    return pd.DataFrame(data.reshape(data.shape[:2]))
    
def write_csv_to_gcs(bucket_name, blob_name, service_account_key_file, df):
    """Write and read a blob from GCS using file-like IO"""
    storage_client = storage.Client.from_service_account_json(service_account_key_file)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    with blob.open("w") as f:
        df.to_csv(f, index=False)
        