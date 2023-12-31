import json

from google.cloud import storage
from mongodb import *
from pyspark.sql import Row, SparkSession

from user_definition import *


def retreive_gee_data(spark, bucket_name, date):
    gee_data = (
        spark.read.format("csv")
        .option("header", True)
        .load(f"gs://{bucket_name}/{date}/field.csv") 
    )
    return gee_data


def return_json(service_account_key_file,
                bucket_name,
                blob_name):
    storage_client = storage.Client.from_service_account_json(service_account_key_file)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    json_str = blob.download_as_string().decode("utf8")
    json_data = json.loads(json_str)
    return json_data

def add_json_data_to_rdd(rdd, json_data, json_field_name):
    rdd_dict = rdd.asDict()
    rdd_dict[json_field_name] = json_data
    id = rdd_dict['id']
    rdd_dict['_id'] = id
    rdd_dict.pop('id', None)
    
    return rdd_dict

def insert_aggregates_to_mongo():
    spark = SparkSession.builder.getOrCreate()
    conf = spark.sparkContext._jsc.hadoopConfiguration()
    conf.set("google.cloud.auth.service.account.json.keyfile", service_account_key_file)
    conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")

    image_df = retreive_gee_data(spark, bucket_name, three_days_ago)


    # aggregates = image_df.rdd.map(
    #     lambda x: add_json_data_to_rdd(x, weather_json, json_field_name)
    # ) 

    mongodb = MongoDBCollection(mongo_username,
                                mongo_password,
                                mongo_ip_address,
                                database_name,
                                collection_name)

    for aggregate in aggregates.collect():
        print(aggregate)
        mongodb.insert_one(aggregate)

if __name__=="__main__":
    insert_aggregates_to_mongo()
