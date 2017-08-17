import os 
import shutil
import uuid
import time 
import csv

import pandas as pd
import boto3

from .. import PG_CONNECTION
from ..modeling.model_labeling import score_unlabeled_tweets
from ..utils import prepare_tweets_for_modeling


def load_resources_from_server(redownload=True):
    
    result = PG_CONNECTION.execute("select filename from tweet_data.loaded_files;")
    loaded_files = [x['filename'] for x in result]
    bucket = boto3.resource('s3').Bucket(os.environ['S3_BUCKET_NAME'])
    raw_directory = "tmp-resource-"+str(uuid.uuid4())
    for x in os.listdir():
        if "tmp-resource" in x and os.path.isdir(x):
            shutil.rmtree(x) 
    os.mkdir(raw_directory)
    for i in bucket.objects.filter(Prefix='{}/downloads'.format(os.environ['PROJECT_NAME'])):
        filename = i.key.split("/")[-1]
        if filename not in loaded_files and filename[-3:] == 'csv':
            download_dest = "{}/{}".format(raw_directory, filename)
            bucket.download_file(i.key, download_dest)

    return None


def get_available_tweet_csvs():

    result = PG_CONNECTION.execute("select filename from tweet_data.loaded_files;")
    loaded_files = [x['filename'] for x in result]

    resource_directories = [x for x in os.listdir() if "tmp-resource" in x]
    if len(resource_directories) == 1:
        download_directory = resource_directories[0]
        available_tweet_csvs = [f for f in os.listdir(download_directory) 
                                    if os.path.isfile(os.path.join(download_directory, f)) and 
                                        f not in loaded_files]
        return available_tweet_csvs, download_directory

    else:
        raise ValueError ("tmp-resources error")

def get_csv_copy_statement(table, filename):
    statement = """COPY {}
            FROM '{}' 
            WITH (FORMAT CSV, HEADER TRUE);""".format(table, filename)

    return statement


def process_tweet_csv(tweet_csv_filename, tweet_csv_file_directory):
    '''
    Add the tweets to the tweet_data schema
    '''

    tweetcsv_filepath = os.path.join(tweet_csv_file_directory, tweet_csv_filename)
    tweetcsv_file = open(tweetcsv_filepath)
    tweet_frame = pd.DataFrame.from_csv(tweetcsv_file, index_col=None)

    tweet_csv_filepath2 = os.path.join(os.path.dirname(__file__)+"/../../", tweetcsv_filepath)
    records_copy_statement = get_csv_copy_statement("tweet_data.scraping_raw_records", tweet_csv_filepath2)
    PG_CONNECTION.execute(records_copy_statement)
    
    file_name = tweet_csv_filename
    file_created = tweet_csv_filename.split("_")[1].replace(".csv", "")
    file_records = len(tweet_frame)
    print("adding {} records to db...".format(file_records))

    PG_CONNECTION.execute(
        """INSERT INTO tweet_data.loaded_files (filename, file_created, count_records) 
                VALUES ('{}', '{}', {});""".format(file_name, file_created, file_records))

    return "Success"


def process_for_label_pipeline():

    insertion_script_1 = open(os.path.join(os.path.dirname(__file__), "sqlscripts/label_data_pipeline.sql")).read()
    PG_CONNECTION.execute(insertion_script_1)

    tweets_df = pd.read_sql("select * from label_data.unique_tweets", PG_CONNECTION)
    original_columns = list(tweets_df.columns.values) + ['priority']
    
    prepare_tweets_for_modeling(tweets_df)
    tweets_df['priority'] = score_unlabeled_tweets(tweets_df)

    tweets_df.drop([x for x in tweets_df.columns.values if x not in original_columns], axis=1, inplace=True)

    temporary_filepath = os.path.join(os.path.dirname(__file__), "staging_data.csv")
    tweets_df.to_csv(temporary_filepath, index=False, quoting=csv.QUOTE_ALL)
    
    pipeline_copy_statement = get_csv_copy_statement("label_data.tweets_in_pipeline", temporary_filepath)
    PG_CONNECTION.execute(pipeline_copy_statement)

    PG_CONNECTION.execute("""
        insert into label_data.tweets_not_labeled
        (select *, False 
        from label_data.tweets_in_pipeline
        where id not in 
        (select id from label_data.tweets_labeled))""")

    return 


def run_loading_process():

    # load_resources_from_server()

    available_files, resource_directory = get_available_tweet_csvs()
    print(available_files)
    for file in available_files[:10]:
        print("adding file: {}".format(file))
        process_tweet_csv(file, resource_directory)

    process_for_label_pipeline()


