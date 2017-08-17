import datetime
import os 
import shutil
import subprocess
import uuid

import pandas as pd
from sqlalchemy import create_engine
import boto3

from .. import PG_ENGINE, PG_CONNECTION
from ..modeling.model_labeling import score_unlabeled_tweets
from ..utils import prepare_tweets_for_modeling

execute = True


def load_resources_from_server():
	
	result = PG_CONNECTION.execute("select filename from tweet_data.loaded_files;")
	loaded_files = [x['filename'] for x in result]
	bucket = boto3.resource('s3').Bucket(os.environ['S3_BUCKET_NAME'])
	raw_directory = "tmp-resource-"+str(uuid.uuid4())
	[shutil.rmtree(x) for x in os.listdir() if "tmp-resource" in x and os.path.isdir(x)]
	os.mkdir(raw_directory)
	for i in bucket.objects.filter(Prefix='{}/downloads'.format(os.environ['PROJECT_NAME'])):
		if i.key.split("/")[-1] not in loaded_files and i.key[-3:] == 'csv':
			bucket.download_file(i.key, "{}/{}".format(raw_directory, i.key.split("/")[-1]))

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
	elif len(resource_directories) == 0:
		print("There is no tmp-resource folder")
		return [], None
	else:
		raise ValueError ("There are multiple tmp-resources folders")


def process_tweet_csv(tweet_csv_filename, tweet_csv_file_directory):
	'''
	Add the tweets to the tweet_data schema
	'''

	tweetcsv_filepath = os.path.join(tweet_csv_file_directory, tweet_csv_filename)
	tweetcsv_file = open(tweetcsv_filepath)
	tweet_frame = pd.DataFrame.from_csv(tweetcsv_file, index_col=None)
	
	

	# raw records input
	if execute:
		tweet_csv_filepath2 = os.path.join(os.path.dirname(__file__)+"/../../", tweetcsv_filepath)
		PG_CONNECTION.execute("""
			COPY tweet_data.scraping_raw_records 
			FROM '{}' 
			WITH (FORMAT CSV, HEADER TRUE);""".format(tweet_csv_filepath2))

	# add to list of files
	
	filename = tweet_csv_filename
	file_created = tweet_csv_filename.split("_")[1].replace(".csv", "")
	file_records = len(tweet_frame)

	if execute:
		PG_CONNECTION.execute(
			"""INSERT INTO tweet_data.loaded_files (filename, file_created, count_records) 
					VALUES ('{}', '{}', {});""".format(filename, file_created, file_records))

	process_for_label_pipeline(tweet_frame)

	return "Success"


def process_for_label_pipeline(tweets_df):
	'''
	Add the entries to the label_data pipeline
	'''
	tweets_df.sort_values("tweet_created_at", inplace=True)
	tweets_df.drop_duplicates("tweet_status", inplace=True)

	original_columns = list(tweets_df.columns.values) + ['priority']
	prepare_tweets_for_modeling(tweets_df)
	tweets_df['priority'] = score_unlabeled_tweets(tweets_df)
	tweets_df.drop([x for x in tweets_df.columns.values if x not in original_columns], axis=1, inplace=True)

	if execute:
		tweets_df.to_sql("staging_tweet_data", PG_ENGINE, 
								schema="label_data", if_exists="fail", index=False)

	if execute:
		# Load and execute insertion script
		insertion_script_path = os.path.join(os.path.dirname(__file__), "sqlscripts/label_data_load_to_pipeline.sql")
		insertion_script = open(insertion_script_path).read()
		PG_CONNECTION.execute(insertion_script)

	return 


def run_loading_process():

	load_resources_from_server()

	available_files, resource_directory = get_available_tweet_csvs()
	print(available_files)
	for file in available_files:
		print("adding file: {}".format(file))
		process_tweet_csv(file, resource_directory)


