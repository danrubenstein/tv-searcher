''' 
Script / function to download csv files and transform into SQL tables
'''

import datetime
import os 
import shutil
import subprocess
import uuid

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine

from ..modeling.model_labeling import score_unlabeled_tweets
from .. import PG_ENGINE, PG_CONNECTION
from ..utils import get_tweet_status_cleaned, get_ordered_tag_tokens

execute = True

def load_resources_from_server():
	
	raw_directory = "tmp-resource-"+str(uuid.uuid4())
	[shutil.rmtree(x) for x in os.listdir() if "tmp-resource" in x and os.path.isdir(x)]
	os.mkdir(raw_directory)
	aws_cmd = "aws s3 cp --recursive s3://{}/{}/downloads ./{}".format(os.environ['S3_BUCKET_NAME'], 
														os.environ['PROJECT_NAME'], 
														raw_directory).split()
	subprocess.call(aws_cmd)

	return raw_directory


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
		tweet_frame.to_sql("scraping_raw_records", PG_ENGINE, 
								schema="tweet_data", if_exists="append", index=False)

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


def process_for_label_pipeline(tweet_df):
	'''
	Add the entries to the label_data pipeline
	'''

	tweet_df.sort_values("tweet_created_at", inplace=True)
	tweet_df.drop_duplicates("tweet_status", inplace=True)

	tweet_df['tweet_status_cleaned'] = tweet_df['tweet_status'].apply(get_tweet_status_cleaned)
	tweet_df['tag_tokens'] = tweet_df['tweet_status_cleaned'].apply(lambda x: len([x for x in x.split() if x[0] in ["#", "@"] and len(x) > 1]))
	tweet_df['tag_tokens_starting'] = tweet_df['tweet_status_cleaned'].apply(lambda x: get_ordered_tag_tokens(x))

	tweet_df['priority'] = score_unlabeled_tweets(tweet_df)

	tweet_df.drop('tweet_status_cleaned', axis=1, inplace=True)

	if execute:
		tweet_df.to_sql("staging_tweet_data", PG_ENGINE, 
								schema="label_data", if_exists="fail", index=False)

	if execute:
		# Load and execute insertion script
		insertion_script_path = os.path.join(os.path.dirname(__file__), "sqlscripts/label_data_load_to_pipeline.sql")
		insertion_script = open(insertion_script_path).read()
		PG_CONNECTION.execute(insertion_script)

	return 


def run_loading_process():

	# load_resources_from_server()

	available_files, resource_directory = get_available_tweet_csvs()
	print(available_files)
	for file in available_files[:3]:
		print("adding file: {}".format(file))
		process_tweet_csv(file, resource_directory)


