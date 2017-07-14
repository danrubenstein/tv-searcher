''' 
Script / function to download csv files and transform into SQL tables
'''

import argparse
import datetime
import json
import os 
import shutil
import subprocess
import time
import uuid

import boto3
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine






def download_files(temporary_output_directory, force_update=True):
	'''
	Download the files from the s3 bucket
	'''
	if force_update or len([x for x in os.listdir() if "tmp-resource" in x]) == 0:
		clean_temporary_resources()
		os.mkdir(temporary_output_directory)
		aws_cmd = "aws s3 cp --recursive s3://{}/{}/downloads ./{}".format(os.environ['S3_BUCKET_NAME'], 
															os.environ['PROJECT_NAME'], 
															temporary_output_directory).split()
		subprocess.call(aws_cmd)


def get_tweets_as_dataframe(download_directory):
	'''
	Transform the json into a pandas dataframe

	Returns that dataframe
	'''
	tweet_csv_files = [f for f in os.listdir(download_directory) if os.path.isfile(os.path.join(download_directory, f))]

	dataframes = []

	for file in tweet_csv_files:

		tweet_frame = pd.DataFrame.from_csv(open(os.path.join(download_directory, file)))
		dataframes.append(tweet_frame)

	return pd.concat(dataframes).reset_index()


def clean_temporary_resources():
	''' 
	Deletes all temporary folder files
	'''
	resource_directories = [x for x in os.listdir() if "tmp-resource" in x and os.path.isdir(x)]
	for dir in resource_directories:
		shutil.rmtree(dir)


def load_resources(update=False):
	
	'''
	Load resources, by downloading if necessary
	'''

	if update:
		raw_directory = "tmp-resource-"+str(uuid.uuid4())
		download_files(raw_directory, force_update=update)
		articles_df = get_tweets_as_dataframe(raw_directory)

	else:
		resource_directories = [x for x in os.listdir() if "tmp-resource" in x]
		if len(resource_directories) == 1:
			articles_df = get_tweets_as_dataframe(resource_directories[0])
		else:
			print("I'm sorry, but there are no available resource directories.")

	return articles_df




if __name__ == "__main__":
	tweet_records = load_resources(update=False)
	print(tweet_records.index)
	connection_string = 'postgresql://{}:{}@localhost:5432/{}'.format(os.environ['PG_USERNAME'], 
		os.environ['PG_PASSWORD'], os.environ['PG_DATABASE'])
	engine = create_engine(connection_string)

	# raw records input
	tweet_records.to_sql("scraping_raw_records", connection_string, 
							schema="tweet_data", if_exists="append", index=False)

	# raw tweets input 
	unique_tweets_columns = ["tweet_id", "tweet_created_at", "tweet_status",
								 "user_id", "user_verified"]

	tweet_records_unique = tweet_records[unique_tweets_columns].drop_duplicates()

	tweet_records_unique.rename(columns = {'tweet_id':'id'}, inplace = True)
	tweet_records_unique.to_sql("scraping_raw_tweets", connection_string, 
								schema="tweet_data", if_exists="append", index=False)
		





