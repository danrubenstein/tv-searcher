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



dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
load_dotenv(dotenv_path)


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

	return pd.concat(dataframes)


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

# def get_spam_retweets(tweet_statuses)
	
# 	'''
# 	Identify original tweets and count number of "retweets"

# 	tweet_statuses is a pandas series
# 	'''

# 	_retweets = []
# 	status_counts = tweet_statuses.value_counts()
	
# 	for status in tweet_statuses:
# 		if status[:2] == "RT":
# 			_retweets.append(0)



if __name__ == "__main__":

	tweets_df = load_resources(update=False)
	
	connection_string = 'postgresql://{}:{}@localhost:5432/{}'.format(os.environ['PG_USERNAME'], 
		os.environ['PG_PASSWORD'], os.environ['PG_DATABASE'])
	engine = create_engine(connection_string)

	tweets_df.to_sql("scraping_raw_records", connection_string, schema="tweet_data", if_exists="append")


