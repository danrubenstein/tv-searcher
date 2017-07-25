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


def load_resources_from_server():
	
	raw_directory = "tmp-resource-"+str(uuid.uuid4())
	[shutil.rmtree(x) for x in os.listdir() if "tmp-resource" in x and os.path.isdir(x)]
	os.mkdir(raw_directory)
	aws_cmd = "aws s3 cp --recursive s3://{}/{}/downloads ./{}".format(os.environ['S3_BUCKET_NAME'], 
														os.environ['PROJECT_NAME'], 
														raw_directory).split()
	subprocess.call(aws_cmd)

	return raw_directory


def get_available_tweet_csvs(pg_connection):

	result = connection.execute("select filename from tweet_data.loaded_files;")
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


def process_tweet_csv(tweet_csv_filename, tweet_csv_file_directory,
						pg_engine, pg_connection):
	'''
	Add the tweets to the tweet_data schema
	'''
	tweet_frame = pd.DataFrame.from_csv(open(os.path.join(tweet_csv_file_directory, tweet_csv_filename))
											, index_col=None)

	# raw records input
	tweet_frame.to_sql("scraping_raw_records", pg_engine, 
							schema="tweet_data", if_exists="append", index=False)

	# add to list of files
	
	filename = tweet_csv_filename
	file_created = tweet_csv_filename.split("_")[1].replace(".csv", "")
	file_records = len(tweet_frame)

	pg_connection.execute(
		"""INSERT INTO tweet_data.loaded_files (filename, file_created, count_records) 
				VALUES ('{}', '{}', {});""".format(filename, file_created, file_records))

	process_for_label_pipeline(tweet_frame, pg_engine, pg_connection)

	return "Success"


def process_for_label_pipeline(tweet_df, pg_engine, pg_connection):
	'''
	Add the entries to the label_data pipeline
	'''

	tweet_df.sort_values("tweet_created_at", inplace=True)
	tweet_df.drop_duplicates("tweet_status", inplace=True)
	tweet_df['priority'] = 0 # this can be subject to change...
	tweet_df.to_sql("staging_tweet_data", pg_engine, 
							schema="label_data", if_exists="fail", index=False)

	# Load and execute insertion script
	insertion_script = open("sqlscripts/label_data_load_to_pipeline.sql").read()
	pg_connection.execute(insertion_script)

	return 


if __name__ == "__main__":

	dotenv_path = os.path.join(os.path.dirname(__file__)+"../", '.env')
	load_dotenv(dotenv_path)

	load_resources_from_server()

	connection_string = 'postgresql://{}:{}@localhost:5432/{}'.format(os.environ['PG_USERNAME'], 
		os.environ['PG_PASSWORD'], os.environ['PG_DATABASE'])

	engine = create_engine(connection_string)
	connection = engine.connect()

	available_files, resource_directory = get_available_tweet_csvs(connection)
	for file in available_files:
		print("adding file: {}".format(file))
		process_tweet_csv(file, resource_directory, engine, connection)


