''' 
Analysis utility functions
'''

import os
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine

dotenv_path = os.path.join(os.path.dirname(__file__)+"/../", '.env')
load_dotenv(dotenv_path)
print(os.environ["PG_USERNAME"])

CONNECTION_STRING = 'postgresql://{}:{}@localhost:5432/{}'.format(os.environ['PG_USERNAME'], 
		os.environ['PG_PASSWORD'], os.environ['PG_DATABASE'])

PG_ENGINE = create_engine(CONNECTION_STRING)


def get_ordered_tag_tokens(status):

	status_tokens = status.split()
	count = 0 

	for i in range(len(status_tokens)):
		if status_tokens[i][0] in ["#", "@"] and len(status_tokens[i]) > 1:
			count += 1 
		else:
			return count 
	return count


def get_contained_elsewhere(status, statuses):
	'''
	Returns 1(0) if a status is (not) a smaller substring of another status
	'''
	len_status = len(status)
	for s in statuses:
		if status in s and len_status < len(s):
			return True
	return False


def load_tweets_as_dataframe():

	df = pd.read_sql("select * from tweet_data.scraping_raw_tweets", PG_ENGINE)
	
	# Analytics pre-processing
	df['tweet_status_cleaned'] = df['tweet_status'].apply(lambda x: ":".join(x.split(":")[1:]).strip()
																				if x[:2] == "RT" else x)
	df['tweet_status_cleaned'] = df['tweet_status_cleaned'].str.lower()
	df['tweet_status_cleaned'] = df['tweet_status_cleaned'].str.rstrip('…')

	adj_statuses = df['tweet_status_cleaned'].drop_duplicates().values

	df['tweet_contained_elsewhere'] = df['tweet_status_cleaned'].apply(lambda x: get_contained_elsewhere(x, adj_statuses))
	df['tag_tokens'] = df['tweet_status_cleaned'].apply(lambda x: len([x for x in x.split() if x[0] in ["#", "@"] and len(x) > 1]))
	df['tag_tokens_starting'] = df['tweet_status_cleaned'].apply(lambda x: get_ordered_tag_tokens(x))

	return df
