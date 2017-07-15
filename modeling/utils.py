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

def load_tweets_as_dataframe():

	df = pd.read_sql("select * from tweet_data.scraping_raw_tweets", PG_ENGINE)
	return df
