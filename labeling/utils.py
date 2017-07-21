''' 
Analysis utility functions
'''

import os
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

dotenv_path = os.path.join(os.path.dirname(__file__)+"/../", '.env')
load_dotenv(dotenv_path)
print(os.environ["PG_USERNAME"])



CONNECTION_STRING = 'postgresql://{}:{}@localhost:5432/{}'.format(os.environ['PG_USERNAME'], 
		os.environ['PG_PASSWORD'], os.environ['PG_DATABASE'])

PG_ENGINE = create_engine(CONNECTION_STRING)
PG_CONNECTION = PG_ENGINE.connect()

def load_set_to_label():

	df = pd.read_sql(
		"""
		select * from label_data.tweets_not_labeled 
		where label_in_process is False
		and user_verified is False
		limit 2
		""", PG_ENGINE).to_dict('records')


	id_list = list([x['id'] for x in df])
	print(id_list)
	if len(id_list) > 0:
		query = ("""
		UPDATE label_data.tweets_not_labeled
		SET label_in_process = True
		WHERE id in %s
		""")
		PG_CONNECTION.execute(query, [(tuple(id_list),)])

	return df
