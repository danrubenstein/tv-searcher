'''
What are the users who have the most tweeted appearances?
'''
import os

import pandas as pd 

from .. import PG_ENGINE, PG_CONNECTION

def get_search_timelines():

	df = pd.read_sql("""
		select user_id, count(*) as valuable_count
		from label_data.tweets_in_pipeline 
		where label_priority < 0.3

		group by 1 
		having count(*) > 2
		""", PG_ENGINE)

	timeline_filepath = os.path.join(os.path.dirname(__file__), "../resources/user_timelines.txt")

	with open(timeline_filepath, 'w') as timeline_file:
		
		account_ids = list(df['user_id'])
		for id in account_ids:
			timeline_file.write("{}\n".format(id))

	return 