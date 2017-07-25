import pandas as pd

from . import PG_ENGINE, PG_CONNECTION

def load_set_to_label():

	df = pd.read_sql(
		"""
		select * from label_data.tweets_not_labeled 
		where label_in_process is False
		and user_verified is True
		order by label_priority
		limit 100
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


def get_tweet_status_cleaned(status):

	rt_strip = ":".join(status.split(":")[1:]).strip() if status[:2] == "RT" else status
	lower = rt_strip.lower()
	extension_strip = rt_strip.rstrip('…')

	return extension_strip

def load_tweets_as_dataframe(labels=None):

	if labels == "only":
		df = pd.read_sql("""
			select a.*, b.label from label_data.tweets_in_pipeline a 
			inner join label_data.tweets_labeled b on a.id = b.id""", PG_ENGINE)

	elif labels == "mixed":
		df = pd.read_sql("""
			select a.*, b.label from label_data.tweets_in_pipeline a 
			left join label_data.tweets_labeled b on a.id = b.id""", PG_ENGINE)
	elif labels == "unlabeled":
		df = pd.read_sql("""
			select * from label_data.tweets_in_pipeline 
			where id not in (select id from label_data.tweets_labeled)""", PG_ENGINE)
	else:
		df = pd.read_sql("""
			select * from label_data.tweets_in_pipeline a 
			""", PG_ENGINE)
	
		
	df['tweet_status_cleaned'] = df['tweet_status'].apply(get_tweet_status_cleaned)

	adj_statuses = df['tweet_status_cleaned'].drop_duplicates().values

	# df['tweet_contained_elsewhere'] = df['tweet_status_cleaned'].apply(lambda x: get_contained_elsewhere(x, adj_statuses))
	df['tag_tokens'] = df['tweet_status_cleaned'].apply(lambda x: len([x for x in x.split() if x[0] in ["#", "@"] and len(x) > 1]))
	df['tag_tokens_starting'] = df['tweet_status_cleaned'].apply(lambda x: get_ordered_tag_tokens(x))

	return df



