'''
Strategies to find relevant accounts to search for
'''
import re 

import pandas as pd 
import numpy as np

from utils import load_tweets_as_dataframe

def get_account_network_pairings(tweet_statuses, networks, tags_max=4, tags_start_max=3):

	'''
	Identify frequent personality network pairings using a loose tf-idf strategy
	'''

	tweet_statuses = tweets_df[(tweets_df['tag_tokens'] <= tags_max) & 
								(tweets_df['tag_tokens_starting'] <= tags_start_max) &
								(~tweets_df['tweet_contained_elsewhere'])]['tweet_status_rt_adjusted'].drop_duplicates()

	
	print(len(tweet_statuses))

	# Generating a "corpus"
	network_frequencies = []

	pattern = re.compile("@[a-z0-9_]+")

	for network in networks:
		network_tweets = tweet_statuses[tweet_statuses.str.contains(network)].copy()
		account_words = pd.Series([pattern.search(x).group(0)
									for y in network_tweets.str.split() 
										for x in y if x[0] == "@" and len(x) > 1])
		network_frequencies.append((network, account_words.value_counts()))

	all_accounts = sorted(list(set([y for (a,x) in network_frequencies for y in list(x.index)])))

	# Generating the idf
	stat_dicts = []

	for account in all_accounts:
		n_docs = len(network_frequencies)
		n_t = len([x for x in network_frequencies if account in x[1].index])
		idf = np.log(n_docs / n_t)

		for (network, freqs) in network_frequencies:
			if account in freqs.index:
				tf_idf = (1 + np.log(freqs[account])) * idf
			else:
				tf_idf = 0
			tf_idf_dict = {
					"account" : account,
					"network" : network, 
					"tf_idf" : tf_idf
				}
			stat_dicts.append(tf_idf_dict)
	
	return stat_dicts


def update_search_accounts(pairings, tweet_df, n_accounts_to_search):

	f = open("../resources/accounts.txt", 'w+')
	accounts = list(pd.Series([x['account'] for x in sorted(pairings, key=lambda x: -x['tf_idf'])]).drop_duplicates())
	
	count = 0 
	x = 0 
	
	while count < n_accounts_to_search and x < len(accounts):
		if len(search_tweets_for_account(tweet_df, accounts[x])) > 2:
			f.write("{}\n".format(accounts[x]))
			count += 1 
		x += 1

	f.close()


def search_tweets_for_account(tweets_df, account_name):
	'''
	this is easier
	'''
	return tweets_df[(tweets_df['tweet_status_rt_adjusted'].str.contains(account_name)) 
							& (tweets_df['tag_tokens'] <= 4) 
							& (tweets_df['tag_tokens_starting'] <= 0)
							& (~tweets_df['tweet_contained_elsewhere'])]['tweet_status_rt_adjusted'].drop_duplicates()


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
if __name__ == "__main__":
	
	tweets_df = load_tweets_as_dataframe()
	
	networks = ["msnbc", "cnn", "pbs", "fox", "cnbc"]

	# Preprocessing

	tweets_df['tweet_status_rt_adjusted'] = tweets_df['tweet_status'].apply(lambda x: ":".join(x.split(":")[1:]).strip()
																				if x[:2] == "RT" else x)

	tweets_df['tweet_status_rt_adjusted'] = tweets_df['tweet_status_rt_adjusted'].str.lower()
	tweets_df['tweet_status_rt_adjusted'] = tweets_df['tweet_status_rt_adjusted'].str.rstrip('…')

	adj_statuses = tweets_df['tweet_status_rt_adjusted'].values

	tweets_df['tweet_contained_elsewhere'] = tweets_df['tweet_status_rt_adjusted'].apply(lambda x: get_contained_elsewhere(x, adj_statuses))
	tweets_df['tag_tokens'] = tweets_df['tweet_status_rt_adjusted'].apply(lambda x: len([x for x in x.split() if x[0] in ["#", "@"] and len(x) > 1]))
	tweets_df['tag_tokens_starting'] = tweets_df['tweet_status_rt_adjusted'].apply(lambda x: get_ordered_tag_tokens(x))

	# Implement TF-IDF strategy
	pairings = get_account_network_pairings(tweets_df, networks, tags_max=4, tags_start_max=1)
	biggest_tf_idf = sorted(pairings, key=lambda x: -x['tf_idf'])[:100]

	for i in biggest_tf_idf:
		pass
		x = len(search_tweets_for_account(tweets_df, i['account']))
		if x > 0:
			print(i, x)
			print(search_tweets_for_account(tweets_df, i['account']))

	# Update search
	update_search_accounts(pairings, tweets_df, 25)
