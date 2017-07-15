'''
Strategies to find relevant accounts to search for
'''

import pandas as pd 
import numpy as np
from utils import load_tweets_as_dataframe

def get_account_network_pairings(tweet_statuses, networks):

	'''
	Identify frequent personality network pairings using a loose tf-idf strategy
	'''

	# Generating a "corpus"

	network_frequencies = []

	for network in networks:
		network_tweets = tweet_statuses[tweet_statuses.str.lower().str.contains(network.lower())].copy()
		account_words = pd.Series([x.replace(":","") 
									for y in network_tweets.str.lower().str.split() 
										for x in y if x[0] == "@"])
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

def update_search_accounts(pairings, num_accounts):

	f = open("../resources/accounts.txt", 'w+')
	accounts = [x['account'] for x in sorted(pairings, key=lambda x: x['tf_idf'])[-num_accounts:]]
	for account in accounts:
		f.write("{}\n".format(account))
	f.close()


if __name__ == "__main__":
	
	tweets_df = load_tweets_as_dataframe()
	
	networks = ["MSNBC", "CNN", "PBS", "FOX"]

	tweets_df['tweet_status_rt_adjusted'] = tweets_df['tweet_status'].apply(lambda x: ":".join(x.split(":")[1:])
																				if x[:2] == "RT" else x)

	pairings = get_account_network_pairings(tweets_df['tweet_status_rt_adjusted'].drop_duplicates(), networks)
	update_search_accounts(pairings, 15)