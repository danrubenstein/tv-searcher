import pandas as pd 
from ..utils import load_tweets_as_dataframe

def get_spam_retweets(tweet_statuses):
	
	'''
	Identify original tweets and count number of "retweets"
	tweet_statuses is a pandas series
	'''

	_retweets = []
	status_counts = tweet_statuses.value_counts()
	adapted_series = pd.Series(index=[":".join(x.split(":")[1:]).strip() for x in status_counts.index], data=status_counts.values)

	for status in tweet_statuses:
		if status[:2] == "RT":
			_retweets.append(0)

		else:
			# search for retweets
			if status in adapted_series.index:
				print(status, '***', adapted_series[status])
				print()
				_retweets.append(adapted_series[status])
			else:
				_retweets.append(0)

	retweet_counts = _retweets
	return retweet_counts


if __name__ == "__main__":
	tweets_df = load_tweets_as_dataframe()
	
	# get_spam_retweets(tweets_df['tweet_status'])
	
	networks = ["MSNBC", "CNN", "PBS"]
	