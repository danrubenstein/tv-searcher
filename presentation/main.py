
import requests
from urllib.parse import quote_plus
from .. import PG_ENGINE, PG_CONNECTION

def get_url_embed_text(account_name, tweet_id)
	
	oembed_url = "https://publish.twitter.com/oembed"
	url = "https://twitter.com/{}/status/{}".format(account_name, tweet_id)

	params = {
		"url": url, 
		"hide_media" : 1 
		"hide_thread" : 1
		"omit_script" : 1
	}

	r = requests.get(oembed_url, params=params)
	if not r.ok:
		print(r.text)
		return None
	
	return r.json()["html"]

def get_tweets_for_embedding():

	df = pd.read_sql(open("retrieval.sql").read(), PG_ENGINE)

	embeds = []
	for index, row in df.iterrows():
		embeds.append(get_url_embed_text(row['user_screen_name'], row['id']))

	print(embeds)

	return embeds 

if __name__ == "__main__":

	get_tweets_for_embedding()