
import requests
from urllib.parse import quote_plus
from .. import PG_ENGINE, PG_CONNECTION
import pandas as pd
import os

def get_url_embed_text(account_name, tweet_id):
	
	oembed_url = "https://publish.twitter.com/oembed"
	url = "https://twitter.com/{}/status/{}".format(account_name, tweet_id)

	params = {
		"url": url, 
		"hide_media" : 1,
		"hide_thread" : 1,
		"omit_script" : 1, 
		"maxwidth" : 550,
	}

	r = requests.get(oembed_url, params=params)
	if not r.ok:
		# print(r.text)
		return ""
	
	return r.json()["html"]

def get_tweets_for_embedding():

	# PG_CONNECTION.execute(open(retrieval_filepath).read())
	retrieval_filepath = os.path.join(os.path.dirname(__file__), "retrieval.sql")
	df = pd.read_sql(open(retrieval_filepath).read(), PG_ENGINE)
	embeds = []
	for index, row in df[:50].iterrows():
		a = get_url_embed_text(row['user_screen_name'], row['id'])
		embeds.append(a)

	return embeds 

def constructHtml():

	template_filepath = os.path.join(os.path.dirname(__file__), "index_template.html")
	template = open(template_filepath).read()

	embeds = get_tweets_for_embedding()
	output = template.replace("[EMBEDS]", "".join(embeds))

	output_filepath = os.path.join(os.path.dirname(__file__), "../index.html")
	with open(output_filepath, 'w') as f:
		f.write(output)

	return 

if __name__ == "__main__":

	get_tweets_for_embedding()