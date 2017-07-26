
import re 
import json
import pickle

import pandas as pd 
import numpy as np
import os

import nltk
from nltk.collocations import *

from ..utils import load_tweets_as_dataframe, prepare_tweets_for_modeling


def find_ngrams(input_string, n):
	input_list = input_string.split()
	return [" ".join(x) for x in list(zip(*[input_list[i:] for i in range(n)]))]


def get_ngram_frequency_tf_idf(df, n, tags_max=4, tags_start_max=1):
	
	relevant_counts = df[df['label'] == 1]['tweet_status_cleaned'].apply(lambda x: pd.value_counts(find_ngrams(x, n))).sum(axis = 0)
	unrelevant_counts =  df[df['label'] == 0]['tweet_status_cleaned'].apply(lambda x: pd.value_counts(find_ngrams(x, n))).sum(axis = 0)

	relevance_frequencies = [(1, relevant_counts), (0, unrelevant_counts)]
	all_words = sorted(list(set([y for (a,x) in relevance_frequencies for y in list(x.index)])))
	stat_dicts = []
	n_docs = len(relevance_frequencies)

	for word in all_words:
		
		n_t = len([x for x in relevance_frequencies if word in x[1].index])
		idf = np.log(n_docs / n_t)

		for (tweet_relevant, freqs) in relevance_frequencies:
			if word in freqs.index and freqs[word] > 4:
				tf_idf = (1 + np.log(freqs[word])) * idf
			else:
				tf_idf = 0
			tf_idf_dict = {
					"token" : word,
					"tweet_relevant" : tweet_relevant, 
					"tf_idf" : tf_idf
				}
			stat_dicts.append(tf_idf_dict)
	
	return stat_dicts

NGRAM_SIZE = 100

def generate_word_frequency_tf_idf():

	tweets_df = load_tweets_as_dataframe(labels="only")
	prepare_tweets_for_modeling(tweets_df)
	n_gram_stats = {}
	for i in range(1,4):
		pairings = get_ngram_frequency_tf_idf(tweets_df, i)
		max_relevant = sorted([x for x in pairings if x['tweet_relevant'] == 1], key=lambda x: -x['tf_idf'])[:100]
		max_nonrelevant = sorted([x for x in pairings if x['tweet_relevant'] == 0], key=lambda x: -x['tf_idf'])[:100]
		biggest_word_tf_idf = max_relevant + max_nonrelevant
		n_gram_stats[i] = biggest_word_tf_idf
	
	resource_filepath = os.path.join(os.path.dirname(__file__), "resources/ngram_tf_idf.json")	
	f = open(resource_filepath, 'w') 
	json.dump(n_gram_stats, f)
	f.close()


def get_word_tf_idf_from_status(status, tf_idf, for_relevant, cumulative=False):
	'''
	Assume tf_idf is sorted high to low
	'''
	total = 0
	for i in range(len(tf_idf)):
		if for_relevant == tf_idf[i]['tweet_relevant'] and tf_idf[i]['token'] in status:
			if cumulative:
				total += tf_idf[i]["tf_idf"] 
			else:
				return tf_idf[i]["tf_idf"] 
	return 0 


def get_word_tf_idf(tweets_df):
	'''
	From a given file of word tf-idf values... impute max and sum into statuses
	'''

	tfidf_filepath = os.path.join(os.path.dirname(__file__), "resources/ngram_tf_idf.json")
	tf_idf = json.load(open(tfidf_filepath))
	
	# Max keys...
	for key in tf_idf.keys():
		tweets_df['ngram_{}_relevant_tf_idf_max'.format(key)] = tweets_df['tweet_status_cleaned'].apply(lambda x: 
			get_word_tf_idf_from_status(x, tf_idf[key], for_relevant=1))
		tweets_df['ngram_{}_relevant_tf_idf_sum'.format(key)] = tweets_df['tweet_status_cleaned'].apply(lambda x: 
			get_word_tf_idf_from_status(x, tf_idf[key], for_relevant=1, cumulative=True))

		tweets_df['ngram_{}_nonrelevant_tf_idf_max'.format(key)] = tweets_df['tweet_status_cleaned'].apply(lambda x: 
			get_word_tf_idf_from_status(x, tf_idf[key], for_relevant=0))
		tweets_df['ngram_{}_word_nonrelevant_tf_idf_sum'.format(key)] = tweets_df['tweet_status_cleaned'].apply(lambda x: 
			get_word_tf_idf_from_status(x, tf_idf[key], for_relevant=0, cumulative=True))

	# Individual keys
	for key in tf_idf.keys():
		for i in range(len(tf_idf[key])):
			tweets_df['ngram_{}_{}'.format(key, tf_idf[key][i]['token'])] = tweets_df['tweet_status_cleaned'].str.contains(tf_idf[key][i]['token'], regex=False).astype(int)

	return tweets_df

def set_first_word_frequency(status, frequencies):

	first_word = status.split()[0]
	if first_word in frequencies.index:
		return frequencies[first_word]
	
	return 0


def get_corpus_first_word_frequencies():

	tweets_df = load_tweets_as_dataframe(labels="only")

	pos_first_words = tweets_df[tweets_df['label'] == 1]['tweet_status'].apply(lambda x: x.split()[0].lower()).value_counts()
	neg_first_words = tweets_df[tweets_df['label'] == 0]['tweet_status'].apply(lambda x: x.split()[0].lower()).value_counts()

	relative_result = (pos_first_words / (pos_first_words + neg_first_words)).fillna(0)
	total_result = ((pos_first_words + neg_first_words) / ((pos_first_words + neg_first_words).sum())).fillna(0)

	
	
	resource_filepath = os.path.join(os.path.dirname(__file__), "resources/leading_word_freqs.json")	
	f = open(resource_filepath, 'wb') 
	pickle.dump([relative_result, pos_first_words, total_result], f)
	f.close()


def apply_first_word_frequencies(tweets_df):
	
	freqs_filepath = os.path.join(os.path.dirname(__file__), "resources/leading_word_freqs.json")
	freqs_file = open(freqs_filepath, 'rb')
	[relative_result, pos_first_words, total_result] = pickle.load(freqs_file)

	tweets_df['leading_word_rel_freq'] = tweets_df['tweet_status'].apply(lambda x: set_first_word_frequency(x, relative_result))
	tweets_df['leading_word_pos_freq'] = tweets_df['tweet_status'].apply(lambda x: set_first_word_frequency(x, pos_first_words))
	tweets_df['leading_word_total_freq'] = tweets_df['tweet_status'].apply(lambda x: set_first_word_frequency(x, total_result))

	return tweets_df

def get_token_finding(tweets_df):
	'''
	Apply all account pre-processing
	'''
	a = tweets_df

	functions = [get_word_tf_idf, apply_first_word_frequencies]
	for f in functions:
		a = f(a)

	return a




def get_collocations(tweets_df):
	print('hi')
	bigram_measures = nltk.collocations.BigramAssocMeasures()
	trigram_measures = nltk.collocations.TrigramAssocMeasures()
	x = "\n".join(list(tweets_df['tweet_status_cleaned']))
	print(x)
	finder = TrigramCollocationFinder.from_words(x)
	finder.apply_freq_filter(5)
	print(finder.nbest(trigram_measures.pmi, 10))

