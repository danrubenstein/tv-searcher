'''
Builds the latest model for estimating probability (label prioritization) 
'''
import os 
import pickle
import subprocess
import time

import pandas as pd 
import numpy as np 

from sklearn.ensemble import RandomForestClassifier

from ..utils import load_tweets_as_dataframe, prepare_tweets_for_modeling
from .. import PG_ENGINE, PG_CONNECTION

from .time_searching import get_time_searching
from .account_finding import generate_account_frequency_tf_idf, get_account_finding
from .word_frequency import generate_word_frequency_tf_idf, get_token_finding, get_corpus_first_word_frequencies
from .timeline_searching import get_search_timelines
from .nlp_relevance import set_word2vec_scores, get_word2vec_models


model_preprocessing_functions = [get_account_finding, 
									get_time_searching, 
									get_token_finding, 
									set_word2vec_scores]

id_columns = ['id', 'tweet_created_at', 'tweet_status', 'user_id', 
				'user_screen_name', 'label_priority', 'label', 
				'tweet_status_cleaned', 'tweet_status_cleaned']

unlabeled_id_cols = ['search_term_raw','search_term_input',
						'tweet_id','user_followers', 'user_following']


def update_modeling_resources():
	'''
	Wrapper for the categories of feature generation resources
	'''
	timer = time.time()

	# generate_word_frequency_tf_idf()

	print(time.time() - timer)
	timer = time.time()

	generate_account_frequency_tf_idf()

	print(time.time() - timer)
	timer = time.time()

	get_corpus_first_word_frequencies()

	print(time.time() - timer)
	timer = time.time()

	get_word2vec_models()

	print(time.time() - timer)

	return None


def rescore_labeling_pipeline(clf):
	
	pipeline_df = load_tweets_as_dataframe()
	original_columns = pipeline_df.columns.values

	prepare_tweets_for_modeling(pipeline_df)

	for f in model_preprocessing_functions:
		pipeline_df = f(pipeline_df)

	pipeline_df_filtered = pipeline_df.drop(id_columns+unlabeled_id_cols, axis=1, errors='ignore')
	pipeline_X = pipeline_df_filtered.as_matrix()

	pipeline_df['label_priority'] = [x[0] for x in clf.predict_proba(pipeline_X)]
	pipeline_df.drop([x for x in pipeline_df.columns if x not in original_columns], inplace=True, axis=1, errors='ignore')

	pipeline_df.to_sql("tweets_in_pipeline", PG_ENGINE, 
		if_exists='replace', schema='label_data', index=False)

	query = """
		UPDATE label_data.tweets_not_labeled a
		SET label_priority = b.label_priority
		FROM label_data.tweets_in_pipeline b
		WHERE a.id = b.id
		"""
	PG_CONNECTION.execute(query)

	return None


def get_latest_model(update_resources=True, rescore=True, 
						regenerate_search_timelines=True, sync_resources=False):

	if update_resources:
		update_modeling_resources()

	labeled_df = load_tweets_as_dataframe(labels='only')
	if len(labeled_df) == 0:
		print("There's not any data to model from, finishing...")
		return 

	prepare_tweets_for_modeling(labeled_df)

	for f in model_preprocessing_functions:
		labeled_df = f(labeled_df)

	y = np.array(labeled_df['label'])

	labeled_df_filtered = labeled_df.drop(id_columns, axis=1, errors='ignore')
	X = labeled_df_filtered.as_matrix()
	
	clf = RandomForestClassifier(n_estimators=25)
	clf.fit(X,y)

	model_filepath = os.path.join(os.path.dirname(__file__), "resources/model_latest")
	with open(model_filepath, 'wb') as model_file:
		pickle.dump(clf, model_file)

	model_cols_filepath = os.path.join(os.path.dirname(__file__), "resources/model_latest_cols.txt")
	with open(model_cols_filepath, 'w') as model_cols_file:
		for f in labeled_df_filtered.columns.values:
			model_cols_file.write("{}\n".format(f))
	
	if rescore:
		rescore_labeling_pipeline(clf)

	if regenerate_search_timelines:
		get_search_timelines()

	if sync_resources:
		cmd = "aws s3 sync {} s3://{}/tv-searcher/resources".format(os.path.join(os.path.dirname(__file__), "../resources/"), os.environ['S3_BUCKET_NAME'])
		subprocess.call(cmd.split())

	return None



def score_unlabeled_tweets(unlabeled_df):
	
	try:
		model_filepath = os.path.join(os.path.dirname(__file__), "resources/model_latest")
		model_file = open(model_filepath, 'rb')
		clf = pickle.load(model_file)
	except FileNotFoundError:
		print("there's not an existing file, returning 0s")
		priority_labels = np.zeros(len(unlabeled_df))
		return priority_labels

	for f in model_preprocessing_functions:
		unlabeled_df = f(unlabeled_df)

	unlabeled_df_filtered = unlabeled_df.drop(id_columns+unlabeled_id_cols, axis=1, errors='ignore')
	X_unlabeled = unlabeled_df_filtered.as_matrix()


	labels = [x[0] for x in clf.predict_proba(X_unlabeled)]

	return labels


