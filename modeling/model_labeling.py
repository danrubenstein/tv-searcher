'''
Builds the latest model for estimating probability (label prioritization) 
'''
import os 

import pandas as pd 
import numpy as np 
import pickle
from sklearn.ensemble import RandomForestClassifier

from ..utils import load_tweets_as_dataframe, get_tweet_status_cleaned
from .time_searching import get_time_searching
from .account_finding import get_account_finding	


model_preprocessing_functions = [get_account_finding, get_time_searching]
id_columns = ['id', 'tweet_created_at', 'tweet_status', 'user_id', 'user_screen_name',
       'user_verified', 'label_priority', 'label', 'tweet_status_cleaned', 'tweet_status_cleaned']


unlabeled_id_cols = ['search_term_raw','search_term_input','tweet_id','user_followers', 'user_following']

def get_latest_model():
	
	labeled_df = load_tweets_as_dataframe(labels='only')
	
	if len(labeled_df) == 0:
		print("There's not any data to model from, finishing...")
		return 
	
	for f in model_preprocessing_functions:
		labeled_df = f(labeled_df)

	y = np.array(labeled_df['label'])
	labeled_df_filtered = labeled_df.drop(id_columns, axis=1, errors='ignore')
	X = labeled_df_filtered.as_matrix()
	
	clf = RandomForestClassifier()
	clf.fit(X,y)

	model_filepath = os.path.join(os.path.dirname(__file__), "resources/model_latest")
	model_file = open(model_filepath, 'wb')
	pickle.dump(clf, model_file)
	model_file.close()

	model_cols_filepath = os.path.join(os.path.dirname(__file__), "resources/model_latest_cols.txt")
	model_cols_file = open(model_cols_filepath, 'w')
	for f in labeled_df_filtered.columns.values:
		model_cols_file.write("{}\n".format(f))
	model_cols_file.close()


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
