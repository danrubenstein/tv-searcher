
from ..utils import load_tweets_as_dataframe, prepare_tweets_for_modeling
from gensim.models import Word2Vec


def get_word2vec_model_for_tweets(relevance):

	df = load_tweets_as_dataframe(labels='only')
	df = df[df['label']==relevance]
	
	prepare_tweets_for_modeling(df)
	tweets = df['tweet_status_cleaned'].str.split().as_matrix()
	
	model = Word2Vec(tweets, hs=1, negative=0)
	
	return model


def get_word2vec_models():
	'''
	Generate the word2vec models for relevant and non-relevant tweets
	'''
	for i in range(2):
		model = get_word2vec_model_for_tweets(i)
		model_filepath = os.path.join(os.path.dirname(__file__), "../resources/word2vec_{}".format(1))
		model.save(model_filepath)


def set_word2vec_score_for_model(model, df, model_name):
	'''
	Apply the word2vec models for a given model
	'''
	df['word2vec_{}'.format(model_name)] = model.score(df['tweet_status_cleaned'].str.split().as_matrix())


def set_word2vec_scores(df):

	# relevant and non-relevant models
	for i in range(2):
		model_filepath = os.path.join(os.path.dirname(__file__), "../resources/word2vec_{}".format(1))
		model = Word2Vec.load(model_filepath)
		set_word2vec_score_for_model(model, df, i)