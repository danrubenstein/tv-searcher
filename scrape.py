'''
Scraper for tweet data
'''

import csv 
import os
from urllib.parse import quote_plus
from base64 import b64encode
import datetime

import boto3
import requests

def load_resources(filename):
    '''
    Load .env files
    '''
    for i in open(filename).readlines():
        if len(i) > 2 and i[0] != "#":
            [key, value] = "".join(i.split()).split("=")
            os.environ[key] = value


def get_app_only_auth_token(consumer_key, consumer_secret):
    '''
    Generate a Bearer Token from consumer_key and consumer_secret - 
    courtesy of github.com/bear/python-twitter
    '''

    key = quote_plus(consumer_key)
    secret = quote_plus(consumer_secret)
    bearer_token = b64encode('{}:{}'.format(key, secret).encode('utf-8')).decode('utf-8')

    post_headers = {
        'Authorization': 'Basic ' + bearer_token,
        'Content-Type': 'application/x-www-form-urlencoded;charset=UTF-8',
        'Accept-Encoding' : 'gzip'
    }

    res = requests.post('https://api.twitter.com/oauth2/token',
                        data={'grant_type': 'client_credentials'},
                        headers=post_headers)
    
    bearer_credentials = res.json()['access_token']

    return bearer_credentials


def get_search_query(key, search_string, result_type='recent'):
    '''
    Run a simple get request from the search_string
    '''
    print(search_string)

    get_headers = {
        'Authorization': 'Bearer ' + key,
    }

    url = "https://api.twitter.com/1.1/search/tweets.json"

    params = {
        'q' : quote_plus(search_string),
        'count' : 100, 
        'result_type' : result_type
    }

    res = requests.get(url, params=params, headers=get_headers)
    if not res.ok: 
        print(res.text)
        return None

    return res.json()


def get_user_timeline_query(key, user_id):
    '''
    Pull a user timeline
    '''
    get_headers = {
        'Authorization': 'Bearer ' + key,
    }

    url = "https://api.twitter.com/1.1/statuses/user_timeline.json"

    params = {
        'user_id' : user_id,
        'count' : 200
    }

    res = requests.get(url, params=params, headers=get_headers)
    if not res.ok: 
        print(res.text)
        return None

    return res.json()




def clean_tweet_data(raw_statuses, search_term_raw, search_term_input):
    '''
    Reduce tweets into manageable data sets
    '''
    statuses = []
    
    for status in raw_statuses:   
        tweet_dict = {

            # Search data 
            "search_term_raw" : search_term_raw, 
            "search_term_input" : search_term_input,

            # Tweet data
            "tweet_id" : status['id'],
            "tweet_created_at" : status['created_at'],
            "tweet_status" : status['text'], 
            
            # User data
            "user_screen_name" : status['user']['screen_name'], 
            "user_id" : status['user']['id'], 
            "user_verified" : status['user']['verified'],
            "user_followers" : status['user']['followers_count'], 
            "user_following" : status['user']['friends_count'],
        }

        statuses.append(tweet_dict)

    return statuses


def load_search_resources(filename):
    """
    Prepares abstract network information (networks, shownames, userids)
    """
    search_resources = open(filename).read().split()
    search_resources = [x for x in search_resources if x.strip()[0] is not "#"]
    return search_resources


def load_search_terms(filename, networks=[''], accounts=[''], search_as_exact="loose"):
    """
    Prepares search queries
    search_as_exact - "only", "loose", "all"

    Returns a list of tuples with the raw term and the imputed term - 
        sometimes they are the same
    """
    raw_search_terms = [x.strip() for x in open(filename).readlines()]
    uncommented_terms = [x for x in raw_search_terms if x.strip()[0] is not "#"]
    collected_queries = []
    for term in uncommented_terms:
        if "[NETWORK]" in term: 
            collected_queries += [(term, term.replace("[NETWORK]", n)) for n in networks]
        elif "[ACCOUNT]" in term:
            collected_queries += [(term, term.replace("[ACCOUNT]", n)) for n in accounts]
        else:
            collected_queries.append((term, term))

    if search_as_exact == "only":
        collected_queries = [(x[0], "\"{0}\"".format(x[1])) for x in collected_queries]
    elif search_as_exact == "all":
        collected_queries += [(x[0], "\"{0}\"".format(x[1])) for x in collected_queries]
    elif search_as_exact == "loose":
        pass    

    print(collected_queries)

    return collected_queries


def handler(event, context):
    '''
    Lambda execution
    '''

    client = boto3.client('s3')
    
    network_resource_file = 'networks.txt'
    client.download_file(os.environ['S3_BUCKET_NAME'], 
            'tv-searcher/resources/{}'.format(network_resource_file), 
            '/tmp/{}'.format(network_resource_file))

    search_term_resource_file = 'exactableSearchTerms.txt'
    client.download_file(os.environ['S3_BUCKET_NAME'], 
            'tv-searcher/resources/{}'.format(search_term_resource_file), 
            '/tmp/{}'.format(search_term_resource_file))

    account_resource_file = 'accounts.txt'
    client.download_file(os.environ['S3_BUCKET_NAME'], 
            'tv-searcher/resources/{}'.format(account_resource_file), 
            '/tmp/{}'.format(account_resource_file))

    user_timeline_resource_file = "user_timelines.txt"
    client.download_file(os.environ['S3_BUCKET_NAME'], 
        'tv-searcher/resources/{}'.format(user_timeline_resource_file), 
        '/tmp/{}'.format(user_timeline_resource_file))


    # load_resources(".env")
    twitter_credential = get_app_only_auth_token(os.environ['TWITTER_CONSUMER_TOKEN'], os.environ['TWITTER_CONSUMER_SECRET'])

    networks = load_search_resources("/tmp/{}".format(network_resource_file))
    accounts = load_search_resources("/tmp/{}".format(account_resource_file))
    user_timeline_accounts = load_search_resources("/tmp/{}".format(user_timeline_resource_file))
    search_terms = load_search_terms("/tmp/{}".format(search_term_resource_file), networks, accounts, search_as_exact="loose")
    
    cleaned_tweets = []
    
    for term in search_terms:
        result = get_search_query(twitter_credential, term[1])
        if result is not None:
            cleaned_tweets += clean_tweet_data(result['statuses'], term[0], term[1])

    for user in user_timeline_accounts:
        result = get_user_timeline_query(twitter_credential, int(user))
        if result is not None:
            cleaned_tweets += clean_tweet_data(result, "#USERTIMELINE", str(user))

    output_filename = "output_{}.csv".format(str(datetime.datetime.now()))
    output_path = "/tmp/{}".format(output_filename)

    with open(output_path, 'w') as csvfile:
        fieldnames = cleaned_tweets[0].keys()
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        for item in cleaned_tweets:
            writer.writerow(item)

    client.upload_file(output_path, os.environ['S3_BUCKET_NAME'], 'tv-searcher/downloads/{}'.format(output_filename))

    # TODO: invalidate ouath token
        

