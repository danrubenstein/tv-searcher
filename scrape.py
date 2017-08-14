'''
Scraper for tweet data
'''

import csv 
import os
from urllib.parse import quote_plus
from base64 import b64encode
import datetime
import json

import boto3
import requests

network_resource_file = 'networks.txt'
search_term_resource_file = 'exactableSearchTerms.txt'
account_resource_file = 'accounts.txt'
user_timeline_resource_file = "user_timelines.txt"
term_id_reference_resource_file = 'term_id_reference.json'

temp_folder = "/tmp"
# temp_folder = "tmp-folder" # For local testing

resource_files = [network_resource_file,
                    search_term_resource_file,
                    account_resource_file,
                    user_timeline_resource_file,
                    term_id_reference_resource_file]


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


def get_search_query(key, search_string, result_type='recent', max_id=None, since_id=None):
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

    if max_id is not None:
        params['max_id'] = max_id
    if since_id is not None:
        params['since_id'] = since_id

    print(params)

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


def get_resource_limits_query(key):
    
    get_headers = {
        'Authorization' : 'Bearer ' + key, 
    }

    url = "https://api.twitter.com/1.1/application/rate_limit_status.json"
    params = {
        "resources" : "users,search"
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

    return collected_queries


def load_search_resource_files(client):

    for resource_file in resource_files:
        client.download_file(os.environ['S3_BUCKET_NAME'], 
                'tv-searcher/resources/{}'.format(resource_file), 
                '{}/{}'.format(temp_folder, resource_file))

    return


def get_search_terms():

    networks = load_search_resources("{}/{}".format(temp_folder, network_resource_file))
    accounts = load_search_resources("{}/{}".format(temp_folder, account_resource_file))
    user_timeline_accounts = load_search_resources("{}/{}".format(temp_folder, user_timeline_resource_file))
    search_terms = load_search_terms("{}/{}".format(temp_folder, search_term_resource_file), networks, accounts)
    reference_tweet_search_ids = json.load(open("{}/{}".format(temp_folder, term_id_reference_resource_file)))

    return search_terms, reference_tweet_search_ids, user_timeline_accounts


def get_remaining_searches_dict(client):

    search_status_filename = "search_status.json"
    client.download_file(os.environ['S3_BUCKET_NAME'], 
                'tv-searcher/resources/{}'.format(search_status_filename), 
                '{}/{}'.format(temp_folder, search_status_filename))

    with open('{}/{}'.format(temp_folder, search_status_filename)) as f:
        remaining_searches_dict = json.load(f)
        return remaining_searches_dict


def get_term_search(term, twitter_credential, reference_id=None):

    cleaned_tweets = []
    oldest_in_search = None
    max_term_id = None
    latest_in_search = None
    counter = 0
    
    if reference_id is None:
        reference_id = 0
    
    while ((oldest_in_search is None or oldest_in_search >= reference_id)
            and counter <= 4):

        result = get_search_query(twitter_credential, term[1], max_id=oldest_in_search, since_id=reference_id)

        if result is not None:
            latest_in_search = result['search_metadata']['max_id']
            

            cleaned_tweets += clean_tweet_data(result['statuses'], term[0], term[1])
            if counter == 0:
                max_term_id = latest_in_search
            counter += 1
        else:

            print("error in search for search term {} with max_id {}".format(term, oldest_in_search))
            return cleaned_tweets, max_term_id, oldest_in_search, True

        try:
            oldest_in_search = int(result['search_metadata']['next_results'].split("max_id=")[1].split("&")[0])
        except KeyError:
            return cleaned_tweets, max_term_id, oldest_in_search, False
            
    return cleaned_tweets, max_term_id, oldest_in_search, False


def process_reference_id_files(client, reference_tweet_search_ids, latest_tweet_search_ids):

    for key in reference_tweet_search_ids.keys():
        if key not in latest_tweet_search_ids.keys():
            latest_tweet_search_ids[key] = reference_tweet_search_ids[key]

    id_reference_filepath = "{}/{}".format(temp_folder, term_id_reference_resource_file)
    with open(id_reference_filepath, 'w') as id_file:
        json.dump(latest_tweet_search_ids, id_file)
        
    client.upload_file(id_reference_filepath, os.environ['S3_BUCKET_NAME'], 
                            'tv-searcher/resources/{}'.format(term_id_reference_resource_file))  

    return 


def upload_search_status(client, search_status_dict):

    search_status_filename = "search_status.json"
    search_status_path = "{}/{}".format(temp_folder, search_status_filename)
    search_file = open(search_status_path, 'w')
    json.dump(search_status_dict, search_file)
    search_file.close()

    client.upload_file(search_status_path, 
                        os.environ['S3_BUCKET_NAME'], 
                        'tv-searcher/resources/{}'.format(search_status_filename))

    return


def upload_output(client, tweet_output):

    output_filename = "output_{}.csv".format(str(datetime.datetime.now()))
    output_path = "{}/{}".format(temp_folder, output_filename)

    with open(output_path, 'w') as csvfile:
        fieldnames = tweet_output[0].keys()
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        for item in tweet_output:
            writer.writerow(item)

    client.upload_file(output_path, 
                        os.environ['S3_BUCKET_NAME'], 
                        'tv-searcher/downloads/{}'.format(output_filename))

    return


def get_searches(search_terms, reference_tweet_search_ids, twitter_credential):

    cleaned_tweets = []
    latest_tweet_search_ids = {}

    search_term_limit = 0

    for i, term in enumerate(search_terms):
        if term in reference_tweet_search_ids.keys():
            reference_id = reference_tweet_search_ids[term]
        else:
            reference_id = None
        
        (results, max_term_id, old_tweet_term_search, rate_limited) = get_term_search(term, twitter_credential, reference_id)
        print("{}, {}".format(old_tweet_term_search, reference_id))

        cleaned_tweets += results

        if max_term_id is not None:
            latest_tweet_search_ids[term[1]] = max_term_id
        else:
            latest_tweet_search_ids[term[1]] = 0

        if rate_limited:
            search_term_limit = i
            break

    return cleaned_tweets, latest_tweet_search_ids, search_term_limit

def handler(event, context):
    '''
    Lambda execution
    '''

    s3_client = boto3.client('s3')
    load_search_resource_files(s3_client)
    
    credential = get_app_only_auth_token(os.environ['TWITTER_CONSUMER_TOKEN'], os.environ['TWITTER_CONSUMER_SECRET'])

    search_status_dict = {}
    remaining_searches = get_remaining_searches_dict(s3_client)
    cleanupHandler = True

    if remaining_searches['time'] == 0 or "searches" in remaining_searches.keys():
        
        cleanupHandler = False
        search_terms, reference_tweet_search_ids, timeline_accounts = get_search_terms()
        search_status_dict["time"] = 23 
        
        if "searches" in remaining_searches.keys():
            search_terms = remaining_searches["searches"]
            search_status_dict["time"] = remaining_searches["time"] - 1

        print("Hours left {}".format(search_status_dict["time"]))
        
        tweet_results, id_results, search_term_result = get_searches(search_terms, reference_tweet_search_ids, credential)
        
        if search_term_result:
            search_status_dict["searches"] = search_terms[search_term_result:]

    else:
        print("There's nothing to search right now")
        search_status_dict["time"] = remaining_searches["time"] - 1
  
    if remaining_searches['time'] == 0:
        for account in timeline_accounts:
            result = get_user_timeline_query(credential, int(account))
            if result is not None:
                tweet_results += clean_tweet_data(result, "#USERTIMELINE", str(account))
            else:
                print("missed search for timeline {}".format(account))

    upload_search_status(s3_client, search_status_dict)

    if not cleanupHandler:
        process_reference_id_files(s3_client, reference_tweet_search_ids, id_results)
        upload_output(s3_client, tweet_results)

    return
        

