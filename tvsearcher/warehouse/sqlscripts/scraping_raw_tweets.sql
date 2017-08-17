begin; 

truncate tweet_data.scraping_raw_tweets;


INSERT INTO tweet_data.scraping_raw_tweets
	select distinct tweet_id as id, 
		tweet_created_at, 
		tweet_status, 
		user_id, 
		user_verified

		from tweet_data.scraping_raw_records; 

commit;