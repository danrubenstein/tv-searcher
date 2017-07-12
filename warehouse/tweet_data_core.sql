/*
 * These tables hold the primary information about scraping 
 * and the associated metadata with the searches. 
 */

CREATE schema if not exists tweet_data;

DROP TABLE if exists tweet_data.scraping_raw_records;
CREATE table if not exists tweet_data.scraping_raw_records
(
	search_term_raw character varying(100),
	search_term_input character varying(100),
	tweet_id bigint, 
	tweet_created_at timestamp, 
	tweet_status character varying(250), 
	user_screen_name character varying(100), 
	user_id bigint, 
	user_verified boolean, 
	user_followers integer, 
	user_following integer
); 

DROP TABLE if exists tweet_data.scraping_raw_tweets;
CREATE table if not exists tweet_data.scraping_raw_tweets
(
	id bigint, 
	tweet_created_at timestamp, 
	tweet_status character varying(250), 

	user_id bigint, 
	user_verified boolean, 
	PRIMARY KEY(id)
); 


CREATE table if not exists tweet_data.scraping_users_found 
(
	screen_name character varying(100), 
	id int, 
	verified boolean, 
	followers integer, 
	following integer,
	added_at timestamp, 
	last_seen timestamp,

	PRIMARY KEY(id)
); 

CREATE table if not exists tweet_data.scraping_search_meta
(
	search_term_raw character varying(100), 
	search_term_input character varying(100), 
	search_time timestamp

)

