/* 
 * Create statements for the label_data schema
 */

create schema if not exists label_data; 

drop table if exists label_data.tweets_in_pipeline; 
create table if not exists label_data.tweets_in_pipeline

(
	id bigint, 
	tweet_created_at timestamp, 
	tweet_status character varying(250) unique,

	user_id bigint,
	user_screen_name character varying(50), 
	user_verified boolean, 

	label_priority real,

	PRIMARY KEY(id)
); 

drop table if exists label_data.tweets_not_labeled;
create table if not exists label_data.tweets_not_labeled
(
	id bigint, 
	tweet_created_at timestamp, 
	tweet_status character varying(250) unique,

	user_id bigint,
	user_screen_name character varying(50), 
	user_verified boolean, 

	label_priority real,
	label_in_process boolean,
	PRIMARY KEY(id)

); 

drop table if exists label_data.tweets_labeled;
create table if not exists label_data.tweets_labeled
(
	id bigint, 
	label smallint, 
	input_time timestamp, 

	PRIMARY KEY(id)
); 

