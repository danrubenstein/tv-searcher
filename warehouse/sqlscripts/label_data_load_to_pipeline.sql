/* 
 * Loading new tweets into the labeling pipeline 
 * called out of cleaning.py
 */

create local temporary table staging_tweet_data_2 as 

	select tweet_id as id,
			tweet_created_at::timestamp,
			tweet_status,
			user_id,
			user_screen_name, 
			user_verified, 
			priority

		from label_data.staging_tweet_data a 
		where a.tweet_id not in 
			(select id from label_data.tweets_in_pipeline)
		and a.tweet_status not in 
			(select tweet_status from label_data.tweets_in_pipeline)
;  


insert into label_data.tweets_in_pipeline
	select * from staging_tweet_data_2;

insert into label_data.tweets_not_labeled 
	select *, False from staging_tweet_data_2;

commit; 

drop table staging_tweet_data_2;
drop table label_data.staging_tweet_data;
