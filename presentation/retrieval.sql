
select 	
	a.*, b.user_screen_name

	from 

	(select id from label_data.tweets_not_labeled 
		where label_priority<0.3
	union all 
	select id from label_data.tweets_labeled 
		where label=1) a 

	inner join 

	label_data.tweets_in_pipeline b 

	on a.id = b.id 
	order by b.tweet_created_at desc