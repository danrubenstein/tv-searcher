### Warehousing

Once taken from the S3 bucket, the data is then stored in a local PostgreSQL environment, the construction of which you can do with the Postgres App (AWS hosting is expensive). 

Look, I have a query and it works!

```
tv_searcher=> select DATE(tweet_created_at), count(*) from tweet_data.scraping_raw_tweets group by 1 order by 1;
    date    | count
------------+-------
 2017-07-02 |     3
 2017-07-03 |     6
 2017-07-04 |    27
 2017-07-05 |    27
 2017-07-06 |    17
 2017-07-07 |   100
 2017-07-08 |    45
 2017-07-09 |    62
 2017-07-10 |    84
 2017-07-11 |   931
 2017-07-12 |   630
 2017-07-13 |   545
(12 rows)

tv_searcher=>
```