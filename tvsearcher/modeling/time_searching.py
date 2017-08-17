import re
import string
import os
import sys

import pandas as pd 
import numpy as np

# Predefined strings.
numbers = "(^a(?=\s)|one|two|three|four|five|six|seven|eight|nine|ten| \
          eleven|twelve|thirteen|fourteen|fifteen|sixteen|seventeen| \
          eighteen|nineteen|twenty|thirty|forty|fifty|sixty|seventy|eighty| \
          ninety|hundred|thousand)"
day = "(monday|tuesday|wednesday|thursday|friday|saturday|sunday)"
week_day = "(monday|tuesday|wednesday|thursday|friday|saturday|sunday)"
month = "(january|february|march|april|may|june|july|august|september|\
          october|november|december)"
dmy = "(year|day|week|month)"
time_exp0 = "(today|yesterday|tomorrow|tonight|tonite)"
time_exp1 = "(before|after|earlier|later|ago)"
time_exp2 = "(this|next|last)"
time_exp3 = "(up\ next|soon)"
time_exp4 = "(morning|afternoon|noon|night)"

time_exp_tz = "(e\.t\.|et|ct|c\.t\.|mt|m\.t\.|pt|p\.t\.)"
time_exp_tod = "(a\.m\.|p\.m\.)"
time_exp_units = "(hour|hours|minutes|minute|day|days)"

regxp1 = "((\d+|(" + numbers + "[-\s]?)+) " + dmy + "s? " + time_exp1 + ")"
regxp2 = "(" + time_exp2 + " (" + dmy + "|" + week_day + "|" + month + "))"

reg1 = re.compile(regxp1, re.IGNORECASE)
reg2 = re.compile(regxp2, re.IGNORECASE)

def get_time_searching(tweets_df):

    tweets_df['time_exp0_match'] = tweets_df['tweet_status_cleaned'].str.contains(time_exp0).astype(int)
    tweets_df['time_exp1_match'] = tweets_df['tweet_status_cleaned'].str.contains(time_exp1).astype(int)
    tweets_df['time_exp2_match'] = tweets_df['tweet_status_cleaned'].str.contains(time_exp2).astype(int)
    tweets_df['time_exp3_match'] = tweets_df['tweet_status_cleaned'].str.contains(time_exp3).astype(int)
    tweets_df['time_exp4_match'] = tweets_df['tweet_status_cleaned'].str.contains(time_exp4).astype(int)

    tweets_df['tz_match'] = tweets_df['tweet_status_cleaned'].str.contains(time_exp_tz).astype(int)
    tweets_df['tod_match'] = tweets_df['tweet_status_cleaned'].str.contains(time_exp_tod).astype(int)
    tweets_df['units_match'] = tweets_df['tweet_status_cleaned'].str.contains(time_exp_units).astype(int)

    tweets_df['time_match'] = ((tweets_df['time_exp0_match'] + tweets_df['time_exp1_match'] 
                                + tweets_df['time_exp2_match'] + tweets_df['time_exp3_match'] + tweets_df['time_exp4_match'])
                                * (tweets_df['tod_match'] + tweets_df['tz_match'] + tweets_df['units_match']))

    return tweets_df