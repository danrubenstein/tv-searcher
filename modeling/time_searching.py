import re
import string
import os
import sys

import pandas as pd 
import numpy as np

from utils import load_tweets_as_dataframe

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
exp0 = "(today|yesterday|tomorrow|tonight|tonite)"
exp1 = "(before|after|earlier|later|ago)"
exp2 = "(this|next|last)"
exp3 = "(up\ next|soon)"
exp4 = "(morning|afternoon|noon|night)"

exp_tz = "(e\.t\.|et|ct|c\.t\.|mt|m\.t\.|pt|p\.t\.)"
exp_tod = "(a\.m\.|p\.m\.)"
exp_units = "(hour|hours|minutes|minute|day|days)"

regxp1 = "((\d+|(" + numbers + "[-\s]?)+) " + dmy + "s? " + exp1 + ")"
regxp2 = "(" + exp2 + " (" + dmy + "|" + week_day + "|" + month + "))"

reg1 = re.compile(regxp1, re.IGNORECASE)
reg2 = re.compile(regxp2, re.IGNORECASE)

if __name__ == "__main__":
    
    tweets_df = load_tweets_as_dataframe()

    tweets_df['exp0_match'] = tweets_df['tweet_status_cleaned'].str.contains(exp0).astype(int)
    tweets_df['exp1_match'] = tweets_df['tweet_status_cleaned'].str.contains(exp1).astype(int)
    tweets_df['exp2_match'] = tweets_df['tweet_status_cleaned'].str.contains(exp2).astype(int)
    tweets_df['exp3_match'] = tweets_df['tweet_status_cleaned'].str.contains(exp3).astype(int)
    tweets_df['exp4_match'] = tweets_df['tweet_status_cleaned'].str.contains(exp4).astype(int)

    tweets_df['tz_match'] = tweets_df['tweet_status_cleaned'].str.contains(exp_tz).astype(int)
    tweets_df['tod_match'] = tweets_df['tweet_status_cleaned'].str.contains(exp_tod).astype(int)
    tweets_df['units_match'] = tweets_df['tweet_status_cleaned'].str.contains(exp_units).astype(int)

    tweets_df['time_match'] = ((tweets_df['exp0_match'] + tweets_df['exp1_match'] 
                                + tweets_df['exp2_match'] + tweets_df['exp3_match'] + tweets_df['exp4_match'])
                                * (tweets_df['tod_match'] + tweets_df['tz_match'] + tweets_df['units_match']))

    # Pre-processing