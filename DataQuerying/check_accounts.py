import botometer
import pandas as pd
import numpy as np
import os
from dotenv import load_dotenv

load_dotenv()  # loads .env file that provides the API keys

# keys = open(".env", "x")  # use this to create an env file to load the API keys
# The file should look like this:
# rapidapi_key = XXXXXXXXXXXXXXXXXX
# consumer_key = XXXXXXXXXXXXXXXXXX
# consumer_secret = XXXXXXXXXXXXXXX
# access_token = XXXXXXXXXXXXXXXXXX
# access_token_secret = XXXXXXXXXXX

# botometer stuff
rapidapi_key = os.getenv('rapidapi_key')
twitter_app_auth = {
    'consumer_key': os.getenv('consumer_key'),
    'consumer_secret': os.getenv('consumer_secret'),
    'access_token': os.getenv('access_token'),
    'access_token_secret': os.getenv('access_token_secret'),
}
bom = botometer.Botometer(wait_on_ratelimit=True,
                          rapidapi_key=rapidapi_key,
                          **twitter_app_auth)

start_at = 34000  # start at specific line
checks_number = 2000  # number of accounts to be checked

TARGET_FILE = 'random100,000_cleaned_df_final.csv'  # file to be opened
OUTFILE = "results_" + str(start_at) + "_to_" + str(start_at + checks_number) + "_" + TARGET_FILE  # results file

uid = []  # list with User Ids to be checked

df = pd.read_csv(TARGET_FILE)  # use first column as indices

uid = df['From-User-Id'].astype("int64")  # extracts User Ids

# result is dict of dict with 'cap' containing 'english' and 'universal',
# 'display_scores' which is irrelevant, 'raw_scores' containing 'english' and 'universal' containing the
# different types of Bots and 'user' containing 'majority_lang' and 'user data' which contains 'id_str' and
# 'screen name'

# list of results for each account
df_results = pd.DataFrame([], columns=['en_cap', 'univ_cap', 'en_raw_score_overall', 'univ_raw_score_overall',
                                       'en_raw_score_astroturf', 'univ_raw_score_astroturf',
                                       'en_raw_score_fake_follower',
                                       'univ_raw_score_fake_follower', 'en_raw_score_financial',
                                       'univ_raw_score_financial', 'en_raw_score_self_declared',
                                       'univ_raw_score_self_declared',
                                       'en_raw_score_spammer', 'univ_raw_score_spammer', 'en_raw_score_other',
                                       'univ_raw_score_other', 'user_maj_lang', 'user_id', 'user_screen_name'])

print(
    'opened file ' + TARGET_FILE + ', starting at ' + str(start_at) + ', checking ' + str(checks_number) + ' accounts')
for screen_name, result in bom.check_accounts_in(uid[start_at:start_at + checks_number]):  # checks number of accounts
    if len(result) > 1:
        print('Screen name: ' + result['user']['user_data']['screen_name'])
        print(result['cap'])
        #  create list of results
        data = [result['cap']['english'], result['cap']['universal'], result['raw_scores']['english']['overall'],
                result['raw_scores']['universal']['overall'], \
                result['raw_scores']['english']['astroturf'], result['raw_scores']['universal']['astroturf'],
                result['raw_scores']['english']['fake_follower'], result['raw_scores']['universal']['fake_follower'], \
                result['raw_scores']['english']['financial'], result['raw_scores']['universal']['financial'],
                result['raw_scores']['english']['self_declared'], result['raw_scores']['universal']['self_declared'], \
                result['raw_scores']['english']['spammer'], result['raw_scores']['universal']['spammer'],
                result['raw_scores']['english']['other'], result['raw_scores']['universal']['other'], \
                result['user']['majority_lang'], result['user']['user_data']['id_str'],
                result['user']['user_data']['screen_name']]
        #  create Dataframe from data
        df_result = pd.DataFrame([data],
                                 columns=['en_cap', 'univ_cap', 'en_raw_score_overall', 'univ_raw_score_overall',
                                          'en_raw_score_astroturf', 'univ_raw_score_astroturf',
                                          'en_raw_score_fake_follower',
                                          'univ_raw_score_fake_follower', 'en_raw_score_financial',
                                          'univ_raw_score_financial', 'en_raw_score_self_declared',
                                          'univ_raw_score_self_declared',
                                          'en_raw_score_spammer', 'univ_raw_score_spammer', 'en_raw_score_other',
                                          'univ_raw_score_other', 'user_maj_lang', 'user_id', 'user_screen_name'])
        df_results = df_results.append(df_result)  # add results to df_results
    else:
        df_results = df_results.append(pd.Series(dtype='int64'), ignore_index=True)
        print("User ID not found")  # if results is not as expected, UID is probably wrong

df_results.index = np.arange(start_at, len(df_results) + start_at)  # keeps original indices
df = df.reset_index(drop=True)
df = df[start_at:start_at + checks_number].join(df_results)  # joins with original file
df.to_csv(OUTFILE, sep=';')
print('saved results to ' + OUTFILE)
