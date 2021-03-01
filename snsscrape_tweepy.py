import os
import json
import time
from itertools import chain
import snscrape.modules.twitter as sntwitter
import tweepy
from tweepy import RateLimitError

from utils import save_to_csv, merge_txt_files_scraped, ROOT_DIR, SCRAPED_TWEET_PATH

# get twitter app data saved in a json file
twitter_auth_data = open("twitter_auth_data.json").read()
twitter_auth_data_json = json.loads(twitter_auth_data)

access_token = twitter_auth_data_json["access_token"]
access_token_secret = twitter_auth_data_json["access_token_secret"]
consumer_key = twitter_auth_data_json["consumer_key"]
consumer_secret = twitter_auth_data_json["consumer_secret"]

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tweepy.API(auth)


def snscrape_ids(keyword_user_search_param, keywords_users_list, since, until, lang):
    dir_name = f"{since.replace('-', '')}_{until.replace('-', '')}"

    try:
        os.mkdir("scraped_tweet")
    except FileExistsError:
        print("Directory 'scraped_tweet' already exists")

    try:
        os.chdir(SCRAPED_TWEET_PATH)
        os.mkdir(dir_name)
        print("Directory", dir_name, "Created ")
    except FileExistsError:
        print("Directory", dir_name, "already exists")

    for keyword in keywords_users_list:
        if len(keyword) > 0:
            output_name = f"{keyword.replace(' ', '_')}_{since.replace('-', '')}_{until.replace('-', '')}.txt"
            output_path = os.path.join(ROOT_DIR, 'scraped_tweet', dir_name, output_name)

            print(f'scraping tweets with keyword: "{keyword}" ...')
            try:
                os.system(
                    f'snscrape twitter-{keyword_user_search_param} "{keyword} since:{since} until:{until} lang:{lang}" > {output_path}')
            except Exception as err:
                print(f"SNSCRAPE ERROR: {err}")

    print(f'Scraped all tweets in keywords list.')

    # merge all txt files in a folder in a single txt file and delete duplicated ids
    joined_txt_no_duplicate = merge_txt_files_scraped(dir_name)

    with open(f"tweets_ids_{dir_name}.txt", "w") as outfile:
        outfile.writelines(joined_txt_no_duplicate)
        print(f"'tweets_ids_{dir_name}.txt' saved in folder {dir_name}")

    return joined_txt_no_duplicate


# send request to twitter using tweepy (input: batch of 50 ids, output: for each ids a tweet containing:
# {id, username, text, date, location, keyword} )
def twitter_api_caller(keyword_user_search_param, keywords_list, ids, batch_size, save_dir, csv_name, collect_replies):
    if keyword_user_search_param == 'search':
        csv_columns = ['id', 'username', 'text', 'keywords', 'date', 'location']
    else:
        csv_columns = ['id', 'username', 'text', 'date', 'location']

    if collect_replies:
        csv_columns.append('replies')

    try:
        os.chdir(SCRAPED_TWEET_PATH)
        os.mkdir(save_dir)

        print("Directory 'final_tweet_csv' Created")
    except FileExistsError:
        print("Directory 'final_tweet_csv' already exists")

    n_chunks = int((len(ids) - 1) // batch_size + 1)

    tweets = []
    i = 0
    while i < n_chunks:

        if i > 0 and i % 300 == 0:
            # if batch number exceed 300 request could fail
            time.sleep(60)

        if i != n_chunks - 1:
            batch = ids[i * batch_size:(i + 1) * batch_size]
        else:
            batch = ids[i * batch_size:]

        print(f"Processing batch n° {i + 1}/{n_chunks} ...")
        try:
            list_of_tw_status = api.statuses_lookup(batch, tweet_mode="extended")
        except RateLimitError as err:
            print('Tweepy: Rate Limit exceeded')
            # https://developer.twitter.com/en/docs/twitter-api/v1/tweets/timelines/faq
            save_to_csv(tweets, save_dir, f"{csv_name}_last_batch_{i}", csv_columns)
            break
        except Exception as err:
            save_to_csv(tweets, save_dir, f"{csv_name}_last_batch_{i}", csv_columns)
            print(f"General Error: {str(err)}")
            break

        tweets_batch = []
        for status in list_of_tw_status:
            try:
                tweet = {"id": status.id,
                         "username": status.user.screen_name,
                         "text": status.full_text.replace('\n', ' '),
                         "date": str(status.created_at),
                         "location": status.user.location}

                if keyword_user_search_param == 'search':
                    keywords_in_tweet = get_tweet_keywords(keywords_list, status)

                    tweet["keywords"] = list(set(keywords_in_tweet))

                if collect_replies:
                    replies = collect_tweet_replies(status.id, max_num_replies=100)
                    tweet['replies'] = replies

            except Exception as err:
                print(f"General Error: {str(err)}")
                continue
            tweets_batch.append(tweet)
        print(f"Processed - scraped {len(tweets_batch)} tweets.")
        if len(tweets_batch) == 0:
            save_to_csv(tweets, save_dir, f"{csv_name}_last_batch_{i}", csv_columns)
            print("No tweets scraped")
            break

        i += 1
        tweets.append(tweets_batch)

    save_to_csv(tweets, save_dir, csv_name, csv_columns)


def get_tweet_keywords(keywords_list, status):
    keywords_tweet = []
    for keyword in keywords_list:
        if len(keyword.split()) == 1:
            if keyword.lower() in status.full_text.lower() or keyword.lower() in status.user.screen_name.lower():
                keywords_tweet.append(keyword)
        elif len(keyword.split()) > 1:
            boolean_parameter = []
            for word in keyword.split():
                is_present = word.lower() in status.full_text.lower()
                boolean_parameter.append(is_present)
            if all(boolean_parameter):
                keywords_tweet.append(keyword)
        else:
            continue
    return list(set(keywords_tweet))


def collect_tweet_replies(tweet_id, max_num_replies):
    replies_ids = []

    for reply in sntwitter.TwitterSearchScraper(
            query=f"conversation_id:{tweet_id} (filter:safe OR -filter:safe)").get_items():
        replies_ids.append(reply.id)

    batch_size_replies = 50
    n_chunks_repl = int((len(replies_ids) - 1) // batch_size_replies + 1)

    replies = []
    i = 0
    while i < n_chunks_repl:

        if i > 0 and i % 300 == 0:
            # if batch number exceed 300 request could fail
            time.sleep(60)

        if max_num_replies <= i*batch_size_replies:
            # if too many replies
            break

        if i != n_chunks_repl - 1:
            batch = replies_ids[i * batch_size_replies:(i + 1) * batch_size_replies]
        else:
            batch = replies_ids[i * batch_size_replies:]

        print(f"Processing REPLIES batch n° {i + 1}/{n_chunks_repl} ...")
        try:
            list_of_tw_status_reply = api.statuses_lookup(batch, tweet_mode="extended")
        except:
            break

        replies_batch = []
        for status_reply in list_of_tw_status_reply:
            # print(status_reply)
            if hasattr(status_reply, 'full_text'):
                reply = {
                    "id": status_reply.id,
                    "username": status_reply.user.screen_name,
                    "text": status_reply.full_text.replace('\n', ' ')
                }
                # print(reply)
                replies_batch.append(reply)
        i += 1
        replies.append(replies_batch)

    return list(chain.from_iterable(replies))


def fetch_tweets(keyword_user_search_param, keywords_users_list, since, until, lang, batch_size, save_dir, csv_name,
                 collect_replies):
    users_and_ids = snscrape_ids(keyword_user_search_param, keywords_users_list, since, until, lang)
    ids = list(map(lambda x: x.split(" ")[1].strip('\n'), users_and_ids))
    twitter_api_caller(keyword_user_search_param, keywords_users_list, ids, batch_size, save_dir, csv_name,
                       collect_replies)
