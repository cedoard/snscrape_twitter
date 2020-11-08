import os
import glob
from itertools import chain
import json
import time

import tweepy
from tweepy import RateLimitError, TweepError

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))

twitter_auth_data = open("twitter_auth_data").read()
twitter_auth_data_json = json.loads(twitter_auth_data)

access_token = twitter_auth_data_json["access_token"]
access_token_secret = twitter_auth_data_json["access_token_secret"]
consumer_key = twitter_auth_data_json["consumer_key"]
consumer_secret = twitter_auth_data_json["consumer_secret"]

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tweepy.API(auth)


def snscrape_ids(keywords_list, since, until):
    try:
        os.mkdir("scraped_tweet")
    except FileExistsError:
        print("Directory 'scraped_tweet' already exists")

    try:
        os.chdir(os.path.join(ROOT_DIR, "scraped_tweet"))

        dir_name = f"{since.replace('-', '')}_{until.replace('-', '')}"
        os.mkdir(dir_name)
        print("Directory", dir_name, "Created ")
    except FileExistsError:
        print("Directory", dir_name, "already exists")

    for keyword in keywords_list:
        if len(keyword) > 0:
            output_name = f"{keyword.replace(' ', '_')}_{since.replace('-', '')}_{until.replace('-', '')}.txt"
            output_path = os.path.join(ROOT_DIR, 'scraped_tweet', dir_name, output_name)

            print(f'scraping tweets with keyword: "{keyword}" ...')
            try:
                os.system(f'snscrape twitter-search "{keyword} since:{since} until:{until} lang:it" > {output_path}')
            except Exception as err:
                print(f"SNSCRAPE ERROR: {err}")

    print(f'Scraped all tweets in keywords list.')

    # merge all txt files in single txt file and delete duplicated ids
    os.chdir(os.path.join(ROOT_DIR, "scraped_tweet", dir_name))
    read_files = glob.glob("*.txt")

    joined_txt = [open(f, "r").readlines() for f in read_files if not f.startswith("tweets_ids")]

    joined_txt_no_dupl = list(set(list(chain.from_iterable(joined_txt))))
    print(f"Deleted {len(list(chain.from_iterable(joined_txt))) - len(joined_txt_no_dupl)} duplicated tweets")

    joined_txt_no_dupl_2 = [str(j.split('/')[3] + " " + j.split('/')[-1]) for j in joined_txt_no_dupl]

    with open(f"tweets_ids_{dir_name}.txt", "w") as outfile:
        outfile.writelines(joined_txt_no_dupl_2)
        print(f"raw file saved in folder {dir_name}")

    return joined_txt_no_dupl_2


def twitter_api_caller(keywords_list, ids, batch_size, save_dir, csv_name):
    try:
        os.chdir(os.path.join(ROOT_DIR, "scraped_tweet"))
        os.mkdir('final_tweet_csv')
        print("Directory 'final_tweet_csv' Created")
    except FileExistsError:
        print("Directory 'final_tweet_csv' already exists")

    n_chunks = int((len(ids) - 1) // 50 + 1)

    if n_chunks == 1:
        n_chunks += 1

    tweets = []
    i = 0
    while i < n_chunks - 1:
        i += 1

        if i % 300 == 0:
            # if batch number exceed 300 request could fail
            time.sleep(60)

        batch = ids[i * batch_size:(i + 1) * batch_size]

        print(f"Processing batch n° {i}/{n_chunks} ...")
        try:
            list_of_tw_status = api.statuses_lookup(batch, tweet_mode="extended")
        except RateLimitError as err:
            print('Tweepy: Rate Limit exceeded')
            # https://developer.twitter.com/en/docs/twitter-api/v1/tweets/timelines/faq
            save_to_csv(tweets, save_dir, f"{csv_name}_last_batch_{i}")
            break
        except Exception as err:
            save_to_csv(tweets, save_dir, f"{csv_name}_last_batch_{i}")
            print(f"General Error: {str(err)}")
            break

        tweets_batch = []
        for status in list_of_tw_status:
            try:
                tweet = {}
                tweet["id"] = status.id
                tweet["username"] = status.user.screen_name
                tweet["comment"] = status.full_text
                tweet["date"] = str(status.created_at)
                tweet["location"] = status.user.location

                kl1 = [e for e in keywords_list if e.lower() in status.full_text.lower()]
                kl2 = [e for e in keywords_list if e.lower() in status.user.screen_name.lower()]
                keywords = [x for x in set(kl1 + kl2) if len(x) > 0]
                tweet["keywords"] = keywords

            except Exception as err:
                print(f"General Error: {str(err)}")
                continue
            tweets_batch.append(tweet)

        tweets.append(tweets_batch)

    print(tweets)
    save_to_csv(tweets, save_dir, csv_name)


def save_to_csv(tweets, csv_name):
    tweets_final = list(chain.from_iterable(tweets))
    os.chdir(os.path.join(ROOT_DIR, "scraped_tweet", "final_tweet_csv"))
    with open(f'{csv_name}.csv', 'w',
              encoding='utf-8') as outfile:
        json.dump(tweets_final, outfile, indent=4, sort_keys=True, ensure_ascii=False)
        print(f"Scraped {len(tweets_final)} tweets | File '{csv_name}' csv saved successfully.")


def fetch_tweets(keywords_list, since, until, batch_size, csv_name):
    users_and_ids = snscrape_ids(keywords_list, since, until)
    ids = list(map(lambda x: x.split(" ")[1], users_and_ids))

    twitter_api_caller(ids, batch_size, csv_name)


if __name__ == '__main__':
    since = '2019-01-01'
    until = '2019-12-31'
    batch_size = 50  # reccomended batch size
    csv_name = 'tweet_'

    keywords_list = open("keyword_lists/keyword_elections.txt", mode='r', encoding='utf-8').read().splitlines()

    fetch_tweets(keywords_list, since, until, batch_size, csv_name)

    ## use this if you need to fetch tweet using ids
    # file_ids = '20200101_20201029'
    # single_txt = open(os.path.join("scraped_tweet", file_ids, f"tweets_ids_{file_ids}.txt"), 'r').read().splitlines()

    # ids1 = list(map(lambda x: x.split(" ")[1], single_txt))
    # twitter_api_caller(ids, batch_size, csv_name)
