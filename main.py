import os

from snsscrape_tweepy import fetch_tweets, twitter_api_caller

if __name__ == '__main__':
    since = '2019-10-28'
    until = '2019-10-30'
    lang = 'en'
    batch_size = 50  # recommended batch size
    save_dir = "final_tweet_csv" # final saving subdir located in scraped_tweet
    csv_name = 'tweets_2019'

    # load txt file containg a list of keywords
    keywords_list = open("keyword_lists/keyword_elections.txt", mode='r', encoding='utf-8').read().splitlines()

    fetch_tweets(keywords_list, since, until, lang, batch_size, save_dir, csv_name)

    ## use this if you need to fetch tweet using ids
    # file_ids = '20191129_20191130'
    # single_txt = open(os.path.join("scraped_tweet", file_ids, f"tweets_ids_{file_ids}.txt"), 'r').read().splitlines()
    #
    # ids = list(map(lambda x: x.split(" ")[1], single_txt))

    # twitter_api_caller(keywords_list, ids, batch_size, save_dir, csv_name)
