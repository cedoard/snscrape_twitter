import glob
import json
import os
from itertools import chain

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))


def save_to_csv(tweets, csv_name):
    tweets_final = list(chain.from_iterable(tweets))
    os.chdir(os.path.join(ROOT_DIR, "scraped_tweet", "final_tweet_csv"))
    with open(f'{csv_name}.csv', 'w',
              encoding='utf-8') as outfile:
        json.dump(tweets_final, outfile, indent=4, sort_keys=True, ensure_ascii=False)
        print(f"Scraped {len(tweets_final)} tweets | File '{csv_name}' csv saved successfully.")


def merge_txt_files_scraped(dir_name):
    os.chdir(os.path.join(ROOT_DIR, "scraped_tweet", dir_name))
    read_files = glob.glob("*.txt")

    joined_txt = [open(f, "r").readlines() for f in read_files if not f.startswith("tweets_ids")]

    joined_txt_no_duplicate_url = list(set(list(chain.from_iterable(joined_txt))))
    print(f"Deleted {len(list(chain.from_iterable(joined_txt))) - len(joined_txt_no_duplicate_url)} duplicated tweets")

    joined_txt_no_duplicate = [str(j.split('/')[3] + " " + j.split('/')[-1]) for j in joined_txt_no_duplicate_url]

    return joined_txt_no_duplicate
