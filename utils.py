import glob
import json
import os
from itertools import chain

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))


def save_to_csv(list_to_save, save_dir, csv_name, par=True):
    if par:
        list_to_save = list(chain.from_iterable(list_to_save))

    save_dir = os.path.join(ROOT_DIR, "scraped_tweet", save_dir)
    os.chdir(save_dir)

    with open(f'{csv_name}_len_{len(list_to_save)}.csv', 'w', encoding='utf-8') as outfile:
        json.dump(list_to_save, outfile, indent=4, sort_keys=True, ensure_ascii=False)
        print(f"Scraped {len(list_to_save)} comments | File '{csv_name}' csv saved successfully.")


def merge_txt_files_scraped(dir_name):
    
    read_files = glob.glob("*.txt")
    joined_txt = [open(f, "r").readlines() for f in read_files if not f.startswith("tweets_ids")]
    joined_txt_no_duplicate_url = list(set(list(chain.from_iterable(joined_txt))))
    
    print(f"Deleted {len(list(chain.from_iterable(joined_txt))) - len(joined_txt_no_duplicate_url)} duplicated tweets")
    
    return [str(j.split('/')[3] + " " + j.split('/')[-1]) for j in joined_txt_no_duplicate_url]
