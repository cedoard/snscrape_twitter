import csv
import glob
import os
from itertools import chain

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))


def save_to_csv(list_to_save, save_dir, csv_name, csv_columns, par=True):
    if par:
        list_to_save = list(chain.from_iterable(list_to_save))

    os.chdir(os.path.join(ROOT_DIR, save_dir))
    try:
        with open(f'{csv_name}_len_{len(list_to_save)}.csv', 'w', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
            writer.writeheader()
            for data in list_to_save:
                writer.writerow(data)
            print(f"Scraped a total of {len(list_to_save)} tweets | File '{csv_name}' csv saved successfully.")
    except IOError:
        print("I/O error")


def merge_txt_files_scraped(dir_name):
    os.chdir(os.path.join(ROOT_DIR, dir_name))
    read_files = glob.glob("*.txt")
    joined_txt = [open(f, "r").readlines() for f in read_files if not f.startswith("tweets_ids")]
    joined_txt_no_duplicate_url = list(set(list(chain.from_iterable(joined_txt))))
    
    print(f"Deleted {len(list(chain.from_iterable(joined_txt))) - len(joined_txt_no_duplicate_url)} duplicated tweets")
    
    return [str(j.split('/')[3] + " " + j.split('/')[-1]) for j in joined_txt_no_duplicate_url]
