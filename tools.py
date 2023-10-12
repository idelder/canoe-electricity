"""
Various tools
Written by Ian David Elder for the TEMOA Canada / CANOE model
"""


import os
import pandas as pd
import requests
import xmltodict
import json



this_dir = os.path.realpath(os.path.dirname(__file__)) + "/"
cache_dir = this_dir + "download_cache/"



def string_cleaner(string):

    clean_string = ''.join(letter for letter in string if letter in '- ()' or letter.isalnum())

    return clean_string



def get_file(url, file_type=None, name=None, use_cache=True):

    # Get the original file name
    if name == None: name = url.split("/")[-1].split("\\")[-1]
    if file_type == None: file_type = url.split(".")[-1]
    cache_file = cache_dir + name

    data = None
    if (use_cache and os.path.isfile(cache_file)):

        # Get from existing local cache
        if file_type == "csv": data = pd.read_csv(cache_file)
        elif "xl" in file_type: data = pd.read_excel(cache_file)
        elif file_type == "xml": data = json.load(cache_file)
        print(f"Got {name} from local cache.")
        
    else:

        # Download from url
        if file_type == "csv": data = pd.read_csv(url)
        elif "xl" in file_type: data = pd.read_excel(url)
        elif file_type == "xml": data = json.dumps(xmltodict.parse(requests.get(url).content))
        print(f"Downloaded {name}.")

        # Try to cache
        try:
            if file_type == "csv": data.to_csv(url)
            elif "xl" in file_type: data.to_excel(url)
            elif file_type == "xml": data.dump(cache_file)
            print(f"Cached {name}.")
        except: pass

    return data