import prefect
import urllib3
import zipfile

import requests
import pandas as pd
import datetime
import random
import kaggle
from prefect import task, Flow
from time import sleep
from prefect.triggers import manual_only
from prefect.executors import LocalDaskExecutor
from kaggle.api.kaggle_api_extended import KaggleApi

api = KaggleApi()
api.authenticate()
from prefect import Flow, task, Parameter
url = "https://www.kaggle.com/gpreda/covid-world-vaccination-progress/download"
# downloading from kaggle.com/c/sentiment-analysis-on-movie-reviews
# there are two files, train.tsv.zip and test.tsv.zip
# we write to the current directory with './'
api.dataset_download_file('priteshraj10/covid-vaccination-all-countries-data',
                              file_name = 'vaccinations.csv'
                              , path='./')


with zipfile.ZipFile('C:/Users/dbabj/developement/python/prefect_testing/vaccinations.csv.zip', 'r') as zipref:
    zipref.extractall('C:/Users/dbabj/developement/python/prefect_testing')
#kaggle datasets download -d priteshraj10/covid-vaccination-all-countries-data