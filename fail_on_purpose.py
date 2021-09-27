import prefect
import pandas as pd
import datetime
import random
import kaggle
import zipfile
from datetime import timedelta
from prefect import task, Flow
from time import sleep
from prefect.triggers import manual_only
from prefect.executors import LocalDaskExecutor
from kaggle.api.kaggle_api_extended import KaggleApi
from prefect import Flow, task, Parameter, unmapped
from prefect.schedules import IntervalSchedule


@task(max_retries=3, retry_delay=timedelta(seconds=10))
def get_covid_data():
    api = KaggleApi()
    api.authenticate()
    from prefect import Flow, task, Parameter
    url = "https://www.kaggle.com/gpreda/covid-world-vaccination-progress/download"

    # downloading from kaggle.com/c/sentiment-analysis-on-movie-reviews
    # there are two files, train.tsv.zip and test.tsv.zip
    # we write to the current directory with './'
    api.dataset_download_file('priteshraj10/WILL_NOT_WORK_covid-vaccination-all-countries-data',
                                file_name = 'vaccinations.csv'
                                , path='./')


    with zipfile.ZipFile('C:/Users/dbabj/developement/python/prefect_testing/vaccinations.csv.zip', 'r') as zipref:
        zipref.extractall('C:/Users/dbabj/developement/python/prefect_testing')
    covid_pd = pd.read_csv('C:/Users/dbabj/developement/python/prefect_testing/vaccinations.csv')
    covid_pd.fillna(0)
    covid_pd.to_csv('C:/Users/dbabj/developement/python/prefect_testing/vaccinations_new.csv')
    print(type(covid_pd))
    return covid_pd

schedule = IntervalSchedule(interval=timedelta(minutes=10))

with Flow('failure_test', schedule) as flow:
    covid_pd = get_covid_data()
    

flow.register(project_name="Tutorial")
flow.run_agent()
