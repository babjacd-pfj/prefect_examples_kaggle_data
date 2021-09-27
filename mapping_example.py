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


@task
def get_covid_data():
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
    covid_pd = pd.read_csv('C:/Users/dbabj/developement/python/prefect_testing/vaccinations.csv')
    covid_pd.fillna(0)
    covid_pd.to_csv('C:/Users/dbabj/developement/python/prefect_testing/vaccinations_new.csv')
    print(type(covid_pd))
    return covid_pd

@task
def get_covid_location_list(covid_pd):
    location_list = covid_pd['location'].unique().tolist()
    #a_list = ["abc", "def", "ghi"]
    textfile = open("location_file.txt", "w")
    for element in location_list:
        textfile.write(element + "\n")
    textfile.close()
    return location_list

@task
def sum_covid_data_by_location(location, covid_pd):
    location_pd = covid_pd.loc[covid_pd['location'] == location]
    vax_sum = location_pd['total_vaccinations'].sum()
    return vax_sum

@task
def sum_vax(vax_sum):

    total_vax = sum(vax_sum)
    f = open("sum.txt", "w")
    integer = total_vax
    f.write(str(integer))
    f.close()
    #total_vax.to_csv('C:/Users/dbabj/developement/python/prefect_testing/vaccinations_new.csv')
    print(total_vax)
    return total_vax

schedule = IntervalSchedule(interval=timedelta(minutes=10))

with Flow('map_test', schedule) as flow:
    covid_pd = get_covid_data()
    location_list = get_covid_location_list(covid_pd)
    mapped_results = sum_covid_data_by_location.map(location_list, unmapped(covid_pd))
    total = sum_vax(mapped_results)

flow.register(project_name="Tutorial")
flow.run_agent()
