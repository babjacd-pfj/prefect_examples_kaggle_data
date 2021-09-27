import prefect
import pandas as pd
import datetime
import random
import kaggle
import zipfile
from prefect import task, Flow
from time import sleep
from prefect.triggers import manual_only
from prefect.executors import LocalDaskExecutor
from kaggle.api.kaggle_api_extended import KaggleApi
from prefect import Flow, task, Parameter, unmapped, case, Task

class get_covid_data(Task):
    def run(self):
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
        covid_pd.fillna(0, inplace=True)
        covid_pd.to_csv('C:/Users/dbabj/developement/python/prefect_testing/vaccinations_new.csv')
        print(type(covid_pd))
        if not covid_pd.empty:
            return True 

class ActionIfTrue(Task):
    def run(self):
        return "Dataframe exists"

class ActionIfFalse(Task):
    def run(self):
        return "Dataframe does not exist"

class AnotherAction(Task):
    def run(self, val):
        print(val)

flow = Flow("conditional-branches")

check_condition = get_covid_data()
flow.add_task(check_condition)

with case(check_condition, True):
    val = ActionIfTrue()
    flow.add_task(val)
    AnotherAction().bind(val=val, flow=flow)

with case(check_condition, False):
    val = ActionIfFalse()
    flow.add_task(val)
    AnotherAction().bind(val=val, flow=flow)

flow.register(project_name="Tutorial")
flow.run_agent()
