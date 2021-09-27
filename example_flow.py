import prefect
from prefect import task, Flow
import datetime
import random
from time import sleep
from prefect.triggers import manual_only
# import the line below at the top of your file
from prefect.executors import LocalDaskExecutor


@task
def inc(x):
    sleep(random.random() / 10)
    return x + 1


@task
def dec(x):
    sleep(random.random() / 10)
    x = x-1
    return x


@task
def add(x, y):
    sleep(random.random() / 10)
    return x + y


@task(name="sum")
def list_sum(arr):
    logger = prefect.context.get("logger")
    logger.info(f"total sum : {sum(arr)}")
    return sum(arr)


with Flow("getting-started-example") as flow:

    incs = inc.map(x=range(10))
    decs = dec.map(x=range(10))
    adds = add.map(incs, decs)
    total = list_sum(adds)

# in the last two lines, add the new executor and modify
# flow.run to use said executor
executor = LocalDaskExecutor()
flow.run(executor=executor)
flow.register(project_name="Tutorial")
flow.run_agent()

