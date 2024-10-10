import os
import sys
import logging
import requests
from celery import Celery
from celery.signals import task_postrun, after_setup_logger
from celery.utils.log import get_task_logger
from os import getenv

logger = get_task_logger(__name__)
cel = Celery('worker',
             broker=f"redis://{getenv('REDIS_HOST', 'redis')}:{getenv('REDIS_PORT_NUMBER', 6379)}",
             backend=f"redis://{getenv('REDIS_HOST', 'redis')}:{getenv('REDIS_PORT_NUMBER', 6379)}")
API_URL = os.environ.get('API_URL', "http://172.20.28.128:8000")

datas = {}


@cel.task(name="create_task", bind=True)
def create_task(current_task, path, hook):
    url = f'{API_URL}/{path.replace("#", "%23")}'
    logger.info(url)
    campaign_id = current_task.request.id
    headers = {
        "campaign_id": campaign_id
    }
    r = requests.get(url, headers=headers)
    szip = r.json()

    datas[current_task.request.id] = hook
    zipfile = szip.get("zipfile")
    if not zipfile:
        logger.info(f"-------------------------------------- no zipfile")
        logger.info(vars(r))
        logger.info(str(szip))
        logger.info(f"-------------------------------------------------")
        return None
    return zipfile


# @task_success.connect()
# def task_success_notifier(sender=None, **kwargs):
#     print("DONE")
@after_setup_logger.connect
def setup_loggers(logger, *args, **kwargs):
    logger.addHandler(logging.StreamHandler(sys.stdout))


@task_postrun.connect()
def task_postrun_notifier(sender=None, **kwargs):
    if kwargs["state"] == "SUCCESS":
        logger.info(f"Task id: {kwargs['task_id']} {kwargs['state']} -> {kwargs['retval']}")
        if datas[kwargs["task_id"]] is not None:
            r = requests.post(datas[kwargs["task_id"]], data=kwargs["retval"], verify=False)
            logger.debug(r.text)
    else:
        logger.warning(f"Task id: {kwargs['task_id']} {kwargs['state']}")
