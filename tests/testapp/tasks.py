import logging

from django.tasks import task
from grinder import cron

logger = logging.getLogger(__name__)


@cron("*/5 * * * *")
@task
def my_task():
    logger.info("Hello World!")
