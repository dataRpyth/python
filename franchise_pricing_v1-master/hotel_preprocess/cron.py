import os
import sys
from os.path import join as join_path

cur_path = os.path.dirname(os.path.realpath(__file__))
sys.path.append(join_path(cur_path, '..'))
from hotel_preprocess.configuration import JobConfig
from common.alert.alert import PricingAlert
from common.util.utils import *
from hotel_preprocess.hotel_daily_preprocess_job_runner import HotelDailyPreprocessJob


def _get_config_env():
    config_env = 'SED_REPLACE_CONFIG_ENV'
    if config_env.startswith('SED_REPLACE'):
        config_env = 'local'
    return config_env


if __name__ == "__main__":
    logger = LogUtil.create_logger(join_path(cur_path, 'log'), 'log.txt')
    LogUtil.set_cur_logger(logger)
    job_config = JobConfig(_get_config_env())
    job = HotelDailyPreprocessJob(job_config)
    PricingAlert.run_job(job)
