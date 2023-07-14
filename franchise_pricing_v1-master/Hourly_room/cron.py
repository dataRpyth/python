import os
import sys
from os.path import join as join_path


cur_path = os.path.dirname(os.path.realpath(__file__))
sys.path.append(join_path(cur_path, '..'))

from common.alert.alert import PricingAlert
from Hourly_room.configuration import JobConfig
from Hourly_room.hourly_room_job_runner import HourlyRoomJob


def _get_config_env():
    config_env = 'SED_REPLACE_CONFIG_ENV'
    if config_env.startswith('SED_REPLACE'):
        config_env = 'local'
    return config_env


if __name__ == "__main__":
    job_config = JobConfig(_get_config_env())
    job = HourlyRoomJob(job_config)
    PricingAlert.run_job(job)
