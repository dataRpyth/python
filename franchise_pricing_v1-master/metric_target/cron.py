import os
import sys
from os.path import join as join_path


cur_path = os.path.dirname(os.path.realpath(__file__))
sys.path.append(join_path(cur_path, '..'))
from metric_target.configuration import JobConfig
from metric_target.metric_target_job_runner import MetricTargetJob
from common.alert.alert import PricingAlert


def _get_config_env():
    config_env = 'SED_REPLACE_CONFIG_ENV'
    if config_env.startswith('SED_REPLACE'):
        config_env = 'local'
    return config_env


if __name__ == "__main__":
    job_config = JobConfig(_get_config_env())
    job = MetricTargetJob(job_config)
    PricingAlert.run_job(job)
