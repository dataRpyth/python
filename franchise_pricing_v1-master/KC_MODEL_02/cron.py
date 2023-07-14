import os
import sys
from os.path import join as join_path

cur_path = os.path.dirname(os.path.realpath(__file__))
sys.path.append(join_path(cur_path, '..'))
from KC_MODEL_02.mail_send import MailSend
from KC_MODEL_02.kc_job_runner import KcJob
from KC_MODEL_02.configuration import JobConfig
from common.alert.alert import PricingAlert
from common.util.utils import EnvUtil, LogUtil


def _get_config_env():
    config_env = 'SED_REPLACE_CONFIG_ENV'
    if config_env.startswith('SED_REPLACE'):
        config_env = 'local'
    return config_env


if __name__ == "__main__":
    logger = LogUtil.create_logger(join_path(cur_path, 'log'), 'log.txt')
    LogUtil.set_cur_logger(logger)
    job_config = JobConfig(_get_config_env())
    job_config.set_mail_send(MailSend())
    job = KcJob(job_config)
    PricingAlert.run_job(job)
