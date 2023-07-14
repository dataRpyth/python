import os
import sys
from os.path import join as join_path


cur_path = os.path.dirname(os.path.realpath(__file__))
sys.path.append(join_path(cur_path, '..'))

from CHINA_Rebirth.mail_send import MailSend
from CHINA_Rebirth.run_rule_base_version import ChinaRebirthRuleBaseJob
# from CHINA_Rebirth.run_cluster_version import ChinaRebirthClusterBaseJob
from common.alert.alert import PricingAlert
from CHINA_Rebirth.configuration import JobConfig
from common.util.utils import LogUtil


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
    job = ChinaRebirthRuleBaseJob(job_config)
    PricingAlert.run_job(job)
