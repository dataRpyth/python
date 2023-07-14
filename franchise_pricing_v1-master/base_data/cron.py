import os
import sys
from os.path import join as join_path

cur_path = os.path.dirname(os.path.realpath(__file__))
sys.path.append(join_path(cur_path, '..'))
from base_data.price_history_job import PriceHistoryJob
from base_data.sku_sales_job import SkuSalesJob
from base_data.booking_price_alert_job import BookingPriceAlertJob
from base_data.configuration import BaseDataJobConfig
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
    job_config = BaseDataJobConfig(_get_config_env())
    job = BookingPriceAlertJob(job_config)
    PricingAlert.run_job(job)
