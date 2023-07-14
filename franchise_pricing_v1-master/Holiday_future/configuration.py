# -*- coding: UTF-8 -*-
import os
import sys
import warnings
from os.path import join as join_path

cur_path = os.path.dirname(os.path.realpath(__file__))
sys.path.append(join_path(cur_path, '..'))

from Holiday_future.mail_send import MailSend
from common.enum.pricing_enum import EmailReceiver
from common.util.utils import EnvUtil
from common.configuration.config_base import ConfigBase

warnings.filterwarnings("ignore")

FUTURE_BATCH_7_1 = "7"
FUTURE_BATCH_14_1 = "14"
FUTURE_BATCH_21_1 = "21"

HOLIDAY_CODE_MID_AUTUMN = "1"
HOLIDAY_CODE_NATIONAL_DAY = "2"


class JobConfig(ConfigBase):
    DEFAULT_BASE_PRICE_CHANGE_BATCH_SIZE = 200

    HOLIDAY_CODE_MAP = {
        HOLIDAY_CODE_MID_AUTUMN: "MID_AUTUMN",
        HOLIDAY_CODE_NATIONAL_DAY: "NATIONAL_DAY"
    }

    def __init__(self, config_env):
        ConfigBase.__init__(self, config_env)
        self._config = ConfigBase.create_config_for_file_path(
            join_path(cur_path, 'config', 'config_{0}.ini'.format(self.get_config_env())))
        self.set_mail_send(MailSend())

    def add_args_for_parser(self, parser):
        ConfigBase.add_args_for_parser(self, parser)
        parser.add_argument('-futureBatch', help='The future batch in {0}: 7 days pre,{1}: 14 days pre,{2}: 21 days pre'
                            .format(FUTURE_BATCH_7_1, FUTURE_BATCH_14_1, FUTURE_BATCH_21_1), default="7", type=str)
        parser.add_argument('-holidayCode', help='The holiday code in {0}: MID_AUTUMN, {1}: NATIONAL_DAY'.format(
            HOLIDAY_CODE_MID_AUTUMN, HOLIDAY_CODE_NATIONAL_DAY), default="1", type=str)
        parser.add_argument('-disablePriceDiff', help='if including hotels those price same as recent price, '
                                                      'default FALSE', default=False, action='store_true')

    def get_future_batch(self):
        return self.args.futureBatch

    def get_holiday_code(self):
        return self.args.holidayCode

    def get_holiday_desc(self):
        return self.HOLIDAY_CODE_MAP.get(self.args.holidayCode)

    def disable_price_diff(self):
        return self.args.disablePriceDiff

    def get_child_config(self):
        return self._config

    def get_base_price_change_url(self):
        return self.get_child_config().get('env', 'base_price_change_url')

    @staticmethod
    def get_base_price_change_batch_size():
        return JobConfig.DEFAULT_BASE_PRICE_CHANGE_BATCH_SIZE

    def get_holiday_future_v1_mail_receivers(self):
        if EnvUtil.is_prod_env(self.get_env()):
            return self.get_mail_receiver().get_receivers_by_email_id(EmailReceiver.EMAIL_ID_3011.value)
        else:
            return ''.join(
                self.get_child_config().get('email', 'holiday_future_v1_mail_receivers').split()).split(';')

    def get_hotel_price_data_receivers(self):
        if EnvUtil.is_prod_env(self.get_env()):
            return self.get_mail_receiver().get_receivers_by_email_id(EmailReceiver.EMAIL_ID_3004.value)
        else:
            return ''.join(self.get_child_config().get('email', 'hotel_price_data_receivers').split()).split(';')

    def get_ota_prices_receivers(self):
        if EnvUtil.is_prod_env(self.get_env()):
            return self.get_mail_receiver().get_receivers_by_email_id(EmailReceiver.EMAIL_ID_3002.value)
        else:
            return ''.join(self.get_child_config().get('email', 'ota_prices_receivers').split()).split(';')

    def get_hotel_price_insert_result_mail_receivers(self):
        if EnvUtil.is_prod_env(self.get_env()):
            return self.get_mail_receiver().get_receivers_by_email_id(EmailReceiver.EMAIL_ID_3005.value)
        else:
            return ''.join(self.get_child_config().get('email', 'price_insert_result_mail_receivers').split()).split(
                ';')

    def get_ota_plugin_result_receivers(self):
        return self.get_ota_prices_receivers()

    def get_holiday_future_v1_report_receivers(self):
        return ''.join(self.get_child_config().get('email', 'holiday_future_v1_mail_receivers').split()).split(';')

    def get_local_file_folder(self):
        return join_path(cur_path, self.get_result_folder())
