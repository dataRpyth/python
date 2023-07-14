# -*- coding: UTF-8 -*-
import os
import sys
import time
from datetime import datetime as dt
from os.path import join as join_path

cur_path = os.path.dirname(os.path.realpath(__file__))
sys.path.append(join_path(cur_path, '..'))

from common.enum.pricing_enum import EmailReceiver
from common.configuration.config_base import ConfigBase, EnvUtil


# =========================config read end====================================================
class JobConfig(ConfigBase):

    DEFAULT_DAY_SHIFT_START = 0
    DEFAULT_DAY_SHIFT_END = 3
    DEFAULT_PRESET_DAY_SHIFT_START = 24
    DEFAULT_PRESET_DAY_SHIFT_END = 53
    DEFAULT_SORT_VALUES = True

    def __init__(self, config_env):
        ConfigBase.__init__(self, config_env)
        self._config = ConfigBase.create_config_for_file_path(
            join_path(cur_path, 'config', 'config_{0}.ini'.format(self.get_config_env())))
        self._file_date_str = dt.strftime(dt.fromtimestamp(time.time()), "%Y_%m_%d_%H_%M")

    def add_args_for_parser(self, parser):

        ConfigBase.add_args_for_parser(self, parser)

        parser.add_argument('-ds', help='The day shift start for calculating days',
                            default=JobConfig.DEFAULT_DAY_SHIFT_START)

        parser.add_argument('-de', help='The day shift end for calculating days',
                            default=JobConfig.DEFAULT_DAY_SHIFT_END)

        parser.add_argument('-otaPricing', help='switch on ota pricing', default=False, action='store_true')

    def get_day_shift_start(self):
        return self.args.ds if not self.is_preset() else JobConfig.DEFAULT_PRESET_DAY_SHIFT_START

    def get_day_shift_end(self):
        return self.args.de if not self.is_preset() else JobConfig.DEFAULT_PRESET_DAY_SHIFT_END

    def get_ota_pricing(self):
        return self.args.otaPricing

    def is_sort_values(self):
        return JobConfig.DEFAULT_SORT_VALUES

    def get_preset_days(self):
        return JobConfig.DEFAULT_PRESET_DAY_SHIFT_END if self.is_preset() else JobConfig.DEFAULT_PRESET_DAY_SHIFT_START

    def get_file_date_str(self):
        return self._file_date_str

    def get_day_range_len(self):
        return self.get_day_shift_end() - self.get_day_shift_start() + 1

    def get_local_file_folder(self):
        return join_path(cur_path, self.get_result_folder())

    def get_child_config(self):
        return self._config

    def get_report_insert_franchise_v1_inter_result_url(self):
        return self.get_child_config().get('env', 'report_insert_franchise_v1_inter_result_url')

    def get_report_insert_price_url(self):
        return self.get_child_config().get('env', 'report_insert_price_url')

    def get_report_insert_om_v1_inter_result_url(self):
        return self.get_child_config().get('env', 'report_insert_om_v1_inter_result_url')

    def get_report_insert_ota_price_url(self):
        return self.get_child_config().get('env', 'report_insert_ota_price_url')

    def get_hotel_price_data_receivers(self):
        if EnvUtil.is_prod_env(self.get_env()):
            return self.get_mail_receiver().get_receivers_by_email_id(EmailReceiver.EMAIL_ID_3004.value)
        else:
            return ''.join(self.get_child_config().get('email', 'hotel_price_data_receivers').split()).split(';')

    def get_ota_plugin_result_receivers(self):
        return ''.join(self.get_child_config().get('email', 'ota_plugin_result_receivers').split()).split(';')

    def get_final_result_receivers(self):
        if EnvUtil.is_prod_env(self.get_env()):
            return self.get_mail_receiver().get_receivers_by_email_id(EmailReceiver.EMAIL_ID_3003.value)
        final_result_receivers = ''.join(
            self.get_child_config().get('email', 'final_result_receivers').split()).split(';')
        ota_plugin_final_result_receivers = ''.join(
            self.get_child_config().get('email', 'ota_plugin_final_result_receivers').split()).split(';')
        return final_result_receivers if not self.is_ota_plugin() else ota_plugin_final_result_receivers

    def get_ota_prices_receivers(self):
        if EnvUtil.is_prod_env(self.get_env()):
            return self.get_mail_receiver().get_receivers_by_email_id(EmailReceiver.EMAIL_ID_3002.value)
        ota_prices_receivers = ''.join(self.get_child_config().get('email', 'ota_prices_receivers').split()).split(';')
        ota_plugin_ota_prices_receivers = ''.join(
            self.get_child_config().get('email', 'ota_plugin_ota_prices_receivers').split()).split(';')
        return ota_prices_receivers if not self.is_ota_plugin() else ota_plugin_ota_prices_receivers

    def get_hotel_price_insert_result_mail_receivers(self):
        if EnvUtil.is_prod_env(self.get_env()):
            return self.get_mail_receiver().get_receivers_by_email_id(EmailReceiver.EMAIL_ID_3005.value)
        price_insert_result_mail_receivers = ''.join(
            self.get_child_config().get('email', 'price_insert_result_mail_receivers').split()).split(';')
        ota_plugin_price_insert_result_mail_receivers = ''.join(
            self.get_child_config().get('email', 'ota_plugin_price_insert_result_mail_receivers').split()).split(
            ';')
        return price_insert_result_mail_receivers if not self.is_ota_plugin() \
            else ota_plugin_price_insert_result_mail_receivers
