# -*- coding: UTF-8 -*-
import os
import sys
from os.path import join as join_path

cur_path = os.path.dirname(os.path.realpath(__file__))
sys.path.append(join_path(cur_path, '..'))

from common.enum.pricing_enum import EmailReceiver
from common.configuration.config_base import ConfigBase, EnvUtil


class JobConfig(ConfigBase):
    def __init__(self, config_env):
        ConfigBase.__init__(self, config_env)
        self._config = ConfigBase.create_config_for_file_path(
            join_path(cur_path, 'config', 'config_{0}.ini'.format(self.get_config_env())))

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

    def toggle_on_pricing_log(self):
        return self.get_child_config().get('misc', 'toggle_on_pricing_log', fallback="False") == "True"

    def get_ota_prices_receivers(self):
        if EnvUtil.is_prod_env(self.get_env()):
            return self.get_mail_receiver().get_receivers_by_email_id(EmailReceiver.EMAIL_ID_3002.value)
        else:
            return ''.join(self.get_child_config().get('email', 'ota_prices_receivers').split()).split(';')

    def get_final_result_receivers(self):
        return ''.join(self.get_child_config().get('email', 'final_result_receivers').split()).split(';')

    def get_hotel_price_data_receivers(self):
        return ''.join(self.get_child_config().get('email', 'hotel_price_data_receivers').split()).split(';')

    def get_hotel_price_insert_result_mail_receivers(self):
        if EnvUtil.is_prod_env(self.get_env()):
            return self.get_mail_receiver().get_receivers_by_email_id(EmailReceiver.EMAIL_ID_3005.value)
        else:
            return ''.join(self.get_child_config().get('email', 'price_insert_result_mail_receivers').split()).split(
                ';')

    def get_ota_plugin_result_receivers(self):
        return ''.join(self.get_child_config().get('email', 'ota_plugin_result_receivers').split()).split(';')

    def get_preset_days(self):
        return 2

    def get_local_file_folder(self):
        return join_path(cur_path, ConfigBase.get_result_folder())
