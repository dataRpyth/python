# -*- coding: UTF-8 -*-
import os
import sys
import warnings
from os.path import join as join_path

cur_path = os.path.dirname(os.path.realpath(__file__))
sys.path.append(join_path(cur_path, '..'))

from common.enum.pricing_enum import EmailReceiver
from common.configuration.config_base import ConfigBase, EnvUtil

# ==========================config read begin===================================================
warnings.filterwarnings("ignore")


class JobConfig(ConfigBase):
    DEFAULT_HOTEL_SPECIAL_SALE_SLICES_SIZE = 500
    DEFAULT_INTERMEDIUM_BATCH_SIZE_BATCH_SIZE = 500
    LIQUIDATION_FLAG_LIQUIDATION_29_V1_2 = "29_2"

    def __init__(self, config_env):
        ConfigBase.__init__(self, config_env)
        self._config = ConfigBase.create_config_for_file_path(
            join_path(cur_path, 'config', 'config_{0}.ini'.format(self.get_config_env())))

    def add_args_for_parser(self, parser):
        ConfigBase.add_args_for_parser(self, parser)
        parser.add_argument('-liquidationFlag', help='The env in 29_2:special_sale_29_2',
                            default=None, type=str)
        parser.add_argument('-includeOtaNonDirectHotels', help='The Hotels include OtaNonDirect or not', default=False,
                            action='store_true')
        parser.add_argument('-disableCrsPriceDiff', help='disable crs price diff if present'
                                                         'default FALSE', default=False, action='store_true')
        parser.add_argument('-disableOtaPriceDiff', help='disable ota price diff if present'
                                                         'default FALSE', default=False, action='store_true')
        parser.add_argument('-disableTag', help='if including the hotels tagged', default=False, action='store_true')

        parser.add_argument('-disableEbase', help='disable ebase if appeared', default=False, action='store_true')

    def get_liquidation_flag(self):
        return self.args.liquidationFlag

    def is_inc_ota_non_direct_hotels(self):
        return self.args.includeOtaNonDirectHotels

    def is_tagged(self):
        return not self.args.disableTag

    def disable_crs_price_diff(self):
        return self.args.disableCrsPriceDiff

    def disable_ota_price_diff(self):
        return self.args.disableOtaPriceDiff

    def get_child_config(self):
        return self._config

    def disable_ebase(self):
        return self.args.disableEbase

    @staticmethod
    def get_hotel_special_sale_slices_size():
        return JobConfig.DEFAULT_HOTEL_SPECIAL_SALE_SLICES_SIZE

    @staticmethod
    def get_intermedium_batch_size():
        return JobConfig.DEFAULT_INTERMEDIUM_BATCH_SIZE_BATCH_SIZE

    def get_special_sale_to_pc_url(self):
        return self.get_child_config().get('env', 'special_sale_to_pc_url')

    def get_toggle_on_price_to_intermedium(self):
        return self.get_child_config().get('misc', 'toggle_on_price_to_intermedium', fallback="False") == "True"

    def get_price_to_intermedium_url(self):
        return self.get_child_config().get('env', 'price_to_intermedium_url')

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
        if EnvUtil.is_prod_env(self.get_env()) and self.is_inc_ota_non_direct_hotels():
            return self.get_mail_receiver().get_receivers_by_email_id(EmailReceiver.EMAIL_ID_3002.value)
        else:
            return ''.join(self.get_child_config().get('email', 'ota_prices_receivers').split()).split(';')

    def get_final_result_receivers(self):
        if EnvUtil.is_prod_env(self.get_env()):
            return self.get_mail_receiver().get_receivers_by_email_id(EmailReceiver.EMAIL_ID_3003.value)
        else:
            return ''.join(self.get_child_config().get('email', 'final_result_receivers').split()).split(';')

    def get_hotel_price_data_receivers(self):
        if EnvUtil.is_prod_env(self.get_env()):
            return self.get_mail_receiver().get_receivers_by_email_id(EmailReceiver.EMAIL_ID_3004.value)
        else:
            return ''.join(self.get_child_config().get('email', 'hotel_price_data_receivers').split()).split(';')

    def get_hotel_price_insert_result_mail_receivers(self):
        if EnvUtil.is_prod_env(self.get_env()):
            return self.get_mail_receiver().get_receivers_by_email_id(EmailReceiver.EMAIL_ID_3005.value)
        else:
            return ''.join(self.get_child_config().get('email', 'price_insert_result_mail_receivers').split()).split(
                ';')

    def get_walkin_raise_hotel_list_table(self):
        return self.get_child_config().get('misc', 'walkin_raise_hotel_list_table')

    def get_ota_plugin_result_receivers(self):
        return self.get_ota_prices_receivers()

    def get_liquidation_v1_hotel_list_mail_receivers(self):
        if EnvUtil.is_prod_env(self.get_env()):
            return self.get_mail_receiver().get_receivers_by_email_id(EmailReceiver.EMAIL_ID_3011.value)
        else:
            return ''.join(
                self.get_child_config().get('email', 'liquidation_v1_hotel_list_mail_receivers').split()).split(';')

    def get_liquidation_to_pc_report_receivers(self):
        return ''.join(self.get_child_config().get('email', 'liquidation_to_pc_report_receivers').split()).split(';')

    def get_preset_days(self):
        return 2

    def get_local_file_folder(self):
        return join_path(cur_path, ConfigBase.get_result_folder())
