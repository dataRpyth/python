# -*- coding: UTF-8 -*-
import os
import sys
import warnings
from os.path import join as join_path

cur_path = os.path.dirname(os.path.realpath(__file__))
sys.path.append(join_path(cur_path, '..'))

from Liquidation.mail_send import MailSend
from common.enum.pricing_enum import EmailReceiver
from common.util.utils import EnvUtil, LogUtil
from common.configuration.config_base import ConfigBase

JOB_TYPE_LIQUIDATION_RATE_SET = 1
JOB_TYPE_LIQUIDATION_RELIEF = 2

LIQUIDATION_BATCH_29_1 = "29"
LIQUIDATION_BATCH_29_2 = "29_2"
LIQUIDATION_BATCH_39 = "39"
LIQUIDATION_BATCH_49 = "49"

STRATEGY_TYPE_LIQUIDATION_29_V1 = "Liquidation29V1"
STRATEGY_TYPE_LIQUIDATION_29_V1_2 = "Liquidation29V1_2"
STRATEGY_TYPE_LIQUIDATION_39_V1 = "Liquidation39V1"
STRATEGY_TYPE_LIQUIDATION_49_V1 = "Liquidation49V1"

BATCH_LIQ_DICT = {
            LIQUIDATION_BATCH_49: STRATEGY_TYPE_LIQUIDATION_49_V1,
            LIQUIDATION_BATCH_39: STRATEGY_TYPE_LIQUIDATION_39_V1,
            LIQUIDATION_BATCH_29_1: STRATEGY_TYPE_LIQUIDATION_29_V1,
            LIQUIDATION_BATCH_29_2: STRATEGY_TYPE_LIQUIDATION_29_V1_2
        }

warnings.filterwarnings("ignore")


class LiqJobConfig(ConfigBase):
    DEFAULT_HOTEL_SPECIAL_SALE_SLICES_SIZE = 200
    DEFAULT_LMS_TO_UMP_BATCH_SIZE = 500

    def __init__(self, config_env):
        self.init_before()
        super(LiqJobConfig, self).__init__(config_env)
        self._config = None
        self.init_after()

    def init_before(self):
        LogUtil.init_cur_logger(cur_path, 'log', 'log.txt')

    def init_after(self):
        self._config = self.create_config_for_file_path(
            join_path(cur_path, 'config', 'config_{0}.ini'.format(self.get_config_env())))
        self.set_mail_send(MailSend())

    def add_args_for_parser(self, parser):
        ConfigBase.add_args_for_parser(self, parser)
        parser.add_argument('-jobType', help='The type in {0}:liquidation'.format(
            JOB_TYPE_LIQUIDATION_RATE_SET), default=1, type=int)
        parser.add_argument('-liquidationBatch', help='The type in {0},{1},{2},{3}'.format(
            LIQUIDATION_BATCH_29_1, LIQUIDATION_BATCH_29_2, LIQUIDATION_BATCH_39, LIQUIDATION_BATCH_49), type=str)
        parser.add_argument('-predOcc', help='The predicted occ threshold for filtering hotels', type=float)
        parser.add_argument('-disableTag', help='if including the hotels tagged', default=False, action='store_true')

    def get_job_type(self):
        return self.args.jobType

    def get_job_type_liq_rate(self):
        return self.args.jobType == JOB_TYPE_LIQUIDATION_RATE_SET

    def get_liq_batch(self):
        if self.args.liquidationBatch is None:
            raise Exception('argument: liquidationBatch must be defined')
        return self.args.liquidationBatch

    def get_pred_occ(self):
        if self.args.predOcc is None:
            raise Exception('argument: predOcc must be defined')
        return self.args.predOcc

    def is_tagged(self):
        return not self.args.disableTag

    def get_child_config(self):
        return self._config

    def get_hotel_daily_prepare_table(self):
        return self.get_child_config().get('misc', 'hotel_daily_prepare_table')

    def get_liquidation_rule_id(self):
        return self.get_child_config().get('misc', 'liquidation_rule_id')

    def get_special_sale_to_pc_url(self):
        return self.get_child_config().get('env', 'special_sale_to_pc_url')

    def get_lms_to_ump_url(self):
        return self.get_child_config().get('env', 'lms_to_ump_url')

    def get_lms_to_ump_batch_size(self):
        return self.DEFAULT_LMS_TO_UMP_BATCH_SIZE

    def get_hotel_special_sale_slices_size(self):
        return self.DEFAULT_HOTEL_SPECIAL_SALE_SLICES_SIZE

    def get_liquidation_v1_hotel_list_mail_receivers(self):
        if EnvUtil.is_prod_env(self.get_env()):
            return self.get_mail_receiver().get_receivers_by_email_id(EmailReceiver.EMAIL_ID_3011.value)
        else:
            return ''.join(
                self.get_child_config().get('email', 'liquidation_v1_hotel_list_mail_receivers').split()).split(';')

    def get_liquidation_to_pc_report_receivers(self):
        return ''.join(self.get_child_config().get('email', 'liquidation_to_pc_report_receivers').split()).split(';')

    def get_local_file_folder(self):
        return join_path(cur_path, self.get_result_folder())

    @staticmethod
    def get_liq_batch_strategy_dict():
        return BATCH_LIQ_DICT
