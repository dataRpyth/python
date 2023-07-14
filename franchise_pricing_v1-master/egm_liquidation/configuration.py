# -*- coding: UTF-8 -*-
import os
import sys
import warnings
from os.path import join as join_path

cur_path = os.path.dirname(os.path.realpath(__file__))
sys.path.append(join_path(cur_path, '..'))

from egm_liquidation.mail_send import MailSend
from Liquidation.configuration import LiqJobConfig
from common.util.utils import LogUtil

warnings.filterwarnings("ignore")


EGM_LIQUIDATION_BATCH_29_1 = "egm_29"
EGM_LIQUIDATION_BATCH_29_2 = "egm_29_2"
EGM_LIQUIDATION_BATCH_39 = "egm_39"
EGM_LIQUIDATION_BATCH_49 = "egm_49"

STRATEGY_TYPE_EGM_LIQUIDATION_29_V1 = "EgmLiquidation29V1"
STRATEGY_TYPE_EGM_LIQUIDATION_29_V1_2 = "EgmLiquidation29V1_2"
STRATEGY_TYPE_EGM_LIQUIDATION_39_V1 = "EgmLiquidation39V1"
STRATEGY_TYPE_EGM_LIQUIDATION_49_V1 = "EgmLiquidation49V1"

BATCH_LIQ_DICT = {
            EGM_LIQUIDATION_BATCH_29_1: STRATEGY_TYPE_EGM_LIQUIDATION_29_V1,
            EGM_LIQUIDATION_BATCH_29_2: STRATEGY_TYPE_EGM_LIQUIDATION_29_V1_2,
            EGM_LIQUIDATION_BATCH_39: STRATEGY_TYPE_EGM_LIQUIDATION_39_V1,
            EGM_LIQUIDATION_BATCH_49: STRATEGY_TYPE_EGM_LIQUIDATION_49_V1
        }


class EgmLiqJobConfig(LiqJobConfig):

    def __init__(self, config_env):
        super(EgmLiqJobConfig, self).__init__(config_env)

    def init_before(self):
        LogUtil.init_cur_logger(cur_path, 'log', 'log.log')

    def init_after(self):
        super(EgmLiqJobConfig, self).init_after()
        self._config = self.create_config_for_file_path(
            join_path(cur_path, 'config', 'config_{0}.ini'.format(self.get_config_env())))
        self.set_mail_send(MailSend())

    def add_args_for_parser(self, parser):
        LiqJobConfig.add_args_for_parser(self, parser)
        parser.add_argument('-doExclude', help='The value could be 1', default=0, type=int)
        parser.add_argument('-doSpecialBatch', help='The value could be 1', default=0, type=int)

    def get_do_exclude(self):
        return self.args.doExclude

    def get_do_special_batch(self):
        return self.args.doSpecialBatch

    def get_child_config(self):
        return self._config

    def get_hotel_daily_prepare_table(self):
        return self.get_child_config().get('misc', 'hotel_daily_prepare_table')

    def get_liquidation_rule_id(self):
        return self.get_child_config().get('misc', 'liquidation_rule_id')

    def get_liquidation_v1_hotel_list_mail_receivers(self):
        return ''.join(self.get_child_config().get('email', 'liquidation_v1_hotel_list_mail_receivers').split()).split(
            ';')

    def get_liquidation_to_pc_report_receivers(self):
        return ''.join(self.get_child_config().get('email', 'liquidation_to_pc_report_receivers').split()).split(';')

    def get_exclude_oyo_ids(self):
        return ''.join(self.get_child_config().get('env', 'exclude_oyo_ids').split()).split(';')

    def get_special_batch_oyo_ids(self):
        return ''.join(self.get_child_config().get('env', 'special_batch_oyo_ids').split()).split(';')

    def get_local_file_folder(self):
        return join_path(cur_path, self.get_result_folder())

    @staticmethod
    def get_liq_batch_strategy_dict():
        return BATCH_LIQ_DICT
