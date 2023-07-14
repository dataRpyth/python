# -*- coding: UTF-8 -*-
import os
import sys
import warnings
from os.path import join as join_path

warnings.filterwarnings("ignore")

cur_path = os.path.dirname(os.path.realpath(__file__))
sys.path.append(join_path(cur_path, '..'))

from common.configuration.config_base import ConfigBase

PROCESS_TYPE_LIQUIDATION_PREPARE='liquidationPrepare'

PROCESS_TYPE_LIQUIDATION_WALKIN_RAISE='walkinRaisePrepare'


class JobConfig(ConfigBase):
    def __init__(self, config_env):
        ConfigBase.__init__(self, config_env)
        self._config = ConfigBase.create_config_for_file_path(
            join_path(cur_path, 'config', 'config_{0}.ini'.format(self.get_config_env())))

    def add_args_for_parser(self, parser):
        ConfigBase.add_args_for_parser(self, parser)
        parser.add_argument('-pt', help='The process type, can be {} or {}'.format(PROCESS_TYPE_LIQUIDATION_PREPARE,
                                                                                   PROCESS_TYPE_LIQUIDATION_WALKIN_RAISE),
                            type=str,default=None)
        parser.add_argument('-disableEbase', help='disable ebase if appeared', default=False, action='store_true')

    def get_hotel_metrics_sink_table(self):
        return self.get_child_config().get('misc', 'hotel_metrics_sink_table')

    def get_child_config(self):
        return self._config

    def get_process_type(self):
        return self.args.pt

    def disable_ebase(self):
        return self.args.disableEbase

    def get_walkin_raise_hotel_list_sink_table(self):
        return self.get_child_config().get('misc', 'walkin_raise_hotel_list_table')