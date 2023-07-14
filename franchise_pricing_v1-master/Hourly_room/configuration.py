# -*- coding: UTF-8 -*-
import os
import sys
import warnings
from os.path import join as join_path

cur_path = os.path.dirname(os.path.realpath(__file__))
sys.path.append(join_path(cur_path, '..'))

from Hourly_room.mail_send import MailSend
from common.util.utils import LogUtil
from common.dingtalk_sdk import dingtalk_py_cmd
from common.configuration.config_base import ConfigBase

warnings.filterwarnings("ignore")


class JobConfig(ConfigBase):
    DEFAULT_CHANNEL_HOTEL_INFOS_BATCH_SIZE = 3000

    def __init__(self, config_env):
        LogUtil.init_cur_logger(cur_path, 'log', 'log.txt')
        ConfigBase.__init__(self, config_env)
        self._config = ConfigBase.create_config_for_file_path(
            join_path(cur_path, 'config', 'config_{0}.ini'.format(self.get_config_env())))
        dingtalk_py_cmd.init_jvm_for_ddt_disk()
        self.set_mail_send(MailSend())

    def add_args_for_parser(self, parser):
        ConfigBase.add_args_for_parser(self, parser)
        parser.add_argument('-modLen', help='The model length of hash(set_oyo_id)',  default=1, type=int)
        parser.add_argument('-modIdx', help='The index of the model for hash(set_oyo_id), begin with 1',  default=1, type=int)

    def get_mod_len(self):
        return self.args.modLen

    def get_mod_idx(self):
        return self.args.modIdx

    def get_child_config(self):
        return self._config

    def get_local_file_folder(self):
        return join_path(cur_path, self.get_result_folder())

    def get_channel_hotel_infos_batch_size(self):
        return self.DEFAULT_CHANNEL_HOTEL_INFOS_BATCH_SIZE
