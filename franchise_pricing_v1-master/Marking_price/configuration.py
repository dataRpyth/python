# -*- coding: UTF-8 -*-
import os
import sys
import warnings
from os.path import join as join_path

cur_path = os.path.dirname(os.path.realpath(__file__))
sys.path.append(join_path(cur_path, '..'))

from Marking_price.mail_send import MailSend
from common.enum.pricing_enum import EmailReceiver
from common.util.utils import EnvUtil, LogUtil
from common.configuration.config_base import ConfigBase

warnings.filterwarnings("ignore")


class JobConfig(ConfigBase):
    DEFAULT_MARKING_PRICE_BATCH_SIZE = 500

    def __init__(self, config_env):
        LogUtil.init_cur_logger(cur_path, 'log', 'log.txt')
        ConfigBase.__init__(self, config_env)
        self._config = ConfigBase.create_config_for_file_path(
            join_path(cur_path, 'config', 'config_{0}.ini'.format(self.get_config_env())))
        self.set_mail_send(MailSend())

    def add_args_for_parser(self, parser):
        ConfigBase.add_args_for_parser(self, parser)

    def get_child_config(self):
        return self._config

    def get_marking_price_post_url(self):
        return self.get_child_config().get('env', 'marking_price_post_url')

    @staticmethod
    def get_marking_price_change_batch_size():
        return JobConfig.DEFAULT_MARKING_PRICE_BATCH_SIZE

    def get_marking_price_v1_mail_receivers(self):
        if EnvUtil.is_prod_env(self.get_env()):
            return self.get_mail_receiver().get_receivers_by_email_id(EmailReceiver.EMAIL_ID_3011.value)
        else:
            return ''.join(
                self.get_child_config().get('email', 'marking_price_v1_mail_receivers').split()).split(';')

    def get_local_file_folder(self):
        return join_path(cur_path, self.get_result_folder())
