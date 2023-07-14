# -*- coding: UTF-8 -*-
import os
import sys
from os.path import join as join_path

cur_path = os.path.dirname(os.path.realpath(__file__))
sys.path.append(join_path(cur_path, '..'))

from common.configuration.config_base import ConfigBase


class BaseDataJobConfig(ConfigBase):

    def get_child_config(self):
        return None
