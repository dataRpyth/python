#!/usr/bin/env python
# -*- coding:utf-8 -*-

import os
import sys
import warnings
from os.path import join as join_path


cur_path = os.path.dirname(os.path.realpath(__file__))
sys.path.append(join_path(cur_path, '..'))
warnings.filterwarnings("ignore")

from common.util.utils import *
from common.job_base.job_base import JobBase
from common.job_common.job_source import JobSource
from common.priceop.occ_target_to_pc import OccTarget


class OccTargetJob(JobBase):

    def __init__(self, job_config):
        JobBase.__init__(self, job_config)
        self.job_source = JobSource(self.get_adb_query_manager(), self.get_mysql_query_manager(), self.get_oracle_query_manager())
        self.config = self.get_job_config()

    def get_job_name(self):
        return 'OccTarget'

    def run_job(self):
        # 计价时间
        start_stamp = time.time()

        # 获取数仓occ_target
        df_occ_target = self.job_source.get_df_for_occ_target(start_stamp)
        df_occ_target["occ_target"] = df_occ_target[["occ_mean", "occ_std"]].apply(lambda values: self.calc_occ_target(*values), axis=1)
        # occ_target_to_pc
        self.occ_target_to_pc(self.config, df_occ_target.copy())

    def calc_occ_target(self, occ_mean, occ_std):
        '''
            对occ_std做[0, 0.1]区间限制：occ_std_clipped
            occ目标爬坡，在occ_mean上加上2*occ_std_clipped：occ_rampup
            对occ_rampup做[0.15, 2*occ_std_clipped]区间限制：occ_target

            :param occ_mean:  区分周中周末，计算过去7天的occ均值
            :param occ_std: 区分周中周末，计算过去7天occ的方差
            :return: occ_target
        '''
        occ_std = min(0.1, occ_std)
        occ = occ_mean + max(0.05, occ_std)
        occ_target = min(1 - occ_std, max(0.15, occ))
        return round(occ_target, 4)

    def occ_target_to_pc(self, config, df_occ_target):
        LogUtil.get_cur_logger().info("set occ_target to pc, size: {}, ...".format(len(df_occ_target)))
        # set_occ_target
        OccTarget(config).set_occ_target(df_occ_target)
