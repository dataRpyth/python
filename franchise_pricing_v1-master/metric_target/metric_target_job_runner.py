#!/usr/bin/env python
# -*- coding:utf-8 -*-

import os
import sys
import warnings
from concurrent.futures import as_completed
from os.path import join as join_path

import numpy as np
from pandas import DataFrame

from common.util.my_thread_executor import MyThreadExecutor

cur_path = os.path.dirname(os.path.realpath(__file__))
sys.path.append(join_path(cur_path, '..'))
warnings.filterwarnings("ignore")

from common.util.utils import *
from common.job_base.job_base import JobBase
from common.job_common.job_source import JobSource
from common.priceop.metrics_target_to_pc import MetricsTarget


class MetricTargetJob(JobBase):

    def __init__(self, job_config):
        JobBase.__init__(self, job_config)
        self.job_source = JobSource(self.get_adb_query_manager(), self.get_mysql_query_manager(),
                                    self.get_oracle_query_manager(),
                                    self.get_hive_query_manager())
        self.config = self.get_job_config()

    def get_job_name(self):
        return 'MetricsTarget'

    def run_job(self):
        # 计价时间
        start_stamp = time.time()

        # 获取pc中metrics_config
        df_metrics_config = self.job_source.get_df_for_metrics_config()

        # 查询指标源 metrics
        df_metrics_all = self.get_df_metrics_all(df_metrics_config, start_stamp)
        if len(df_metrics_all) == 0:
            LogUtil.get_cur_logger().info("get_df_metrics_from_db is empty")
            return

        # 分组
        df_metrics_target_grouped_lst = self.group_metrics_target_lst(df_metrics_all)

        # post grouped metrics_target to pc
        self.post_grouped_metrics_target(df_metrics_target_grouped_lst)

    def get_df_metrics_all(self, df_metrics_config, start_stamp):
        df_metrics_all = DataFrame()
        if len(df_metrics_config) == 0:
            return df_metrics_all
        for idx, row in df_metrics_config.iterrows():
            #  metrics_id, source_type, source_url, source_table, source_metrics_column, priority
            metrics_source_id = row['metrics_source_id']
            df_metrics = self.job_source.get_df_metrics_from_db(start_stamp, row)
            df_metrics['metrics_details'] = row['source_aux_columns']
            df_metrics['metrics_id'] = row['metrics_id']
            df_metrics['priority'] = row['priority']
            df_metrics['metrics_source_id'] = metrics_source_id
            #  data_date、hour、metrics_details
            df_metrics_all = df_metrics_all.append(df_metrics, ignore_index=True)
        if len(df_metrics_all) == 0:
            return df_metrics_all
        # join后去空
        df_metrics_all.filter = np.where(np.isnan(df_metrics_all.metrics_value) | np.isinf(df_metrics_all.metrics_value), False, True)
        df_metrics_all = df_metrics_all[df_metrics_all.filter == True]
        df_metrics_all.sort_values(['oyo_id', 'priority', 'metrics_id'], inplace=True, ascending=[True, True, True])
        LogUtil.get_cur_logger().info("get_df_metrics_all, size: {}".format(len(df_metrics_all)))
        return df_metrics_all

    def post_grouped_metrics_target(self, df_metrics_target_grouped_lst):
        t_executor = MyThreadExecutor(max_workers=10, t_name_prefix="get_marking_price")
        for idx, df_metrics_target in enumerate(df_metrics_target_grouped_lst):
            t_executor.sub_task(self.metrics_target_to_pc, self.config, df_metrics_target)
        df_metrics_target_res = DataFrame()
        succeeded_pojo_chunk_lst = list()
        failed_pojo_chunk_lst = list()
        for future in as_completed(t_executor.get_all_task(), timeout=1800):
            df_metrics_target_res = df_metrics_target_res.append(future.result()[0], ignore_index=True)
            succeeded_pojo_chunk_lst.extend(future.result()[1])
            failed_pojo_chunk_lst.extend(future.result()[2])
        metric_target_post = MetricsTarget(self.config)
        metric_target_post.set_df_metrics_target(df_metrics_target_res)
        metric_target_post.set_sued_pojo_chunk_list(succeeded_pojo_chunk_lst)
        metric_target_post.set_failed_pojo_chunk_list(failed_pojo_chunk_lst)
        metric_target_post.mail_send()
        metric_target_post.send_mail_for_monitor()

    def metrics_target_to_pc(self, config, df_metrics_target):
        LogUtil.get_cur_logger().info("set metrics_target to pc, size: {}, ...".format(len(df_metrics_target)))
        # set_metrics_target
        metric_target_post = MetricsTarget(config)
        metric_target_post.set_and_post_metrics_target(df_metrics_target)
        return metric_target_post.get_df_metrics_target(), \
               metric_target_post.get_sued_pojo_chunk_list(), metric_target_post.get_failed_pojo_chunk_list()

    def group_metrics_target_lst(self, df_metrics_targets):
        # 分组data_date、hour、metrics_id
        df_metrics_targets.sort_values(['data_date', 'hour', 'metrics_id'], inplace=True, ascending=[True, True, True])
        df_metrics_targets.reset_index(drop=True, inplace=True)
        u_key = ""
        df_metrics_target_lst = list()
        df_metrics_target = DataFrame()
        for idx, row in df_metrics_targets.iterrows():
            u_key_0 = str(row.get("data_date")) + "/" + str(row.get("hour")) + "/" + str(row.get("metrics_id"))
            if u_key != "" and u_key_0 != u_key:
                df_metrics_target_lst.append(df_metrics_target.copy())
                df_metrics_target = DataFrame()
            u_key = u_key_0
            df_metrics_target = df_metrics_target.append(row, ignore_index=True)
            if idx == len(df_metrics_targets) - 1:
                df_metrics_target_lst.append(df_metrics_target.copy())
        return df_metrics_target_lst
