#!/usr/bin/env python
# -*- coding:utf-8 -*-

import os
import sys
import warnings
from concurrent.futures import as_completed
from os.path import join as join_path

from pandas import DataFrame

from common.util.my_thread_executor import MyThreadExecutor

cur_path = os.path.dirname(os.path.realpath(__file__))
sys.path.append(join_path(cur_path, '..'))
warnings.filterwarnings("ignore")

from common.util.utils import *
from common.job_base.job_base import JobBase
from common.job_common.job_source import JobSource
from common.job_common.job_sinker import JobSinker


class MarkingPriceJob(JobBase):

    def __init__(self, job_config):
        JobBase.__init__(self, job_config)
        self.job_source = JobSource(self.get_adb_query_manager(), self.get_mysql_query_manager(), self.get_oracle_query_manager())
        self.job_sinker = JobSinker(self.job_source)
        self.config = self.get_job_config()

    def get_job_name(self):
        return 'MarkingPrice'

    def run(self):
        logger = LogUtil.get_cur_logger()

        begin_time = time.time()
        logger.info('*********************run begin******************************')
        # 计价时间
        start_stamp = begin_time
        date_start = DateUtil.stamp_to_date_format(start_stamp, "%Y-%m-%d", -7)
        date_end = DateUtil.stamp_to_date_format(start_stamp, "%Y-%m-%d", -1)

        # 获取China2.0酒店
        set_oyo_id = self.job_source.get_hotel_batch_oyo_set_from_center(self.config.get_batch_order(), start_stamp)

        # 获取过去7天成交价
        last7d_price_df = self.job_source.get_last7d_trade_price_df(set_oyo_id, date_start, date_end)

        # 获取划线价
        marking_price_df = self.get_marking_price_by_multi_thread(set_oyo_id, last7d_price_df)

        # mark_up_marking_price_df
        marking_price_df = self.mark_up_marking_price_df(marking_price_df)

        # marking_prices_to_channel
        self.job_sinker.marking_prices_to_channel(self.config, marking_price_df.copy())
        logger.info('*******************run end, time cost: %.2fs********************', time.time() - begin_time)

    def get_marking_price_by_multi_thread(self, set_oyo_id, last7d_price_df):
        lst_oyo_id = list(set_oyo_id)
        lst_oyo_id.sort()
        len_oyo_id = len(lst_oyo_id)
        slices = 500
        slices_len = int(len_oyo_id / slices) if (len_oyo_id % slices) == 0 else int(len_oyo_id / slices) + 1
        set_oyo_id_lst = [set(lst_oyo_id[idx * slices:min((idx + 1) * slices, len_oyo_id)]) for idx in
                          range(slices_len)]
        marking_price_df = DataFrame()
        t_executor = MyThreadExecutor(max_workers=10, t_name_prefix="get_marking_price")
        for idx, oyo_ids in enumerate(set_oyo_id_lst):
            price_df = last7d_price_df[last7d_price_df.oyo_id.isin(oyo_ids)]
            t_executor.sub_task(self.get_marking_price, price_df)
        for future in as_completed(t_executor.get_all_task(), timeout=1800):
            marking_price_df = marking_price_df.append(future.result())
        return marking_price_df

    def get_marking_price(self, price_df):
        log = LogUtil.get_cur_logger()
        # compose_marking_price 0.95分位数
        log.info("start compose marking-price, size: {}, ...".format(len(price_df)))
        marking_price_df = self.compose_marking_price(price_df)
        log.info("end compose marking-price")
        return marking_price_df

    def compose_marking_price(self, last7d_price_df):
        last7d_price_df.sort_values(axis=0, by=['oyo_id', 'room_type_id'], ascending=True)
        hotel_price_map = {}
        for index, row in last7d_price_df.iterrows():
            oyo_id = row.get("oyo_id")
            room_type_id = row.get("room_type_id")
            price = row.get("price")
            room_type_map = MiscUtil.get_or_create_from_map(hotel_price_map, oyo_id, lambda: {})
            room_type_price_lst = MiscUtil.get_or_create_from_map(room_type_map, room_type_id, lambda: [])
            room_type_price_lst.append(price)
        hotel_price_lst = []
        for (oyo_id, room_type_prices) in hotel_price_map.items():
            for (room_type_id, prices) in room_type_prices.items():
                idx = ceil(len(prices) * 0.95) - 1
                price_tgt = prices[idx]
                hotel_price_lst.append([oyo_id, room_type_id, price_tgt])
        marking_price_df = pd.DataFrame(hotel_price_lst, columns=["oyo_id", "room_type_id", "price"])
        return marking_price_df

    def mark_up_marking_price_df(self, marking_price_df):
        LogUtil.get_cur_logger().info("start mark up marking-price, size: {}, ...".format(len(marking_price_df)))
        # override price with recent_crs_price
        start_date = DateUtil.stamp_to_date_format(time.time(), "%Y-%m-%d")
        end_date = DateUtil.stamp_to_date_format(time.time(), "%Y-%m-%d")
        marking_price_df = self.job_source.override_empty_price_by_recent_crs_price(marking_price_df, start_date,
                                                                                    end_date)

        # contact “hotel_id”
        pricing_hotel_df = self.job_source.get_product_hotel_df(set(marking_price_df.oyo_id))
        all_trade_price_df = pd.merge(marking_price_df, pricing_hotel_df, on=["oyo_id"], how="left")
        all_trade_price_df = all_trade_price_df[~all_trade_price_df.hotel_id.apply(lambda x: pd.isna(x))]
        LogUtil.get_cur_logger().info("end mark up marking-price")
        return all_trade_price_df
