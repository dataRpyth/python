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
from common.job_common.job_sinker import JobSinker
from common.enum.pricing_enum import PriceChannel
from common.priceop.share_inventory_to_trade import ShareInventorApi


class VirtualClubbingJob(JobBase):

    def __init__(self, job_config):
        JobBase.__init__(self, job_config)
        self.job_source = JobSource(self.get_adb_query_manager(), self.get_mysql_query_manager(),
                                    self.get_oracle_query_manager(), self.get_hive_query_manager())
        self.job_sinker = JobSinker(self.job_source)
        self.config = self.get_job_config()

    def get_job_name(self):
        return 'VirtualClubbingV1'

    def run(self):
        logger = LogUtil.get_cur_logger()

        begin_time = time.time()
        logger.info('*********************{0} run begin ***********************'.format(self.get_job_name()))
        # 计价时间
        pricing_start_stamp = begin_time

        # share_inventory_price
        share_inventory_df = self.job_source.get_df_for_share_inventory_value(pricing_start_stamp, -7)

        # mark_up_marking_price_df
        share_inventory_df = self.mark_up_share_inventory_df(share_inventory_df)

        # available_room_share_inventory
        share_inventory_df = self.get_available_share_inventory(share_inventory_df)

        # share_inventory_to_trade
        ShareInventorApi(self.config).set_share_inventor(share_inventory_df, PriceChannel.CHANNEL_IDS_DIRECT_OTA_AND_APP)
        logger.info('*******************run end, time elapsed: %.2fs********************', time.time() - begin_time)

    def get_available_share_inventory(self, share_inventory_df):

        def available_flag(available_map, row):
            oyo_id = row["oyo_id"]
            room_type_id = row["room_type_id"]
            share_room_type_id = row["share_room_type_id"]
            if available_map.get(oyo_id) is None:
                return False
            if int(room_type_id) not in available_map.get(oyo_id):
                return False
            if int(share_room_type_id) not in available_map.get(oyo_id):
                return False
            return True

        room_type_df = self.job_source.get_room_type_df(set(share_inventory_df.oyo_id))
        room_type_map = {}
        for index, row in room_type_df.iterrows():
            if room_type_map.get(row["oyo_id"]) is None:
                room_type_map[row["oyo_id"]] = []
            room_type_map[row["oyo_id"]].append(row.get("room_type_id"))
        share_inventory_df["available"] = share_inventory_df.apply(lambda row: available_flag(room_type_map, row),
                                                                   axis=1)
        share_inventory_df = share_inventory_df[share_inventory_df.available == True]

        return share_inventory_df

    def mark_up_share_inventory_df(self, share_inventory_df):
        LogUtil.get_cur_logger().info("start mark up share-inventory, size: {}, ...".format(len(share_inventory_df)))
        start_date = DateUtil.stamp_to_date_format(time.time(), "%Y-%m-%d")
        end_date = DateUtil.stamp_to_date_format(time.time(), "%Y-%m-%d", 7)

        def vc_pattern(vc_p):
            return vc_p.split("-")

        share_inventory_df = share_inventory_df[["oyo_id", "hotel_id", "vc_pattern", "vc"]]
        share_inventory_df["share_type"] = 1
        share_inventory_df["room_type_id"] = share_inventory_df.vc_pattern.map(lambda value: vc_pattern(value)[0])
        share_inventory_df["share_room_type_id"] = share_inventory_df.vc_pattern.map(lambda value: vc_pattern(value)[1])
        share_inventory_df["share_count"] = share_inventory_df["vc"]
        share_inventory_df["is_valid"] = 1
        share_inventory_df["valid_from"] = start_date
        share_inventory_df["valid_to"] = end_date
        LogUtil.get_cur_logger().info("end mark up share-inventory")
        return share_inventory_df
