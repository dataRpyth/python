#!/usr/bin/env python
# -*- coding:utf-8 -*-

import os
import sys
import warnings
import numpy as np

from os.path import join as join_path

cur_path = os.path.dirname(os.path.realpath(__file__))
sys.path.append(join_path(cur_path, '..'))
warnings.filterwarnings("ignore")

from common.util.utils import *
from common.job_base.job_base import JobBase
from common.job_common.job_source import JobSource
from common.job_common.job_sinker import JobSinker
from common.enum.pricing_enum import PriceChannel
from common.priceop.price_to_channel_rate import ChannelRate


def price_by_hour(price, hour):
    if hour == '3':
        price *= 0.6
        price = max(price, 40)
    if hour == '4':
        price *= 0.6
        price = max(price, 40)
    return ceil(price)


class HourlyRoomJob(JobBase):

    def __init__(self, job_config):
        JobBase.__init__(self, job_config)
        self.job_source = JobSource(self.get_adb_query_manager(), self.get_mysql_query_manager(),
                                    self.get_oracle_query_manager())
        self.job_sinker = JobSinker(self.job_source)

    def get_job_name(self):
        return 'HourlyRoomV1'

    def run(self):
        logger = LogUtil.get_cur_logger()

        config = self.get_job_config()
        mod_len = config.get_mod_len()
        mod_idx = config.get_mod_idx()
        batch_order = config.get_batch_order()
        job_begin = time.time()
        logger.info('********************* run begin ******************************')
        # 计价时间
        start_stamp = job_begin
        start_date = DateUtil.stamp_to_date_format0(start_stamp)
        last7d_date = DateUtil.stamp_to_date_format(start_stamp, format_str="%Y-%m-%d", offset=-7)

        # 获取China2.0酒店
        set_oyo_id_china2 = self.job_source.get_hotel_batch_oyo_set_from_center(batch_order, start_stamp, 5)
        set_oyo_id_egm = self.job_source.get_hotel_batch_oyo_set_from_center(batch_order, start_stamp, 6)
        set_oyo_id = set_oyo_id_china2 | set_oyo_id_egm
        set_oyo_id = self.get_oyo_id_mode_hashed(set_oyo_id, mod_len, mod_idx)
        # 前7天的成交价
        last7d_trade_price_df = self.job_source.get_last7d_trade_price_df(set_oyo_id, last7d_date, start_date)

        # 组织小时房价
        hourly_room_price_df = self.compose_hourly_room_price(last7d_trade_price_df)

        # 得到房型价差
        all_room_type_price_df = self.get_all_room_type_price_df(hourly_room_price_df, set_oyo_id, start_date)

        # 关联小时房
        all_room_type_price_df = self.get_hour_room_price_df(all_room_type_price_df, set_oyo_id)

        # 小时房调价
        hourly_room_price_df = DFUtil.apply_func_for_df(all_room_type_price_df, "pms_price",
                                                        ["pms_price", "hour_room_duration"],
                                                        lambda x: price_by_hour(*x))
        # 分渠道发送
        self.sink_to_channel_rate(config, hourly_room_price_df)

        logger.info('******************* run end, %.2fs elapsed *********************', time.time() - job_begin)

    def get_oyo_id_mode_hashed(self, set_oyo_id, mod_len, mod_idx):
        oyo_id_arr = [set() for i in range(mod_len)]
        for oyo_id in set_oyo_id:
            mod = HashUtil.get_hash_code(oyo_id) % mod_len
            oyo_id_arr[mod].add(oyo_id)
        set_oyo_id = oyo_id_arr[mod_idx - 1]
        LogUtil.get_cur_logger().info("get set_oyo_id after mod-hashed, mod_len: %s, mod_idx: %d, hotel-size: %d",
                                      mod_len, mod_idx, len(set_oyo_id))
        return set_oyo_id

    def get_hour_room_price_df(self, all_room_type_price_df, set_oyo_id):
        hotel_hour_room_df = self.job_source.get_hotel_hour_room(set_oyo_id)
        all_room_type_price_df = pd.merge(all_room_type_price_df, hotel_hour_room_df, how='left',
                                          on=['oyo_id', 'room_type_id'])
        all_room_type_price_df = all_room_type_price_df[
            all_room_type_price_df.hour_room_duration.apply(lambda x: MiscUtil.is_not_empty_value(x))][
            all_room_type_price_df.pms_price.apply(lambda x: MiscUtil.is_not_empty_value(x))]
        LogUtil.get_cur_logger().info("get hour_room, hotel-size: %d",  len(set(all_room_type_price_df.oyo_id)))
        return all_room_type_price_df

    def compose_hourly_room_price(self, last7d_price_df):
        log = LogUtil.get_cur_logger()
        # compose_hourly_room_price 平均值
        log.info("start compose hourly_room_price, size: {}, ...".format(len(last7d_price_df)))
        last7d_price_df.sort_values(axis=0, by=['oyo_id', 'room_type_id'], inplace=True)
        last7d_price_df['price'].replace([np.nan], 0, inplace=True)
        last7d_mean_price_df = last7d_price_df.groupby(['oyo_id'])['price'].mean().reset_index()
        log.info("end compose hourly_room_price")
        return last7d_mean_price_df

    def sink_to_channel_rate(self, config, prices_to_channel_rate_df):
        ChannelRate.hourly_room_price_to_channel_rate(config, prices_to_channel_rate_df,
                                                      PriceChannel.CHANNEL_IDS_WALKIN_AND_APP_DIRECT_OTA)

    def get_all_room_type_price_df(self, df_for_hotel_base, set_oyo_id, start_date):
        df_for_hotel_base = df_for_hotel_base[["oyo_id", "price"]]
        df_for_hotel_base["base"] = df_for_hotel_base["price"]
        start_stamp = DateUtil.string_to_timestamp(start_date)
        date_lst = [[oyo_id, DateUtil.stamp_to_date_format(start_stamp, format_str="%Y-%m-%d", offset=i)] for oyo_id in
                    set_oyo_id for i in range(14)]
        date_df = pd.DataFrame(date_lst, columns=["oyo_id", 'date'])
        df_for_hotel_base = pd.merge(df_for_hotel_base, date_df, how="left", on="oyo_id")
        smart_room_type_price_df = self.job_source.get_hourly_room_type_price_df(df_for_hotel_base, set_oyo_id)
        smart_room_type_price_df = self.get_available_room_df(smart_room_type_price_df, set_oyo_id)
        all_room_type_price_df = smart_room_type_price_df[
            ['date', 'hotel_name', 'hotel_id', 'unique_code', 'oyo_id', 'room_type_id', 'room_type_name', 'pms_price']]
        return all_room_type_price_df

    def get_available_room_df(self, prices_for_room_type_df, set_oyo_id):
        room_type_prices_df = self.job_source.get_available_room_type_df(prices_for_room_type_df, set_oyo_id)
        set_not_in_hotel_room_type = set(prices_for_room_type_df.oyo_id) - set(room_type_prices_df.oyo_id)
        if len(set_not_in_hotel_room_type) > 0:
            msg = "The following hotels have no hotel_room_type in pc: {}".format(set_not_in_hotel_room_type)
            LogUtil.get_cur_logger().info(msg)
            DdtUtil.robot_send_ddt_msg(msg, self.get_robot_token_op_alert())
        return room_type_prices_df
