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
from pathos.multiprocessing import ProcessingPool
from common.job_base.job_base import JobBase
from common.job_common.job_source import JobSource
from common.job_common.job_sinker import JobSinker
from common.enum.pricing_enum import PriceChannel
from common.priceop.price_to_channel_rate import ChannelRate
from common.priceop.base_price_to_pc import HotelBasePrice
from common.util.my_thread_executor import MyThreadExecutor
from strategy.holiday_future import HolidayFutureStrategy
from Holiday_future.configuration import HOLIDAY_CODE_MID_AUTUMN, HOLIDAY_CODE_NATIONAL_DAY


class HolidayFutureJob(JobBase):

    def __init__(self, job_config):
        JobBase.__init__(self, job_config)

    def get_job_name(self):
        return 'HolidayFutureV1'

    def run(self):
        logger = LogUtil.get_cur_logger()
        pool = ProcessingPool()
        config = self.get_job_config()
        job_name = self.get_job_name()
        future_batch = config.get_future_batch()
        holiday_code = config.get_holiday_code()
        begin_time = time.time()
        job_source = JobSource(self.get_adb_query_manager(), self.get_mysql_query_manager(), self.get_oracle_query_manager())
        job_sinker = JobSinker(job_source)
        t_executor = MyThreadExecutor(6, job_name)
        logger.info('*********************run begin******************************')
        # 计价时间
        start_stamp = begin_time
        date_pre = DateUtil.stamp_to_date_format(start_stamp, "%Y-")
        date_start = date_pre + HolidayFutureStrategy.get_holiday_ratio(holiday_code, 0, "date")
        date_end = date_pre + HolidayFutureStrategy.get_holiday_ratio(holiday_code, -1, "date")

        # 获取China2.0酒店
        set_oyo_id = job_source.get_hotel_batch_oyo_set_from_center(config.get_batch_order(), start_stamp)

        # 获取base_price 与 df_for_hotel_occ
        df_base_price = self.get_base_price_df(config, job_source, set_oyo_id, date_end, date_start)

        df_hotel_occ = job_source.get_df_for_hotel_occ(set_oyo_id, start_stamp)

        # override base by occ
        df_for_hotel_base = self.calc_rate_diff(df_base_price, df_hotel_occ, future_batch)

        # base_override to pc
        t_executor.sub_task(self.override_base_price_to_pc, config, job_source, df_for_hotel_base.copy())

        prices_for_room_type_df = self.get_room_type_price_df(config, job_source, df_for_hotel_base, set_oyo_id)

        # prices_to_crs
        t_executor.sub_task(job_sinker.pms_prices_to_crs, config, prices_for_room_type_df.copy(), date_start, date_end, set())

        # prices_to_ota
        if holiday_code == HOLIDAY_CODE_MID_AUTUMN:
            t_executor.sub_task(job_sinker.pms_prices_to_ota_and_plugin, config, prices_for_room_type_df.copy(),
                                set_oyo_id, start_stamp, date_end, set(), pool)
        elif holiday_code == HOLIDAY_CODE_NATIONAL_DAY:
            # deprecated?
            t_executor.sub_task(job_sinker.pms_prices_to_ota_plugin, config, prices_for_room_type_df.copy(),
                                set_oyo_id, start_stamp, date_end, set(), pool)
            # price_to_channel_rate-ota
            t_executor.sub_task(ChannelRate.price_to_channel_rate, config, prices_for_room_type_df.copy(),
                                PriceChannel.CHANNEL_IDS_WALKIN_AND_APP)
        t_executor.completed()
        logger.info('*********************run begin******************************')

    def get_base_price_df(self, config, job_source, set_oyo_id, date_end, date_start):
        df_base_price = job_source.get_base_price(set_oyo_id, date_start, date_end)
        set_not_in_oyo_id = set_oyo_id - set(df_base_price.oyo_id)
        if len(set_not_in_oyo_id) > 0:
            msg = "The following hotels have no base price in pc: {}".format(set_not_in_oyo_id)
            LogUtil.get_cur_logger().info(msg)
            DdtUtil.robot_send_ddt_msg(msg, self.get_robot_token_op_alert())
        return df_base_price

    def get_room_type_price_df(self, config, job_source, df_for_hotel_base, set_oyo_id):
        df_for_hotel_base = df_for_hotel_base[["oyo_id", "date", "base", "price_start_date"]]
        df_for_hotel_base["price"] = df_for_hotel_base["base"].apply(lambda x: x)
        room_type_p_df = job_source.get_hotel_room_type_price_df(df_for_hotel_base, set_oyo_id)
        set_not_in_room_price_diff = set(df_for_hotel_base.oyo_id) - set(room_type_p_df.oyo_id)

        room_type_price_df = job_source.get_available_room_type_df(room_type_p_df, set_oyo_id)
        set_not_in_room_type = set(room_type_p_df.oyo_id) - set(room_type_price_df.oyo_id)

        if len(set_not_in_room_price_diff | set_not_in_room_type) > 0:
            msg = "The following hotels have neither room_price_diff in pc: {}, nor hotel_room_type: {}".format(
                set_not_in_room_price_diff, set_not_in_room_type)
            LogUtil.get_cur_logger().info(msg)
            DdtUtil.robot_send_ddt_msg(msg, self.get_robot_token_op_alert())

        prices_for_all_room_type_df = room_type_price_df[
            ['date', 'hotel_name', 'hotel_id', 'unique_code', 'oyo_id', 'room_type_id', 'room_type_name', 'pms_price',
             'hourly_price']]
        return prices_for_all_room_type_df

    def override_base_price_to_pc(self, config, job_source, df_for_hotel):
        _job_name = config.get_job().get_job_name()
        batch = config.get_future_batch()
        holiday_desc = config.get_holiday_desc()
        df_for_hotel["strategy_type"] = _job_name + "_" + holiday_desc + "_" + batch
        df_for_hotel["reason_id"] = job_source.get_base_change_reason_id()
        df_for_hotel["room_type_id"] = 20
        df_for_hotel["price_start_date"] = DateUtil.stamp_to_date_format0(time.time())
        hotel_base_price = HotelBasePrice()
        hotel_base_price.set_hotel_base_price(config, df_for_hotel)

    def calc_rate_diff(self, df_base_price, df_for_hotel_occ, future_batch):
        df_for_hotel = pd.merge(df_for_hotel_occ, df_base_price, how="right", on=["oyo_id"])

        df_for_hotel = DFUtil.apply_func_for_df(df_for_hotel, "rate_diff", ["occ"],
                                                lambda values: HolidayFutureStrategy.get_rate_diff(future_batch,
                                                                                                   *values))
        df_for_hotel = DFUtil.apply_func_for_df(df_for_hotel, "base_override", ["base", "rate_diff"],
                                                lambda values: int(values[0] * (1 + values[1])))
        return df_for_hotel
