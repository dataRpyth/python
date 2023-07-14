#!/usr/bin/env python
# -*- coding:utf-8 -*-
import os
import sys
from os.path import join as join_path

cur_path = os.path.dirname(os.path.realpath(__file__))
sys.path.append(join_path(cur_path, '..'))
import warnings

from pathos.multiprocessing import ProcessingPool
from common.priceop.price_to_intermedium import IntermediumHandler
from common.job_base.job_base import JobBase
from Liquidation.configuration import STRATEGY_TYPE_LIQUIDATION_29_V1, STRATEGY_TYPE_LIQUIDATION_29_V1_2, BATCH_LIQ_DICT
from common.job_common.job_sinker import JobSinker
from common.job_common.job_source import JobSource
from common.util.my_thread_executor import MyThreadExecutor
from strategy.china_rebirth import *
from strategy.rule_base import *
from CHINA_Rebirth.channel_rate_test import ChannelPriceSinker
from CHINA_Rebirth.activity_dynamic_price_blacklist import ACTIVITY_DYNAMIC_PRICE_BLACKLIST_DATE_MAP, \
    NATIONA_DAY_2019_OTA_RAISE_END_DATE

warnings.filterwarnings("ignore")
from datetime import datetime, timedelta
from strategy.gray import GRAY_WALKIN_MAX_PRICE_EXP_SET


class ChinaRebirthJobBase(JobBase):

    def __init__(self, job_config):
        JobBase.__init__(self, job_config)

    def run_core(self, oyo_id_list, start_time, pool, disable_ebase):
        raise Exception('must implement')

    def get_min_price(self):
        raise Exception('must implement')

    @staticmethod
    def add_diff_flag(row):
        if row.get("ota_code") == "ctrip":
            flag1 = (MiscUtil.is_empty_value(row.get('ctrip_pre_sell_price')) and MiscUtil.is_empty_value(
                row.get('ota_recent_price')))
            if row.get('ctrip_pre_sell_price') == row.get('ota_recent_price') or flag1:
                return False
        if row.get("ota_code") == "meituan":
            flag2 = (MiscUtil.is_empty_value(row.get('meituan_pre_sell_price')) and MiscUtil.is_empty_value(
                row.get('ota_recent_price')))
            if row.get('meituan_pre_sell_price') == row.get('ota_recent_price') or flag2:
                return False
        return True

    @staticmethod
    def add_ota_filter(row):
        if MiscUtil.is_not_empty_value(row.get("meituan_pre_sell_price")) or \
                MiscUtil.is_not_empty_value(row.get("ctrip_pre_sell_price")):
            return True
        return False

    @staticmethod
    def price_raise(rate, date, pms_price):
        if MiscUtil.is_empty_value(pms_price):
            return pms_price
        if date > NATIONA_DAY_2019_OTA_RAISE_END_DATE:
            return pms_price
        return round(pms_price * rate)

    def run(self):

        logger = LogUtil.get_cur_logger()

        DateUtil.init_preset_time()

        job_name = self.get_job_name()

        job_source = JobSource(self.get_adb_query_manager(), self.get_mysql_query_manager(),
                               self.get_oracle_query_manager())

        job_sinker = JobSinker(job_source)

        config = self.get_job_config()

        batch_order = config.get_batch_order()

        internal_alert_token = self.get_robot_token_internal_alert()

        t_executor1 = MyThreadExecutor(3, job_name + "_1", internal_alert_token)

        t_executor2 = MyThreadExecutor(6, job_name + "_2", internal_alert_token)

        job_begin = time.time()

        logger.info('*********************{0} run begin ***********************'.format(job_name))

        start_stamp = job_begin

        start_time = dt.datetime.fromtimestamp(start_stamp)

        pricing_start_date = DateUtil.stamp_to_date_format0(start_stamp)

        pricing_end_date = DateUtil.stamp_to_date_format(start_stamp, format_str="%Y-%m-%d", offset=7)

        set_oyo_id = job_source.get_hotel_batch_oyo_set_from_center(batch_order, start_stamp)

        # 筛除非直联
        if not config.is_inc_ota_non_direct_hotels():
            set_oyo_id = set_oyo_id - job_source.get_non_ota_direct_oyo_id(set_oyo_id)
            logger.info("after excluded inc_ota_non_direct_hotels, hotel-size: {0}".format(len(set_oyo_id)))

        # 筛除hotel_tagged
        if config.is_tagged():
            set_oyo_id = set_oyo_id - job_source.get_hotel_tagged_oyo_id('pricing-China2.0-high-star')
            logger.info("after excluded hotel-tagged, hotel-size: {0}".format(len(set_oyo_id)))

        if len(set_oyo_id) == 0:
            logger.warning('hotel set is empty, halt execution！！！')
            return

        new_activated_hotels = self.get_new_activated_hotels(set_oyo_id, pricing_start_date, job_source.mysql_query_mgr)

        oyo_id_list_groups = MiscUtil.group_by_list(list(set_oyo_id), 4000)

        smart_hotel_params_df_list = list()

        pool = ProcessingPool()

        disable_ebase = config.disable_ebase()

        for oyo_id_list in oyo_id_list_groups:
            group_smart_hotel_params_df = self.run_core(oyo_id_list, start_time, pool, disable_ebase)

            smart_hotel_params_df_list.append(group_smart_hotel_params_df)

        smart_hotel_params_df = pd.concat(smart_hotel_params_df_list, ignore_index=True)

        def _max_walkin_raise_exp_delta(oyo_id, date, corrected_base, price):
            if oyo_id not in GRAY_WALKIN_MAX_PRICE_EXP_SET or date != pricing_start_date:
                return 0
            return max(corrected_base, price) - price

        smart_hotel_params_df = DFUtil.apply_func_for_df(smart_hotel_params_df, 'max_walkin_raise_delta',
                                                         ['oyo_id', 'date', 'corrected_base', 'price'],
                                                         lambda values: _max_walkin_raise_exp_delta(*values))

        activity_blacklist_days = ACTIVITY_DYNAMIC_PRICE_BLACKLIST_DATE_MAP.get(pricing_start_date)

        if activity_blacklist_days is not None:
            smart_hotel_params_df = smart_hotel_params_df[~smart_hotel_params_df.date.isin(activity_blacklist_days)]

        price_multiplier_df = job_source.get_price_multiplier(pricing_start_date, set_oyo_id)

        smart_hotel_params_df = self.apply_price_multiplier(smart_hotel_params_df, price_multiplier_df)

        smart_room_type_price_df = self.get_room_type_price_df(job_source, smart_hotel_params_df, set_oyo_id)

        smart_room_type_price_df = smart_room_type_price_df[smart_room_type_price_df.date <= pricing_end_date]

        # 加入甩卖flag
        liquidation_batch = config.get_liquidation_flag()

        if liquidation_batch is not None:

            # override_price_for_special_sale
            liquidation_strategy = BATCH_LIQ_DICT.get(liquidation_batch)

            smart_room_type_price_df = self.override_price_for_special_sale(config, job_source,
                                                                            smart_room_type_price_df, start_stamp,
                                                                            liquidation_strategy)

        smart_room_type_price_for_inter_df = smart_room_type_price_df.copy()

        smart_room_type_price_df = self.get_available_room_df(job_source, smart_room_type_price_df, set_oyo_id)

        def _special_hack(oyo_id, room_type_id, pms_price):
            if oyo_id != 'CN_QAX1030' or room_type_id != 20:
                return pms_price
            return min(max(pms_price, 35), 39)

        smart_room_type_price_df = DFUtil.apply_func_for_df(smart_room_type_price_df, 'pms_price',
                                                            ['oyo_id', 'room_type_id', 'pms_price'],
                                                            lambda values: _special_hack(*values))

        room_type_prices_df = smart_room_type_price_df[
            ['date', 'hotel_name', 'hotel_id', 'unique_code', 'oyo_id', 'room_type_id', 'room_type_name', 'pms_price',
             'hourly_price']]


        # send for ota
        if config.is_inc_ota_non_direct_hotels():
            t_executor2.sub_task(job_sinker.pms_prices_to_ota_and_plugin, config, room_type_prices_df.copy(),
                                 set_oyo_id, start_stamp, pricing_end_date, new_activated_hotels, pool)

        all_calendar_prices = room_type_prices_df

        # send for crs
        t_executor2.sub_task(job_sinker.pms_prices_to_crs, config, all_calendar_prices, pricing_start_date,
                             pricing_end_date, new_activated_hotels)

        t_executor2.completed(timeout=3600)

        pool.close()

        pool.join()

        # send for intermedium
        t_executor1.sub_task(IntermediumHandler.post_intermedium_for_china_rebirth, config,
                             smart_room_type_price_for_inter_df)

        df_room_type_price_after_ota_liquidation = smart_room_type_price_df.copy()

        is_afternoon = start_time.hour > 12

        t_executor1.sub_task(ChannelPriceSinker.price_to_crs_via_channel_rate, config,
                             df_room_type_price_after_ota_liquidation, set_oyo_id,
                             pricing_start_date, job_source, is_afternoon)

        t_executor1.completed(timeout=1800)

        logger.info('*********************run end******************************')
        return

    def get_new_activated_hotels(self, oyo_id_set, start_date, mysql_query_mgr):
        hotel_start_day_df = get_all_hotel_active_day(list(oyo_id_set), start_date, 3, mysql_query_mgr)
        new_hotels_start_day = (datetime.strptime(start_date, '%Y-%m-%d') - timedelta(days=3)).strftime('%Y-%m-%d')
        new_hotels_df = hotel_start_day_df[hotel_start_day_df.start_day >= new_hotels_start_day]
        new_hotels_set = set(new_hotels_df.oyo_id)
        LogUtil.get_cur_logger().info('get_new_activated_hotels, new_hotels_set: {}'.format(new_hotels_set))
        return new_hotels_set

    def get_available_room_df(self, job_source, prices_for_room_type_df, set_oyo_id):
        room_type_prices_df = job_source.get_available_room_type_df(prices_for_room_type_df, set_oyo_id)
        set_not_in_hotel_room_type = set(prices_for_room_type_df.oyo_id) - set(room_type_prices_df.oyo_id)
        if len(set_not_in_hotel_room_type) > 0:
            msg = "The following hotels have no hotel_room_type in pc: {}".format(set_not_in_hotel_room_type)
            LogUtil.get_cur_logger().info(msg)
            DdtUtil.robot_send_ddt_msg(msg, self.get_robot_token_op_alert())
        return room_type_prices_df


    def apply_price_multiplier(self, smart_hotel_params_df, price_multiplier_df):

        smart_hotel_params_df = smart_hotel_params_df.merge(price_multiplier_df, left_on=['oyo_id', 'date'],
                                                            right_on=['oyo_id', 'sku_date'], how='left',
                                                            suffixes=['', '_y'])

        def apply_multiplier(price, pricing_multiplier):
            if MiscUtil.is_empty_value(pricing_multiplier):
                return price
            return price * (1 + pricing_multiplier)

        smart_hotel_params_df = DFUtil.apply_func_for_df(smart_hotel_params_df, 'price', ['price', 'pricing_multiplier'],
                                                         lambda values: apply_multiplier(*values))

        smart_hotel_params_df.drop(columns=['sku_date_y', 'pricing_multiplier'],inplace=True)

        return smart_hotel_params_df

    def get_room_type_price_df(self, job_source, smart_hotel_params_df, set_oyo_id):
        hotel_room_type_price_df = job_source.get_hotel_room_type_price_df(smart_hotel_params_df, set_oyo_id)
        set_not_in_room_price_diff = set(smart_hotel_params_df.oyo_id) - set(hotel_room_type_price_df.oyo_id)
        if len(set_not_in_room_price_diff) > 0:
            msg = "The following hotels have no room_price_diff in pc: {}".format(set_not_in_room_price_diff)
            LogUtil.get_cur_logger().info(msg)
            DdtUtil.robot_send_ddt_msg(msg, self.get_robot_token_op_alert())
        return hotel_room_type_price_df

    def override_price_for_special_sale(self, config, job_common, prices_for_room_type_df, start_stamp, strategy_type):
        LogUtil.get_cur_logger().info('*********************special sale process start******************************')
        df_special_sale = job_common.get_df_for_special_sale(start_stamp, [strategy_type])
        prices_for_room_type_df.drop(['sale_price', 'strategy_type'], axis=1, inplace=True)
        # 取交集
        prices_for_room_type_df = pd.merge(prices_for_room_type_df, df_special_sale, how='left',
                                           on=['oyo_id', 'room_type_id', 'date'])
        # 发送改价前后酒店列表
        rpt_prices_for_room_type_df = prices_for_room_type_df[~pd.isna(prices_for_room_type_df.sale_price)]
        rpt_prices_for_room_type_df.rename(
            columns={'sale_price': 'price_after_liquidation', 'pms_price': 'price_before_liquidation',
                     'date': 'pricing_date'}, inplace=True)
        rpt_prices_for_room_type_df = rpt_prices_for_room_type_df[["oyo_id", "room_type_id", "room_type_name",
                                                                   "price_before_liquidation",
                                                                   "price_after_liquidation",
                                                                   "strategy_type", "pricing_date"]]
        DFUtil.print_data_frame(rpt_prices_for_room_type_df, "rpt_prices_for_room_type_df", True)
        config.get_mail_send().send_mail_for_hotel_special_sale_override(config, rpt_prices_for_room_type_df)

        # 重设pms_price
        def override_special_sale_price(pms_price, sale_price):
            return sale_price if MiscUtil.is_not_empty_value(sale_price) else pms_price

        prices_for_room_type_df = DFUtil.apply_func_for_df(prices_for_room_type_df, 'pms_price',
                                                           ['pms_price', 'sale_price'],
                                                           lambda values: override_special_sale_price(*values))
        return prices_for_room_type_df
