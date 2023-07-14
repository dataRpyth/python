#!/usr/bin/env python
# -*- coding:utf-8 -*-
import copy
import os
import sys
from os.path import join as join_path

cur_path = os.path.dirname(os.path.realpath(__file__))
sys.path.append(join_path(cur_path, '..'))

from common.enum.pricing_enum import PriceChannel
from common.priceop.api_base import ApiBase
from common.priceop.price_to_channel_rate import ChannelRate
from common.util.utils import *
from strategy.gray import GRAY_WALKIN_DYNAMIC_RATIO_SET


def adjust_price_by_ratio(price, adjust_ratio):
    if MiscUtil.is_empty_value(price):
        return price
    return float(int(float(price) * adjust_ratio))


class ChannelPriceSinker:

    @staticmethod
    def price_to_crs_via_channel_rate(config, df_room_type_price_after_ota_liquidation,
                                      set_daily_walkin_raise_hotels, pricing_start_date, job_source, is_afternoon):
        try:
            LogUtil.get_cur_logger().info("start ChannelRateTest.price_to_crs_via_channel_rate")

            channel_rate_test = ChannelPriceSinker()

            df_walkin_raise_price = df_room_type_price_after_ota_liquidation[
                df_room_type_price_after_ota_liquidation.oyo_id.isin(set_daily_walkin_raise_hotels)][
                df_room_type_price_after_ota_liquidation.date == pricing_start_date]

            def _add_max_walkin_raise_delta(pms_price, delta):
                return pms_price + delta

            df_walkin_raise_price = DFUtil.apply_func_for_df(df_walkin_raise_price, 'pms_price',
                                                             ['pms_price', 'max_walkin_raise_delta'],
                                                             lambda values: _add_max_walkin_raise_delta(*values))

            normal_ratio_hotels_set = set_daily_walkin_raise_hotels - GRAY_WALKIN_DYNAMIC_RATIO_SET

            df_normal_ratio_walkin_raise_price = df_walkin_raise_price[df_walkin_raise_price.oyo_id.isin(normal_ratio_hotels_set)]

            df_normal_ratio_walkin_raise_price = channel_rate_test.adjust_channel_price_sink_to_crs_by_ratio(
                df_normal_ratio_walkin_raise_price, 1.12, set_daily_walkin_raise_hotels, job_source
            )

            df_dynamic_ratio_walkin_raise_price = df_walkin_raise_price[df_walkin_raise_price.oyo_id.isin(GRAY_WALKIN_DYNAMIC_RATIO_SET)]

            ratio = 1.16 if is_afternoon else 1.08

            df_dynamic_ratio_walkin_raise_price = channel_rate_test.adjust_channel_price_sink_to_crs_by_ratio(
                df_dynamic_ratio_walkin_raise_price, ratio, GRAY_WALKIN_DYNAMIC_RATIO_SET, job_source)

            df_walkin_prices = pd.concat([df_normal_ratio_walkin_raise_price, df_dynamic_ratio_walkin_raise_price], ignore_index=True)

            ChannelRate.price_to_channel_rate(config, df_walkin_prices, PriceChannel.CHANNEL_IDS_WALKIN)

            LogUtil.get_cur_logger().info("end ChannelRateTest.price_to_crs_via_channel_rate")
        except Exception:
            ApiBase.send_stack_trace("price_to_crs_via_channel_rate", config)

    def adjust_channel_price_sink_to_crs_by_ratio(self, filtered_room_type_prices_df, adjust_ratio, test_hotel_set,
                                                  job_source):

        filtered_room_type_prices_df = filtered_room_type_prices_df[
            filtered_room_type_prices_df.oyo_id.isin(test_hotel_set)]

        filtered_room_type_prices_df['pms_price'] = filtered_room_type_prices_df.pms_price.map(
            lambda x: adjust_price_by_ratio(x, adjust_ratio))

        job_source.set_global_min_price(filtered_room_type_prices_df)

        filtered_room_type_prices_df = DFUtil.apply_func_for_df(filtered_room_type_prices_df, 'pms_price',
                                                                ['pms_price', 'floor_price_type', 'floor_price'],
                                                                lambda values: PriceUtil.floor_price_check(
                                                                    *values))
        filtered_room_type_prices_df = DFUtil.apply_func_for_df(filtered_room_type_prices_df, 'pms_price',
                                                                ['pms_price', 'ceiling_price_type',
                                                                 'ceiling_price'],
                                                                lambda values: PriceUtil.ceiling_price_check(
                                                                    *values))

        filtered_room_type_prices_df = DFUtil.apply_func_for_df(filtered_room_type_prices_df, 'pms_price',
                                                                ['pms_price', 'floor_override_price'],
                                                                lambda values: PriceUtil.override_floor_price_check(
                                                                    *values))

        filtered_room_type_prices_df = DFUtil.apply_func_for_df(filtered_room_type_prices_df, 'pms_price',
                                                                ['pms_price', 'ceiling_override_price'],
                                                                lambda
                                                                    values: PriceUtil.override_ceiling_price_check(
                                                                    *values))

        prices_for_all_room_type_df = filtered_room_type_prices_df[
            ['date', 'hotel_name', 'hotel_id', 'unique_code', 'oyo_id', 'room_type_id', 'room_type_name',
             'pms_price', 'hourly_price']]

        return prices_for_all_room_type_df

    @staticmethod
    def check_df_price_column(df_to_check, column_to_check, query_mgr):
        all_smart_hotels_str = MiscUtil.convert_list_to_tuple_list_str(list(df_to_check.oyo_id))
        floor_price_query = """
                    select oyo_id, room_type_id, price_type as floor_price_type, floor_price
                    from hotel_floor_price
                    where oyo_id in {0}
                    and deleted = 0
                """.format(all_smart_hotels_str)

        floor_price_df = query_mgr.read_sql(floor_price_query)
        if df_to_check.empty:
            return
        df_to_check = pd.merge(df_to_check, floor_price_df, on=['oyo_id', 'room_type_id'], how='left')

        df_to_check = DFUtil.apply_func_for_df(df_to_check, column_to_check,
                                               [column_to_check, 'floor_price_type', 'floor_price'],
                                               lambda values: PriceUtil.floor_price_check(
                                                   *values))

        ceiling_price_query = """
                    select oyo_id, room_type_id, price_type as ceiling_price_type, ceiling_price
                    from hotel_ceiling_price
                    where oyo_id in {0}
                    and deleted = 0
                """.format(all_smart_hotels_str)

        ceiling_price_df = query_mgr.read_sql(ceiling_price_query)

        df_to_check = pd.merge(df_to_check, ceiling_price_df, on=['oyo_id', 'room_type_id'],
                               how='left')

        df_to_check = DFUtil.apply_func_for_df(df_to_check, column_to_check,
                                               [column_to_check, 'ceiling_price_type', 'ceiling_price'],
                                               lambda values: PriceUtil.ceiling_price_check(
                                                   *values))

        override_floor_price_query = """
                    select oyo_id, room_type_id, pricing_date as date, over_ride_price as floor_override_price
                    from hotel_over_ride_price
                    where over_ride_type = 2
                      and deleted = 0
                      and oyo_id in {0}
                """.format(all_smart_hotels_str)

        override_floor_price_df = query_mgr.read_sql(override_floor_price_query)

        override_ceiling_price_query = """
                    select oyo_id, room_type_id, pricing_date as date, over_ride_price as ceiling_override_price
                    from hotel_over_ride_price
                    where over_ride_type = 3
                      and deleted = 0
                      and oyo_id in {0}
                """.format(all_smart_hotels_str)

        override_ceiling_price_df = query_mgr.read_sql(override_ceiling_price_query)

        def date_to_str(x):
            return x.strftime('%Y-%m-%d')

        override_floor_price_df = DFUtil.apply_func_for_df(override_floor_price_df, 'date', ['date'],
                                                           lambda values: date_to_str(*values))

        override_ceiling_price_df = DFUtil.apply_func_for_df(override_ceiling_price_df, 'date', ['date'],
                                                             lambda values: date_to_str(*values))

        if not override_floor_price_df.empty:
            df_to_check = pd.merge(df_to_check, override_floor_price_df, how='left',
                                   on=['oyo_id', 'room_type_id', 'date'])

            DFUtil.apply_func_for_df(df_to_check, column_to_check,
                                     [column_to_check, 'floor_override_price'],
                                     lambda values: PriceUtil.override_floor_price_check(
                                         *values))

        if not override_ceiling_price_df.empty:
            df_to_check = pd.merge(df_to_check, override_ceiling_price_df, how='left',
                                   on=['oyo_id', 'room_type_id', 'date'])

            DFUtil.apply_func_for_df(df_to_check, column_to_check,
                                     [column_to_check, 'ceiling_override_price'],
                                     lambda values: PriceUtil.override_ceiling_price_check(
                                         *values))

    def check_df_price_columns(self, df_to_check, columns_to_check, query_mgr):
        for column_to_check in columns_to_check:
            self.check_df_price_column(df_to_check, column_to_check, query_mgr)
