#!/usr/bin/env python
# -*- coding:utf-8 -*-

import os
import re
import sys
import time
from math import ceil
from os.path import join as join_path

cur_path = os.path.dirname(os.path.realpath(__file__))
sys.path.append(join_path(cur_path, '..'))

from common.util.utils import MiscUtil, LogUtil, DateUtil, DFUtil, DEFAULT_NULL_VALUE, PriceUtil, \
    FLOOR_PRICE_TYPE_NUMBER
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

ROOM_TYPE_ID_PREFERENTIAL = 51
from common.job_common.global_floor_hard_code import HARD_CODE_EX_GRATIA_GLOBAL_HOTELS, HARD_CODE_EX_GRATIA_GLOBAL_FLOOR_DF
from strategy.china_rebirth import get_max_calc_day_from_etl_log, get_max_create_time


class JobSource:

    def __init__(self, adb_query_mgr=None, mysql_query_mgr=None, oracle_query_mgr=None, hive_query_mgr=None):
        self.adb_query_mgr = adb_query_mgr
        self.mysql_query_mgr = mysql_query_mgr
        self.oracle_query_mgr = oracle_query_mgr
        self.hive_query_mgr = hive_query_mgr
        self.set_temp_referential_floor_price_hotels = set(self.get_df_for_temp_preferential_hotels().oyo_id)

    def get_hotel_batch_oyo_set_from_center(self, batch_orders_str='1', start_time_stamp=None, model_id=5):
        start_date = DateUtil.stamp_to_date_format0(start_time_stamp)
        hotel_batch_df = self.get_df_for_all_available_hotels(batch_orders_str, start_date, model_id)
        LogUtil.get_cur_logger().info("get_hotel_batch_oyo_set_from_center, batch: %s, model_id: %d, hotel-size: %d",
                                      batch_orders_str, model_id, len(hotel_batch_df))
        return set(hotel_batch_df.oyo_id)

    def get_new_pricing_hotels_set(self, start_date):
        pricing_start_hotels_query = """
            select distinct oyo_id
            from pricing_hotel
            where status = 1
              and pricing_start_date > '{0}' - interval 7 day
              and deleted = 0
            order by oyo_id
        """.format(start_date)
        pricing_start_hotels_df = self.mysql_query_mgr.read_sql(pricing_start_hotels_query)
        return set(pricing_start_hotels_df.oyo_id)

    def get_df_for_all_available_hotels(self, batch_orders_str, start_date, model_id):
        batch_orders_lst = batch_orders_str.split(',')
        tuple_str = MiscUtil.convert_list_to_tuple_list_str(batch_orders_lst)
        mg_batch_hotels_query = """
            select distinct(oyo_id)
            from hotel_batch
            where enabled = 1
              and batch_order_id in {0}
              and oyo_id in
                  (select distinct oyo_id
                   from hotel_business_model
                   where business_model_id = {2})
              and oyo_id in (select distinct oyo_id
                             from pricing_hotel
                             where deleted = 0
                               and status = 1
                               and '{1}' >= pricing_start_date)
        """.format(tuple_str, start_date, model_id)
        return self.mysql_query_mgr.read_sql(mg_batch_hotels_query)

    def get_non_ota_direct_oyo_id(self, set_oyo_id):
        all_smart_hotels_str = MiscUtil.convert_set_to_tuple_list_str(set_oyo_id)
        non_ota_direct_query = """
            select distinct oyo_id
                from hotel_ota_room_type
                where ota_channel_id in (1, 2)
                  and (ota_pre_commission is not null or ota_post_commission is not null)
                  and oyo_id in {}
        """.format(all_smart_hotels_str)
        non_ota_direct_df = self.mysql_query_mgr.read_sql(non_ota_direct_query)
        return set(non_ota_direct_df.oyo_id)

    def get_hotel_tagged_oyo_id(self, code):
        sql = """
            select distinct oyo_id
                from hotel_tag
                where tag_id = (
                  select id from pricing_dict where type_code = 'hotel_tag' and `code` = '{}'
                )
        """.format(code)
        hotel_tagged_oyo_df = self.mysql_query_mgr.read_sql(sql)
        return set(hotel_tagged_oyo_df.oyo_id)

    def _get_walkin_raise_max_date(self, walkin_raise_table_name):
        sql = """
            select to_char(max(created_at), 'yyyy-mm-dd') as max_date
            from {0}
        """.format(walkin_raise_table_name)
        return self.oracle_query_mgr.read_sql(sql, 30)

    def get_walkin_raise_hotel_set(self, set_oyo_id, walkin_raise_table_name):
        df_max_date = self._get_walkin_raise_max_date(walkin_raise_table_name)
        if df_max_date is None or df_max_date.empty:
            return None, set()
        calc_date = list(df_max_date.max_date)[0]
        if calc_date is None:
            return None, set()
        oyo_id_in_query = MiscUtil.convert_to_oracle_query_oyo_id_list_str('oyo_id', list(set_oyo_id), 1000)
        sql = """
            select oyo_id, calc_date, max(created_at) as created_at
            from {0}
            where calc_date = '{1}'
            and {2}
            group by oyo_id, calc_date
        """.format(walkin_raise_table_name, calc_date, oyo_id_in_query)
        df_walkin_raise_hotels = self.oracle_query_mgr.read_sql(sql, 60)
        return calc_date, set(df_walkin_raise_hotels.oyo_id)

    def get_df_for_hotel_occ(self, set_oyo_id, time_stamp):
        hotel_srn_df = MiscUtil.wrap_read_adb_df(self.get_df_for_hotel_srn, set_oyo_id)
        hotel_brn_df = MiscUtil.wrap_read_adb_df(self.get_df_for_hotel_brn, set_oyo_id, time_stamp)
        df_for_hotel_oc = pd.merge(hotel_srn_df, hotel_brn_df, on=['oyo_id'], how='left')
        df_for_hotel_oc.brn = df_for_hotel_oc.brn.replace([np.nan], 0)
        df_for_hotel_oc['occ'] = df_for_hotel_oc.brn / df_for_hotel_oc.srn
        return df_for_hotel_oc

    def get_df_for_hotel_brn(self, set_oyo_id, time_stamp):
        adb_query_str = MiscUtil.convert_to_oracle_query_oyo_id_list_str('oyo_id', list(set_oyo_id), 2000)
        start_date = DateUtil.stamp_to_date_format0(time_stamp)
        brn_query = """
                      select oyo_id, ifnull(brn, 0) as brn
                        from (select id as hotel_id, oyo_id from product_hotel where {0}) hotel_with_oyo_id
                               left join
                             (select hotel_id, count(*) as brn
                              from trade_room_reservation_room_detail
                              where booking_id in (select booking_id
                                                   from (select booking_id
                                                         from trade_room_reservation
                                                         where status in (0, 1, 2, 4)
                                                           and is_deleted = 0
                                                           and hotel_id in (select id as hotel_id
                                                                            from product_hotel
                                                                            where {0})
                                                           and date_format(check_in, '%%Y-%%m-%%d') <= ('{1}' + interval 0 day)
                                                           and date_format(check_out, '%%Y-%%m-%%d') > '{1}'
                                                           and (check_out_time is null or
                                                                date_format(check_out_time, '%%Y-%%m-%%d') = '1970-01-01' or
                                                                date_format(check_out_time, '%%Y-%%m-%%d') = '0000-00-00' or
                                                                date_format(check_out_time, '%%Y-%%m-%%d') > '{1}')) r
                                                          inner join (select id, is_deleted from trade_booking) b on r.booking_id = b.id
                                                   where b.is_deleted = 0)
                              and is_deleted = 0
                              group by hotel_id) hotel_with_room_type_brn
                             on hotel_with_oyo_id.hotel_id = hotel_with_room_type_brn.hotel_id
                    """.format(adb_query_str, start_date)
        return self.adb_query_mgr.read_sql(brn_query)

    def get_df_for_hotel_srn(self, set_oyo_id):
        adb_query_str = MiscUtil.convert_to_oracle_query_oyo_id_list_str('oyo_id', list(set_oyo_id), 2000)
        srn_query = """
                        select oyo_id, hotel.hotel_id, hotel.hotel_name,
                            sum(ifnull(total_count, 0)) + sum(ifnull(additional_count, 0)) total_count,
                            (sum(ifnull(total_count, 0)) - sum(ifnull(blocked_count, 0))) + 
                            (sum(ifnull(additional_count, 0)) - sum(ifnull(additional_blocked_count, 0))) as srn
                        from (select id as hotel_id, oyo_id, name as hotel_name
                              from product_hotel
                              where {0}) hotel
                        left join (select hotel_id, total_count, blocked_count, additional_count, additional_blocked_count
                                  from trade_hotel_inventory
                                  where is_deleted = 0) inventory
                        on hotel.hotel_id = inventory.hotel_id
                        group by oyo_id, hotel.hotel_id, hotel.hotel_name
                    """.format(adb_query_str)
        return self.adb_query_mgr.read_sql(srn_query)

    def get_df_for_room_type_srn(self, hotel_set):
        srn_query = """
                        select oyo_id, hotel.hotel_id,hotel.hotel_name, cast(inventory.room_type_id as integer) as room_type_id,
                            sum(ifnull(total_count, 0)) + sum(ifnull(additional_count, 0)) total_count,
                            (sum(ifnull(total_count, 0)) - sum(ifnull(blocked_count, 0))) + 
                            (sum(ifnull(additional_count, 0)) - sum(ifnull(additional_blocked_count, 0))) as srn
                        from (select id as hotel_id, oyo_id, name as hotel_name
                              from product_hotel
                              where {0}) hotel
                        left join (select hotel_id, room_type_id, total_count, blocked_count, additional_count, additional_blocked_count
                                  from trade_hotel_inventory
                                  where is_deleted = 0) inventory
                        on hotel.hotel_id = inventory.hotel_id
                        group by oyo_id, hotel.hotel_id, hotel.hotel_name, inventory.room_type_id
                    """.format(MiscUtil.convert_to_oracle_query_oyo_id_list_str('oyo_id', list(hotel_set), 2000))
        return self.adb_query_mgr.read_sql(srn_query)

    def get_df_for_room_type_brn(self, hotel_set, start_time_stamp):
        adb_query_str = MiscUtil.convert_to_oracle_query_oyo_id_list_str('oyo_id', list(hotel_set), 2000)
        start_date = DateUtil.stamp_to_date_format0(start_time_stamp)
        brn_query = """
                      select oyo_id, ifnull(cast(room_type_id as integer), 0) as room_type_id, ifnull(brn, 0) as brn
                        from (select id as hotel_id, oyo_id from product_hotel where {0}) hotel_with_oyo_id
                               left join
                             (select hotel_id,room_type as room_type_id, count(*) as brn
                              from trade_room_reservation_room_detail
                              where booking_id in (select booking_id
                                                   from (select booking_id
                                                         from trade_room_reservation
                                                         where status in (0, 1, 2, 4)
                                                           and is_deleted = 0
                                                           and hotel_id in (select id as hotel_id
                                                                            from product_hotel
                                                                            where {0})
                                                           and date_format(check_in, '%%Y-%%m-%%d') <= ('{1}' + interval 0 day)
                                                           and date_format(check_out, '%%Y-%%m-%%d') > '{1}'
                                                           and (check_out_time is null or
                                                                date_format(check_out_time, '%%Y-%%m-%%d') = '1970-01-01' or
                                                                date_format(check_out_time, '%%Y-%%m-%%d') = '0000-00-00' or
                                                                date_format(check_out_time, '%%Y-%%m-%%d') > '{1}')) r
                                                          inner join (select id, is_deleted from trade_booking) b on r.booking_id = b.id
                                                   where b.is_deleted = 0)
                              and is_deleted = 0
                              group by hotel_id, room_type) hotel_with_room_type_brn
                             on hotel_with_oyo_id.hotel_id = hotel_with_room_type_brn.hotel_id
                    """.format(adb_query_str, start_date)
        return self.adb_query_mgr.read_sql(brn_query)

    def get_base_price(self, set_oyo_id, date_start, date_end):
        all_hotels_str = self.get_tuple_str_for_oyo_id(set_oyo_id)
        base_price_query = '''
            select bp.oyo_id as oyo_id, bp.pricing_date as date, bp.base_price as base, h.pricing_start_date as price_start_date
            from hotel_base_price bp left join pricing_hotel h on bp.oyo_id = h.oyo_id
            where bp.pricing_date between '{0}' and '{1}'
              and bp.deleted = 0
              and bp.room_type_id = 20
              and bp.oyo_id in {2}
              and h.deleted = 0
              and h.status = 1
            '''.format(date_start, date_end, all_hotels_str)
        df_for_base = self.mysql_query_mgr.read_sql(base_price_query)
        df_for_base["date"] = df_for_base["date"].apply(lambda x: str(x))
        return df_for_base

    def get_floor_price_df(self, set_oyo_id):
        oyo_id_str = self.get_tuple_str_for_oyo_id(set_oyo_id)
        floor_price_query = """
            select oyo_id, room_type_id, price_type as floor_price_type, floor_price
            from hotel_floor_price
            where oyo_id in {0}
            and deleted = 0
        """.format(oyo_id_str)
        LogUtil.get_cur_logger().info('start floor price query')
        floor_price_df = self.mysql_query_mgr.read_sql(floor_price_query)
        LogUtil.get_cur_logger().info('end floor price query, size: {}'.format(len(floor_price_df)))
        return floor_price_df

    def get_ceiling_price_df(self, set_oyo_id):
        oyo_id_str = self.get_tuple_str_for_oyo_id(set_oyo_id)
        ceiling_price_query = """
            select oyo_id, room_type_id, price_type as ceiling_price_type, ceiling_price
            from hotel_ceiling_price
            where oyo_id in {0}
            and deleted = 0
        """.format(oyo_id_str)
        LogUtil.get_cur_logger().info('start ceiling price query')
        ceiling_price_df = self.mysql_query_mgr.read_sql(ceiling_price_query)
        LogUtil.get_cur_logger().info('end ceiling price query, size: {}'.format(len(ceiling_price_df)))
        return ceiling_price_df

    def get_override_floor_price_df(self, set_oyo_id):
        oyo_id_str = self.get_tuple_str_for_oyo_id(set_oyo_id)
        override_floor_price_query = """
            select oyo_id, room_type_id, pricing_date as date, over_ride_price as floor_override_price
            from hotel_over_ride_price
            where over_ride_type = 2
              and deleted = 0
              and oyo_id in {0}
        """.format(oyo_id_str)
        override_floor_price_df = self.mysql_query_mgr.read_sql(override_floor_price_query)
        override_floor_price_df = DFUtil.apply_func_for_df(override_floor_price_df, 'date', ['date'],
                                                           lambda values: self.date_to_str(*values))
        return override_floor_price_df

    def get_override_ceiling_price_df(self, set_oyo_id):
        oyo_id_str = self.get_tuple_str_for_oyo_id(set_oyo_id)
        override_ceiling_price_query = """
            select oyo_id, room_type_id, pricing_date as date, over_ride_price as ceiling_override_price
            from hotel_over_ride_price
            where over_ride_type = 3
              and deleted = 0
              and oyo_id in {0}
        """.format(oyo_id_str)
        override_ceiling_price_df = self.mysql_query_mgr.read_sql(override_ceiling_price_query)

        override_ceiling_price_df = DFUtil.apply_func_for_df(override_ceiling_price_df, 'date', ['date'],
                                                             lambda values: self.date_to_str(*values))
        return override_ceiling_price_df

    def get_room_type_diff_df(self, set_oyo_id, default_room_type_diff=False):
        begin0 = time.time()
        all_smart_hotels_str = MiscUtil.convert_set_to_tuple_list_str(set_oyo_id)

        room_type_difference_query = '''
                    select
                        oyo_id, room_type_id, difference_type, price_delta, price_multiplier, b.NAME as room_type_name 
                    from
                        hotel_room_type_price_difference a
                        left join room_type b on a.room_type_id = b.id 
                    where
                        a.oyo_id in {0}
                    '''.format(all_smart_hotels_str)
        room_type_difference_df = self.mysql_query_mgr.read_sql(room_type_difference_query, 10)

        if default_room_type_diff:
            default_room_type_difference = {'room_type_id': [20, 29, 26, 30, 33, 34],
                                            'default_room_type_name': ['标准大床房', '标准双床房', '豪华大床房', '豪华双床房', '三人房',
                                                                       '主题房'],
                                            'default_price_delta': [0, 0, 0, 0, 0, 0],
                                            'default_price_multiplier': [1, 1, 1.2, 1.3, 1.3, 1.3],
                                            'default_difference_type': [2, 2, 2, 2, 2, 2]
                                            }

            default_room_type_difference_df = pd.DataFrame(default_room_type_difference)
            filtered_hotels_oyo_id_series = pd.DataFrame({'oyo_id': list(set_oyo_id)}).oyo_id

            filtered_hotels_oyo_id_df = pd.DataFrame(filtered_hotels_oyo_id_series)

            filtered_hotels_oyo_id_df['dummy'] = 0

            default_room_type_difference_df['dummy'] = 0

            default_room_type_difference_df = pd.merge(default_room_type_difference_df, filtered_hotels_oyo_id_df,
                                                       on=['dummy'], how='outer')

            default_room_type_difference_df = default_room_type_difference_df[
                ['oyo_id', 'room_type_id', 'default_room_type_name', 'default_price_delta', 'default_price_multiplier',
                 'default_difference_type']]

            room_type_difference_df = pd.merge(room_type_difference_df, default_room_type_difference_df, how='left',
                                               on=['oyo_id', 'room_type_id'])
            room_type_difference_series = pd.Series(room_type_difference_df.index)
            room_type_difference_df['price_multiplier'] = room_type_difference_series.map(
                lambda x: np.where(pd.isnull(room_type_difference_df.price_multiplier[x]),
                                   room_type_difference_df.default_price_multiplier[x],
                                   room_type_difference_df.price_multiplier[x]))
            room_type_difference_df['room_type_name'] = room_type_difference_series.map(
                lambda x: np.where(pd.isnull(room_type_difference_df.room_type_name[x]),
                                   room_type_difference_df.default_room_type_name[x],
                                   room_type_difference_df.room_type_name[x]))
            room_type_difference_df['difference_type'] = room_type_difference_series.map(
                lambda x: np.where(pd.isnull(room_type_difference_df.difference_type[x]),
                                   room_type_difference_df.default_difference_type[x],
                                   room_type_difference_df.difference_type[x]))
        LogUtil.get_cur_logger().info('room_type_difference_query, %0.2fs elapsed, size: %d', time.time() - begin0,
                                      room_type_difference_df.size)
        return room_type_difference_df

    def get_df_for_crs_recent_price(self, set_oyo_id, start_date, end_date):
        if len(set_oyo_id) == 0:
            return pd.DataFrame(columns=['oyo_id', 'room_type_id', 'date', 'crs_recent_price'])
        oyo_id_tuple_list_str = MiscUtil.convert_to_oracle_query_oyo_id_list_str('oyo_id', set_oyo_id, 2000)
        sql = """
            select oyo_id, room_type_id, rate_date as date, rate as crs_recent_price
                from (select hotel_id, rate_date, room_type_id, rate
                      from price_list_rate
                      where hotel_id in (select id as hotel_id
                                         from product_hotel
                                         where {2})
                        and operator_id = 16067
                        and rate_date between '{0}' and '{1}'
                        and is_deleted = 0) a
                       join (select id as hotel_id, oyo_id
                             from product_hotel
                             where {2} 
                             and is_deleted = 0) b on a.hotel_id = b.hotel_id
        """.format(start_date, end_date, oyo_id_tuple_list_str)
        df_recent_price = self.adb_query_mgr.read_sql(sql, 300)
        df_recent_price['date'] = df_recent_price.date.map(lambda x: x.strftime('%Y-%m-%d'))
        return df_recent_price

    def get_ota_recent_price(self, oyo_id_list, start_date, end_date):
        adb_query_str = MiscUtil.convert_to_oracle_query_oyo_id_list_str('oocpa.oyo_id', oyo_id_list, 2000)
        sql = """
                        select d.oyo_id, d.room_type_id, d.date, d.ota_code, d.ota_recent_price
                            from (select oocpa.hotel_id, oocpa.oyo_id, oocpa.room_type_id, oocpa.date, a.ota_code, a.price as ota_recent_price, oocpa.create_time
                                       from ota_ota_change_price_apply oocpa
                                       left join (
                                         select distinct oocpi.change_price_id, oocpi.ota_code, oocps.price
                                         from ota_ota_change_price_info oocpi
                                                left join ota_ota_change_price_strategy oocps
                                                          on oocpi.id = oocps.change_price_info_id
                                         where oocps.price_type = 0
                                           and oocps.is_deleted = 0
                                           and oocpi.is_deleted = 0
                                       ) a
                                       on oocpa.id = a.change_price_id
                                       where oocpa.is_deleted = 0
                                          and oocpa.date between '{0}' and '{1}'
                                          and {2}
                        ) d
                        join (
                             select oocpa.hotel_id, oocpa.oyo_id, oocpa.room_type_id, oocpa.date, a1.ota_code, max(oocpa.create_time) as create_time
                                  from ota_ota_change_price_apply oocpa
                                  left join (
                                    select distinct oocpi.change_price_id, oocpi.ota_code, oocps.price
                                    from ota_ota_change_price_info oocpi
                                           left join ota_ota_change_price_strategy oocps
                                                     on oocpi.id = oocps.change_price_info_id
                                    where oocps.price_type = 0 and oocps.is_deleted = 0 and oocpi.is_deleted = 0
                                  ) a1
                                  on oocpa.id = a1.change_price_id
                                  where oocpa.is_deleted = 0
                                    and oocpa.date between '{0}' and '{1}'
                                    and {2}
                             group by oocpa.hotel_id, oocpa.oyo_id, oocpa.room_type_id, oocpa.date, a1.ota_code
                        ) e
                        on d.hotel_id = e.hotel_id
                           and d.oyo_id = e.oyo_id
                           and d.date = e.date
                           and d.room_type_id = e.room_type_id
                           and d.ota_code = e.ota_code
                           and d.create_time = e.create_time
                        order by d.hotel_id, d.oyo_id, d.date, d.room_type_id
                """.format(start_date, end_date, adb_query_str)
        df_recent_price = self.adb_query_mgr.read_sql(sql, 300)
        return df_recent_price

    def get_df_for_ota_recent_price(self, oyo_id_list, start_date, end_date):
        if len(oyo_id_list) == 0:
            return pd.DataFrame(columns=['oyo_id', 'room_type_id', 'date', 'ota_code', 'ota_recent_price'])
        df_recent_price = MiscUtil.wrap_read_adb_df(self.get_ota_recent_price, oyo_id_list, start_date, end_date)
        if df_recent_price.empty:
            return pd.DataFrame(columns=['oyo_id', 'room_type_id', 'date', 'ota_code', 'ota_recent_price'])
        df_recent_price['date'] = df_recent_price.date.map(lambda x: x.strftime('%Y-%m-%d'))
        df_recent_price['room_type_id'] = df_recent_price.room_type_id.map(lambda x: int(x))
        df_recent_price['oyo_id'] = df_recent_price.oyo_id.map(lambda x: str(x))
        df_recent_price = df_recent_price.drop_duplicates(
            ['oyo_id', 'room_type_id', 'date', 'ota_code', 'ota_recent_price'], keep="last")
        return df_recent_price

    def get_df_for_recent_channel_rate(self, date_start, date_end, channel_id=8):
        sql = """
            select oyo_id, room_type_id, rate_date as "date", rate as channel_price
            from (select hotel_id, room_type_id, rate_date, min(rate) as rate
                  from price_channel_list_rate
                  where channel_id = {0}
                    and rate_date between '{1}' and '{2}'
                    and pay_type = 1
                  group by hotel_id, room_type_id, rate_date
                  order by hotel_id, room_type_id, rate_date) price
                   left join (select id, oyo_id from product_hotel) hotel on price.hotel_id = hotel.id
        """.format(channel_id, date_start, date_end)
        df_for_channel = self.adb_query_mgr.read_sql(sql)
        df_for_channel['date'] = df_for_channel.date.map(lambda x: x.strftime('%Y-%m-%d'))
        return df_for_channel

    def get_price_multiplier(self, pricing_date, set_oyo_id):
        oyo_ids = MiscUtil.convert_list_to_tuple_list_str(list(set_oyo_id))
        sql = """
        select oyo_id, date_format(effective_date, '%%Y-%%m-%%d') as sku_date, pricing_multiplier
        from hotel_pricing_multiplier
        where pricing_date = '{pricing_date}'
          and oyo_id in {oyo_ids}
        """.format(pricing_date=pricing_date, oyo_ids=oyo_ids)
        return self.mysql_query_mgr.read_sql(sql, 60)

    def get_df_for_temp_preferential_hotels(self):
        sql = """
        select distinct oyo_id
        from hotel_tag
        where tag_id = (
          select id from pricing_dict where type_code = 'hotel_tag' and code = 'SPR_FLOOR_PRICE'
        )
        """
        return self.mysql_query_mgr.read_sql(sql, 60)

    def get_diff_channel_price_df(self, room_price_df, start_date, pricing_end_date):
        df_for_channel = self.get_df_for_recent_channel_rate(start_date, pricing_end_date)
        room_price_df = pd.merge(room_price_df, df_for_channel, how="left", on=['oyo_id', 'room_type_id', 'date'])
        room_price_df["diff_price"] = room_price_df.apply(lambda row: row["pms_price"] != row["channel_price"], axis=1)
        room_price_df = room_price_df[room_price_df.diff_price == True]
        room_price_df.drop(['diff_price', 'channel_price'], axis=1, inplace=True)
        return room_price_df

    def get_hourly_room_type_price_df(self, smart_hotel_params_df, set_oyo_id):
        smart_hotel_params_df = smart_hotel_params_df.sort_values(axis=0, by=['date', 'oyo_id'], ascending=True)
        # merge room_type_difference_df
        room_type_diff_df = self.get_room_type_diff_df(set_oyo_id, True)
        room_type_diff_temp_df = room_type_diff_df[
            ['oyo_id', 'room_type_name', 'room_type_id', 'price_delta', 'price_multiplier', 'difference_type']]
        smart_hotel_params_df = pd.merge(room_type_diff_temp_df, smart_hotel_params_df, how='inner', on=['oyo_id'])
        smart_hotel_params_df = smart_hotel_params_df.sort_values(axis=0, by=['date', 'oyo_id', 'room_type_id'],
                                                                  ascending=True)
        smart_hotel_params_df = DFUtil.apply_func_for_df(smart_hotel_params_df, 'pms_price',
                                                         ['price', 'difference_type', 'price_delta',
                                                          'price_multiplier'],
                                                         lambda values: PriceUtil.calc_room_type_difference(*values))
        smart_hotel_params_df.price = round(smart_hotel_params_df.price, 0)
        smart_hotel_params_df.base = round(smart_hotel_params_df.base, 0)

        hotel_info_df = MiscUtil.wrap_read_adb_df(self.get_hotel_info_df, set_oyo_id)

        smart_hotel_params_df = pd.merge(smart_hotel_params_df, hotel_info_df, how='left', on=['oyo_id'])

        smart_hotel_params_df = smart_hotel_params_df.drop_duplicates(['date', 'oyo_id', 'room_type_id'], keep="last")
        return smart_hotel_params_df

    def get_hotel_room_type_price_df(self, smart_hotel_params_df, set_oyo_id):
        smart_hotel_params_df = smart_hotel_params_df.sort_values(axis=0, by=['date', 'oyo_id'], ascending=True)
        # merge room_type_difference_df
        room_type_diff_df = self.get_room_type_diff_df(set_oyo_id, True)
        room_type_diff_temp_df = room_type_diff_df[
            ['oyo_id', 'room_type_name', 'room_type_id', 'price_delta', 'price_multiplier', 'difference_type']]
        smart_hotel_params_df = pd.merge(room_type_diff_temp_df, smart_hotel_params_df, how='inner', on=['oyo_id'])
        smart_hotel_params_df = smart_hotel_params_df.sort_values(axis=0, by=['date', 'oyo_id', 'room_type_id'],
                                                                  ascending=True)
        smart_hotel_params_df = DFUtil.apply_func_for_df(smart_hotel_params_df, 'pms_price',
                                                         ['price', 'difference_type', 'price_delta',
                                                          'price_multiplier'],
                                                         lambda values: PriceUtil.calc_room_type_difference(*values))
        smart_hotel_params_df = self.set_global_min_price(smart_hotel_params_df)
        smart_hotel_params_df['hourly_price'] = DEFAULT_NULL_VALUE
        smart_hotel_params_df.price = round(smart_hotel_params_df.price, 0)
        smart_hotel_params_df.base = round(smart_hotel_params_df.base, 0)
        smart_hotel_params_df.pms_price = round(smart_hotel_params_df.pms_price, 0)
        # merge floor_price_df
        floor_price_df = self.get_floor_price_df(set_oyo_id)
        smart_hotel_params_df = pd.merge(smart_hotel_params_df, floor_price_df, on=['oyo_id', 'room_type_id'],
                                         how='left')
        smart_hotel_params_df = DFUtil.apply_func_for_df(smart_hotel_params_df, 'pms_price',
                                                         ['pms_price', 'floor_price_type', 'floor_price'],
                                                         lambda values: PriceUtil.floor_price_check(*values))
        # merge ceiling_price_df
        ceiling_price_df = self.get_ceiling_price_df(set_oyo_id)
        smart_hotel_params_df = pd.merge(smart_hotel_params_df, ceiling_price_df, on=['oyo_id', 'room_type_id'],
                                         how='left')
        smart_hotel_params_df = DFUtil.apply_func_for_df(smart_hotel_params_df, 'pms_price',
                                                         ['pms_price', 'ceiling_price_type', 'ceiling_price'],
                                                         lambda values: PriceUtil.ceiling_price_check(*values))
        # merge override_floor_price_df
        override_floor_price_df = self.get_override_floor_price_df(set_oyo_id)
        if not override_floor_price_df.empty:
            smart_hotel_params_df = pd.merge(smart_hotel_params_df, override_floor_price_df, how='left',
                                             on=['oyo_id', 'room_type_id', 'date'])

            smart_hotel_params_df = DFUtil.apply_func_for_df(smart_hotel_params_df, 'pms_price',
                                                             ['pms_price', 'floor_override_price'],
                                                             lambda values: PriceUtil.override_floor_price_check(
                                                                 *values))
        else:
            smart_hotel_params_df['floor_override_price'] = DEFAULT_NULL_VALUE
        # merge override_ceiling_price_df
        override_ceiling_price_df = self.get_override_ceiling_price_df(set_oyo_id)
        if not override_ceiling_price_df.empty:
            smart_hotel_params_df = pd.merge(smart_hotel_params_df, override_ceiling_price_df, how='left',
                                             on=['oyo_id', 'room_type_id', 'date'])

            smart_hotel_params_df = DFUtil.apply_func_for_df(smart_hotel_params_df, 'pms_price',
                                                             ['pms_price', 'ceiling_override_price'],
                                                             lambda values: PriceUtil.override_ceiling_price_check(
                                                                 *values))
        else:
            smart_hotel_params_df['ceiling_override_price'] = DEFAULT_NULL_VALUE

        # merge hotel_info_df
        hotel_info_df = MiscUtil.wrap_read_adb_df(self.get_hotel_info_df, set_oyo_id)

        smart_hotel_params_df = pd.merge(smart_hotel_params_df, hotel_info_df, how='left', on=['oyo_id'])

        smart_hotel_params_df['sale_price'] = DEFAULT_NULL_VALUE

        smart_hotel_params_df['strategy_type'] = DEFAULT_NULL_VALUE

        smart_hotel_params_df = smart_hotel_params_df.drop_duplicates(['date', 'oyo_id', 'room_type_id'], keep="last")
        return smart_hotel_params_df

    def process_special_global_floor_price(self, smart_hotel_params_df, price_column):

        if smart_hotel_params_df.empty:
            return smart_hotel_params_df

        smart_hotel_params_df.drop(columns=['use_special_global_floor'], inplace=True)

        def get_weekday_weekend(date_str):
            weekday = datetime.strptime(date_str, '%Y-%m-%d').weekday()
            return 'WKD' if weekday in [4, 5] else 'WKY'

        smart_hotel_params_df['weekday_weekend'] = smart_hotel_params_df.date.map(lambda x: get_weekday_weekend(x))

        smart_hotel_params_df = smart_hotel_params_df.merge(HARD_CODE_EX_GRATIA_GLOBAL_FLOOR_DF, how='left',
                                                            on=['oyo_id', 'weekday_weekend'])

        def ensure_floor(global_floor, pms_price):
            if MiscUtil.is_empty_value(global_floor):
                return pms_price
            return max(global_floor, pms_price)

        smart_hotel_params_df = DFUtil.apply_func_for_df(smart_hotel_params_df, price_column, ['global_floor_price', price_column],
                                                         lambda values: ensure_floor(*values))

        smart_hotel_params_df.drop(columns=['weekday_weekend', 'global_floor_price'], inplace=True)

        return smart_hotel_params_df

    def process_normal_global_floor_price(self, smart_hotel_params_df, price_column):

        if smart_hotel_params_df.empty:
            return smart_hotel_params_df

        smart_hotel_params_df.drop(columns=['use_special_global_floor'], inplace=True)

        def get_global_min_price():
            return 42

        lowest_room_type_prices_df = smart_hotel_params_df.sort_values(['oyo_id', 'date', 'price_delta']).groupby(
            ['oyo_id', 'date'], as_index=False).head(1)

        lowest_room_type_prices_df['floor_raise_delta'] = get_global_min_price() - lowest_room_type_prices_df.pms_price

        lowest_room_type_prices_df = lowest_room_type_prices_df[['oyo_id', 'date', 'floor_raise_delta']]

        smart_hotel_params_df = smart_hotel_params_df.merge(lowest_room_type_prices_df, how='left', on=['oyo_id', 'date'])

        def raise_by_floor_raise_delta(pms_price, floor_raise_delta):
            if floor_raise_delta <= 0:
                return pms_price
            return pms_price + floor_raise_delta

        DFUtil.apply_func_for_df(smart_hotel_params_df, price_column, [price_column, 'floor_raise_delta'],
                                 lambda values: raise_by_floor_raise_delta(*values))

        smart_hotel_params_df.drop(columns=['floor_raise_delta'], inplace=True)

        return smart_hotel_params_df

    def set_global_min_price(self, smart_hotel_params_df, price_column='pms_price'):

        def use_special_global_floor(oyo_id, room_type_id):
            return room_type_id == ROOM_TYPE_ID_PREFERENTIAL and oyo_id in HARD_CODE_EX_GRATIA_GLOBAL_HOTELS

        all_smart_hotel_params_df = DFUtil.apply_func_for_df(smart_hotel_params_df, 'use_special_global_floor',
                                                         ['oyo_id', 'room_type_id'],
                                                         lambda values: use_special_global_floor(*values))

        normal_smart_hotel_params_df = all_smart_hotel_params_df[~all_smart_hotel_params_df.use_special_global_floor]

        special_smart_hotel_params_df = all_smart_hotel_params_df[all_smart_hotel_params_df.use_special_global_floor]

        normal_smart_hotel_params_df = self.process_normal_global_floor_price(normal_smart_hotel_params_df, price_column)

        special_smart_hotel_params_df = self.process_special_global_floor_price(special_smart_hotel_params_df, price_column)

        smart_hotel_params_df = pd.concat([normal_smart_hotel_params_df, special_smart_hotel_params_df], ignore_index=True)

        return smart_hotel_params_df

    def get_hotel_info_df(self, set_oyo_id):
        oyo_id_tuple_list_str = MiscUtil.convert_to_oracle_query_oyo_id_list_str('oyo_id', list(set_oyo_id), 2000)
        hotel_info_query = '''
            select id as hotel_id, oyo_id, unique_code, name as hotel_name
            from product_hotel
            where {0}
        '''.format(oyo_id_tuple_list_str)
        hotel_info_df = self.adb_query_mgr.read_sql(hotel_info_query)
        return hotel_info_df

    def get_product_hotel_df(self, set_oyo_id):
        oyo_id_tuple_list_str = MiscUtil.convert_to_oracle_query_oyo_id_list_str('oyo_id', list(set_oyo_id), 2000)
        sql = '''
                select id as hotel_id, oyo_id from product_hotel
                    where {0}  and is_deleted = 0 
                '''.format(oyo_id_tuple_list_str)
        pricing_hotel_df = self.adb_query_mgr.read_sql(sql)
        return pricing_hotel_df

    def override_empty_price_by_recent_crs_price(self, trade_price_df, start_date, end_date):
        set_oyo_id = set(trade_price_df.oyo_id)
        df_crs_recent_price = MiscUtil.wrap_read_adb_df(self.get_df_for_crs_recent_price, set_oyo_id, start_date,
                                                        end_date)
        trade_price_df = pd.merge(trade_price_df, df_crs_recent_price, how='left', on=['oyo_id', 'room_type_id'])

        def override_price(price, crs_recent_price):
            try:
                if MiscUtil.is_empty_value(price):
                    return -1 if MiscUtil.is_empty_value(crs_recent_price) else ceil(crs_recent_price)
                return ceil(price)
            except Exception:
                pass

        # trade_price_df = trade_price_df[trade_price_df.price.map(lambda x: MiscUtil.is_not_empty_value(x))]
        trade_price_df = DFUtil.apply_func_for_df(trade_price_df, ["price"], ["price", "crs_recent_price"],
                                                  lambda x: override_price(*x))
        trade_price_df = trade_price_df[["oyo_id", "room_type_id", "price"]]
        return trade_price_df

    def get_last7d_trade_price_df(self, set_oyo_id, start_date, end_date):
        LogUtil.get_cur_logger().info("start get last7d trade prices, oyo_id_size: {}, ...".format(len(set_oyo_id)))
        trade_price_df = MiscUtil.wrap_read_adb_df(self.get_trade_price_df, set_oyo_id, start_date, end_date)
        trade_price_df = trade_price_df[trade_price_df.source != 11]
        room_type_df = self.get_room_type_df(set_oyo_id)
        all_trade_price_df = pd.merge(trade_price_df, room_type_df, on=["oyo_id", "room_type_id"], how="outer")
        all_trade_price_df.drop(['hotel_id'], axis=1, inplace=True)
        LogUtil.get_cur_logger().info("end get last7d trade prices")
        return all_trade_price_df

    def get_trade_price_df(self, set_oyo_id, start_date, end_date):
        oyo_id_tuple_list_str = MiscUtil.convert_to_oracle_query_oyo_id_list_str('oyo_id', list(set_oyo_id), 2000)
        sql = '''
                select cast(hotel_id as char) as hotel_id, oyo_id, source, room_type_id, biz_date, price
                    from (select hotel_id, source, room_type_id, biz_date, price
                          from (select id                                            as order_id,
                                       hotel_id,
                                       source, 
                                       date_format(check_in, '%%Y-%%m-%%d')                  as checkin,
                                       date_format(check_out, '%%Y-%%m-%%d')                 as chekout,
                                       check_in_time,
                                       case
                                         when check_out_time is null or date_format(check_out_time, '%%Y-%%m-%%d') < '2015-11-10'
                                           then check_out
                                         else check_out_time end  as real_checkout_time,
                                       date_format(case
                                                     when check_out_time is null or
                                                          date_format(check_out_time, '%%Y-%%m-%%d') < '2015-11-10'
                                                       then check_out
                                                     else check_out_time end, '%%Y-%%m-%%d') as real_checkout_date,
                                       check_out_time,
                                       check_out,
                                       booking_id
                                from trade_room_reservation
                where status in (0, 1, 2, 4)
                  and type <> 2 -- exclude hourly rooms
                  and is_deleted = 0
                  and (
                    date_format(check_in, '%%Y-%%m-%%d') between '{1}' and '{2}'
                    or
                    (date_format(check_in, '%%Y-%%m-%%d') < '{1}' and date_format(check_out, '%%Y-%%m-%%d') > '{1}')
                  )
                  and hotel_id in (select id as hotel_id
                                   from product_hotel where {0})) raw_room
                 left join (select date_format(date, '%%Y-%%m-%%d') as date from oyoprod_calendar) calendar
                           on raw_room.checkin <= calendar.date and calendar.date < raw_room.real_checkout_date
                 left join (select order_id,
                                   room_type as room_type_id
                            from trade_room_reservation_room_detail) room_detail
                           on raw_room.order_id = room_detail.order_id
                 left join (select order_id,
                                   date_format(date, '%%Y-%%m-%%d') as biz_date,
                                   price
                            from trade_room_reservation_room_price) room_price on raw_room.order_id = room_price.order_id and room_price.biz_date = date
          where biz_date between '{1}' and '{2}'
          order by hotel_id, room_type_id, price) room_type_price
        left join (select id, oyo_id from product_hotel) hotel on room_type_price.hotel_id = hotel.id
        '''.format(oyo_id_tuple_list_str, start_date, end_date)
        hotel_info_df = self.adb_query_mgr.read_sql(sql, 1200)
        return hotel_info_df

    def get_pricing_dict(self, type_code, code):
        sql = """
                select id, name, chinese_name from pricing_dict  
                where type_code='{0}' 
                and code= '{1}'
                """.format(type_code, code)
        return self.mysql_query_mgr.read_sql(sql)

    def get_room_type_df(self, set_oyo_id):
        all_smart_hotels_str = MiscUtil.convert_set_to_tuple_list_str(set_oyo_id)
        sql = """
                select oyo_id, room_type_id from hotel_room_type
                where oyo_id in {0} 
                """.format(all_smart_hotels_str)
        return self.mysql_query_mgr.read_sql(sql)

    def get_room_type_name_df(self):
        room_type_name_query = """
                    select id as room_type_id, name as room_type_name
                    from room_type
                """
        return self.mysql_query_mgr.read_sql(room_type_name_query)

    def get_city_cnname_df(self, set_oyo_id):
        oyo_id_tuple_list_str = MiscUtil.convert_to_oracle_query_oyo_id_list_str('oyo_id', set_oyo_id, 2000)
        city_cnname_query = """
                            select oyo_id, city_cnname
                            from (select id as hotel_id, oyo_id, name as hotel_name, city_id
                                  from product_hotel
                                  where {0}) hotel
                                   left join (select id as city_id, cname as city_cnname from product_city) city
                                             on hotel.city_id = city.city_id
                        """.format(oyo_id_tuple_list_str)
        return self.adb_query_mgr.read_sql(city_cnname_query)

    def get_available_room_type_df(self, prices_for_room_type_df, set_oyo_id):
        room_type_df = self.get_room_type_df(set_oyo_id)
        prices_for_room_type_df = pd.merge(prices_for_room_type_df, room_type_df, on=["oyo_id", "room_type_id"])
        return prices_for_room_type_df

    def get_base_change_reason_id(self, code="6"):
        df_strategy_type = self.get_pricing_dict('basePrice_modify', code)
        return list(df_strategy_type.id)[0]

    def get_hotel_hour_room(self, set_oyo_id, type_code="hotel_tag"):
        LogUtil.get_cur_logger().info("start get_hotel_hour_room...")
        all_smart_hotels_str = MiscUtil.convert_set_to_tuple_list_str(set_oyo_id)
        sql = """
                select h.id as hotel_id,
                       t.oyo_id as oyo_id,
                       d.code,
                       d.name as room_type_id,
                       r.name as room_type_name,
                       d.chinese_name,
                       d.description as hour_room_duration
                  from hotel_tag t 
                left join pricing_dict d on t.tag_id= d.id 
                left join hotel h on t.oyo_id= h.oyo_id,
                       room_type r
                 where d.type_code= '{0}'
                and r.id= d.name
                and t.oyo_id in {1}
                """.format(type_code, all_smart_hotels_str)
        hotel_tag_df = self.mysql_query_mgr.read_sql(sql)
        hotel_tag_df = hotel_tag_df[["oyo_id", "room_type_id", "hour_room_duration"]]
        hotel_tag_df["room_type_id"] = hotel_tag_df.room_type_id.apply(lambda x: int(x))
        LogUtil.get_cur_logger().info("end get_hotel_hour_room, size: {}".format(len(hotel_tag_df)))
        return hotel_tag_df

    def get_strategy_type_liquidation_id(self, batch_name):
        sql = """
                select id, name, chinese_name from pricing_dict  
                where type_code='strategy_type_liquidation'  
                and name= '{}'
                """.format(batch_name)
        df_strategy_type = self.mysql_query_mgr.read_sql(sql)
        return list(df_strategy_type.id)[0] if len(list(df_strategy_type.id)) > 0 else 0

    def get_white_or_black_list_set(self, start_date, batch_name, _type=1):
        strategy_type_id = self.get_strategy_type_liquidation_id(batch_name)
        sql = """
                    select
                        oyo_id
                    from
                        hotel_special_sale_white_list 
                    where
                        deleted = 0
                        and  begin_date <= date('{0}')
                        and  end_date >= date('{0}')
                        and strategy_type_liquidation = {1}
                        and  type = {2} 
                    """.format(start_date, strategy_type_id, _type)
        df_white_list = self.mysql_query_mgr.read_sql(sql)
        return set(df_white_list.oyo_id) if df_white_list is not None else set()

    def get_new_pricing_hotels_set(self, start_date):
        pricing_start_hotels_query = """
            select distinct oyo_id
            from pricing_hotel
            where status = 1
              and pricing_start_date > '{0}' - interval 7 day
              and deleted = 0
            order by oyo_id
        """.format(start_date)
        pricing_start_hotels_df = self.mysql_query_mgr.read_sql(pricing_start_hotels_query)
        return set(pricing_start_hotels_df.oyo_id)

    def get_df_for_special_sale(self, start_time_stamp, strategy_type_lst):
        start_date = DateUtil.stamp_to_date_format0(start_time_stamp)
        hotel_special_sale_query = """
            SELECT id, oyo_id, room_type_id, pricing_date as date, sale_price, strategy_type
            from hotel_special_sale
            where id in
                  ( select id
                    from (SELECT id, oyo_id, room_type_id, pricing_date as date, sale_price, strategy_type, max(update_time)
                          from hotel_special_sale h
                          where h.enabled = 1
                            and h.deleted = 0
                            and date_format(pricing_date, '%%Y-%%m-%%d') = '{0}'
                            and strategy_type in {1}
                          group by oyo_id, room_type_id, pricing_date, sale_price, strategy_type) t1
                  )
        """.format(start_date, MiscUtil.convert_list_to_tuple_list_str(strategy_type_lst))
        df_hotel_special_sale = self.mysql_query_mgr.read_sql(hotel_special_sale_query)
        df_hotel_special_sale['date'] = df_hotel_special_sale.date.map(lambda x: x.strftime('%Y-%m-%d'))
        return DFUtil.check_duplicate(df_hotel_special_sale, "df_hotel_special_sale", ['oyo_id', 'room_type_id'])

    def get_df_for_override_floor_price_hotels(self, hotel_oyo_id_tuple, start_date):
        override_price_hotel_query = """
            select oyo_id
            from hotel_over_ride_price
            where over_ride_type = 2
              and deleted = 0
              and pricing_date = '{1}'
              and oyo_id in {0}
        """.format(hotel_oyo_id_tuple, start_date)
        override_price_hotel_df = self.mysql_query_mgr.read_sql(override_price_hotel_query)
        return override_price_hotel_df

    def get_df_for_urn_with_pre_7d(self, oyo_id_set, start_date):
        adb_query_str = MiscUtil.convert_to_oracle_query_oyo_id_list_str('oyo_id', list(oyo_id_set), 2000)
        urn_for_rolling_query = """
                select oyo_id,
                       calc_date         as "date",
                       sum(case
                             when calc_date >= checkin
                               and calc_date < checkout
                               then 1
                             else 0 end) as urn
                from (
                       select oyo_id, hotel_id, calc_date
                       from (select id as hotel_id, oyo_id
                             from product_hotel
                             where {0}) hotel
                              cross join (select date_format('{1}' + interval indices.idx day, '%%Y-%%m-%%d') as calc_date
                                          from (select -1 as idx
                                                union
                                                select -2
                                                union
                                                select -3
                                                union
                                                select -4
                                                union
                                                select -5
                                                union
                                                select -6
                                                union
                                                select -7
                                               ) as indices) dates) hotel_with_dates
                       left join (select hotel_id, status, checkin, checkout
                                  from (select hotel_id,
                                               status,
                                               date_format(check_in, '%%Y-%%m-%%d')                  as checkin,
                                               date_format(check_out, '%%Y-%%m-%%d')                 as checkout,
                                               booking_id
                                        from trade_room_reservation
                                        where status in (0, 1, 2, 4)
                                          and is_deleted = 0
                                          and type <> 2
                                          and hotel_id in (select id as hotel_id
                                                           from product_hotel
                                                           where {0})
                                          and (
                                            (date_format(check_in, '%%Y-%%m-%%d') < '{1}' + interval -7 day
                                              and date_format(check_out, '%%Y-%%m-%%d') > '{1}' + interval -7 day)
                                            or
                                            (date_format(check_in, '%%Y-%%m-%%d') >= '{1}' + interval -7 day
                                              and date_format(check_in, '%%Y-%%m-%%d') < '{1}'))) r
                                         inner join (select id, is_deleted, type from trade_booking) b on r.booking_id = b.id
                                  where b.is_deleted = 0
                                    and b.type <> 2) order_rooms on hotel_with_dates.hotel_id = order_rooms.hotel_id
                group by oyo_id, hotel_with_dates.hotel_id, calc_date
                order by 1, 2
            """.format(adb_query_str, str(start_date))
        urn_for_rolling_df = self.adb_query_mgr.read_sql(urn_for_rolling_query, 60)
        return urn_for_rolling_df

    def get_df_for_hotel_daily_prepare(self, table_name, set_oyo_id):
        oyo_id_in = MiscUtil.convert_to_oracle_query_oyo_id_list_str('a.oyo_id', list(set_oyo_id))
        query = """
            select a.oyo_id, a.calc_date, a.base, a.two_week_arr as arr, a.created_at
                from {0} a
            join (
              select oyo_id, max(created_at) as created_at
              from {0}  group by oyo_id, calc_date
            ) b
            on a.oyo_id = b.oyo_id and a.created_at = b.created_at
            where {1}
            order by a.oyo_id, a.calc_date 
        """.format(table_name, oyo_id_in)
        hotel_daily_df = self.oracle_query_mgr.read_sql(query)
        hotel_daily_df = hotel_daily_df.drop_duplicates(['oyo_id'], keep="last")
        return hotel_daily_df

    def get_df_for_share_inventory_value(self, timestamp, offset):
        date_str = DateUtil.stamp_to_date_format(timestamp, '%Y%m%d', offset)
        query_sql = """
        select b.oyo_id,
               b.HOTEL_ID,
               a.CTRL_PRICE_DATE,
               a.OCC,
               '20-26'                                          VC_pattern,
               b.srn_room_20                                    Low_room_srn,
               b.OCC_room_20                                    Low_room_OCC,
               b.srn_room_26                                    High_room_srn,
               b.OCC_room_26                                    High_room_OCC,
               round(0.5 * (1 - b.OCC_room_26) * b.srn_room_26) VC
        from (select sum(TOT_URN) / sum(TOT_SRN) OCC, crs_id, CTRL_PRICE_DATE
              FROM oyo_dm.rpt_hotel_srn_urn a
                       JOIN (
                  select OYO_ID, FIRST_PRICING_START_DATE CTRL_PRICE_DATE, CTRL_STATUS, SHARE_START_DATE, rn
                  from (
                           select oyo_id,
                                  FIRST_PRICING_START_DATE,
                                  CTRL_STATUS,
                                  date_s,
                                  SHARE_START_DATE,
                                  row_number() over (partition by oyo_id order by date_s desc) rn
                           from oyo_dw.dim_pricing_hotel_status
                       ) t
                  where rn = 1
                    and CTRL_STATUS = 1
              ) c on a.CRS_ID = c.OYO_ID
              WHERE cast(a.date_s as string) >= '{date_str}'
                and a.HOTEL_NAME like 'OYO%'
                and TOT_SRN > 0
                and translate(c.CTRL_PRICE_DATE, '-', '') <= '{date_str}'
              group by crs_id, CTRL_PRICE_DATE
             ) a
                 join (select a.oyo_id,
                              a.HOTEL_ID,
                              avg(case when a.room_type = 20 then srn else NULL end)              srn_room_20,
                              sum(case when a.room_type = 20 then srn else 0 end),
                              sum(case when a.room_type = 26 then srn else 0 end),
                              sum(case when a.room_type = 29 then srn else 0 end),
                              case
                                  when sum(case when a.room_type = 20 then srn else 0 end) > 0 then
                                          sum(case when a.room_type = 20 then urn else 0 end) /
                                          sum(case when a.room_type = 20 then srn else 0 end) end OCC_room_20,
                              avg(case when a.room_type = 26 then srn else NULL end)              srn_room_26,
                              case
                                  when sum(case when a.room_type = 26 then srn else 0 end) > 0 then
                                          sum(case when a.room_type = 26 then urn else 0 end) /
                                          sum(case when a.room_type = 26 then srn else 0 end) end OCC_room_26,
                              avg(case when a.room_type = 29 then srn else NULL end)              srn_room_29,
                              case
                                  when sum(case when a.room_type = 29 then srn else 0 end) > 0 then
                                          sum(case when a.room_type = 29 then urn else 0 end) /
                                          sum(case when a.room_type = 29 then srn else 0 end) end OCC_room_29,
                              avg(case when a.room_type = 30 then srn else NULL end)              srn_room_30,
                              case
                                  when sum(case when a.room_type = 30 then srn else 0 end) > 0 then
                                          sum(case when a.room_type = 30 then urn else 0 end) /
                                          sum(case when a.room_type = 30 then srn else 0 end) end OCC_room_30
        
                       from (select oyo_id, a.date_s, HOTEL_ID, sum(urn) urn, room_type
                             from (select oyo_id, hotel_id, cast(date_s as string) as date_s, room_type, status, urn
                                   from oyo_dw.fact_booking_room) a
                             where status in (1, 2)
                               and a.date_s >= '{date_str}'
                             group by oyo_id, a.date_s, HOTEL_ID, room_type) a
                                join oyo_dw.dws_room_type_srn b
                                     on a.HOTEL_ID = b.HOTEL_ID and a.room_type = b.room_type_id and a.date_s = b.date_s
                       group by a.oyo_id, a.HOTEL_ID) b on a.crs_id = b.oyo_id
        where round(0.5 * (1 - b.OCC_room_26) * b.srn_room_26) >= 1
          and b.srn_room_20 > 0
        
        union all
        
        select b.oyo_id,
               b.HOTEL_ID,
               a.CTRL_PRICE_DATE,
               a.OCC,
               '29-30'                                          VC_pattern,
               b.srn_room_29                                    Low_room_srn,
               b.OCC_room_29                                    Low_room_OCC,
               b.srn_room_30                                    High_room_srn,
               b.OCC_room_30                                    High_room_OCC,
               round(0.5 * (1 - b.OCC_room_30) * b.srn_room_30) VC
        from (select sum(TOT_URN) / sum(TOT_SRN) OCC, crs_id, CTRL_PRICE_DATE
              FROM oyo_dm.rpt_hotel_srn_urn A
                       JOIN (
                  select OYO_ID, FIRST_PRICING_START_DATE CTRL_PRICE_DATE, CTRL_STATUS, SHARE_START_DATE, rn
                  from (
                           select oyo_id,
                                  FIRST_PRICING_START_DATE,
                                  CTRL_STATUS,
                                  date_s,
                                  SHARE_START_DATE,
                                  row_number() over (partition by oyo_id order by date_s desc) rn
                           from oyo_dw.dim_pricing_hotel_status
                       ) t
                  where rn = 1
                    and CTRL_STATUS = 1
              ) c on A.CRS_ID = c.OYO_ID
              WHERE cast(a.date_s as string) >= '{date_str}'
                and a.HOTEL_NAME like 'OYO%'
                and TOT_SRN > 0
                AND translate(c.CTRL_PRICE_DATE, '-', '') <= '{date_str}'
              group by crs_id, CTRL_PRICE_DATE
             ) a
                 join (select a.oyo_id,
                              a.HOTEL_ID,
                              avg(case when a.room_type = 20 then srn else NULL end)              srn_room_20,
                              sum(case when a.room_type = 20 then srn else 0 end),
                              sum(case when a.room_type = 26 then srn else 0 end),
                              sum(case when a.room_type = 29 then srn else 0 end),
                              case
                                  when sum(case when a.room_type = 20 then srn else 0 end) > 0 then
                                          sum(case when a.room_type = 20 then urn else 0 end) /
                                          sum(case when a.room_type = 20 then srn else 0 end) end OCC_room_20,
                              avg(case when a.room_type = 26 then srn else NULL end)              srn_room_26,
                              case
                                  when sum(case when a.room_type = 26 then srn else 0 end) > 0 then
                                          sum(case when a.room_type = 26 then urn else 0 end) /
                                          sum(case when a.room_type = 26 then srn else 0 end) end OCC_room_26,
                              avg(case when a.room_type = 29 then srn else NULL end)              srn_room_29,
                              case
                                  when sum(case when a.room_type = 29 then srn else 0 end) > 0 then
                                          sum(case when a.room_type = 29 then urn else 0 end) /
                                          sum(case when a.room_type = 29 then srn else 0 end) end OCC_room_29,
                              avg(case when a.room_type = 30 then srn else NULL end)              srn_room_30,
                              case
                                  when sum(case when a.room_type = 30 then srn else 0 end) > 0 then
                                          sum(case when a.room_type = 30 then urn else 0 end) /
                                          sum(case when a.room_type = 30 then srn else 0 end) end OCC_room_30
        
                       from (select oyo_id, a.date_s, hotel_id, sum(urn) urn, room_type
                             from (select oyo_id, hotel_id, cast(date_s as string) as date_s, room_type, status, urn
                                   from oyo_dw.fact_booking_room) a
                             where status in (1, 2)
                               and cast(a.date_s as string) >= '{date_str}'
                             group by oyo_id, a.date_s, hotel_id, room_type) a
                                join oyo_dw.dws_room_type_srn b
                                     on a.HOTEL_ID = b.HOTEL_ID and a.room_type = b.room_type_id and a.date_s = b.date_s
        
                       group by a.oyo_id, a.HOTEL_ID) b on a.crs_id = b.oyo_id
        where round(0.5 * (1 - b.OCC_room_30) * b.srn_room_30) >= 1
          and b.srn_room_29 > 0
        """.format(date_str=date_str)
        return self.hive_query_mgr.read_sql(query_sql, 1200)

    def get_df_for_occ_target(self, timestamp, offset1=-9, offset2=-28):
        calc_day = get_max_calc_day_from_etl_log('rpt_hotel_srn_urn', self.mysql_query_mgr)
        urn_max_created_at = get_max_create_time(calc_day, 'oyo_pricing_offline.prod_rpt_hotel_srn_urn', self.mysql_query_mgr)
        common_end = DateUtil.stamp_to_date_format(timestamp, '%Y%m%d', -1)
        wky_start = DateUtil.stamp_to_date_format(timestamp, '%Y%m%d', offset1)
        wkd_start = DateUtil.stamp_to_date_format(timestamp, '%Y%m%d', offset2)
        query_sql = """
        select oyo_id,
               SUM(TOT_URN) / SUM(TOT_SRN) occ_mean,
               stddev(TOT_URN / TOT_SRN)   occ_std,
               'WKY'                       week
        from (
                 select h.oyo_id, case when tot_urn < 0 then 0 else tot_urn end as tot_urn, case when tot_srn <= 0 then 1 else tot_srn end as tot_srn
                 from (select oyo_id, (tot_urn - case when tot_long_private_urn is null then 0 else tot_long_private_urn end) as tot_urn, (tot_srn - case when tot_long_private_urn is null then 0 else tot_long_private_urn end) as tot_srn
                       from oyo_pricing_offline.prod_rpt_hotel_srn_urn
                       where calc_date = '{calc_day}'
                         and created_at = '{created_at}'
                         and date_s <= '{common_end}'
                         and date_s >= '{wky_start}'
                         and dayofweek(str_to_date(date_s, '%%Y%%m%%d')) not in (6, 7)
                         and tot_urn > 0
                         and tot_srn > 0) u
                          join oyo_pricing.hotel h
                 where u.oyo_id = h.oyo_id
             ) a
        group by oyo_id
        having count(1) >= 3
        union all
        select oyo_id,
               SUM(TOT_URN) / SUM(TOT_SRN) occ_mean,
               stddev(TOT_URN / TOT_SRN)   occ_std,
               'WKD'                       week
        from (
                 select h.oyo_id, case when tot_urn < 0 then 0 else tot_urn end as tot_urn, case when tot_srn <= 0 then 1 else tot_srn end as tot_srn
                 from (select oyo_id, (tot_urn - case when tot_long_private_urn is null then 0 else tot_long_private_urn end) as tot_urn, (tot_srn - case when tot_long_private_urn is null then 0 else tot_long_private_urn end) as tot_srn, date_s
                       from oyo_pricing_offline.prod_rpt_hotel_srn_urn
                       where calc_date = '{calc_day}'
                         and created_at = '{created_at}'
                         and date_s <= '{common_end}'
                         and date_s >= '{wkd_start}'
                         and dayofweek(str_to_date(date_s, '%%Y%%m%%d')) in (6, 7)
                         and tot_urn > 0
                         and tot_srn > 0) u
                          join oyo_pricing.hotel h
                 where u.oyo_id = h.oyo_id
             ) a
        group by oyo_id
        having count(1) >= 3
        
        union all
        
        select a.oyo_id, b.occ_mean, b.occ_std, a.week
        from (
                 select city,
                        oyo_id,
                        SUM(TOT_URN) / SUM(TOT_SRN) occ_mean,
                        stddev(TOT_URN / TOT_SRN)   occ_std,
                        'WKY'                       week
                 from (
                          select h.oyo_id, case when tot_urn < 0 then 0 else tot_urn end as tot_urn, case when tot_srn <= 0 then 1 else tot_srn end as tot_srn, city_name as city
                          from (select oyo_id, (tot_urn - case when tot_long_private_urn is null then 0 else tot_long_private_urn end) as tot_urn, (tot_srn - case when tot_long_private_urn is null then 0 else tot_long_private_urn end) as tot_srn
                                from oyo_pricing_offline.prod_rpt_hotel_srn_urn
                                where calc_date = '{calc_day}'
                                  and created_at = '{created_at}'
                                  and date_s <= '{common_end}'
                                  and date_s >= '{wky_start}'
                                  and dayofweek(str_to_date(date_s, '%%Y%%m%%d')) not in (6, 7)
                                  and tot_urn > 0
                                  and tot_srn > 0) u
                                   join oyo_pricing.hotel h
                          where u.oyo_id = h.oyo_id
                      ) a
                 group by oyo_id, city
                 having count(1) < 3
                 union all
                 select city,
                        oyo_id,
                        SUM(TOT_URN) / SUM(TOT_SRN) occ_mean,
                        stddev(TOT_URN / TOT_SRN)   occ_std,
                        'WKD'                       week
                 from (
                          select h.oyo_id, case when tot_urn < 0 then 0 else tot_urn end as tot_urn, case when tot_srn <= 0 then 1 else tot_srn end as tot_srn, city_name as city
                          from (select oyo_id, (tot_urn - case when tot_long_private_urn is null then 0 else tot_long_private_urn end) as tot_urn, (tot_srn - case when tot_long_private_urn is null then 0 else tot_long_private_urn end) as tot_srn
                                from oyo_pricing_offline.prod_rpt_hotel_srn_urn
                                where calc_date = '{calc_day}'
                                  and created_at = '{created_at}'
                                  and date_s <= '{common_end}'
                                  and date_s >= '{wkd_start}'
                                  and dayofweek(str_to_date(date_s, '%%Y%%m%%d')) in (6, 7)
                                  and tot_urn > 0
                                  and tot_srn > 0) u
                                   join oyo_pricing.hotel h
                          where u.oyo_id = h.oyo_id
                      ) a
                 group by oyo_id, city
                 having count(1) < 3
             ) a
                 join (
            select city,
                   SUM(TOT_URN) / SUM(TOT_SRN) occ_mean,
                   stddev(TOT_URN / TOT_SRN)   occ_std,
                   'WKY'                       week
            from (
                     select h.oyo_id, case when tot_urn < 0 then 0 else tot_urn end as tot_urn, case when tot_srn <= 0 then 1 else tot_srn end as tot_srn, city_name as city
                     from (select oyo_id, (tot_urn - case when tot_long_private_urn is null then 0 else tot_long_private_urn end) as tot_urn, (tot_srn - case when tot_long_private_urn is null then 0 else tot_long_private_urn end) as tot_srn
                           from oyo_pricing_offline.prod_rpt_hotel_srn_urn
                           where calc_date = '{calc_day}'
                             and created_at = '{created_at}'
                             and date_s <= '{common_end}'
                             and date_s >= '{wky_start}'
                             and dayofweek(str_to_date(date_s, '%%Y%%m%%d')) not in (6, 7)
                             and tot_urn > 0
                             and tot_srn > 0) u
                              join oyo_pricing.hotel h
                     where u.oyo_id = h.oyo_id
                 ) a
            group by city
            having count(1) >= 3
            union all
            select city,
                   SUM(TOT_URN) / SUM(TOT_SRN) occ_mean,
                   stddev(TOT_URN / TOT_SRN)   occ_std,
                   'WKD'                       week
            from (
                     select h.oyo_id, case when tot_urn < 0 then 0 else tot_urn end as tot_urn, case when tot_srn <= 0 then 1 else tot_srn end as tot_srn, city_name as city
                     from (select oyo_id, (tot_urn - case when tot_long_private_urn is null then 0 else tot_long_private_urn end) as tot_urn, (tot_srn - case when tot_long_private_urn is null then 0 else tot_long_private_urn end) as tot_srn
                           from oyo_pricing_offline.prod_rpt_hotel_srn_urn
                           where calc_date = '{calc_day}'
                             and created_at = '{created_at}'
                             and date_s <= '{common_end}'
                             and date_s >= '{wkd_start}'
                             and dayofweek(str_to_date(date_s, '%%Y%%m%%d')) in (6, 7)
                             and tot_urn > 0
                             and tot_srn > 0) u
                              join oyo_pricing.hotel h
                     where u.oyo_id = h.oyo_id
                 ) a
            group by city
        ) b on a.city = b.city and a.week = b.week
        order by oyo_id
        """.format(calc_day=calc_day, created_at=urn_max_created_at, common_end=common_end, wky_start=wky_start, wkd_start=wkd_start)
        return self.mysql_query_mgr.read_sql(query_sql, 600)

    def get_df_for_metrics_config(self):
        sql = """
            SELECT 
                id, metrics_id, metrics_name, metrics_source_id, source_type, source_url, source_table, 
                source_metrics_column, source_aux_columns, priority, tag_id, day_offset, metrics_filling, max_recur
                FROM pricing_metrics_config
            """
        return self.mysql_query_mgr.read_sql(sql, 600)

    def get_df_metrics_from_db(self, start_stamp, source_config):
        start_hour = datetime.now().hour
        source_type = source_config['source_type']
        source_table = source_config['source_table']
        source_metrics_column = source_config['source_metrics_column']
        day_offset = 0 if MiscUtil.is_empty_value(source_config['day_offset']) else source_config['day_offset']
        metrics_filling = source_config['metrics_filling']
        max_recur = 1 if MiscUtil.is_empty_value(source_config['max_recur']) else int(source_config['max_recur'])
        start_stamp = start_stamp + 3600 * 24 * day_offset
        start_date = DateUtil.stamp_to_date_format0(start_stamp)
        df_metrics = self.recur_metrics(source_metrics_column, source_table, source_type, start_stamp, metrics_filling, max_recur)

        df_metrics['metrics_value'] = df_metrics.get(source_metrics_column)
        df_metrics['data_date'] = start_date
        df_metrics['hour'] = df_metrics['hour'] if "hour" in df_metrics.index else start_hour
        return df_metrics

    def recur_metrics(self, source_metrics_column, source_table, source_type, start_stamp, metrics_filling, times=7):
        start_date = DateUtil.stamp_to_date_format0(start_stamp)
        sql = """
               select oyo_id, {0} from {1} where create_day = '{2}';
        """.format(source_metrics_column, source_table, start_date)
        if source_type == 0:
            df_metrics = self.mysql_query_mgr.read_sql(sql, 600)
        if source_type == 1:
            df_metrics = self.oracle_query_mgr.read_sql(sql, 600)
        if source_type == 2:
            df_metrics = self.adb_query_mgr.read_sql(sql, 600)
        if source_type == 3:
            df_metrics = self.hive_query_mgr.read_sql(sql, 600)
        if df_metrics is None or len(df_metrics) == 0:
            if times > 0:
                times = times - 1
                start_stamp = start_stamp - 3600 * 24 * metrics_filling
                LogUtil.get_cur_logger().warn("rec_metrics_target is empty, SQL: {0}, next_start_date: {1}, left_times: {2}"
                                              .format(sql, DateUtil.stamp_to_date_format0(start_stamp), times))
                return self.recur_metrics(source_metrics_column, source_table, source_type, start_stamp, metrics_filling, times)
            else:
                df_metrics = pd.DataFrame()
        df_metrics["last_update"] = start_date
        return df_metrics

    def get_df_tagged_hotel(self, valid_tags):
        # 正则过滤
        valid_tag_str = ",".join(tuple((re.compile(r'\d+').findall(valid_tags))))
        if MiscUtil.is_empty_value(valid_tag_str):
            LogUtil.get_cur_logger().warn("valid_tags is empty")
            return pd.DataFrame(columns=["tag_id", "oyo_id"])
        sql = """
                select tag_id, oyo_id from hotel_tag  where tag_id in ({0});
            """.format(valid_tag_str)
        df_tagged_hotel = self.mysql_query_mgr.read_sql(sql, 600)
        return df_tagged_hotel

    @staticmethod
    def date_to_str(x):
        return x.strftime('%Y-%m-%d')

    @staticmethod
    def set_min_price(df, columns, min_price=35):
        for column in columns:
            df[column] = df[column].apply(
                lambda price: PriceUtil.floor_price_check(price, FLOOR_PRICE_TYPE_NUMBER, min_price))

    @staticmethod
    def get_tuple_str_for_oyo_id(smart_hotel_oyo_id_iter):
        if len(smart_hotel_oyo_id_iter) == 0:
            LogUtil.get_cur_logger().warning('hotel list is empty！')
        return MiscUtil.convert_set_to_tuple_list_str(smart_hotel_oyo_id_iter)
