#!/usr/bin/env python
# -*- coding:utf-8 -*-

import os
import sys
import warnings
from os.path import join as join_path

cur_path = os.path.dirname(os.path.realpath(__file__))
sys.path.append(join_path(cur_path, '..'))
warnings.filterwarnings("ignore")

from common.job_base.job_base import JobBase
from common.job_common.job_source import JobSource
from common.util.utils import *
import datetime as dt
from datetime import timedelta
from common.dingtalk_sdk.dingtalk_py_cmd import DingTalkPy
from common.dingtalk_sdk.dingtalk_py_cmd import init_jvm_for_ddt_disk


class BookingPriceAlertJob(JobBase):

    def __init__(self, job_config):
        JobBase.__init__(self, job_config)

    def get_job_name(self):
        return 'BookingPriceAlertJob'

    def run(self):

        config = self.get_job_config()

        init_jvm_for_ddt_disk()

        job_begin = time.time()

        start_time = dt.datetime.fromtimestamp(job_begin)

        DateUtil.init_preset_time()

        job_source = JobSource(self.get_adb_query_manager(), self.get_mysql_query_manager(), self.get_oracle_query_manager())

        set_all_hotels = job_source.get_hotel_batch_oyo_set_from_center(1, job_begin)

        oyo_id_tuple_list_str = MiscUtil.convert_to_oracle_query_oyo_id_list_str('oyo_id',
                                                                                 list(set_all_hotels),
                                                                                 2000)
        check_start_to_hour = (start_time + timedelta(hours=-2)).strftime('%Y-%m-%d %H') + ":00:00"

        check_end_to_hour = start_time.strftime('%Y-%m-%d %H') + ":00:00"


        price_query_sql = """
            select oyo_id,
                   price_info.hotel_id,
                   booking_id,
                   booking_sn,
                   channel_id,
                   channel_name,
                   room_type_id,
                   source,
                   create_time,
                   source_create_time,
                   check_in,
                   check_out,
                   biz_date,
                   price as booking_price
            from (
                   select hotel_id,
                          booking_id,
                          booking_sn,
                          source,
                          room_type_id,
                          create_time,
                          source_create_time,
                          check_in,
                          check_out,
                          biz_date,
                          price
                   from (select booking_id,
                                booking_sn,
                                hotel_id,
                                source,
                                create_time,
                                source_create_time,
                                order_id,
                                check_in,
                                check_out
                         from (select id, booking_sn, hotel_id, source, create_time, source_create_time
                               from trade_booking
                               where hotel_id in (select id as hotel_id
                                                  from product_hotel
                                                  where {0})
                                 and (date_format(create_time, '%%Y-%%m-%%d %%H:%%i:%%s') between '{1}' and '{2}')
                                 and status in (0, 1, 2, 4)) booking
                                left join (select id                                 as order_id,
                                                  booking_id,
                                                  date_format(check_in, '%%Y-%%m-%%d')  as check_in,
                                                  date_format(check_out, '%%Y-%%m-%%d') as check_out
                                           from trade_room_reservation) room on booking.id = room.booking_id) booking_with_room
                          left join (select order_id,
                                            date_format(date, '%%Y-%%m-%%d') as biz_date,
                                            price
                                     from trade_room_reservation_room_price) room_price
                                    on booking_with_room.order_id = room_price.order_id
                          left join (select order_id, room_type as room_type_id
                                     from trade_room_reservation_room_detail) room_detail
                                    on booking_with_room.order_id = room_detail.order_id) price_info
                   join (select id as hotel_id, oyo_id
                         from product_hotel
                         where {0}) hotel on price_info.hotel_id = hotel.hotel_id
                   join (select id as channel_id, channel_name from product_channel) channel
                        on price_info.source = channel.channel_id
        """.format(oyo_id_tuple_list_str, check_start_to_hour, check_end_to_hour)

        all_orders_df = self.get_adb_query_manager().read_sql(price_query_sql, 300)

        pricing_history_query = """
            select hotel_id, room_type_id, date_format(rate_date, '%%Y-%%m-%%d') as rate_date, rate as pricing_price
            from price_list_rate
            where hotel_id in (select id as hotel_id
                               from product_hotel
                               where {0})
            and rate_date between '2019-09-12' and '2019-09-15'
            and is_deleted = 0
        """.format(oyo_id_tuple_list_str)

        pricing_history_df = self.get_adb_query_manager().read_sql(pricing_history_query, 300)

        date_range_orders_df = all_orders_df[((all_orders_df.biz_date >= '2019-09-12') &
                                         (all_orders_df.biz_date <= '2019-09-15'))]

        merged_df = pd.merge(date_range_orders_df, pricing_history_df, left_on=['hotel_id', 'room_type_id', 'biz_date'],
                             right_on=['hotel_id', 'room_type_id', 'rate_date'], how='left')

        def filter_exceptional_rows(price, rate):
            try:
                if price < 29:
                    return True
                if MiscUtil.is_empty_value(rate):
                    return True
                if price < 0.5 * rate - 5:
                    return True
                return False
            except:
                return True

        filtered_df = merged_df[merged_df[['booking_price', 'pricing_price']].apply(lambda values: filter_exceptional_rows(*values), axis=1)]

        filtered_df.drop(['rate_date'], axis=1, inplace=True)

        output_file_name = 'exceptional_low_prices_{0}.xlsx'.format(start_time.strftime('%Y_%m_%d_%H'))

        report_excel_path = join_path(cur_path, 'log', output_file_name)

        filtered_df.to_excel(report_excel_path)

        robot_token = 'b884a46d68fd57121d63dcbe73f9da73a8e707ac8b80843ff1b86c6fb4a30c1d'

        DingTalkPy().robot_send(robot_token, 'MidAutumnAlert', "5", config.get_job_preset_time(),
                              report_excel_path,
                              'alert list',
                              config.get_ddt_env())

