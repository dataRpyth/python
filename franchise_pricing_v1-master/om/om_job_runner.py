import os
import sys
from os.path import join as join_path

cur_path = os.path.dirname(os.path.realpath(__file__))
sys.path.append(join_path(cur_path, '..'))

from common.dingtalk_sdk.dingtalk_py_cmd import init_jvm_for_ddt_disk

import pandas as pd
import numpy as np
import datetime as dt

pd.options.mode.chained_assignment = None
from datetime import datetime
from datetime import timedelta
from pathos.multiprocessing import ProcessingPool
from common.priceop.price_to_ota import OtaPriceUpload as opriceu
from common.priceop.price_to_crs import PriceInsert as pricei
from common.priceop.price_log import PriceLog as pricel
from common.util.utils import *
from common.pricing_pipeline.pipeline import PricingPipeline, SEVEN_CHANNELS_MAP
from common.job_base.job_base import JobBase
from common.job_common.job_source import JobSource
from common.job_common.job_sinker import JobSinker

DEFAULT_NULL_VALUE = ''

POST_PRE_PRICING_RATIO = 1.1
OTA_TO_PMS_PRICING_RATIO = 1.25
QUHUHU_PRICE_RAISE_RATIO = 1.05
PRESET_WEEK_OF_MONTH = 2


class OmJob(JobBase):

    def __init__(self, job_config):
        JobBase.__init__(self, job_config)

    def get_job_name(self):
        return 'OM_Daily'

    @staticmethod
    def create_output_dir_if_needed(config):
        output_dir = join_path(cur_path, config.get_result_folder())
        if not os.path.exists(output_dir):
            os.makedirs(output_dir, 0o777, True)

    def run(self):

        DateUtil.init_preset_time()

        config = self.get_job_config()

        send_mail = config.get_mail_send()

        is_debug = EnvUtil.is_prod_env(config.get_env())

        OmJob.create_output_dir_if_needed(config)

        logger = LogUtil.get_cur_logger()

        init_jvm_for_ddt_disk()

        logger.info('*********************run begin******************************')

        logger.info("job started")

        batch_order = config.get_batch_order()

        is_preset = config.is_preset()

        job_source = JobSource(self.get_adb_query_manager(), self.get_mysql_query_manager(), self.get_oracle_query_manager())

        if not is_preset:
            batch_order_query = ' and batch_order_id in ({0})'.format(batch_order)
        else:
            batch_order_query = ''

        day_shift_start = config.get_day_shift_start()
        day_shift_end = config.get_day_shift_end()

        job_start_time_str = dt.datetime.strftime(dt.datetime.fromtimestamp(time.time()), "%Y_%m_%d_%H_%M")

        job_start_date = dt.datetime.strftime(dt.datetime.fromtimestamp(time.time()), "%Y-%m-%d")

        logger.info('batch_order_query: %s, is_preset: %s, day_shift_start: %d, day_shift_end: %s', batch_order_query,
                    is_preset, day_shift_start, day_shift_end)

        room_type_query = """
            select id as room_type_id, name as room_type_name
            from room_type
        """

        logger.info('start room type query:\n%s', room_type_query)

        room_type_df = self.get_mysql_query_manager().read_sql(room_type_query)

        room_type_df = DFUtil.check_duplicate(room_type_df, 'room_type_df', ['room_type_id'])

        logger.info('end room type query')

        # 根据批次筛选酒店
        all_om_hotels_query = """
            select distinct(oyo_id)
            from hotel_pricing_strategy
            where strategy_id in (601, 602, 610, 611, 612, 613)
              and enabled = 1
              and oyo_id in (select distinct(oyo_id)
                             from hotel_batch
                             where oyo_id in
                              (select distinct(oyo_id)
                                from hotel_business_model
                                where business_model_id = 2){0}
                              )
                    """.format(batch_order_query)

        logger.info('start all om hotels query:\n%s', all_om_hotels_query)

        all_om_hotels_df = self.get_mysql_query_manager().read_sql(all_om_hotels_query)

        logger.info('end all om hotels query')

        if all_om_hotels_df.empty:
            logger.info('hotel list empty, returning')
            return

        all_om_hotels_str = str(tuple(list(all_om_hotels_df.oyo_id)))

        logger.info('all_om_hotels_str:\n%s', all_om_hotels_str)

        # 防止有的酒店已经解约
        all_hotels_status_check_query = """
            select oyo_id
            from oyo_dw.dim_hotel
            where oyo_id in {0}
              and status = 3
        """.format(all_om_hotels_str)

        logger.info('start all hotels status check query:\n%s', all_hotels_status_check_query)

        all_hotels_df = self.get_oracle_query_manager().read_sql(all_hotels_status_check_query, 60)

        logger.info('end all hotels status check query')

        set_oyo_id = set(all_hotels_df.oyo_id)

        all_hotels_str = str(tuple(all_hotels_df.oyo_id))

        logger.info('all hotels after status check: %s', all_hotels_str)

        hotel_with_zone_name_query = """
            select oyo_id, name as hotel_name, zone_name
            from (select oyo_id, name, cluster_id
                  from oyo_dw.v_dim_hotel
                  where oyo_id in {0}) hotel
                   left join (select cluster_id, zone_name
                              from oyo_dw.v_dim_zone) zone on hotel.cluster_id = zone.cluster_id
        """.format(all_hotels_str)

        logger.info('start hotel with zone name query:\n%s', hotel_with_zone_name_query)

        hotel_with_zone_name_df = self.get_oracle_query_manager().read_sql(hotel_with_zone_name_query)

        logger.info('end hotel with zone name query')

        base_price_query = """
            select oyo_id, pricing_date, base_price
            from hotel_base_price
            where pricing_date >= curdate() + interval {0} day
              and pricing_date <= curdate() + interval {1} day
              and oyo_id in {2}
              and deleted = 0
        """.format(day_shift_start, day_shift_end, all_hotels_str)

        logger.info('start base price query:\n%s', base_price_query)

        base_price_df = self.get_mysql_query_manager().read_sql(base_price_query)

        base_price_df = DFUtil.check_duplicate(base_price_df, 'base_price_df', ['oyo_id', 'pricing_date'])

        logger.info('end base price query')

        floor_price_query = """
            select oyo_id, price_type as floor_price_type, floor_price
            from hotel_floor_price
            where oyo_id in {0}
            and room_type_id = 20
            and deleted = 0
        """.format(all_hotels_str)

        logger.info('start floor price query:\n%s', floor_price_query)

        floor_price_df = self.get_mysql_query_manager().read_sql(floor_price_query)

        floor_price_df = DFUtil.check_duplicate(floor_price_df, 'floor_price_df', ['oyo_id'])

        logger.info('end floor price query')

        # check if the hotel for the dates has special strategy
        special_pricing_strategy_query = """
            select oyo_id, strategy_date, strategy_id
            from hotel_special_pricing_strategy
            where enabled = 1
              and strategy_date >= curdate() + interval {0} day
              and strategy_date <= curdate() + interval {1} day
              and oyo_id in {2}  
        """.format(day_shift_start, day_shift_end, all_hotels_str)

        logger.info('start special pricing strategy query:\n%s', special_pricing_strategy_query)

        special_pricing_strategy_df = self.get_mysql_query_manager().read_sql(special_pricing_strategy_query)

        special_pricing_strategy_df = DFUtil.check_duplicate(special_pricing_strategy_df,
                                                             'special_pricing_strategy_df',
                                                             ['oyo_id', 'strategy_date', 'strategy_id'])

        logger.info('end special pricing strategy query')

        all_hotels_with_date_query = """
            select *
            from (select distinct(oyo_id)
                  from hotel_pricing_strategy
                  where strategy_id in (601, 602, 610, 611, 612, 613)
                    and enabled = 1) s
                   cross join (select date, month, weekdayname from calendar where date >= curdate() + interval {0} day and date <= curdate() + interval {1} day) c
            where oyo_id in {2}
        """.format(day_shift_start, day_shift_end, all_hotels_str)

        logger.info('start all hotels with date query:\n%s', all_hotels_with_date_query)

        all_hotels_with_date_df = self.get_mysql_query_manager().read_sql(all_hotels_with_date_query)

        logger.info('end all hotels with date query')

        merged_strategies = pd.merge(all_hotels_with_date_df, special_pricing_strategy_df, how='left',
                                     left_on=['oyo_id', 'date'], right_on=['oyo_id', 'strategy_date'])

        special_strategies_df = merged_strategies[merged_strategies.strategy_id.notnull()]

        special_strategies_df['new_date'] = special_strategies_df.date.map(lambda date: pd.to_datetime(str(date)))

        strategies_df = merged_strategies[merged_strategies.strategy_id.isnull()]

        start_time = datetime.datetime.fromtimestamp(time.time())

        start_date = datetime.datetime.strftime(start_time + timedelta(days=day_shift_start), '%Y-%m-%d')

        end_date = datetime.datetime.strftime(start_time + timedelta(days=day_shift_end), '%Y-%m-%d')

        oyo_id_query_str = MiscUtil.convert_to_oracle_query_oyo_id_list_str('oyo_id', list(set_oyo_id), 2000)

        brn_query = """
            select oyo_id, dt as date, brn from
                (select hotel_id, dt, count(*) as brn
                from (
                       select hotel_id,
                              date_format(check_in, '%%Y-%%m-%%d')                  as checkin,
                              case
                                when check_out_time is null or date_format(check_out_time, '%%Y-%%m-%%d') < '2015-11-10'
                                  then check_out
                                else check_out_time end                          as real_checkout_time,
                              date_format(case
                                            when check_out_time is null or
                                                 date_format(check_out_time, '%%Y-%%m-%%d') < '2015-11-10'
                                              then check_out
                                            else check_out_time end, '%%Y-%%m-%%d') as real_checkout_date
                       from trade_room_reservation
                       where status in (0, 1, 2, 4)
                         and type <> 2 -- exclude hourly rooms
                         and date_format(check_in, '%%Y-%%m-%%d') <> date_format(check_out, '%%Y-%%m-%%d')
                         and is_deleted = 0
                         and hotel_id in (select id as hotel_id
                                          from product_hotel
                                          where {0})) raw_room
                       join (select date_format(date, '%%Y-%%m-%%d') as dt
                             from oyoprod_calendar
                             where date between '{1}' and '{2}') calendar
                            on raw_room.checkin <= calendar.dt and calendar.dt < raw_room.real_checkout_date
                group by hotel_id, dt
                order by hotel_id, dt) brn
                left join (select id as hotel_id, oyo_id
                                  from product_hotel
                                  where {0}) hotel on brn.hotel_id = hotel.hotel_id
        """.format(oyo_id_query_str, start_date, end_date)

        brn_df = self.get_adb_query_manager().read_sql(brn_query, 200)

        srn_df = job_source.get_df_for_hotel_srn(set_oyo_id)

        occ_calc_df = srn_df.merge(brn_df, how='left', on='oyo_id')

        def occ_calc(brn, srn):
            if MiscUtil.is_empty_value(srn) or MiscUtil.is_empty_value(brn) or srn <= 0:
                return 0
            return brn / srn

        occ_calc_df = DFUtil.apply_func_for_df(occ_calc_df, 'occ', ['brn', 'srn'], lambda values: occ_calc(*values))

        DFUtil.print_data_frame(occ_calc_df, 'occ_calc_df', is_debug)

        occ_calc_df['scaled_occ'] = occ_calc_df.occ.map(lambda occ: round(occ * 1000))

        occ_calc_df['new_date'] = occ_calc_df.date.map(lambda date: pd.to_datetime(str(date)))

        special_strategies_with_occ_df = pd.merge(special_strategies_df, occ_calc_df, on=['oyo_id', 'new_date'],
                                                  suffixes=['', '_y'])

        special_pricing_ratio_by_occ_query = """
            select strategy_id, occ_left_threshold, occ_right_threshold, pricing_ratio
            from om_special_pricing_ratio_by_occ
            where enabled = 1
        """

        logger.info('start special pricing ratio by occ query:\n%s', special_pricing_ratio_by_occ_query)

        special_pricing_ratio_by_occ_df = self.get_mysql_query_manager().read_sql(special_pricing_ratio_by_occ_query)

        logger.info('end special pricing ratio by occ query')

        special_strategies_with_occ_pricing_delta_df = pd.merge(special_strategies_with_occ_df,
                                                                special_pricing_ratio_by_occ_df, how='left',
                                                                on=['strategy_id'], suffixes=['', '_y'])

        DFUtil.print_data_frame(special_strategies_with_occ_pricing_delta_df,
                                'special_strategies_with_occ_pricing_delta_df', is_debug)

        special_strategies_with_occ_pricing_delta_filtered_df = special_strategies_with_occ_pricing_delta_df.query(
            'scaled_occ >= occ_left_threshold and scaled_occ <= occ_right_threshold')

        DFUtil.print_data_frame(special_strategies_with_occ_pricing_delta_filtered_df,
                                'special_strategies_with_occ_pricing_delta_filtered_df', is_debug)

        # join base价，准备计算
        special_pricing_delta_with_base_price_df = pd.merge(special_strategies_with_occ_pricing_delta_filtered_df,
                                                            base_price_df, how='left',
                                                            left_on=['oyo_id', 'strategy_date'],
                                                            right_on=['oyo_id', 'pricing_date'])

        def calc_dynamic_price(base_price, pricing_ratio, preset):
            if preset:
                return base_price * 0.8
            return round(base_price * pricing_ratio, 0)

        special_pricing_delta_with_base_price_df = DFUtil.apply_func_for_df(
            special_pricing_delta_with_base_price_df, 'dynamic_price',
            ['base_price', 'pricing_ratio'],
            lambda values: calc_dynamic_price(*values, is_preset))

        DFUtil.print_data_frame(special_pricing_delta_with_base_price_df, 'special_pricing_delta_with_base_price_df',
                                is_debug)

        # join floor价，为floor价check做准备
        special_pricing_delta_with_base_price_and_floor_price = pd.merge(special_pricing_delta_with_base_price_df,
                                                                         floor_price_df, how='left', on=['oyo_id'])

        DFUtil.print_data_frame(special_pricing_delta_with_base_price_and_floor_price,
                                'special_pricing_delta_with_base_price_and_floor_price', is_debug)

        # 确保floor价不击穿
        special_pricing_delta_with_base_price_and_floor_price = DFUtil.apply_func_for_df(
            special_pricing_delta_with_base_price_and_floor_price, 'price',
            ['dynamic_price', 'floor_price_type', 'floor_price'],
            lambda values: PriceUtil.floor_price_check(*values))

        DFUtil.print_data_frame(special_pricing_delta_with_base_price_and_floor_price,
                                'special_pricing_delta_with_base_price_with_floor_price', is_debug)

        special_pricing_delta_with_base_price_and_floor_price['weekday_weekend'] = ''

        # 整理数据
        special_final_prices_df = special_pricing_delta_with_base_price_and_floor_price[
            ['date', 'oyo_id', 'month', 'weekdayname', 'weekday_weekend', 'strategy_id', 'brn', 'srn', 'occ',
             'scaled_occ', 'occ_left_threshold', 'occ_right_threshold', 'pricing_ratio', 'base_price', 'dynamic_price',
             'floor_price', 'price']]

        special_final_prices_df = special_final_prices_df.rename(columns={'weekdayname': 'weekday_name'})

        special_final_prices_df['is_special'] = 'yes'

        DFUtil.print_data_frame(special_final_prices_df, 'special_final_prices_df', is_debug)

        # 房型价格计算准备
        hotel_room_type_query = """
            select oyo_id, room_type_id
            from hotel_room_type
            where oyo_id in {0}
        """.format(all_hotels_str)

        logger.info('start hotel room type query: %s', hotel_room_type_query)

        hotel_room_type_df = self.get_mysql_query_manager().read_sql(hotel_room_type_query)

        hotel_room_type_df = DFUtil.check_duplicate(hotel_room_type_df, 'hotel_room_type_df',
                                                    ['oyo_id', 'room_type_id'])

        logger.info('end hotel room type query')

        # 展开房型
        special_final_prices_with_room_type_df = pd.merge(hotel_room_type_df, special_final_prices_df, on=['oyo_id'])

        DFUtil.print_data_frame(special_final_prices_with_room_type_df, 'special_final_prices_with_room_type_df',
                                is_debug)

        # 获取特殊节假日价差
        special_room_type_price_difference_query = """
            select oyo_id, room_type_id, price_delta
            from hotel_special_room_type_price_difference
            where oyo_id in {0}
        """.format(all_hotels_str)

        logger.info('start special room type price difference query:\n%s', special_room_type_price_difference_query)

        special_room_type_price_difference_df = self.get_mysql_query_manager().read_sql(
            special_room_type_price_difference_query)

        special_room_type_price_difference_df = DFUtil.check_duplicate(special_room_type_price_difference_df,
                                                                       'special_room_type_price_difference_df',
                                                                       ['oyo_id', 'room_type_id'])

        logger.info('end special room type price difference query')

        # join特殊节假日价差，价差只与oyo_id和room_type有关
        special_room_type_final_prices_df = pd.merge(special_final_prices_with_room_type_df,
                                                     special_room_type_price_difference_df, how='left',
                                                     on=['oyo_id', 'room_type_id'])

        def process_room_type_delta(price, room_type_price_delta):
            if pd.isna(room_type_price_delta):
                return price
            return price + room_type_price_delta

        # 计算特殊节假日各房型pms价
        special_room_type_final_prices_df = DFUtil.apply_func_for_df(
            special_room_type_final_prices_df, 'pms_price', ['price', 'price_delta'],
            lambda values: process_room_type_delta(*values))

        DFUtil.print_data_frame(special_room_type_final_prices_df, 'special_room_type_final_prices_df', is_debug)

        # 整理pms价格数据
        special_pms_prices_df = special_room_type_final_prices_df[['oyo_id', 'date', 'room_type_id', 'pms_price']]

        special_pms_prices_df['hourly_price'] = DEFAULT_NULL_VALUE

        special_pms_prices_df['is_special'] = 'yes'

        special_pms_prices_df = pd.merge(special_pms_prices_df, room_type_df, on='room_type_id')

        special_pms_prices_df = pd.merge(special_pms_prices_df, hotel_with_zone_name_df, on='oyo_id')

        special_pms_prices_to_excel = special_pms_prices_df[
            ['date', 'oyo_id', 'room_type_id', 'room_type_name', 'pms_price', 'hourly_price', 'hotel_name',
             'is_special']]

        DFUtil.print_data_frame(special_pms_prices_df, 'special_pms_prices_df', is_debug)

        # 计算正常日价格
        hotel_with_date_df = strategies_df[['oyo_id', 'date', 'month', 'weekdayname']]

        # 获取周中周末定义
        om_weekday_weekend_mapping_query = """
            select weekday_name, weekday_weekend, day_of_week
            from om_weekday_name_mapping
        """

        logger.info('start om weekday weekend mapping query:\n%s', om_weekday_weekend_mapping_query)

        weekday_weekend_mapping_df = self.get_mysql_query_manager().read_sql(om_weekday_weekend_mapping_query)

        logger.info('end om weekday weekend mapping query')

        # join 周中周末数据
        hotel_with_date_df = pd.merge(hotel_with_date_df, weekday_weekend_mapping_df,
                                      left_on=['weekdayname'], right_on=['weekday_name'])

        DFUtil.print_data_frame(hotel_with_date_df, 'hotel_with_date_df', is_debug)

        # 获取酒店客源结构数据
        hotel_guest_structure_query = """
            select oyo_id, day_of_week, guest_structure_id
            from hotel_guest_structure
            where oyo_id in {0}
        """.format(all_hotels_str)

        logger.info('start hotel guest structure query:\n%s', hotel_guest_structure_query)

        hotel_guest_structure_df = self.get_mysql_query_manager().read_sql(hotel_guest_structure_query)

        hotel_guest_structure_df = DFUtil.check_duplicate(hotel_guest_structure_df, 'hotel_guest_structure_df',
                                                          ['oyo_id', 'day_of_week', 'guest_structure_id'])

        logger.info('end hotel guest structure query')

        DFUtil.print_data_frame(hotel_guest_structure_df, 'hotel_guest_structure_df', is_debug)

        # 根据酒店周中周末获得酒店客源结构
        hotels_with_guest_structure_df = pd.merge(hotel_with_date_df, hotel_guest_structure_df,
                                                  how='left',
                                                  on=['oyo_id', 'day_of_week'])

        DFUtil.print_data_frame(hotels_with_guest_structure_df, 'hotels_with_guest_structure_df', is_debug)

        # 获取客源结构meta数据
        guest_structure_query = """
            select id, strategy_id
            from guest_structure
        """

        logger.info('start guest structure query:\n%s', guest_structure_query)

        guest_structure_df = self.get_mysql_query_manager().read_sql(guest_structure_query)

        logger.info('end guest structure query')

        # join 客源结构meta数据
        hotels_with_strategy_df = pd.merge(hotels_with_guest_structure_df, guest_structure_df,
                                           how='left', left_on=['guest_structure_id'], right_on=['id'])

        DFUtil.print_data_frame(hotels_with_strategy_df, 'hotels_with_strategy_df', is_debug)

        hotels_with_strategy_df['new_date'] = hotels_with_strategy_df.date.map(
            lambda date: pd.to_datetime(str(date)))

        hotels_with_occ = pd.merge(hotels_with_strategy_df, occ_calc_df,
                                   on=['oyo_id', 'new_date'], suffixes=['', '_y'])
        # 获取正常日价差数据
        om_pricing_ratio_by_occ_query = """
            select strategy_id, day_shift_start, day_shift_end, occ_left_threshold, occ_right_threshold, pricing_ratio
            from om_pricing_ratio_by_occ
            where enabled = 1
        """

        logger.info('start om pricing ratio by occ query:\n%s', om_pricing_ratio_by_occ_query)

        om_pricing_ratio_by_occ_df = self.get_mysql_query_manager().read_sql(om_pricing_ratio_by_occ_query)

        logger.info('end om pricing ratio by occ query')

        # join正常日价差数据
        hotels_with_pricing_delta_df = pd.merge(hotels_with_occ,
                                                om_pricing_ratio_by_occ_df, how='left',
                                                on=['strategy_id'])

        DFUtil.print_data_frame(hotels_with_pricing_delta_df, 'hotels_with_pricing_delta_df', is_debug)

        def filter_valid_row(now_date, pricing_date, day_shift_start, day_shift_end, scaled_occ, occ_left_threshold,
                             occ_right_threshold):
            if pd.isna(day_shift_start) or pd.isna(day_shift_end) or pd.isna(scaled_occ) or pd.isna(
                    occ_left_threshold) or pd.isna(occ_right_threshold):
                # TODO(yry): log waring
                return False
            start_date = now_date + timedelta(days=day_shift_start)
            start_date_str = start_date.strftime("%Y-%m-%d")
            end_date = now_date + timedelta(days=day_shift_end)
            end_date_str = end_date.strftime("%Y-%m-%d")
            pricing_datetime = pricing_date.to_pydatetime()
            pricing_date_str = pricing_datetime.strftime("%Y-%m-%d")
            return start_date_str <= pricing_date_str <= end_date_str and occ_left_threshold <= scaled_occ <= occ_right_threshold

        now_date = datetime.datetime.fromtimestamp(time.time())
        # 根据客源结构，T+x日期，实时OCC，获得对应的调价系数
        hotels_valid_ratios = hotels_with_pricing_delta_df[hotels_with_pricing_delta_df[
            ['new_date', 'day_shift_start', 'day_shift_end', 'scaled_occ', 'occ_left_threshold',
             'occ_right_threshold']].apply(lambda values: filter_valid_row(now_date, *values), axis=1)]

        DFUtil.print_data_frame(hotels_valid_ratios, 'hotels_valid_ratios', is_debug)

        # join base价，准备计算base价
        hotels_result_final_df = pd.merge(hotels_valid_ratios, base_price_df,
                                          left_on=['oyo_id', 'date'], right_on=['oyo_id', 'pricing_date'])

        # convert data type in case type mismatch
        hotels_result_final_df['month'] = pd.to_numeric(hotels_result_final_df['month'])

        DFUtil.print_data_frame(hotels_result_final_df, 'hotels_result_final_df', is_debug)

        # 计算获得正常日价格，预埋全部返回base price
        hotels_result_final_df = DFUtil.apply_func_for_df(
            hotels_result_final_df, 'dynamic_price',
            ['base_price', 'pricing_ratio'],
            lambda values: calc_dynamic_price(*values, is_preset))

        DFUtil.print_data_frame(hotels_result_final_df, 'hotels_result_final_df', is_debug)

        hotels_result_final_df = pd.merge(hotels_result_final_df, floor_price_df, on=['oyo_id'])

        # floor 价保证
        hotels_result_final_df = DFUtil.apply_func_for_df(hotels_result_final_df, 'price',
                                                          ['dynamic_price', 'floor_price_type', 'floor_price'],
                                                          lambda values: PriceUtil.floor_price_check(*values))
        final_prices_df = hotels_result_final_df[
            ['date', 'oyo_id', 'month', 'weekday_name', 'weekday_weekend', 'strategy_id', 'brn', 'srn', 'occ',
             'scaled_occ', 'occ_left_threshold', 'occ_right_threshold', 'pricing_ratio', 'base_price', 'dynamic_price',
             'floor_price', 'price']]

        final_prices_df['is_special'] = 'no'

        final_prices_to_excel = pd.concat([special_final_prices_df, final_prices_df])

        final_prices_to_excel.sort_values(['oyo_id', 'date'], inplace=True, ascending=[True, True])

        inter_data_df = final_prices_to_excel.copy()

        logger.info('start send intermediate results')

        # 发送邮件1
        send_mail.send_mail_for_intermediate_result(config, inter_data_df)

        logger.info('end send intermediate results')

        DFUtil.print_data_frame(final_prices_df, 'final_prices_df', is_debug)

        final_prices_with_room_type_df = pd.merge(hotel_room_type_df, final_prices_df, on=['oyo_id'])

        # 获取酒店季节性数据
        hotel_seasonality_query = """
            select oyo_id, month, weekday_weekend, seasonality
            from hotel_seasonality
            where oyo_id in {0}
        """.format(all_hotels_str)

        logger.info('start hotel seasonality query:\n%s', hotel_seasonality_query)

        hotel_seasonality_df = self.get_mysql_query_manager().read_sql(hotel_seasonality_query)

        hotel_seasonality_df = DFUtil.check_duplicate(hotel_seasonality_df, 'hotel_seasonality_df',
                                                      ['oyo_id', 'month', 'weekday_weekend', 'seasonality'])

        logger.info('end hotel seasonality query')

        final_prices_with_seasonality_df = pd.merge(final_prices_with_room_type_df,
                                                    hotel_seasonality_df, how='left',
                                                    on=['oyo_id', 'month', 'weekday_weekend'])

        DFUtil.print_data_frame(final_prices_with_seasonality_df, 'final_prices_with_seasonality_df', is_debug)

        # 获取酒店价差数据
        room_type_price_difference_query = """
            select oyo_id, room_type_id, difference_type, weekday_weekend, seasonality, price_delta, price_multiplier
            from hotel_room_type_price_difference
            where oyo_id in {0}
        """.format(all_hotels_str)

        logger.info('start room type difference query:\n%s', room_type_price_difference_query)

        # attach room type price difference for non special
        room_type_price_difference_df = self.get_mysql_query_manager().read_sql(room_type_price_difference_query)

        room_type_price_difference_df = DFUtil.check_duplicate(room_type_price_difference_df,
                                                               'room_type_price_difference_df',
                                                               ['oyo_id', 'room_type_id', 'weekday_weekend',
                                                                'seasonality'])

        logger.info('end room type difference query')

        room_type_final_prices_df = pd.merge(final_prices_with_seasonality_df,
                                             room_type_price_difference_df, how='left',
                                             on=['oyo_id', 'room_type_id', 'weekday_weekend',
                                                 'seasonality'])

        DFUtil.print_data_frame(room_type_final_prices_df, 'room_type_final_prices_df', is_debug)

        # 根据价差计算各房型价格
        room_type_final_prices_df = DFUtil.apply_func_for_df(room_type_final_prices_df, 'pms_price',
                                                             ['price', 'difference_type',
                                                              'price_delta', 'price_multiplier'],
                                                             lambda values: PriceUtil.calc_room_type_difference(
                                                                 *values))

        pms_prices_df = room_type_final_prices_df[['oyo_id', 'date', 'room_type_id', 'pms_price']]

        pms_prices_df['is_special'] = 'no'

        pms_prices_df['hourly_price'] = DEFAULT_NULL_VALUE

        pms_prices_df = pd.merge(pms_prices_df, room_type_df, on='room_type_id')

        pms_prices_df = pd.merge(pms_prices_df, hotel_with_zone_name_df, on='oyo_id')

        pms_prices_to_excel = pms_prices_df[
            ['date', 'oyo_id', 'room_type_id', 'room_type_name', 'pms_price', 'hourly_price', 'hotel_name',
             'is_special']]

        all_pms_prices_to_excel = pd.concat([special_pms_prices_to_excel, pms_prices_to_excel])

        all_pms_prices_to_pms_df = all_pms_prices_to_excel[['oyo_id', 'date', 'room_type_id', 'pms_price']]

        all_pms_prices_to_excel.sort_values(['oyo_id', 'date', 'room_type_id'], inplace=True,
                                            ascending=[True, True, True])

        logger.info('start send pms prices')

        pms_prices_data_df = all_pms_prices_to_excel.copy()

        DFUtil.print_data_frame(all_pms_prices_to_excel, 'all_pms_prices_to_excel', is_debug)

        # 发送邮件2
        send_mail.send_mail_for_pms_result(config, all_pms_prices_to_excel)

        logger.info('end send pms prices')

        DFUtil.print_data_frame(pms_prices_df, 'pms_prices_df', is_debug)

        all_prices_df = pd.concat([special_pms_prices_df, pms_prices_df], sort=True)

        all_prices_df['pms_price'] = np.ceil(all_prices_df.pms_price * OTA_TO_PMS_PRICING_RATIO)

        DFUtil.print_data_frame(all_prices_df, 'all_prices_df', is_debug)

        ota_channel_map = SEVEN_CHANNELS_MAP

        pool = ProcessingPool()

        all_prices_df = PricingPipeline.pipe_join_ota_for_pms_prices(all_prices_df, self.get_mysql_query_manager(),
                                                                     ota_channel_map, pool, None)

        all_prices_df.sort_values(['oyo_id', 'date', 'room_type_id'], inplace=True, ascending=[True, True, True])

        DFUtil.print_data_frame(all_prices_df, 'all_pms_prices_df_after_all_calc', is_debug)

        ebk_ota_room_type_mapping_df = PricingPipelineUtil.calc_fold_ota_room_type_df(self.get_mysql_query_manager(),
                                                                                      list(set_oyo_id))

        if config.get_ota_pricing() or is_preset:

            ota_plugin_dfs = PricingPipeline.compose_ota_plugin_v2_data_from_pms_prices(all_prices_df,
                                                                                        ebk_ota_room_type_mapping_df, pool)
            if not ota_plugin_dfs.empty:
                # 给CC发手动数据，也发插件数据
                send_mail.send_mail_for_ota_plugin_v2_result(config, ota_plugin_dfs, job_start_time_str, batch_order)

        pool.close()

        pool.join()

        hotel_id_query = """
            select oyo_id, id as hotel_id, unique_code, cluster_id
            from oyo_dw.v_dim_hotel
            where oyo_id in {0}
        """.format(all_hotels_str)

        logger.info('start hotel id query:\n%s', hotel_id_query)

        hotel_id_df = self.get_oracle_query_manager().read_sql(hotel_id_query)

        logger.info('end hotel id query')

        zone_query = """
            select id as cluster_id, zone_name
            from oyo_dw.dim_zones
        """

        logger.info('start zone query:\n%s', hotel_id_query)

        zone_df = self.get_oracle_query_manager().read_sql(zone_query)

        logger.info('end zone query')

        all_pms_prices_to_pms_df = pd.merge(all_pms_prices_to_pms_df, hotel_id_df, on='oyo_id')

        insert_to_pms_df = all_pms_prices_to_pms_df.rename(
            columns={'hotel_id': 'id', 'room_type_id': 'room_type', 'pms_price': 'final_price'})

        logger.info('start inserting prices to pms')

        # price_insert—HotelPriceToPms-发送邮件4
        pricei.batch_insert_pms_price_to_crs_and_send_mail(config, insert_to_pms_df)

        logger.info('end inserting prices to pms')

        pms_prices_data_df = pd.merge(pms_prices_data_df, hotel_id_df, on='oyo_id')

        DFUtil.print_data_frame(pms_prices_data_df, "pms_prices_data_df", is_debug=True)
        DFUtil.print_data_frame(zone_df, "zone_df", is_debug=True)
        if not (pms_prices_data_df.empty or zone_df.empty):
            pms_prices_data_df = pd.merge(pms_prices_data_df, zone_df, on='cluster_id')

        logger.info('start writing pms pricing log')

        # price_log-PriceReport-发送邮件5
        pms_prices_data_df["strategy_type"] = "OM"
        pricel.report_price_and_send_mail(config, pms_prices_data_df)

        logger.info('end writing pms pricing log')

        logger.info('start writing ota pricing log')

        # price_log-OtaPriceReport-发送邮件6
        all_prices_df["strategy_type"] = "OM"

        ebk_ota_room_type_mapping_filter_list = JobSinker.compose_ebk_ota_room_type_mapping_filter_list(ebk_ota_room_type_mapping_df)

        pricel.report_ota_price_and_send_mail(config, all_prices_df, ebk_ota_room_type_mapping_filter_list)

        logger.info('end writing ota pricing log')

        logger.info('start writing inter result pricing log')

        # price_log-OMV1InterResult-发送邮件7
        inter_data_df["strategy_type"] = "OM"

        inter_data_df.rename(columns={'brn': 'urn'}, inplace=True)

        pricel.report_om_v1_inter_result_and_send_mail(config, inter_data_df)

        logger.info('end writing inter result pricing log')

        if config.get_ota_pricing()  or is_preset:
            # price_log-OtaPriceUpload-发送邮件8
            logger.info('start uploading ota prices')

            opriceu.ota_price_upload_and_mail_send(config, all_prices_df, ebk_ota_room_type_mapping_filter_list)

            logger.info('end uploading ota prices')

        logger.info('*********************run end******************************')
