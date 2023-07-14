#!/usr/bin/env python
# -*- coding:utf-8 -*-

import os
import sys
import warnings
from os.path import join as join_path

warnings.filterwarnings("ignore")
cur_path = os.path.dirname(os.path.realpath(__file__))
sys.path.append(join_path(cur_path, '..'))

from CHINA_Rebirth.china_rebirth_base import ChinaRebirthJobBase
from strategy.china_rebirth import *
from strategy.rule_base import *
from strategy.common import *
from common.util.utils import *
from datetime import timedelta


def run_single_hotel(start_time, smart_hotel_oyo_id, all_brn_with_srn_df, df_base_price_override_next_30d,
                     hotel_data_by_day, internal_alert_token, ebase_data, occ_target_map, adjust_ratio_divisor_map,
                     price_ec_map):
    try:
        logger = LogUtil.get_cur_logger()

        logger.info('processing hotel: {}'.format(smart_hotel_oyo_id))

        targeted_hotel_data_by_day = hotel_data_by_day[hotel_data_by_day.oyo_id == smart_hotel_oyo_id]

        hotel_ebase_price = None if ebase_data is None else ebase_data[ebase_data.oyo_id == smart_hotel_oyo_id]

        targeted_hotel_data_by_day = base_correction_with_moving_average(smart_hotel_oyo_id,
                                                                         targeted_hotel_data_by_day,
                                                                         occ_target_map, price_ec_map)

        # only use last 7 days data
        targeted_hotel_data_by_day = targeted_hotel_data_by_day[-7:]

        hotel_price_data = adjust_price_based_on_future_occ(smart_hotel_oyo_id, all_brn_with_srn_df, start_time,
                                                            targeted_hotel_data_by_day, df_base_price_override_next_30d,
                                                            hotel_ebase_price, occ_target_map, adjust_ratio_divisor_map,
                                                            price_ec_map)
        return hotel_price_data
    except Exception:
        try:
            logger = LogUtil.get_cur_logger()
            logger.warning("Error processing hotel {} !!!! ".format(str(smart_hotel_oyo_id)))
            stack_trace = traceback.format_exc()
            logger.warning(stack_trace)
            # TODO(yry): multi processing hang, optimize it
            # DdtUtil.robot_send_ddt_msg('China rebirth, error processing hotel {} !\n stack trace: {}'.format(
            #     smart_hotel_oyo_id, stack_trace),
            #     internal_alert_token, None, True)
        except:
            pass


def run_grouped_hotel(start_time, grouped_smart_hotel_oyo_id, all_brn_with_srn_df,
                      df_base_price_override_next_30d, hotel_data_by_day, internal_alert_token,
                      ebase_data, occ_target_map, adjust_ratio_divisor_map, price_ec_map):
    grouped_hotel_price_data = list()
    for smart_hotel_oyo_id in grouped_smart_hotel_oyo_id:
        hotel_price_data = run_single_hotel(start_time, smart_hotel_oyo_id, all_brn_with_srn_df,
                                            df_base_price_override_next_30d,
                                            hotel_data_by_day, internal_alert_token, ebase_data,
                                            occ_target_map, adjust_ratio_divisor_map, price_ec_map)
        grouped_hotel_price_data.append(hotel_price_data)
    return grouped_hotel_price_data


def run_grouped_hotel_list(start_time, grouped_smart_hotel_list, all_brn_with_srn_df,
                           df_base_price_override_next_30d, hotel_data_by_day, pool,
                           internal_alert_token, ebase_data, occ_target_map,
                           adjust_ratio_divisor_map, price_ec_map):
    hotel_price_list = list()
    cpu_cores = multiprocessing.cpu_count()
    LogUtil.get_cur_logger().info('cpu count: {}'.format(cpu_cores))
    start_time_lst = list()
    grouped_smart_hotel_lst = list()
    all_brn_with_srn_df_lst = list()
    df_base_price_override_next_30d_lst = list()
    hotel_data_by_day_lst = list()
    ebase_data_lst = list()
    occ_target_map_lst = list()
    adjust_ratio_divisor_map_lst = list()
    price_ec_map_lst = list()
    for grouped_smart_hotel in grouped_smart_hotel_list:
        start_time_lst.append(start_time)
        grouped_smart_hotel_lst.append(grouped_smart_hotel)
        all_brn_with_srn_df_lst.append(all_brn_with_srn_df)
        df_base_price_override_next_30d_lst.append(df_base_price_override_next_30d)
        hotel_data_by_day_lst.append(hotel_data_by_day)
        ebase_data_lst.append(ebase_data)
        occ_target_map_lst.append(occ_target_map)
        adjust_ratio_divisor_map_lst.append(adjust_ratio_divisor_map)
        price_ec_map_lst.append(price_ec_map)
    proc_hotel_prices = pool.map(run_grouped_hotel, start_time_lst, grouped_smart_hotel_lst,
                                 all_brn_with_srn_df_lst, df_base_price_override_next_30d_lst,
                                 hotel_data_by_day_lst, internal_alert_token, ebase_data_lst,
                                 occ_target_map_lst, adjust_ratio_divisor_map_lst, price_ec_map_lst)
    for hotel_price in proc_hotel_prices:
        hotel_price_list.extend(hotel_price)
    return hotel_price_list


class ChinaRebirthRuleBaseJob(ChinaRebirthJobBase):

    def __init__(self, job_config):
        ChinaRebirthJobBase.__init__(self, job_config)

    def get_job_name(self):
        return 'ChinaRebirthRuleBase'

    def get_min_price(self):
        return 42

    @staticmethod
    def gen_alert_hotel_set(start_time, check_df):
        date_t3 = (start_time + timedelta(days=3)).strftime("%Y-%m-%d")
        check_fail_set = set()
        for index, row in check_df.iterrows():
            oyo_id = row['oyo_id']
            date = row['date']
            if date <= date_t3:
                check_fail_set.add(oyo_id)
        return check_fail_set

    @staticmethod
    def static_run_core(smart_hotel_oyo_id_list, start_time, pool, mysql_query_mgr, oracle_query_mgr,
                        adb_query_mgr, op_alert_token, internal_alert_token, oracle_upstream_alert_token, disable_ebase):
        start_date = dt.datetime.strftime(start_time, "%Y-%m-%d")

        logger = LogUtil.get_cur_logger()

        day_offset = get_urn_upstream_offset(start_date, mysql_query_mgr)

        logger.info('static_run_core, day_offset: {}, start_date: {}, oracle_upstream_alert_token: {}'.format(day_offset, start_date,
                                                                                             oracle_upstream_alert_token))

        urn_start_day_offset = day_offset + 1

        urn_end_day_offset = urn_start_day_offset + 14

        calc_day = (start_time - timedelta(days=day_offset)).strftime('%Y-%m-%d')

        past_2week_online_data = get_hotel_urn_gmv_by_hotel(smart_hotel_oyo_id_list, urn_end_day_offset,
                                                            urn_start_day_offset, calc_day, mysql_query_mgr)

        past_2wk_pms_price = get_pms_price_from_pricing_context(smart_hotel_oyo_id_list, 15, 1, mysql_query_mgr)

        hot_day_price_map = get_hot_day_price_from_dingtalk(smart_hotel_oyo_id_list, 15, 1, mysql_query_mgr)

        def is_hot_day(oyo_id, date, hot_day_price_map):
            hot_day_lst = hot_day_price_map.get(oyo_id)
            if hot_day_lst is None:
                return False
            date_str = date.strftime('%Y-%m-%d')
            for date_range in hot_day_lst:
                if date_range[0] <= date_str <= date_range[1]:
                    return True
            return False

        # filter hot day prices so the normal routine prices would not fluctuate a lot
        past_2wk_pms_price = DFUtil.apply_func_for_df(past_2wk_pms_price, 'is_hot_day', ['oyo_id', 'date'],
                                                      lambda values: is_hot_day(*values, hot_day_price_map))

        past_2wk_pms_price = DFUtil.apply_func_for_df(past_2wk_pms_price, 'day', ['date'], lambda values: values[0].weekday())

        if past_2wk_pms_price.empty:
            normal_day_price_replacement = past_2wk_pms_price.drop(columns=['is_hot_day'])
        else:
            normal_day_price_replacement = past_2wk_pms_price[~past_2wk_pms_price.is_hot_day].groupby(
                ['oyo_id', 'day'], as_index=False).last().drop(columns=['is_hot_day'])

        past_2wk_pms_price = past_2wk_pms_price.merge(normal_day_price_replacement, on=['oyo_id', 'day'],
                                                      suffixes=['', '_normal'], how='left')

        def replace_with_normal_price_if_hot_day(pms_price, pms_price_normal, is_hot_day):
            if MiscUtil.is_not_empty_value(is_hot_day) and is_hot_day and MiscUtil.is_not_empty_value(pms_price_normal):
                return pms_price_normal
            return pms_price

        past_2wk_pms_price = DFUtil.apply_func_for_df(past_2wk_pms_price, 'pms_price',
                                                      ['pms_price', 'pms_price_normal', 'is_hot_day'],
                                                      lambda values: replace_with_normal_price_if_hot_day(*values))

        past_2wk_pms_price = past_2wk_pms_price[['oyo_id', 'date', 'pms_price']]

        new_active_days_df = get_all_hotel_active_day(smart_hotel_oyo_id_list, start_date, 15, mysql_query_mgr)

        past_2wk_pms_price = past_2wk_pms_price.merge(new_active_days_df, how='left', on='oyo_id')

        def is_valid(date, start_day):
            if MiscUtil.is_empty_value(start_day):
                return True
            return str(date) >= start_day

        past_2wk_pms_price = DFUtil.apply_func_for_df(past_2wk_pms_price, 'is_valid', ['date', 'start_day'],
                                                      lambda values: is_valid(*values))

        past_2wk_pms_price = past_2wk_pms_price[past_2wk_pms_price.is_valid]

        past_2wk_pms_price = pd.DataFrame(columns=['oyo_id', 'date', 'pms_price']) if past_2wk_pms_price.empty else past_2wk_pms_price[['oyo_id', 'date', 'pms_price']]

        past_2week_online_data = past_2week_online_data.merge(past_2wk_pms_price, on=['oyo_id', 'date'], how='left')

        mg_target_data = get_mg_target_by_hotel(smart_hotel_oyo_id_list, start_time, oracle_query_mgr)

        logger.info('smart_hotel_data size =%d', len(smart_hotel_oyo_id_list))

        logger.info('##############smart_hotel_data smart_hotel_oyo_id process  start##############')

        df_base_price_override_next_30d = get_base_price_override(smart_hotel_oyo_id_list,
                                                                  mysql_query_mgr)

        all_brn_with_srn_df = get_all_hotels_next_7d_occ_from_adb(smart_hotel_oyo_id_list, start_date,
                                                                  adb_query_mgr,
                                                                  oracle_query_mgr)
        begin = time.time()

        df_night_order_pct = get_night_order_percent_wo_mm(smart_hotel_oyo_id_list, 24 - start_time.hour,
                                                           adb_query_mgr, start_time)

        logger.info('df_night_order_pct cost: %0.2fs', time.time() - begin)

        all_brn_with_srn_df = apply_night_prediction(all_brn_with_srn_df, df_night_order_pct, start_time)

        hotel_data_by_day = prepare_base_correction_data(past_2week_online_data)

        cpu_cores = multiprocessing.cpu_count()

        group_size = ceil(len(smart_hotel_oyo_id_list) / float(cpu_cores))

        grouped_hotel_list = MiscUtil.group_by_list(smart_hotel_oyo_id_list, group_size)

        ebase_data = None if disable_ebase else get_ebase_data_from_oracle(smart_hotel_oyo_id_list, start_date, oracle_query_mgr, 15)

        occ_target_map = get_occ_target_map_from_pc(smart_hotel_oyo_id_list, mysql_query_mgr)
        data_date = datetime.datetime.strftime(start_time + datetime.timedelta(days=-1), "%Y-%m-%d")
        price_ec_map = get_price_ec_map(smart_hotel_oyo_id_list, data_date, mysql_query_mgr)

        adjust_ratio_divisor_map = get_adjust_ratio_divisor_map_from_oracle(smart_hotel_oyo_id_list, start_date, mysql_query_mgr)

        hotel_price_list = run_grouped_hotel_list(start_time, grouped_hotel_list, all_brn_with_srn_df,
                                                  df_base_price_override_next_30d, hotel_data_by_day, pool,
                                                  internal_alert_token, ebase_data, occ_target_map,
                                                  adjust_ratio_divisor_map, price_ec_map)

        smart_hotel_params_df = pd.concat(hotel_price_list, ignore_index=True)

        smart_hotel_params_df = default_final_price_to_mg_target(mg_target_data, smart_hotel_params_df)

        ChinaRebirthRuleBaseJob.try_check_and_send_data_alert(start_time, op_alert_token, smart_hotel_params_df)

        ChinaRebirthRuleBaseJob.try_check_and_send_resale_alert(start_date, op_alert_token, mysql_query_mgr)

        smart_hotel_params_df = smart_hotel_params_df[
            (smart_hotel_params_df.price.isnull() == False) & (smart_hotel_params_df.price != float('inf'))]

        ChinaRebirthRuleBaseJob.try_send_urn_upstream_data_alert(day_offset, oracle_upstream_alert_token)

        return smart_hotel_params_df

    @staticmethod
    def try_check_and_send_resale_alert(start_date, op_alert_token, mysql_query_mgr):
        resale_hotel_count = get_resale_hotel_count(start_date, mysql_query_mgr)
        if resale_hotel_count <= 0:
            DdtUtil.robot_send_ddt_msg('日期：{start_date} 手工表 (oyo_pricing_offline.prod_upload_resale_hotel_detail) 没有数据!!'.format(
                start_date=start_date), op_alert_token, None, True)

    @staticmethod
    def try_check_and_send_data_alert(start_time, op_alert_token, smart_hotel_params_df):
        try:
            data_check_df = smart_hotel_params_df[
                (smart_hotel_params_df.price.isnull() != False) | (smart_hotel_params_df.price == float('inf'))]
            check_fail_set = ChinaRebirthRuleBaseJob.gen_alert_hotel_set(start_time, data_check_df)
            check_fail_cnt = len(check_fail_set)
            if check_fail_cnt != 0:
                robot_token = op_alert_token
                DdtUtil.robot_send_ddt_msg('快醒醒快醒醒，这里有 {} 个酒店缺失T+3内的数据：{}，赶紧找回来'.format(
                    check_fail_cnt, list(check_fail_set)), robot_token, None, True)
        except:
            pass

    @staticmethod
    def try_send_urn_upstream_data_alert(day_offset, urn_upstream_alert_token):
        try:
            LogUtil.get_cur_logger().info('try_send_urn_upstream_data_alert, day_offset: {}, urn_upstream_alert_token: {}'.format(
                day_offset, urn_upstream_alert_token))
            if day_offset != 0:
                alert_offset = day_offset + 1
                LogUtil.get_cur_logger().info('about to send urn upstream alert!!')
                DdtUtil.robot_send_ddt_msg('T-1 urn 数据读取失败，降级到 T-{} 数据！！！'.format(alert_offset),
                                           urn_upstream_alert_token, None, True)
        except:
            pass

    def run_core(self, smart_hotel_oyo_id_list, start_time, pool, disable_ebase):
        return ChinaRebirthRuleBaseJob.static_run_core(smart_hotel_oyo_id_list, start_time, pool,
                                                       self.get_mysql_query_manager(),
                                                       self.get_oracle_query_manager(),
                                                       self.get_adb_query_manager(),
                                                       self.get_robot_token_op_alert(),
                                                       self.get_robot_token_internal_alert(),
                                                       self.get_robot_token_oracle_upstream(),
                                                       disable_ebase)


