#!/usr/bin/env python
# -*- coding:utf-8 -*-

import os
import sys
import traceback
import warnings
from os.path import join as join_path

warnings.filterwarnings("ignore")
cur_path = os.path.dirname(os.path.realpath(__file__))
sys.path.append(join_path(cur_path, '..'))

from strategy.china_rebirth import *
from common.util.utils import *
from CHINA_Rebirth.china_rebirth_base import ChinaRebirthJobBase


class ChinaRebirthClusterBaseJob(ChinaRebirthJobBase):

    def __init__(self, job_config):
        ChinaRebirthJobBase.__init__(self, job_config)

    def get_job_name(self):
        return 'ChinaRebirthClusterBase'

    def get_min_price(self):
        return 40

    def run_core(self, smart_hotel_oyo_id_list, start_time):
        logger = LogUtil.get_cur_logger()
        all_smart_hotels_str = MiscUtil.convert_list_to_tuple_list_str(smart_hotel_oyo_id_list)
        start_date = dt.datetime.strftime(start_time, "%Y-%m-%d")
        # In[1]
        smart_query_2 = '''
            select a.*,b.valid_from,b.valid_till,b.agreement_type,b.contracted_rooms
            from
            (select a."date", b.hotel_id,b.oyo_id,b.cluster_name,b.city from (select "date" from oyo_dw.DIM_CALENDAR where "date" between current_date and current_date+7) a
              cross join
              (select h.id as hotel_id,h.oyo_id, c.cluster_name,c.CITY_Name as city 
              from oyo_dw.v_dim_hotel h left join oyo_dw.DIM_ZONES c on c.id=h.cluster_id
              --where h.status in (3,2) 
               ) b
            ) a
              LEFT join
              (                                                                                        
              select a.hotel_id,a.valid_from,a.valid_till, b.rooms as contracted_rooms, b.agreement_type from  oyo_dw.DIM_HOTEL_AGREEMENT_DETAILS a
              left join oyo_dw.DIM_PROD_AGREEMENTS b on a.agreement_id = b.id
              where b.active = 'Y' 
              --and b.agreement_type in (6,7) need to clear the type data
              ) b
              on a.hotel_id = b.hotel_id
              and a."date" between b.valid_from and b.valid_till
        '''
        begin = time.time()
        smart_properties = self.get_oracle_query_manager().read_sql(smart_query_2, 60)
        logger.info('smart_properties cost time %d s rowsCount %d', time.time() - begin, smart_properties.size)
        logger.info('smart done')

        smart_hotel_data = smart_properties[smart_properties.oyo_id.isin(smart_hotel_oyo_id_list)]
        raw_city_list = list(smart_hotel_data.city.unique())
        city_list = []
        for city in raw_city_list:
            if city is not None:
                city_list.append(city)
        if len(city_list) == 1:
            city_list.append('testtest')
        elif len(city_list) == 0:
            logger.warning('城市列表为空！')
            # return 0
        city_list = tuple(city_list)
        # =============================================================================
        # =============================================================================

        # 过去三周的online OCC 和 GMV
        past_3week_online_data = get_hotel_urn_gmv_by_city(city_list, 22, 1, self.get_oracle_query_manager())

        ################################ Estimating Smart Occupancy ##############################
        #  选取的实时OCC    用了判断是否达到 目标     选取的线上数据
        # TODO!!!!!!!!!!!!!!!!!!!!!!!! use ADB URN data
        query_srn_data = '''
            select left_data.oyo_id                                                              as oyo_id,
                   left_data."date"                                                              as "date",
                   case when right_data.used_rooms is null then 0 else right_data.used_rooms end as used_rooms,
                   left_data.srn                                                                 as sellable_rooms
            from (select *
                  from (SELECT hs.oyo_id, hs.srn
                        FROM (select a.date_s,
                                     a.oyo_id,
                                     a.srn
                              from oyo_dw.fact_revenue_ht_std a
                                     join oyo_dw_p.impt_hotel_data_trans b1 on a.id = b1.id and b1.trans_date <= a.date_s
                                     LEFT JOIN oyo_dw_sa.dw_bd_cooperation_plan_info c
                                               ON a.ID = c.crs_hotel_id AND c.active = 'Y' AND c.source_env = 'apollo'
                                     LEFT JOIN oyo_dw_sa.dw_tr_audit_process d
                                               ON c.hotel_id = d.hotel_id AND a.oyo_id = d.crs_id) hs
                        WHERE hs.date_s = to_char(SYSDATE - 1, 'yyyyMMdd')
                          and oyo_id in {0}) oyo_id_with_srn
                         cross join (select "date"
                                     from oyo_dw.dim_calendar
                                     where datekey >= to_char(sysdate, 'yyyymmdd')
                                       and datekey <= to_char(sysdate + 29, 'yyyymmdd')) filled_srn) left_data
                   left join
                 (SELECT y.biz_date AS "date", y.OYO_ID, sum(used_rooms) AS used_rooms, max(SRN) AS sellable_rooms
                  FROM (SELECT oyo_id, biz_date, count(*) AS used_rooms
                        FROM oyo_dw_sa.fact_booking_room
                        WHERE oyo_id IN {0}
                          and to_char(biz_date, 'yyyymmdd') >= to_char(sysdate, 'yyyymmdd')
                          and to_char(biz_date, 'yyyymmdd') <= to_char(sysdate + 29, 'yyyymmdd')
                          AND status IN (0, 1, 2)
                          and (check_out_time IS NULL OR TRUNC(check_out_time) > TRUNC(biz_date))
                        GROUP BY oyo_id, biz_date) y
                         LEFT JOIN
                       (SELECT hs.oyo_id, hs.srn
                        FROM (select a.date_s,
                                     a.oyo_id,
                                     a.srn
                              from oyo_dw.fact_revenue_ht_std a
                                     join oyo_dw_p.impt_hotel_data_trans b1 on a.id = b1.id and b1.trans_date <= a.date_s
                                     LEFT JOIN oyo_dw_sa.dw_bd_cooperation_plan_info c
                                               ON a.ID = c.crs_hotel_id AND c.active = 'Y' AND c.source_env = 'apollo'
                                     LEFT JOIN oyo_dw_sa.dw_tr_audit_process d
                                               ON c.hotel_id = d.hotel_id AND a.oyo_id = d.crs_id) hs
                        WHERE hs.date_s = to_char(SYSDATE - 1, 'yyyyMMdd')
                          and oyo_id in {0}) x
                       ON y.oyo_id = x.oyo_id
                  GROUP BY y.biz_date, y.OYO_ID) right_data
                 on left_data.oyo_id = right_data.oyo_id and left_data."date" = right_data."date"
        '''.format(all_smart_hotels_str)

        begin = time.time()

        srn_data = self.get_oracle_query_manager().read_sql(query_srn_data, 240)

        if srn_data is None:
            raise Exception('srn data query failed for all db engines !!!')

        logger.info('srn_data cost time %d  s rowsCount %d', time.time() - begin, srn_data.size)

        # In [51]: srn_data
        # Out[51]:
        # oyo_id       date  online_urn  sellable_rooms
        # 0   CN_SHA013 2019-04-23           1              46
        # 1   CN_SHA013 2019-04-24           1              46
        # 2   CN_SHA013 2019-04-30           1              46
        # 3   CN_SHA013 2019-04-19           2              46
        # 4   CN_SHA013 2019-04-22           1              46
        # 5   CN_SHA013 2019-04-26           1              46
        # 6   CN_SHA013 2019-04-27           1              46
        # 7   CN_SHA013 2019-04-28           1              46
        # 8   CN_SHA013 2019-04-21           1              46
        # 9   CN_SHA013 2019-04-25           1              46
        # 10  CN_SHA013 2019-04-20           1              46
        # 11  CN_SHA013 2019-05-01           1              46
        # 12  CN_SHA013 2019-04-29           1              46
        # 13  CN_SHA013 2019-05-10           0              46
        # 14  CN_SHA013 2019-05-16           0              46
        # 15  CN_SHA013 2019-05-04           0              46
        # 16  CN_SHA013 2019-05-12           0              46
        # 17  CN_SHA013 2019-05-08           0              46
        # 18  CN_SHA013 2019-05-18           0              46
        # 19  CN_SHA013 2019-05-07           0              46
        # 20  CN_SHA013 2019-05-15           0              46
        # 21  CN_SHA013 2019-05-14           0              46
        # 22  CN_SHA013 2019-05-11           0              46
        # 23  CN_SHA013 2019-05-05           0              46
        # 24  CN_SHA013 2019-05-06           0              46
        # 25  CN_SHA013 2019-05-02           0              46
        # 26  CN_SHA013 2019-05-13           0              46
        # 27  CN_SHA013 2019-05-17           0              46
        # 28  CN_SHA013 2019-05-03           0              46
        # 29  CN_SHA013 2019-05-09           0              46

        query_22 = '''
            select ca."date",h.oyo_id as oyo_id,c.cluster_name,zone_name,city_name,hub_name
            from oyo_dw.dim_calendar ca 
            cross join oyo_dw.v_dim_hotel h 
            left join oyo_dw.dim_zones c on c.id=h.cluster_id
            where h.oyo_id in {2}
            and ca."date" >= "TO_DATE"('{0}','YYYY-MM-DD' ) 
            and ca."date" <= "TO_DATE"('{1}','YYYY-MM-DD' )
        '''.format(str(start_date),
                   dt.datetime.strftime(
                       dt.datetime.strptime(start_date, '%Y-%m-%d') + dt.timedelta(days=29),
                       "%Y-%m-%d"),
                   all_smart_hotels_str)
        begin = time.time()
        all_smart_hotel_30d_full_channel_data = self.get_oracle_query_manager().read_sql(query_22)
        logger.info('hotel_data cost time %d  s rowsCount %d', time.time() - begin,
                    all_smart_hotel_30d_full_channel_data.size)

        all_smart_hotel_30d_full_channel_data = pd.merge(all_smart_hotel_30d_full_channel_data, srn_data, how='left',
                                                         on=['date', 'oyo_id'])
        all_smart_hotel_30d_full_channel_data = all_smart_hotel_30d_full_channel_data.fillna(0)

        all_smart_hotel_30d_full_channel_data[
            'occ'] = all_smart_hotel_30d_full_channel_data.used_rooms / all_smart_hotel_30d_full_channel_data.sellable_rooms
        all_smart_hotel_30d_full_channel_data[
            'rem'] = all_smart_hotel_30d_full_channel_data.sellable_rooms - all_smart_hotel_30d_full_channel_data.used_rooms

        all_smart_hotel_30d_full_channel_data.occ = all_smart_hotel_30d_full_channel_data.occ.map(
            lambda x: np.where(pd.isnull(x) | (x == float("inf")), 0, x))
        logger.info('hotel_data process done')

        ## Cluster Trend Data Query ##
        city_total_urn_past_30d = get_total_urn_by_date_city(city_list, start_date, 35, 1,
                                                             self.get_oracle_query_manager())

        all_hotel_data_past_4_weeks = get_hotel_urn_gmv_by_city(city_list, 28, 1, self.get_oracle_query_manager())

        # get all hotel locations
        hotel_data_lat_long_all_prop = get_hotel_location(city_list, self.get_oracle_query_manager())

        sql_tail = '''
            select oyo_id, upper(cluster_name) cluster_name, upper(city_name) city_name,upper(hub_name) hub_name, zone.category as prop_category
            from oyo_dw.v_dim_hotel hotel 
            LEFT JOIN oyo_dw.dim_zones zone ON hotel.cluster_id = zone.id
            where hotel.oyo_id in {0}
        '''.format(all_smart_hotels_str)

        begin = time.time()
        prop_details = self.get_oracle_query_manager().read_sql(sql_tail)
        logger.info('prop_details cost time %d  s rowsCount %d', time.time() - begin, prop_details.size)

        # In [19]: prop_details
        # Out[19]:
        # oyo_id cluster_name city_name  hub_name  prop_category
        # 0  CN_SHA013    GUANGFENG  SHANGRAO  SHANGRAO              0

        logger.info('smart_hotel_data size =%d', len(smart_hotel_oyo_id_list))
        logger.info('##############smart_hotel_data smart_hotel_oyo_id process  start##############')

        df_base_price_override_next_30d = get_base_price_override(all_smart_hotels_str, self.get_mysql_query_manager())

        smart_hotel_params_df = pd.DataFrame()
        complete_raw = pd.DataFrame()

        for smart_hotel_oyo_id in smart_hotel_oyo_id_list:
            try:
                clus_hotel_list = get_cluster_hotel_list(smart_hotel_oyo_id, smart_hotel_data,
                                                         hotel_data_lat_long_all_prop)

                hotel_past_occ, arr_list, past_3wk_data_hotel_sum = rearrange_past_data_to_days(smart_hotel_oyo_id,
                                                                                                clus_hotel_list,
                                                                                                past_3week_online_data,
                                                                                                prop_details)
                # correct base price based on past online arr
                targeted_hotel_online_data_past_4_weeks_by_day = correct_base_price_based_on_past_online_arr(
                    all_hotel_data_past_4_weeks, arr_list, past_3wk_data_hotel_sum)

                # build cluster urn completion rate trendline
                clus_urn_completion_trend = get_cluster_urn_completion_trend_line(start_date, city_total_urn_past_30d,
                                                                                  clus_hotel_list)

                # calculate final price based on hotel occ and cluster urn completion trendline
                hotel_price_data = calculate_final_price(smart_hotel_oyo_id, all_smart_hotel_30d_full_channel_data,
                                                         clus_urn_completion_trend,
                                                         targeted_hotel_online_data_past_4_weeks_by_day,
                                                         df_base_price_override_next_30d
                                                         )
                hotel_price_data = run_last_minute_sale(start_time, hotel_price_data)

                smart_hotel_params_df = pd.concat([smart_hotel_params_df, hotel_price_data[
                    ['date', 'oyo_id', 'price', 'urn_completion', 'occ', 'sellable_rooms', 'rem', 'delta',
                     'corrected_base', 'base',
                     '3wk_online_arr', '3wk_online_occ', 'occ_ftd_target', 'gear_factor', 'price_adj_ratio']]],
                                                  ignore_index=True)
                complete_raw = pd.concat([complete_raw, hotel_price_data], ignore_index=True)
            except Exception as e:
                logger.info("Error processing hotel {} !!!! ".format(str(smart_hotel_oyo_id)))
                try:
                    logger.warning(traceback.format_exc(e))
                except:
                    pass

        smart_hotel_params_df = smart_hotel_params_df[
            (smart_hotel_params_df.price.isnull() == False) & (smart_hotel_params_df.price != float('inf'))]
        return smart_hotel_params_df
