import os
import sys
from os.path import join as join_path

cur_path = os.path.dirname(os.path.realpath(__file__))
sys.path.append(join_path(cur_path, '..'))
from common.dingtalk_sdk.dingtalk_py_cmd import init_jvm_for_ddt_disk
from common.job_base.job_base import JobBase
from common.priceop.price_to_ota import OtaPriceUpload as opriceu
from common.priceop.price_to_crs import PriceInsert as pricei
from common.priceop.price_log import PriceLog as pricel
from common.util.utils import *
from common.pricing_pipeline.pipeline import PricingPipeline, FIVE_CHANNELS_MAP

import datetime as dt
from datetime import datetime, timedelta
import warnings

warnings.filterwarnings("ignore")

import numpy as np


def set_min_price(df, columns):
    for column in columns:
        df[column] = df[column].apply(lambda price: PriceUtil.floor_price_check(price, FLOOR_PRICE_TYPE_NUMBER, 40))


def round_price_to_digits(price, digits):
    if price == DEFAULT_NULL_VALUE or pd.isna(price):
        return price
    return '%.{0}f'.format(digits) % price


def round_digits_for_df_columns(df, columns, digits):
    for column in columns:
        df[column] = df[column].apply(lambda price: round_price_to_digits(price, digits))


def commission_ratio_to_percentile_for_df_columns(df, columns, digits):
    for column in columns:
        df[column] = df[column].apply(
            lambda ratio: NumberUtil.decimal_to_percentile(ratio, DEFAULT_NULL_VALUE, DEFAULT_NULL_VALUE, digits))


class KcJob(JobBase):

    def __init__(self, job_config):
        JobBase.__init__(self, job_config)

    def get_job_name(self):
        return 'KC_Daily'

    def run(self):
        logger = LogUtil.get_cur_logger()

        config = self.get_job_config()

        send_mail = config.get_mail_send()

        init_jvm_for_ddt_disk()

        DateUtil.init_preset_time()

        logger.info('*********************run begin******************************')

        begin = time.time()

        start_time = dt.datetime.fromtimestamp(begin)

        start_date = dt.datetime.strftime(start_time, "%Y-%m-%d")

        job_start_time_str = dt.datetime.strftime(dt.datetime.fromtimestamp(time.time()), "%Y_%m_%d_%H_%M")

        batch_order = config.get_batch_order()

        mg_hotels_query = """
            SELECT DISTINCT OYO_ID FROM oyo_dw.DIM_CHINA_MG_HOTELS WHERE "STATUS" IN (1,3)
        """
        mg_hotels_df = self.get_oracle_query_manager().read_sql(mg_hotels_query)

        mg_batch_hotels_query = """
            select distinct(oyo_id)
            from hotel_batch
            where enabled = 1
              and batch_order_id = {0}
              and oyo_id in
                  (select distinct(oyo_id)
                    from hotel_business_model where business_model_id = 4)
        """.format(batch_order)

        mg_batch_hotels_df = self.get_mysql_query_manager().read_sql(mg_batch_hotels_query)

        filtered_hotels = mg_batch_hotels_df[mg_batch_hotels_df.oyo_id.isin(mg_hotels_df.oyo_id)].oyo_id

        oyo_id_size = len(filtered_hotels)

        if oyo_id_size == 0:
            logger.warning('酒店列表为空！')
            return 0

        smart_hotel_oyo_id_list = list(filtered_hotels)

        all_hotels_str = MiscUtil.convert_list_to_tuple_list_str(smart_hotel_oyo_id_list)

        # In[0]
        base_query = '''
            select oyo_id, pricing_date as date, base_price as base
            from oyo_pricing.hotel_base_price
            where pricing_date between current_date and current_date + interval 30 day
              and oyo_id in {0}
              and deleted = 0
        '''.format(all_hotels_str)
        input_base = self.get_mysql_query_manager().read_sql(base_query)

        logger.info('base_query cost time %d  s rowsCount %d', time.time() - begin, input_base.size)

        input_base.columns = list(pd.Series(input_base.columns).map(lambda x: str(x).replace('base', 'new_base')))

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
            return 0
        city_list = tuple(city_list)
        # =============================================================================
        # =============================================================================

        # In[1] 过去两周的OCC    选取的线上数据
        past_sql = '''
        select oyo_id,to_date(date_s,'yyyy-mm-dd') AS "date",sum(urn) AS used_rooms,sum(brn) ,max(srn) AS sellable_rooms,sum(PER_DAY_GMV) AS gmv
        from oyo_dw.v_fact_china_revenue a
        where to_date(date_s,'yyyy-mm-dd') between (current_date-15) and (current_date -1)
        and  HOTEL_STATUS IN ('Live','Active')
        and source_name in ('OTA','APP')
        and oyo_id in {0}
        group by oyo_id,date_s
        '''.format(all_hotels_str)
        begin = time.time()
        past_data = self.get_oracle_query_manager().read_sql(past_sql)
        logger.info('past_data cost: %0.2fs, records: %d', time.time() - begin, past_data.size)

        past_data['day'] = past_data.date.map(lambda x: x.weekday())
        logger.info('past_data process')

        ################################ Estimating Smart Occupancy ##############################
        # In[1]
        # sql11 = '''
        # select id, oyo_id from oyo_dw.dim_hotel
        # where oyo_id in {0}
        # '''.format(str(smart_hotel_tuple_list))
        # hotel_id_details_srn = pd.read_sql( sql11,con)
        # hotel_oyo_id = hotel_id_details_srn.oyo_id
        # =============================================================================
        # date_start='current_date'
        # date_end='current_date +6'
        #  选取的实时OCC    用了判断是否达到 目标     选取的线上数据
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
        '''.format(all_hotels_str)
        begin = time.time()
        srn_data = self.get_oracle_query_manager().read_sql(query_srn_data, 240)
        DFUtil.print_data_frame(srn_data, 'srn_data', True)
        logger.info('srn_data cost time %d  s rowsCount %d', time.time() - begin, srn_data.size)

        query_22 = '''
            select ca."date",h.oyo_id as oyo_id,c.cluster_name,zone_name,city_name,hub_name
            from oyo_dw.dim_calendar ca 
            cross join oyo_dw.v_dim_hotel h 
            left join oyo_dw.dim_zones c on c.id=h.cluster_id
            where h.oyo_id in {2}
            and ca."date" >= "TO_DATE"('{0}','YYYY-MM-DD' ) 
            and ca."date" <= "TO_DATE"('{1}','YYYY-MM-DD' )
        '''.format(str(start_date),
                   datetime.strftime(dt.datetime.strptime(start_date, '%Y-%m-%d') + timedelta(days=29), "%Y-%m-%d"),
                   all_hotels_str)
        begin = time.time()
        hotel_data = self.get_oracle_query_manager().read_sql(query_22)
        logger.info('hotel_data cost time %d  s rowsCount %d', time.time() - begin, hotel_data.size)

        hotel_data = pd.merge(hotel_data, srn_data, how='left', on=['date', 'oyo_id'])
        hotel_data = hotel_data.fillna(0)

        hotel_data['occ'] = hotel_data.used_rooms / hotel_data.sellable_rooms
        hotel_data['rem'] = hotel_data.sellable_rooms - hotel_data.used_rooms

        hotel_data.occ = hotel_data.occ.map(lambda x: np.where(pd.isnull(x) | (x == float("inf")), 0, x))
        logger.info('hotel_data process done')

        ## Cluster Trend Data Query ##

        query5 = '''
        select ca."date", h.oyo_id as oyo_id, ct.CITY_NAME as city,
        SUM(case when b.status in (0,1,2,4) then 1 else 0 end) as cluster_urn,
                 sum(case when (TRUNC("date") - trunc(fb.create_time) <= 29) or (TRUNC("date") - trunc(fb.create_time)= 30 and to_char(fb.create_time,'hh24:mi') >= to_char(sysdate,'hh24:mi')) then 1 else 0 end) as urn_30
            ,sum(case when (b.status in (0,1,2,4) and TRUNC("date") - trunc(fb.create_time) <= 28) or (b.status in (0,1,2,4) and TRUNC("date") - trunc(fb.create_time)= 29 and to_char(fb.create_time,'hh24:mi') >= to_char(sysdate,'hh24:mi')) then 1 else 0 end) as urn_29
            ,sum(case when (b.status in (0,1,2,4) and TRUNC("date") - trunc(fb.create_time) <= 27) or (b.status in (0,1,2,4) and TRUNC("date") - trunc(fb.create_time)= 28 and to_char(fb.create_time,'hh24:mi') >= to_char(sysdate,'hh24:mi')) then 1 else 0 end) as urn_28
            ,sum(case when (b.status in (0,1,2,4) and TRUNC("date") - trunc(fb.create_time) <= 26) or (b.status in (0,1,2,4) and TRUNC("date") - trunc(fb.create_time)= 27 and to_char(fb.create_time,'hh24:mi') >= to_char(sysdate,'hh24:mi')) then 1 else 0 end) as urn_27
            ,sum(case when (b.status in (0,1,2,4) and TRUNC("date") - trunc(fb.create_time) <= 25) or (b.status in (0,1,2,4) and TRUNC("date") - trunc(fb.create_time)= 26 and to_char(fb.create_time,'hh24:mi') >= to_char(sysdate,'hh24:mi')) then 1 else 0 end) as urn_26
            ,sum(case when (b.status in (0,1,2,4) and TRUNC("date") - trunc(fb.create_time) <= 24) or (b.status in (0,1,2,4) and TRUNC("date") - trunc(fb.create_time)= 25 and to_char(fb.create_time,'hh24:mi') >= to_char(sysdate,'hh24:mi')) then 1 else 0 end) as urn_25
            ,sum(case when (b.status in (0,1,2,4) and TRUNC("date") - trunc(fb.create_time) <= 23) or (b.status in (0,1,2,4) and TRUNC("date") - trunc(fb.create_time)= 24 and to_char(fb.create_time,'hh24:mi') >= to_char(sysdate,'hh24:mi')) then 1 else 0 end) as urn_24
            ,sum(case when (b.status in (0,1,2,4) and TRUNC("date") - trunc(fb.create_time) <= 22) or (b.status in (0,1,2,4) and TRUNC("date") - trunc(fb.create_time)= 23 and to_char(fb.create_time,'hh24:mi') >= to_char(sysdate,'hh24:mi')) then 1 else 0 end) as urn_23
            ,sum(case when (b.status in (0,1,2,4) and TRUNC("date") - trunc(fb.create_time) <= 21) or (b.status in (0,1,2,4) and TRUNC("date") - trunc(fb.create_time)= 22 and to_char(fb.create_time,'hh24:mi') >= to_char(sysdate,'hh24:mi')) then 1 else 0 end) as urn_22
            ,sum(case when (b.status in (0,1,2,4) and TRUNC("date") - trunc(fb.create_time) <= 20) or (b.status in (0,1,2,4) and TRUNC("date") - trunc(fb.create_time)= 21 and to_char(fb.create_time,'hh24:mi') >= to_char(sysdate,'hh24:mi')) then 1 else 0 end) as urn_21
            ,sum(case when (b.status in (0,1,2,4) and TRUNC("date") - trunc(fb.create_time) <= 19) or (b.status in (0,1,2,4) and TRUNC("date") - trunc(fb.create_time)= 20 and to_char(fb.create_time,'hh24:mi') >= to_char(sysdate,'hh24:mi')) then 1 else 0 end) as urn_20
            ,sum(case when (b.status in (0,1,2,4) and TRUNC("date") - trunc(fb.create_time) <= 18) or (b.status in (0,1,2,4) and TRUNC("date") - trunc(fb.create_time)= 19 and to_char(fb.create_time,'hh24:mi') >= to_char(sysdate,'hh24:mi')) then 1 else 0 end) as urn_19
            ,sum(case when (b.status in (0,1,2,4) and TRUNC("date") - trunc(fb.create_time) <= 17) or (b.status in (0,1,2,4) and TRUNC("date") - trunc(fb.create_time)= 18 and to_char(fb.create_time,'hh24:mi') >= to_char(sysdate,'hh24:mi')) then 1 else 0 end) as urn_18
            ,sum(case when (b.status in (0,1,2,4) and TRUNC("date") - trunc(fb.create_time) <= 16) or (b.status in (0,1,2,4) and TRUNC("date") - trunc(fb.create_time)= 17 and to_char(fb.create_time,'hh24:mi') >= to_char(sysdate,'hh24:mi')) then 1 else 0 end) as urn_17
            ,sum(case when (b.status in (0,1,2,4) and TRUNC("date") - trunc(fb.create_time) <= 15) or (b.status in (0,1,2,4) and TRUNC("date") - trunc(fb.create_time)= 16 and to_char(fb.create_time,'hh24:mi') >= to_char(sysdate,'hh24:mi')) then 1 else 0 end) as urn_16
            ,sum(case when (b.status in (0,1,2,4) and TRUNC("date") - trunc(fb.create_time) <= 14) or (b.status in (0,1,2,4) and TRUNC("date") - trunc(fb.create_time)= 15 and to_char(fb.create_time,'hh24:mi') >= to_char(sysdate,'hh24:mi')) then 1 else 0 end) as urn_15
            ,sum(case when (b.status in (0,1,2,4) and TRUNC("date") - trunc(fb.create_time) <= 13) or (b.status in (0,1,2,4) and TRUNC("date") - trunc(fb.create_time)= 14 and to_char(fb.create_time,'hh24:mi') >= to_char(sysdate,'hh24:mi')) then 1 else 0 end) as urn_14
            ,sum(case when (b.status in (0,1,2,4) and TRUNC("date") - trunc(fb.create_time) <= 12) or (b.status in (0,1,2,4) and TRUNC("date") - trunc(fb.create_time)= 13 and to_char(fb.create_time,'hh24:mi') >= to_char(sysdate,'hh24:mi')) then 1 else 0 end) as urn_13
            ,sum(case when (b.status in (0,1,2,4) and TRUNC("date") - trunc(fb.create_time) <= 11) or (b.status in (0,1,2,4) and TRUNC("date") - trunc(fb.create_time)= 12 and to_char(fb.create_time,'hh24:mi') >= to_char(sysdate,'hh24:mi')) then 1 else 0 end) as urn_12
            ,sum(case when (b.status in (0,1,2,4) and TRUNC("date") - trunc(fb.create_time) <= 10) or (b.status in (0,1,2,4) and TRUNC("date") - trunc(fb.create_time)= 11 and to_char(fb.create_time,'hh24:mi') >= to_char(sysdate,'hh24:mi')) then 1 else 0 end) as urn_11
            ,sum(case when (b.status in (0,1,2,4) and TRUNC("date") - trunc(fb.create_time) <= 9 ) or (b.status in (0,1,2,4) and TRUNC("date") - trunc(fb.create_time)= 10 and to_char(fb.create_time,'hh24:mi') >= to_char(sysdate,'hh24:mi')) then 1 else 0 end) as urn_10
            ,sum(case when (b.status in (0,1,2,4) and TRUNC("date") - trunc(fb.create_time) <= 8 ) or (b.status in (0,1,2,4) and TRUNC("date") - trunc(fb.create_time)= 9  and to_char(fb.create_time,'hh24:mi') >= to_char(sysdate,'hh24:mi')) then 1 else 0 end) as urn_9 
            ,sum(case when (b.status in (0,1,2,4) and TRUNC("date") - trunc(fb.create_time) <= 7 ) or (b.status in (0,1,2,4) and TRUNC("date") - trunc(fb.create_time)= 8  and to_char(fb.create_time,'hh24:mi') >= to_char(sysdate,'hh24:mi')) then 1 else 0 end) as urn_8 
            ,sum(case when (b.status in (0,1,2,4) and TRUNC("date") - trunc(fb.create_time) <= 6 ) or (b.status in (0,1,2,4) and TRUNC("date") - trunc(fb.create_time)= 7  and to_char(fb.create_time,'hh24:mi') >= to_char(sysdate,'hh24:mi')) then 1 else 0 end) as urn_7 
            ,sum(case when (b.status in (0,1,2,4) and TRUNC("date") - trunc(fb.create_time) <= 5 ) or (b.status in (0,1,2,4) and TRUNC("date") - trunc(fb.create_time)= 6  and to_char(fb.create_time,'hh24:mi') >= to_char(sysdate,'hh24:mi')) then 1 else 0 end) as urn_6 
            ,sum(case when (b.status in (0,1,2,4) and TRUNC("date") - trunc(fb.create_time) <= 4 ) or (b.status in (0,1,2,4) and TRUNC("date") - trunc(fb.create_time)= 5  and to_char(fb.create_time,'hh24:mi') >= to_char(sysdate,'hh24:mi')) then 1 else 0 end) as urn_5 
            ,sum(case when (b.status in (0,1,2,4) and TRUNC("date") - trunc(fb.create_time) <= 3 ) or (b.status in (0,1,2,4) and TRUNC("date") - trunc(fb.create_time)= 4  and to_char(fb.create_time,'hh24:mi') >= to_char(sysdate,'hh24:mi')) then 1 else 0 end) as urn_4 
            ,sum(case when (b.status in (0,1,2,4) and TRUNC("date") - trunc(fb.create_time) <= 2 ) or (b.status in (0,1,2,4) and TRUNC("date") - trunc(fb.create_time)= 3  and to_char(fb.create_time,'hh24:mi') >= to_char(sysdate,'hh24:mi')) then 1 else 0 end) as urn_3 
            ,sum(case when (b.status in (0,1,2,4) and TRUNC("date") - trunc(fb.create_time) <= 1 ) or (b.status in (0,1,2,4) and TRUNC("date") - trunc(fb.create_time)= 2  and to_char(fb.create_time,'hh24:mi') >= to_char(sysdate,'hh24:mi')) then 1 else 0 end) as urn_2 
            ,sum(case when (b.status in (0,1,2,4) and TRUNC("date") - trunc(fb.create_time) <= 0 ) or (b.status in (0,1,2,4) and TRUNC("date") - trunc(fb.create_time)= 1  and to_char(fb.create_time,'hh24:mi') >= to_char(sysdate,'hh24:mi')) then 1 else 0 end) as urn_1 
            ,sum(case when b.status in (0,1,2,4) and  TRUNC("date") = trunc(fb.create_time) and to_char(fb.create_time,'hh24:mi') >= to_char(sysdate,'hh24:mi') then 1 else 0 end) as urn_0
           from OYO_DW.fact_room_reservation  b
           INNER JOIN oyo_dw.fact_booking fb
             ON b.booking_id = fb.id
          inner join OYO_DW.V_DIM_HOTEL  h
          on b.hotel_id = h.id --and h.status in (3) 
          LEFT join OYO_DW.dim_zones ct
          on h.cluster_id = ct.id
          cross join OYO_DW.DIM_CALENDAR  ca
          where TRUNC(ca."date") >= trunc(b.check_in)
          and TRUNC(ca."date") < trunc(b.check_out)
          and TRUNC(ca."date") >= to_date('{0}','yyyy-mm-dd') 
          and TRUNC(ca."date") <= to_date('{1}','yyyy-mm-dd') 
          and ct.city_name in {2}
          group by ca."date", h.oyo_id , ct.CITY_NAME
          order by 1,2
         '''.format(datetime.strftime(dt.datetime.strptime(start_date, '%Y-%m-%d') - timedelta(days=49), "%Y-%m-%d"),
                    datetime.strftime(dt.datetime.strptime(start_date, '%Y-%m-%d') - timedelta(days=1), "%Y-%m-%d"),
                    str(city_list))

        begin = time.time()
        cluster_data_new = self.get_oracle_query_manager().read_sql(query5, 60)
        logger.info('cluster_data cost time %d  s rowsCount %d', time.time() - begin, cluster_data_new.size)
        logger.info('cluster_data process done')

        cluster_data_new.city = ''

        ###   SOURCE_NAME IN ('OTA','APP')   选取过去4周的线上价格
        sql007 = '''
        SELECT  to_date(DATE_S, 'yyyy-mm-dd') AS "DATE"
                , oyo_id 
                , sum(TOT_ROOMS) AS HOTEL_URN
                , sum(PER_DAY_GMV) AS GMV
        FROM OYO_DW.V_FACT_CHINA_REVENUE 
        WHERE HOTEL_STATUS IN  ('Live','Active')
        AND SOURCE_NAME IN ('OTA','APP')
        AND DATE_S >= TO_CHAR(TRUNC(SYSDATE - 28), 'yyyymmdd')
        AND DATE_S <= TO_CHAR(TRUNC(SYSDATE - 1), 'yyyymmdd')
        GROUP BY DATE_S, oyo_id 
        '''
        begin = time.time()
        hotel_data_all_new = self.get_oracle_query_manager().read_sql(sql007, 60)
        logger.info('hotel_data_all_new cost time %d  s rowsCount %d', time.time() - begin, hotel_data_all_new.size)

        # =============================================================================
        # sold_outs = pd.read_sql('''select distinct hs.oyo_id
        #    from hotels hs LEFT JOIN hotel_restrictions hr ON hs.id = hr.hotel_id LEFT JOIN restrictions rs ON hr.restriction_id = rs.id
        #    where (current_date between from_time and to_time)
        #    and hs.status in (1,2)
        #    and hr.status = 1
        #    and hr.restriction_id IN (29,30,32) and oyo_id like '%CN_%'
        #    ''',con)
        # =============================================================================

        sql09 = '''SELECT a.oyo_id--,a.category
               ,a.latitude,a.longitude,b.city_name as city
               FROM oyo_dw.v_DIM_HOTEL a
               inner join oyo_dw.dim_zones b
               on a.cluster_id=b.id
               where city_name in {0}
         '''.format(str(city_list))

        begin = time.time()
        hotel_data_lat_long_all = self.get_oracle_query_manager().read_sql(sql09)
        logger.info('hotel_data_lat_long_all cost time %d  s rowsCount %d', time.time() - begin,
                    hotel_data_lat_long_all.size)

        hotel_data_lat_long_all.latitude = hotel_data_lat_long_all.latitude.astype('float64')
        hotel_data_lat_long_all.longitude = hotel_data_lat_long_all.longitude.astype('float64')

        sql011 = '''  SELECT H.OYO_ID, 
               --H.CATEGORY,
               H.LATITUDE, 
               H.LONGITUDE, 
               CL.CITY_NAME AS city 
          FROM OYO_DW.v_dim_HOTEL H
         INNER JOIN OYO_DW.DIM_ZONES CL
            ON H.CLUSTER_ID = CL.id
         WHERE H.OYO_ID IN {0} '''.format(all_hotels_str)

        begin = time.time()
        hotel_data_lat_long_all_prop = self.get_oracle_query_manager().read_sql(sql011)
        logger.info('hotel_data_lat_long_all_prop cost time %d  s rowsCount %d', time.time() - begin,
                    hotel_data_lat_long_all_prop.size)

        hotel_data_lat_long_all_prop.latitude = hotel_data_lat_long_all_prop.latitude.astype('float64')
        hotel_data_lat_long_all_prop.longitude = hotel_data_lat_long_all_prop.longitude.astype('float64')

        # =============================================================================

        sql_tail = '''SELECT oyo_id
            ,upper(cluster_name) cluster_name
            , upper(city_name) city_name
            ,upper(hub_name) hub_name
            ,a.CATEGORY AS prop_category
        from OYO_DW.v_dim_HOTEL H 
        LEFT JOIN   OYO_DW.DIM_ZONES a ON h.CLUSTER_ID = a.ID
        where H.oyo_id in {0} '''.format(all_hotels_str)

        begin = time.time()
        prop_details = self.get_oracle_query_manager().read_sql(sql_tail)
        logger.info('prop_details cost time %d  s rowsCount %d', time.time() - begin, prop_details.size)

        # =============================================================================

        smart_hotel_params_df = pd.DataFrame()
        complete_raw = pd.DataFrame()

        logger.info('smart_hotel_data size =%d', len(smart_hotel_oyo_id_list))
        logger.info('##############smart_hotel_data loop process  start##############')

        for smart_hotel_oyo_id in smart_hotel_oyo_id_list:
            logger.info('smart hotel with oyo id: %s start process', str(smart_hotel_oyo_id))

            ##################### Estimating competitor set #######################
            city_name = list(smart_hotel_data[smart_hotel_data.oyo_id == smart_hotel_oyo_id].city.unique())
            # 用smart_hotel的城市 筛选数据 ，
            logger.info('##########用smart_hotel的城市 筛选数据##########')
            hotel_data_lat_long = hotel_data_lat_long_all[hotel_data_lat_long_all.city.map(lambda x: x in city_name)]
            hotel_data_non_smart = hotel_data_lat_long[hotel_data_lat_long.oyo_id != smart_hotel_oyo_id]

            # Selected property Lat Long Details
            smart_lat = \
                hotel_data_lat_long_all_prop[hotel_data_lat_long_all_prop.oyo_id == smart_hotel_oyo_id].latitude.iloc[0]
            smart_long = \
                hotel_data_lat_long_all_prop[hotel_data_lat_long_all_prop.oyo_id == smart_hotel_oyo_id].longitude.iloc[
                    0]

            smart_lat_rad = smart_lat * math.pi / 180
            smart_long_rad = smart_long * math.pi / 180

            # Dergree to radian for selected hotels
            hotel_data_non_smart['long_rad'] = hotel_data_non_smart.longitude * math.pi / 180
            hotel_data_non_smart['lat_rad'] = hotel_data_non_smart.latitude * math.pi / 180
            logger.info('##########Calculating distance of selected hotel from every other properties##########')
            # Calculating distance of selected hotel from every other properties
            R = 6371.0
            hotel_data_non_smart['dist'] = np.arccos(
                np.sin(hotel_data_non_smart.lat_rad)
                * np.sin(smart_lat_rad)
                + np.cos(hotel_data_non_smart.lat_rad)
                * np.cos(smart_lat_rad)
                * np.cos(smart_long_rad - hotel_data_non_smart.long_rad)
            ) * R

            # min(hotel_data_non_smart$dist)
            # Sorting the distance from closest to farthest
            a_hotels = hotel_data_non_smart.sort_values(axis=0, by=['dist'], ascending=True)

            # Selecting size of properties in a city if its less than 20 then total properties else 20
            size = min(5, len(hotel_data_non_smart.oyo_id))
            ###############
            '''
            radius = a_hotels[10]
            '''
            radius = a_hotels.dist.iloc[size - 1]
            # List of hotels nearbuy
            logger.info('##########List of hotels nearbuy##########')
            clus_hotel_list = hotel_data_non_smart[hotel_data_non_smart.dist <= radius].oyo_id.tolist()

            # Adding the selected property in the list
            clus_hotel_list.append(smart_hotel_oyo_id)
            # past_data last 14 days data of URN, SRN and GMV of the selected hotel

            past_data_hotel = past_data[past_data.oyo_id == smart_hotel_oyo_id]
            ####Condition 1####
            # checking if property is active for last 14 days
            logger.info('##########checking if property is active for last 14 days##########')

            if past_data_hotel.shape[0] == 14:
                logger.info('########past_data_hotel.shape[0] == 14########')
                # arr_list means the selected property
                arr_list = smart_hotel_oyo_id
                # past_data_hotel_sum_temp is just givin us days
                tt01 = datetime.strftime(datetime.fromtimestamp(time.time()), "%Y-%m-%d")
                tt02 = datetime.strftime(datetime.fromtimestamp(time.time()) + timedelta(days=6), "%Y-%m-%d")
                past_data_hotel_sum_temp = pd.DataFrame(pd.date_range(tt01, tt02, freq='D'), columns=['date'])
                past_data_hotel_sum_temp['day'] = past_data_hotel_sum_temp.date.map(lambda x: x.weekday())
                # del past_data_hotel_sum_temp['date']

                # adding URN, SRN and GMV on the Days
                past_data_hotel_sum = past_data_hotel.groupby(by=['day', 'oyo_id'], as_index=False).sum()

                # arranging to days
                past_data_hotel_sum = pd.merge(past_data_hotel_sum_temp, past_data_hotel_sum, how='left', on=['day'])

                # adding Occ
                past_data_hotel_sum['occ'] = past_data_hotel_sum.used_rooms / past_data_hotel_sum.sellable_rooms
                # adding ARR
                past_data_hotel_sum['arr'] = past_data_hotel_sum.gmv / past_data_hotel_sum.used_rooms

                # Removing Errors
                past_data_hotel_sum.occ = past_data_hotel_sum.occ.map(lambda x: np.where(pd.isnull(x), 0, x))
                past_data_hotel_sum.arr = past_data_hotel_sum.arr.map(lambda x: np.where(pd.isnull(x), 0, x))

                # last two weeks occ and ARR wrt days
                hotel_past_occ = past_data_hotel_sum[['day', 'occ', 'arr']]
                # past_data_hotel_sum = select(past_data_hotel_sum,day,tau)
                # Tagging property age old becz it has data for last 14 days

                past_data_hotel_sum['prop_age'] = "old"
                # Selecting day,occ,age in this case age is old
                past_data_hotel_sum = past_data_hotel_sum[['day', 'occ', 'arr', 'prop_age']]

                ####Condition 1 Result Day,Occ,Age####
                ####Condition 2 it means property is new. i.e not active for more than 14 days####
            else:
                logger.info(
                    '########arr_list is all the properties which are close by to the selected property. Including the selected properties########')
                # here arr_list is all the properties which are close by to the selected property. Including the selected properties
                arr_list = clus_hotel_list

                # Category of the property
                loop_category = prop_details[prop_details.oyo_id == smart_hotel_oyo_id].prop_category.iloc[0]

                # Selecting the days of the week
                tt01 = datetime.strftime(datetime.fromtimestamp(time.time()), "%Y-%m-%d")
                tt02 = datetime.strftime(datetime.fromtimestamp(time.time()) + timedelta(days=6), "%Y-%m-%d")

                past_data_hotel_sum = pd.DataFrame(pd.date_range(tt01, tt02, freq='D'), columns=['date'])

                past_data_hotel_sum['day'] = past_data_hotel_sum.date.map(lambda x: x.weekday())

                past_data_hotel_sum['tau'] = 1
                # Selecting URN, SRN and GMV of the properties which are nearbuy

                past_data_cluster = past_data[past_data.oyo_id.isin(arr_list)]

                past_data_cluster_sum = past_data_cluster.groupby(by=['day'], as_index=False).sum()

                past_data_cluster_sum['occ'] = past_data_cluster_sum.used_rooms / past_data_cluster_sum.sellable_rooms
                past_data_cluster_sum['arr'] = past_data_cluster_sum.gmv / past_data_cluster_sum.used_rooms

                past_data_hotel_sum = pd.merge(past_data_hotel_sum, past_data_cluster_sum[['occ', 'arr', 'day']],
                                               how='left', on=['day'])
                # Occ ARR Day of the properties nearbuy

                if loop_category == 2:
                    past_data_hotel_sum.tau = 1.2
                # past_data_hotel_sum = select(past_data_hotel_sum,day,tau)
                # past_data_hotel_sum$occ = 1
                # Tagging Property AGE to be new
                past_data_hotel_sum['prop_age'] = "new"
                past_data_hotel_sum = past_data_hotel_sum[['day', 'occ', 'arr', 'prop_age']]
                # Nearbuy Hotels Combines Property level day wise Occ and ARR with New age tag
                hotel_past_occ = past_data_hotel_sum.copy()
                hotel_past_occ.prop_age = ''
                hotel_past_occ.tau = ''

            ####Condition 2 Ends with Nearbuy properties Day level occ,ARR for last 2 weeks.####

            # Last 2 weeks occ, ARR data of the hotel on day level
            hotel_past_occ = hotel_past_occ.rename(columns={'occ': 'past_occ'})
            hotel_past_occ = hotel_past_occ.rename(columns={'arr': 'past_arr'})

            # Select the selected hotels. Hotel_data_all_new contains URN And GMV
            hotel_data_all = hotel_data_all_new[hotel_data_all_new.oyo_id.map(lambda x: x in arr_list)]

            # We calculated the ARR
            hotel_data_all['revenue'] = hotel_data_all.gmv
            hotel_data_all['clus_arr'] = hotel_data_all.revenue / hotel_data_all.hotel_urn
            # Added Days
            hotel_data_all['day'] = hotel_data_all.DATE.map(lambda x: x.weekday())

            # Day level Revenue and URN of a Hotel
            clus_data = hotel_data_all.groupby(by=['day'], as_index=False).sum()[['day', 'revenue', 'hotel_urn']]

            # Calculated the ARR
            clus_data['clus_arr'] = clus_data.revenue / clus_data.hotel_urn

            clus_data_arr = clus_data.copy()

            # Found the base - i.e Day level ARR
            clus_data_arr['base'] = clus_data_arr.clus_arr
            # clus_data_arr['base'] = past_data_hotel_sum.arr

            ############################### Calculating surge in arr for base (aggressive growth) ############################

            # Joining 4 weeks average arr and occ with 14 days occ and age
            clus_data_arr = pd.merge(clus_data_arr, past_data_hotel_sum, how='left', on=['day'])

            ###################Logic Starts#########################
            # We calculated tau here if occ is less than 50% then base price is always is always less than the present ARR which means tau less than 1
            clus_data_arr['tau'] = 1

            tem_list11 = pd.Series(clus_data_arr.index)
            clus_data_arr['tau'] = tem_list11.map(
                lambda i: np.where(((clus_data_arr.occ[i] < 0.65) & (clus_data_arr.occ[i] > 0.5)),
                                   ((95 - 90) / (65 - 50)) * (clus_data_arr.occ[i] - 0.65) + 0.98,
                                   clus_data_arr['tau'][i]))
            clus_data_arr['tau'] = tem_list11.map(
                lambda i: np.where((clus_data_arr.occ[i] < 0.5), 1, clus_data_arr['tau'][i]))
            clus_data_arr['tau'] = tem_list11.map(lambda i: np.where((clus_data_arr.occ[i] >= 0.75),
                                                                     np.where(clus_data_arr.base[i] <= 1000, 1.10,
                                                                              1.10), clus_data_arr['tau'][i]))
            clus_data_arr['tau'] = tem_list11.map(lambda i: np.where((clus_data_arr.occ[i] >= 0.8),
                                                                     np.where(clus_data_arr.base[i] <= 1000, 1.125,
                                                                              1.125), clus_data_arr['tau'][i]))
            clus_data_arr['tau'] = tem_list11.map(lambda i: np.where((clus_data_arr.occ[i] >= 0.9),
                                                                     np.where(clus_data_arr.base[i] <= 1000, 1.15,
                                                                              1.15), clus_data_arr['tau'][i]))
            clus_data_arr['tau'] = tem_list11.map(
                lambda i: np.where(clus_data_arr.prop_age[i] == "new", 1, clus_data_arr['tau'][i]))

            # clus_data_arr$tau = ifelse(loop %in% c('MUM651','KOL088','DEL889','PUN111','BLR699','MUM647','MUM602','NOD325','MUM640','PUN337','MUM280','MUM592','MUM661','MUM540','MUM017','BLR820','BLR775','BLR801','MUM631','PUN351'),pmax(1.15,clus_data_arr$tau),clus_data_arr$tau)
            # clus_data_arr$tau = ifelse(loop %in% c('MUM660'),pmax(1.25,clus_data_arr$tau),clus_data_arr$tau)

            clus_data_arr.prop_age = ''
            ############################### Calculating surge in arr for base (aggressive growth) ############################
            mu = 0

            ##Here we added base = Tau*base, Earlier the base was just the Day level last 4 week ARR
            clus_data_arr['base'] = clus_data_arr.tau * clus_data_arr['base'] + mu

            # All the nearbuy properties cluster
            cluster_data = cluster_data_new[cluster_data_new.oyo_id.isin(clus_hotel_list)]

            # Adding Days
            cluster_data['day'] = cluster_data.date.map(lambda x: x.weekday())

            # On day level urn for 30 days
            cluster_sum = cluster_data.groupby(by=['day'], as_index=False).sum()
            # occ number will tell us how many booking were made before the date of stay.

            for x in range(31):
                cluster_sum['occ_' + str(x)] = 1 - cluster_sum['urn_' + str(x)] / cluster_sum.cluster_urn

            tt02 = datetime.strftime(
                datetime.fromtimestamp(time.mktime(time.strptime(start_date, '%Y-%m-%d'))) + timedelta(days=30),
                "%Y-%m-%d")
            clus_occ = pd.DataFrame(pd.date_range(start_date, tt02, freq='D'), columns=['date'])

            clus_occ['day'] = clus_occ.date.map(lambda x: x.weekday())

            clus_occ = pd.merge(clus_occ, cluster_sum, how='left', on=['day'])
            clus_occ['occupancy'] = 0.0

            start_date = start_date
            end_date = datetime.strftime(
                datetime.fromtimestamp(time.mktime(time.strptime(start_date, '%Y-%m-%d'))) + timedelta(days=30),
                "%Y-%m-%d")

            for i in list(clus_occ.index):
                date_loop = str(clus_occ.date[i])[:10]
                date1 = time.strptime(date_loop, "%Y-%m-%d")
                date2 = time.strptime(start_date, "%Y-%m-%d")
                date11 = datetime(date1[0], date1[1], date1[2])
                date22 = datetime(date2[0], date2[1], date2[2])
                diff = (date11 - date22).days
                clus_occ['occupancy'][i] = clus_occ["occ_" + str(diff)][i]

            clus_occ_trend = clus_occ[['date', 'day', 'occupancy']]
            clus_occ_trend = clus_occ_trend.rename(columns={'occupancy': 'clus_occ'})
            clus_occ_trend.date = clus_occ_trend.date.map(lambda x: str(x)[:10])

            data_temp = hotel_data[['date', 'oyo_id', 'rem', 'occ', 'sellable_rooms']]
            data_temp = data_temp[data_temp.oyo_id == smart_hotel_oyo_id]
            data_temp.date = data_temp.date.map(lambda x: str(x)[:10])

            hotel_price_data = pd.merge(data_temp, clus_occ_trend[['date', 'day', 'clus_occ']], how='left', on=['date'])
            hotel_price_data = pd.merge(hotel_price_data, clus_data_arr[['day', 'clus_arr', 'base', 'tau']], how='left',
                                        on=['day'])

            ## Input Base ##
            if input_base.shape[0] > 0:
                input_base.date = input_base.date.map(lambda x: str(x)[:10])
                hotel_price_data = pd.merge(hotel_price_data, input_base, how='left', on=['date', 'oyo_id'])
                tem_list22 = pd.Series(hotel_price_data.index)
                hotel_price_data['base'] = tem_list22.map(
                    lambda i: np.where(pd.isnull(hotel_price_data.new_base[i]) == False,
                                       hotel_price_data.new_base[i], hotel_price_data.base[i]))
                hotel_price_data.new_base = ''

            hotel_price_data = pd.merge(hotel_price_data, hotel_past_occ, how='left', on=['day'])

            hotel_price_data['occ_ftd_target'] = hotel_price_data.past_occ.map(lambda x: min(0.85, 0.85))

            # hotel_price_data$occ_ftd_target = pmin(pmax(hotel_price_data$past_occ*1.2,hotel_price_data$past_occ+0.15),1)
            hotel_price_data['tg_current'] = hotel_price_data.clus_occ * hotel_price_data.occ_ftd_target
            hotel_price_data['delta'] = hotel_price_data.occ - (
                    hotel_price_data.clus_occ * hotel_price_data.occ_ftd_target)

            # Correction for Positive Increment in prices #

            hotel_price_data.delta = hotel_price_data.delta.map(lambda x: np.where(x >= 0, 1 * x, x))

            # hotel_price_data$delta = ifelse(hotel_price_data$delta>=0.5 & hotel_price_data$date == b, 0.5,hotel_price_data$delta)
            tem_list33 = pd.Series(hotel_price_data.index)

            hotel_price_data.delta = tem_list33.map(lambda x: np.where(
                (hotel_price_data.delta[x] >= 0.2) & (hotel_price_data.date[x] == start_date), 0.2,
                hotel_price_data.delta[x]))

            # hotel_price_data$delta = ifelse(hotel_price_data$delta>=0.3 & hotel_price_data$date >= b+1, 0.3,hotel_price_data$delta)
            temp_b1 = datetime.strftime(
                datetime.fromtimestamp(time.mktime(time.strptime(start_date, '%Y-%m-%d'))) + timedelta(
                    days=1),
                "%Y-%m-%d")
            hotel_price_data.delta = tem_list33.map(lambda x: np.where(
                (hotel_price_data.delta[x] >= 0.2) & (hotel_price_data.date[x] >= temp_b1), 0.2,
                hotel_price_data.delta[x]))

            hotel_price_data['clus_occ'] = tem_list33.map(
                lambda x: np.where((hotel_price_data.clus_occ[x] == 0) | pd.isnull(hotel_price_data.clus_occ[x]),
                                   0.000001, hotel_price_data.clus_occ[x]))
            # Correction for Negative Increment in prices #
            # hotel_price_data$limit = ifelse(hotel_price_data$date == b,0.3,0.25)
            hotel_price_data['limit'] = hotel_price_data.date.map(lambda x: np.where(x == start_date, 0.3, 0.3))

            # In[0] 2天温柔版    occ_ftd_target  0.65
            # =============================================================================
            #    hotel_price_data['delta'] = tem_list33.map(lambda x: np.where(hotel_price_data.occ[x]/hotel_price_data.clus_occ[x]<=0.3,
            #                    -0.3,hotel_price_data.delta[x]))
            #    hotel_price_data['delta'] = tem_list33.map(lambda x: np.where(0.3<hotel_price_data.occ[x]/hotel_price_data.clus_occ[x]<=0.65,
            #                    ((hotel_price_data.occ[x]/hotel_price_data.clus_occ[x])*5/7 -0.51),hotel_price_data.delta[x]))
            # In[1]  2天激进版   occ_ftd_target  0.85  ota 0.25

            def gen_delta_with_for_kc(occ, cluster_occ):
                if MiscUtil.is_empty_value(occ) or MiscUtil.is_empty_value(cluster_occ):
                    return 0
                pred_occ = occ / cluster_occ
                if pred_occ < 0.4:
                    return -0.2
                elif 0.4 <= pred_occ < 0.75:
                    return -((-4 / 7) * pred_occ + 0.42857)
                elif 0.75 <= pred_occ < 0.9:
                    if occ >= 0.1:
                        return pred_occ - 0.75
                    else:
                        return 0
                else:
                    if occ >= 0.15:
                        return 0.2
                    else:
                        return 0

            # Key city 包头算法
            DFUtil.apply_func_for_df(hotel_price_data, 'delta', ['occ', 'clus_occ'],
                                     lambda values: gen_delta_with_for_kc(*values))

            # =============================================================================
            # hotel_price_data['delta'] = tem_list33.map(lambda x: np.where(hotel_price_data.date[x] in
            #                           ('2018-12-29','2018-12-30','2018-12-31'),0,hotel_price_data.delta[x]))

            temp_b2 = datetime.strftime(
                datetime.fromtimestamp(time.mktime(time.strptime(start_date, '%Y-%m-%d'))) + timedelta(days=30),
                "%Y-%m-%d")

            hotel_price_data['limit'] = tem_list33.map(lambda x: np.where(hotel_price_data.date[x] >= temp_b2,
                                                                          0, hotel_price_data.limit[x]))

            hotel_price_data['delta'] = tem_list33.map(
                lambda x: np.where(hotel_price_data.delta[x] <= -hotel_price_data.limit[x],
                                   -hotel_price_data.limit[x], hotel_price_data.delta[x]))

            hotel_price_data['price'] = (1 + hotel_price_data.delta) * hotel_price_data.base

            smart_hotel_params_df = pd.concat([smart_hotel_params_df, hotel_price_data[
                ['date', 'oyo_id', 'price', 'clus_occ', 'occ', 'sellable_rooms', 'rem', 'delta', 'base',
                 'clus_arr', 'past_occ', 'past_arr', 'occ_ftd_target']]], ignore_index=True)
            # result_final = rbind(result_final,select(hotel_price_data,date,oyo_id,city,price_new_single,clus_occ,occ,rem,delta,sellable_rooms,so_do_cat))
            complete_raw = pd.concat([complete_raw, hotel_price_data], ignore_index=True)

        smart_hotel_params_df = smart_hotel_params_df[
            (smart_hotel_params_df.price.isnull() == False) & (smart_hotel_params_df.price != float('inf'))]
        smart_hotel_params_df = smart_hotel_params_df.sort_values(axis=0, by=['date', 'oyo_id'], ascending=True)

        tt04 = datetime.strftime(
            datetime.fromtimestamp(time.mktime(time.strptime(start_date, '%Y-%m-%d'))) + timedelta(
                days=config.get_calc_days() - 1),
            "%Y-%m-%d")

        room_type_difference_query = """
            select oyo_id, room_type_id, difference_type, price_delta, price_multiplier, name as room_type_name
            from (select oyo_id, room_type_id, difference_type, price_delta, price_multiplier
                  from hotel_room_type_price_difference
                  where oyo_id in {0}) a
                   left join (
              select id, name
              from room_type
            ) b on a.room_type_id = b.id
        """.format(all_hotels_str)

        room_type_difference_df = self.get_mysql_query_manager().read_sql(room_type_difference_query)

        filtered_smart_hotel_params_df = smart_hotel_params_df[smart_hotel_params_df.date <= tt04]

        room_type_query = """
            select oyo_id, room_type_id
            from hotel_room_type
            where oyo_id in {0}
        """.format(all_hotels_str)

        room_type_df = self.get_mysql_query_manager().read_sql(room_type_query)

        filtered_smart_hotel_params_df = pd.merge(filtered_smart_hotel_params_df, room_type_df, on='oyo_id')

        DFUtil.print_data_frame(filtered_smart_hotel_params_df, 'result_final_2', True, 1000)

        filtered_smart_hotel_params_df = pd.merge(filtered_smart_hotel_params_df, room_type_difference_df, how='left',
                                                  on=['oyo_id', 'room_type_id'])

        filtered_smart_hotel_params_df = filtered_smart_hotel_params_df.sort_values(axis=0,
                                                                                    by=['date', 'oyo_id',
                                                                                        'room_type_id'],
                                                                                    ascending=True)

        DFUtil.apply_func_for_df(filtered_smart_hotel_params_df, 'pms_price',
                                 ['price', 'difference_type', 'price_delta', 'price_multiplier'],
                                 lambda values: round(PriceUtil.calc_room_type_difference(*values), 0))

        filtered_smart_hotel_params_df.price = round(filtered_smart_hotel_params_df.price, 0)
        filtered_smart_hotel_params_df.base = round(filtered_smart_hotel_params_df.base, 0)
        filtered_smart_hotel_params_df.clus_arr = round(filtered_smart_hotel_params_df.clus_arr, 0)
        filtered_smart_hotel_params_df.past_arr = round(filtered_smart_hotel_params_df.past_arr, 0)
        filtered_smart_hotel_params_df.clus_occ = round(filtered_smart_hotel_params_df.clus_occ, 2)
        filtered_smart_hotel_params_df.occ = round(filtered_smart_hotel_params_df.occ, 2)
        filtered_smart_hotel_params_df.delta = round(filtered_smart_hotel_params_df.delta, 2)
        filtered_smart_hotel_params_df.past_occ = round(filtered_smart_hotel_params_df.past_occ, 2)
        filtered_smart_hotel_params_df.occ_ftd_target = round(filtered_smart_hotel_params_df.occ_ftd_target, 2)

        logger.info('result_final round done')

        floor_price_query = """
            select oyo_id, room_type_id, price_type as floor_price_type, floor_price
            from hotel_floor_price
            where oyo_id in {0}
            and deleted = 0
        """.format(all_hotels_str)

        logger.info('start floor price query:\n%s', floor_price_query)

        floor_price_df = self.get_mysql_query_manager().read_sql(floor_price_query)

        logger.info('end floor price query')

        filtered_smart_hotel_params_df = pd.merge(filtered_smart_hotel_params_df, floor_price_df, how='left',
                                                  on=['oyo_id', 'room_type_id'])

        filtered_smart_hotel_params_df = DFUtil.apply_func_for_df(filtered_smart_hotel_params_df, 'pms_price',
                                                                  ['pms_price', 'floor_price_type', 'floor_price'],
                                                                  lambda values: PriceUtil.floor_price_check(*values))

        ceiling_price_query = """
            select oyo_id, room_type_id, price_type as ceiling_price_type, ceiling_price
            from hotel_ceiling_price
            where oyo_id in {0}
            and deleted = 0
        """.format(all_hotels_str)

        logger.info('start ceiling price query:\n%s', ceiling_price_query)

        ceiling_price_df = self.get_mysql_query_manager().read_sql(ceiling_price_query)

        logger.info('end ceiling price query')

        filtered_smart_hotel_params_df = pd.merge(filtered_smart_hotel_params_df, ceiling_price_df, how='left',
                                                  on=['oyo_id', 'room_type_id'])

        filtered_smart_hotel_params_df = DFUtil.apply_func_for_df(filtered_smart_hotel_params_df, 'pms_price',
                                                                  ['pms_price', 'ceiling_price_type', 'ceiling_price'],
                                                                  lambda values: PriceUtil.ceiling_price_check(*values))

        filtered_smart_hotel_params_df['hourly_price'] = round(filtered_smart_hotel_params_df.pms_price * 0.6, 0)

        DFUtil.print_data_frame(filtered_smart_hotel_params_df, 'result_final_2 after hourly price calc', True)

        prices_for_all_room_type_df = filtered_smart_hotel_params_df[
            ['date', 'oyo_id', 'room_type_id', 'room_type_name', 'pms_price', 'hourly_price']]

        #################################################################################################################

        sql009 = '''
            SELECT OYO_ID,UNIQUE_CODE, NAME AS hotel_name
            FROM OYO_DW.v_DIM_HOTEL
            WHERE status IN (2,3)
            GROUP BY OYO_ID,UNIQUE_CODE,NAME'''

        begin = time.time()
        data_code = self.get_oracle_query_manager().read_sql(sql009)
        logger.info('Data_code cost time %d s rowsCount %d', time.time() - begin, data_code.size)

        prices_for_all_room_type_df = pd.merge(prices_for_all_room_type_df, data_code, how='left', on=['oyo_id'])

        mg_end_date_query = """
            select oyo_id, end_date as pricing_end_date
            from oyo_dw.DIM_CHINA_MG_HOTELS
            where oyo_id in {0}
        """.format(all_hotels_str)

        mg_end_date_df = self.get_oracle_query_manager().read_sql(mg_end_date_query)

        prices_for_all_room_type_df = pd.merge(prices_for_all_room_type_df, mg_end_date_df, how='left', on='oyo_id')

        def mg_date_check(date, pricing_end_date):
            d1 = pd.to_datetime(str(date))
            d2 = pd.to_datetime(str(pricing_end_date))
            return d1 <= d2

        prices_for_all_room_type_df = prices_for_all_room_type_df[
            prices_for_all_room_type_df[['date', 'pricing_end_date']].apply(lambda values: mg_date_check(*values),
                                                                            axis=1)]

        if prices_for_all_room_type_df.shape[0] <= 0:
            LogUtil.get_cur_logger().info('no valid hotels needs pricing, return early')
            return None

        prices_for_all_room_type_df = prices_for_all_room_type_df.sort_values(axis=0, by=['date', 'oyo_id'],
                                                                              ascending=True)

        pms_prices = prices_for_all_room_type_df.copy()

        set_min_price(pms_prices, ['pms_price'])

        ############################################################################################################

        frame = hotel_data.drop_duplicates(['oyo_id', 'zone_name'])[['oyo_id', 'zone_name']]

        all_prices_df = pd.merge(prices_for_all_room_type_df, frame, how='left', on=['oyo_id'])

        ota_channel_map = FIVE_CHANNELS_MAP

        all_prices_df = PricingPipeline.pipe_join_ota_for_pms_prices(all_prices_df, self.get_mysql_query_manager(),
                                                                     self.get_oracle_query_manager(), ota_channel_map,
                                                                     None, all_hotels_str)

        # ==============================send mail start===============================================

        intermediate_result_df = smart_hotel_params_df.copy()

        pms_mail_prices_df = pms_prices.copy()
        pms_mail_prices_df.drop(['hotel_id', 'pricing_end_date'], axis=1, inplace=True)
        pms_mail_prices_df["create_time"] = DateUtil.stamp_to_date_format(time.time())

        logger.info('prepare data send mail start')

        # 发送邮件1，2，3
        set_min_price(all_prices_df,
                      ['pms_price', 'ctrip_post_sell_price', 'ctrip_pre_sell_price', 'ctrip_pre_net_price',
                       'meituan_post_sell_price', 'meituan_pre_sell_price',
                       'fliggy_pre_sell_price',
                       'elong_post_sell_price', 'elong_pre_sell_price', 'elong_pre_net_price',
                       'qunar_post_sell_price', 'qunar_pre_sell_price'])

        ota_final_prices_df = all_prices_df[
            ['date', 'oyo_id', 'hotel_name', 'zone_name', 'room_type_id', 'room_type_name', 'pms_price', 'hourly_price',
             'ctrip_room_type_name', 'ctrip_post_sell_price', 'ctrip_post_commission', 'ctrip_pre_sell_price',
             'ctrip_pre_net_price', 'ctrip_pre_commission',
             'meituan_room_type_name', 'meituan_post_sell_price', 'meituan_pre_sell_price',
             'fliggy_room_type_name', 'fliggy_pre_sell_price',
             'elong_room_type_name', 'elong_post_sell_price', 'elong_pre_sell_price', 'elong_pre_net_price',
             'elong_pre_commission',
             'qunar_room_type_name', 'qunar_post_sell_price', 'qunar_pre_sell_price', 'qunar_pre_commission']]

        send_mail.send_mail_for_ota_prices(config, ota_final_prices_df)

        set_min_price(intermediate_result_df, ['price'])

        send_mail.send_mail_for_final_result(config, intermediate_result_df)

        send_mail.send_mail_for_pms_price(config, pms_mail_prices_df)

        ota_plugin_dfs = PricingPipeline.compose_ota_plugin_v2_data_from_pms_prices(all_prices_df)

        send_mail.send_mail_for_ota_plugin_v2_result(config, ota_plugin_dfs, job_start_time_str, batch_order)

        # ==============================send mail end===============================================

        # ==============================priceInsert start===============================================

        hotel_id_with_oyo_id = """
            select id, oyo_id, cluster_id from OYO_DW.V_DIM_HOTEL
        """

        hotel_id_with_oyo_id_df = self.get_oracle_query_manager().read_sql(hotel_id_with_oyo_id)

        pms_prices = pd.merge(pms_prices, hotel_id_with_oyo_id_df, how='left', on=['oyo_id'])

        # ==============================priceInsert start===============================================

        insert_to_pms_df = pms_prices.rename(
            columns={'hotel_id': 'id', 'room_type_id': 'room_type', 'pms_price': 'final_price'})

        # price_insert-HotelPrice-发送邮件4
        pricei.batch_insert_pms_price_to_crs_and_send_mail(config, insert_to_pms_df)

        # price_log-FranchiseV1InterResult-发送邮件5
        intermediate_result_df["strategy_type"] = "KC_MODEL_02"
        pricel.report_franchise_v1_inter_result_and_send_mail(config, intermediate_result_df)

        zone_query = """
            select id as cluster_id, zone_name
            from oyo_dw.dim_zones
        """

        zone_df = self.get_oracle_query_manager().read_sql(zone_query)

        pms_prices = pd.merge(pms_prices, zone_df, on='cluster_id')

        # price_log-PriceReport-发送邮件6
        pms_prices["strategy_type"] = "KC_MODEL_02"
        pricel.report_price_and_send_mail(config, pms_prices)

        # price_log-OtaPriceReport-发送邮件7
        all_prices_df["strategy_type"] = "KC_MODEL_02"
        pricel.report_ota_price_and_send_mail(config, all_prices_df)

        opriceu.ota_price_upload_and_mail_send(config, all_prices_df)

        logger.info('send mail done')

        logger.info('*********************run end******************************')

        return all_prices_df
