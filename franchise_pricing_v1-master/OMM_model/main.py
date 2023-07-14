import os
import re
from sqlalchemy import create_engine
import cx_Oracle as oracle
import pandas as pd
import numpy as np
import datetime as dt
import difflib
import time
import seaborn
import matplotlib.pyplot as plt
from sklearn.metrics import r2_score
import pickle
import math
from math import radians, cos, sin, asin, sqrt,atan,tan,acos,pi
from scipy.optimize import curve_fit
from astropy.units import Ybarn
import psycopg2
from tzlocal import get_localzone
from sklearn.linear_model import LinearRegression
from sklearn.linear_model import RidgeCV
import pytz
from datetime import datetime,timedelta
import smtplib
from sqlalchemy import create_engine
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from io import StringIO
import json
import configparser
import warnings
warnings.filterwarnings("ignore")
import configparser

def Run():
    
    cur_path=os.getcwd()
    config_path=os.path.join(cur_path,'config.ini')
    conf=configparser.ConfigParser()
    conf.read(config_path)
    # In[0]
    st = time.time()
    print (time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(st)))
    ######################################################################################
    z = str(get_localzone())
    a = datetime.fromtimestamp(time.time())
    b = datetime.strftime(a, "%Y-%m-%d")
    print("Start Time - ",a)
    
    os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'
    dbname= conf.get('Oracle_devon', 'dbname')
    host = conf.get('Oracle_devon', 'host')
    port=conf.get('Oracle_devon', 'port')
    user=conf.get('Oracle_devon', 'user')
    password=conf.get('Oracle_devon', 'password')
    Oracle_devon = 'oracle://'+user+':'+password+'@'+host+':'+port+'/'+dbname
    con_devon = create_engine(Oracle_devon)
    con1 = create_engine(Oracle_devon)
    con = create_engine(Oracle_devon)
    
    print("connect success")
    
    # In[0]
    base_query = '''
    select "oyo_id","date","base","room_category"
    from OYO_DW_DEVON.CHINA_BASE_PRICES 
    where "date" between current_date-1 and current_date + 6 
    and "status" =1
    '''
    input_base = pd.read_sql( base_query,con)
    input_base.columns = list(pd.Series(input_base.columns).map(lambda x: str(x).replace('base','new_base')))
    
    # In[1]
    smart_hotel_list = tuple([ 'CN_HGU070','CN_SHU028']) 

    #smart_hotel_list = tuple([ HGU070 HGU063 ])  # 
    # In[1]
    smart_query_2 = '''
    select a.*,b.valid_from,b.valid_till,b.agreement_type,b.contracted_rooms
    from
    (select a."date", b.hotel_id,b.oyo_id,b.cluster_name,b.city from (select "date" from oyo_dw.DIM_CALENDAR where "date" between current_date and current_date+7) a
      cross join
      (select h.id as hotel_id,h.oyo_id, c.cluster_name,c.CITY_Name as city 
      from oyo_dw.dim_hotel h left join oyo_dw.v_dim_zone c on c.cluster_id=h.cluster_id 
     -- where h.status in (3,2) 
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
    
    smart_properties = pd.read_sql( smart_query_2 ,con)
    print("smart done")
    
    
    
    smart_hotel_data = smart_properties[smart_properties.oyo_id.isin(smart_hotel_list)]
    city_list = tuple(smart_hotel_data.city.unique())
    
    # =============================================================================
    # =============================================================================
    
    # In[1]
    past_data = pd.read_sql('''
    select oyo_id,to_date(date_s,'yyyy-mm-dd') AS "date",sum(urn) AS used_rooms,sum(brn) ,max(srn) AS sellable_rooms,sum(PER_DAY_GMV) AS gmv
    from oyo_dw.v_fact_china_revenue a
    where to_date(date_s,'yyyy-mm-dd') between (current_date-15) and (current_date -1)
    and  HOTEL_STATUS IN ('Live','Active')
    and oyo_id in {0}
    group by oyo_id,date_s
    '''.format(str(smart_hotel_list)),con)
    
    past_data['day']= past_data.date.map(lambda x:  x.weekday() )
    
    
    ################################ Estimating Smart Occupancy ##############################
      
    date_start='current_date'
    date_end='current_date +6'
    
    query_2= '''
    SELECT
      H.OYO_ID,
      "date",
     count(DISTINCT CASE WHEN B.Status IN ('0', '1' ,'2')  and "date" >= r.check_in
      AND "date" < r.check_out  THEN br.id ELSE null END) AS used_rooms ,
      max(fc.srn) AS sellable_rooms
    FROM
      oyo_dw.FACT_BOOKING B
    LEFT JOIN oyo_dw.DIM_BOOKING_ROOM BR ON
      B.ID = BR.BOOKING_ID
    INNER JOIN oyo_dw.DIM_HOTEL H ON
      B.HOTEL_ID = H.ID
    LEFT JOIN oyo_dw.FACT_ROOM_RESERVATION R ON
      B.ID = R.BOOKING_ID
    LEFT JOIN oyo_dw.v_fact_china_revenue fc ON
      fc.OYO_ID = h.oyo_id
      AND fc.DATE_S = TO_CHAR( trunc(sysdate - 1), 'yyyymmdd')
    CROSS JOIN oyo_dw.DIM_CALENDAR CA
    WHERE TO_CHAR( "date", 'yyyymmdd') >= TO_CHAR( {0}, 'yyyymmdd')
      AND  TO_CHAR( "date", 'yyyymmdd') <= TO_CHAR( {1}, 'yyyymmdd')
      --=to_char(sysdate+6,'yyyymmdd')
    GROUP BY
      "date",
      H.OYO_ID 
    '''.format( date_start,date_end )
    
    srn_data = pd.read_sql(query_2,con)
    
    
    query_22= '''
    select ca."date",h.oyo_id as oyo_id,c.cluster_name
    from oyo_dw.dim_calendar ca 
    cross join oyo_dw.dim_hotel h 
    left join oyo_dw.v_dim_zone c on c.cluster_id=h.cluster_id
    where h.oyo_id in {2}
    and ca."date" >= "TO_DATE"('{0}','YYYY-MM-DD' ) 
    and ca."date" <= "TO_DATE"('{1}','YYYY-MM-DD' ) 
    '''.format(str(b),
        datetime.strftime(dt.datetime.strptime(b,'%Y-%m-%d') + timedelta(days = 6), "%Y-%m-%d"),
        str(smart_hotel_list))
    
    hotel_data =  pd.read_sql(query_22,con)
    
    hotel_data = pd.merge(hotel_data,srn_data,how = 'left',on = ['date','oyo_id'])
    hotel_data = hotel_data.fillna(0)
    
    hotel_data['occ'] = hotel_data.used_rooms/hotel_data.sellable_rooms
    hotel_data['rem'] = hotel_data.sellable_rooms - hotel_data.used_rooms
    
    hotel_data.occ = hotel_data.occ.map(lambda x:  np.where(pd.isnull(x)| (x ==float("inf")), 0, x ))
    
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
      LEFT join OYO_DW.V_dim_zone ct
      on h.cluster_id = ct.Cluster_Id
      cross join OYO_DW.DIM_CALENDAR  ca
      where TRUNC(ca."date") >= trunc(b.check_in)
      and TRUNC(ca."date") < trunc(b.check_out)
      AND ct.COUNTY_ID = 50
      and TRUNC(ca."date") >= to_date('{0}','yyyy-mm-dd') 
      and TRUNC(ca."date") <= to_date('{1}','yyyy-mm-dd') 
      and ct.city_name in {2}
      group by ca."date", h.oyo_id , ct.CITY_NAME
      order by 1,2  
    '''.format( datetime.strftime(dt.datetime.strptime(b,'%Y-%m-%d') - timedelta(days = 49), "%Y-%m-%d") ,
    datetime.strftime(dt.datetime.strptime(b,'%Y-%m-%d') - timedelta(days = 1),  "%Y-%m-%d") , str(city_list) )

    cluster_data_new = pd.read_sql(query5 ,con )
    
    cluster_data_new.city = ''
    
    sql007 = '''SELECT *
      FROM (SELECT RANK() OVER(PARTITION BY H.Oyo_Id ORDER BY "date" DESC) RNK,
                   "date",
                   H.OYO_ID,b.id,
                   SUM(CASE WHEN (B.Status IN (0,1,2) AND COALESCE(r.actual_amount, 0) > 0) 
                            THEN r.actual_amount /(trunc(r.check_out) - trunc(r.check_in))
                            ELSE 0 
                        END) AS SELL_AMNT,
                   SUM(CASE WHEN (B.Status IN (0,1,2) AND COALESCE(r.actual_amount, 0) <= 0) 
                            THEN (r.amount - COALESCE(bd.Discount_Money, 0)) /(trunc(r.check_out) - trunc(r.check_in))
                            ELSE  0
                        END) AS AMNT,
                   COUNT(CASE WHEN B.Status IN (0,1,2) AND "date">= r.check_in_time AND "date"< r.check_out_time THEN br.id  ELSE 0 END) AS HOTEL_URN
              FROM oyo_dw.FACT_BOOKING B
             INNER JOIN oyo_dw.v_Dim_Hotel H
                ON B.HOTEL_ID = H.ID
               AND H.Status IN (3)
             INNER JOIN oyo_dw.V_DIM_ZONE DZ
                ON H.CLUSTER_ID = DZ.CLUSTER_ID
              LEFT JOIN oyo_dw.DIM_BOOKING_ROOM BR
                ON B.ID = BR.BOOKING_ID
              LEFT JOIN oyo_dw.FACT_ROOM_RESERVATION R
                ON B.ID = R.BOOKING_ID
              LEFT JOIN oyo_dw.fact_booking_discount bd
                ON B.ID = bd.BOOKING_ID
             CROSS JOIN oyo_dw.DIM_CALENDAR CA
             WHERE "date" >= r.check_in
               AND "date" < r.check_out
               AND B.Source NOT IN ('13', '14')
               AND "date" >= to_date('{0}','yyyy-mm-dd')
               AND "date" <= to_date('{1}','yyyy-mm-dd')
               AND LOWER(OYO_ID) NOT LIKE 'ahm%'
               AND LOWER(OYO_ID) NOT LIKE 'gnr%'
               AND B.Source not IN ('11','18')
               AND B.Status IN (0,1,2) 
              -- AND br.ROOM_TYPE = 20
             GROUP BY "date",H.OYO_ID,b.id
             ORDER BY 2, 3) X
    '''.format(datetime.strftime( datetime.fromtimestamp(time.time())-timedelta(days = 28),"%Y-%m-%d"),
    datetime.strftime( datetime.fromtimestamp(time.time())- timedelta(days = 1),"%Y-%m-%d"))
    
    hotel_data_all_new = pd.read_sql(sql007,con)
    
    
    # =============================================================================
    #==========================================================================
    
    sql09 = '''SELECT a.oyo_id--,a.category
           ,a.latitude,a.longitude,b.city_name as city
           FROM oyo_dw.v_DIM_HOTEL a
           inner join oyo_dw.V_dim_zone b
           on a.cluster_id=b.Cluster_Id
           where city_name in {0}
     '''.format( str(city_list))
    
    hotel_data_lat_long_all = pd.read_sql(sql09, con)
    
      
    hotel_data_lat_long_all.latitude = hotel_data_lat_long_all.latitude.astype('float64')
    hotel_data_lat_long_all.longitude = hotel_data_lat_long_all.longitude.astype('float64')
    
    
    sql011 = '''  SELECT H.OYO_ID, 
           --H.CATEGORY,
           H.LATITUDE, 
           H.LONGITUDE, 
           CL.CITY_NAME AS city 
      FROM OYO_DW.dim_HOTEL H
     INNER JOIN OYO_DW.V_DIM_ZONE CL
        ON H.CLUSTER_ID = CL.Cluster_Id
     WHERE H.OYO_ID IN {0} '''.format( str(smart_hotel_list))
     
     
    hotel_data_lat_long_all_prop = pd.read_sql(sql011 ,con)
    
    hotel_data_lat_long_all_prop.latitude = hotel_data_lat_long_all_prop.latitude.astype('float64')
    hotel_data_lat_long_all_prop.longitude = hotel_data_lat_long_all_prop.longitude.astype('float64')
    
    
    # =============================================================================
    # 
    
    sql_tail = '''SELECT oyo_id
        ,upper(cluster_name) cluster_name
        , upper(city_name) city_name
        ,upper(hub_name) hub_name
        ,a.CATEGORY AS prop_category
    from OYO_DW.dim_HOTEL H 
    LEFT JOIN   OYO_DW.V_DIM_ZONE a ON h.CLUSTER_ID = a.CLUSTER_ID
    where H.oyo_id in {0} '''.format( str(smart_hotel_list))
    
    
    prop_details = pd.read_sql( sql_tail,con)
    
    # 
    # =============================================================================
    
    result_final = pd.DataFrame()
    complete_raw = pd.DataFrame()
    status_list = pd.DataFrame()
    
    
    for loop in smart_hotel_list:
    
        ##################### Estimating competitor set #######################    
        print(loop)
        hotel = loop
        
        city_name = list( smart_hotel_data[smart_hotel_data.oyo_id == loop].city.unique())
        
        #
        hotel_data_lat_long = hotel_data_lat_long_all[hotel_data_lat_long_all.city.map(lambda x:  x in city_name)]
        hotel_data_non_smart = hotel_data_lat_long[hotel_data_lat_long.oyo_id != loop]
        
        #Selected property Lat Long Details
        smart_lat = hotel_data_lat_long_all_prop[hotel_data_lat_long_all_prop.oyo_id == loop].latitude.iloc[0]
        smart_long = hotel_data_lat_long_all_prop[hotel_data_lat_long_all_prop.oyo_id == loop].longitude.iloc[0]
    
        smart_lat_rad = smart_lat*math.pi/180
        smart_long_rad = smart_long*math.pi/180
    
        #Dergree to radian for selected hotels        
        hotel_data_non_smart['long_rad'] = hotel_data_non_smart.longitude*math.pi/180
        hotel_data_non_smart['lat_rad'] = hotel_data_non_smart.latitude*math.pi/180
    
        #Calculating distance of selected hotel from every other properties
        R = 6371.0
        hotel_data_non_smart['dist']  = np.arccos(
                            np.sin(hotel_data_non_smart.lat_rad)
                            *np.sin(smart_lat_rad)
                            + np.cos(hotel_data_non_smart.lat_rad)
                            *np.cos(smart_lat_rad)
                            * np.cos(smart_long_rad-hotel_data_non_smart.long_rad)
                            ) * R
        
        #min(hotel_data_non_smart$dist)
        #Sorting the distance from closest to farthest        
        a_hotels  = hotel_data_non_smart.sort_values(axis=0,by=['dist'],ascending=True)
    
        #Selecting size of properties in a city if its less than 20 then total properties else 20
        size = min(10,len(hotel_data_non_smart.oyo_id))
        ############### 
        '''
        radius = a_hotels[10]
        '''
        radius = a_hotels.dist.iloc[size-1]
        #List of hotels nearbuy
        clus_hotel_list  = hotel_data_non_smart[hotel_data_non_smart.dist <= radius].oyo_id.tolist()
        
        #Adding the selected property in the list
        clus_hotel_list.append(loop)
        #past_data last 14 days data of URN, SRN and GMV of the selected hotel 
        
        past_data_hotel = past_data[past_data.oyo_id == loop]
        ####Condition 1####
        #checking if property is active for last 14 days
    
      
        if past_data_hotel.shape[0] == 14:
          #arr_list means the selected property
            arr_list = loop
            #past_data_hotel_sum_temp is just givin us days 
            tt01 = datetime.strftime(datetime.fromtimestamp(time.time()), "%Y-%m-%d")
            tt02 = datetime.strftime(datetime.fromtimestamp(time.time())+ timedelta(days = 6), "%Y-%m-%d")
            past_data_hotel_sum_temp = pd.DataFrame(pd.date_range(tt01,tt02,dtype = 'yyyy-mm-dd',freq = 'D'), columns = ['date'])
            past_data_hotel_sum_temp['day'] = past_data_hotel_sum_temp.date.map( lambda x:  x.weekday())
            #del past_data_hotel_sum_temp['date']
     
             #adding URN, SRN and GMV on the Days
            past_data_hotel_sum = past_data_hotel.groupby(by = ['day','oyo_id' ],as_index = False).sum()
    
             #arranging to days
            past_data_hotel_sum = pd.merge(past_data_hotel_sum_temp,past_data_hotel_sum,how = 'left',on = ['day'])
    
            #adding Occ
            past_data_hotel_sum['occ'] =  past_data_hotel_sum.used_rooms/past_data_hotel_sum.sellable_rooms
            #adding ARR          
            past_data_hotel_sum['arr'] =  past_data_hotel_sum.gmv/past_data_hotel_sum.used_rooms
          
            #Removing Errors
            past_data_hotel_sum.occ = past_data_hotel_sum.occ.map(lambda x:  np.where(pd.isnull(x), 0, x))
            past_data_hotel_sum.arr = past_data_hotel_sum.arr.map(lambda x:  np.where(pd.isnull(x), 0, x))
              
            #last two weeks occ and ARR wrt days
            hotel_past_occ =  past_data_hotel_sum[['day', 'occ', 'arr']]
            # past_data_hotel_sum = select(past_data_hotel_sum,day,tau)
            #Tagging property age old becz it has data for last 14 days
              
            past_data_hotel_sum['prop_age'] =  "old"
            #Selecting day,occ,age in this case age is old
            past_data_hotel_sum = past_data_hotel_sum[['day','occ','arr','prop_age' ]]
    
            ####Condition 1 Result Day,Occ,Age####   
            ####Condition 2 it means property is new. i.e not active for more than 14 days####
        else:
            #here arr_list is all the properties which are close by to the selected property. Including the selected properties
            arr_list = clus_hotel_list
    
            #Category of the property
            loop_category = prop_details[prop_details.oyo_id==loop].prop_category.iloc[0]
            
            #Selecting the days of the week
            tt01 = datetime.strftime(datetime.fromtimestamp(time.time()), "%Y-%m-%d")
            tt02 = datetime.strftime(datetime.fromtimestamp(time.time())+ timedelta(days = 6), "%Y-%m-%d")
              
            past_data_hotel_sum = pd.DataFrame(pd.date_range(tt01,tt02,dtype = 'yyyy-mm-dd',freq = 'D'), columns = ['date'])
    
            past_data_hotel_sum['day']= past_data_hotel_sum.date.map(lambda x:  x.weekday() )
    
            past_data_hotel_sum['tau'] =  1
            #Selecting URN, SRN and GMV of the properties which are nearbuy
                
            past_data_cluster = past_data[past_data.oyo_id.isin(arr_list)]
     
            past_data_cluster_sum = past_data_cluster.groupby(by = ['day'],as_index = False).sum()        
            
            past_data_cluster_sum['occ'] = past_data_cluster_sum.used_rooms/past_data_cluster_sum.sellable_rooms
            past_data_cluster_sum['arr'] = past_data_cluster_sum.gmv/past_data_cluster_sum.used_rooms
              
            past_data_hotel_sum = pd.merge(past_data_hotel_sum,past_data_cluster_sum[['occ','arr','day']],how = 'left',on = ['day'])
            #Occ ARR Day of the properties nearbuy
            
            if loop_category==2:
                past_data_hotel_sum.tau=1.2
            # past_data_hotel_sum = select(past_data_hotel_sum,day,tau)
            #past_data_hotel_sum$occ = 1
            #Tagging Property AGE to be new
            past_data_hotel_sum['prop_age'] = "new"
            past_data_hotel_sum = past_data_hotel_sum[['day','occ','arr','prop_age']]
            #Nearbuy Hotels Combines Property level day wise Occ and ARR with New age tag
            hotel_past_occ = past_data_hotel_sum.copy()
            hotel_past_occ.prop_age = ''
            hotel_past_occ.tau = ''
        
        ####Condition 2 Ends with Nearbuy properties Day level occ,ARR for last 2 weeks.####
        
        #Last 2 weeks occ, ARR data of the hotel on day level
        hotel_past_occ = hotel_past_occ.rename(columns={'occ':'past_occ' })
        hotel_past_occ = hotel_past_occ.rename(columns={'arr': 'past_arr'})
                
        #Select the selected hotels. Hotel_data_all_new contains URN And GMV                
        hotel_data_all = hotel_data_all_new[hotel_data_all_new.oyo_id.map(lambda x:  x in arr_list)]
      
        #We calculated the ARR
        hotel_data_all['revenue'] = hotel_data_all.amnt + hotel_data_all.sell_amnt      
        hotel_data_all['clus_arr'] = hotel_data_all.revenue/hotel_data_all.hotel_urn
        #Added Days
        hotel_data_all['day'] = hotel_data_all.date.map( lambda x:  x.weekday())
                
        #Day level Revenue and URN of a Hotel
        clus_data = hotel_data_all.groupby(by = ['day'],as_index = False).sum()[['day','revenue','hotel_urn']]     
                
        #Calculated the ARR
        clus_data['clus_arr'] = clus_data.revenue/clus_data.hotel_urn
        
        
        
        clus_data_arr = clus_data.copy()
        
        #Found the base - i.e Day level ARR
        clus_data_arr['base']  =   clus_data_arr.clus_arr
        #clus_data_arr['base'] = past_data_hotel_sum.arr
    
    
        ############################### Calculating surge in arr for base (aggressive growth) ############################
        
        #Joining 4 weeks average arr and occ with 14 days occ and age
        clus_data_arr = pd.merge(clus_data_arr,past_data_hotel_sum,how = 'left',on = ['day'])
        
        ###################Logic Starts#########################
        #We calculated tau here if occ is less than 50% then base price is always is always less than the present ARR which means tau less than 1
        clus_data_arr['tau'] = 1
        
        
        tem_list11 = pd.Series(clus_data_arr.index)
        clus_data_arr['tau'] = tem_list11.map(lambda i: np.where( ((clus_data_arr.occ[i] <0.65)&(clus_data_arr.occ[i] >0.5)),
                     ((95-90)/(65-50))*(clus_data_arr.occ[i]-0.65)+0.95,
                     clus_data_arr['tau'][i]))
        clus_data_arr['tau'] = tem_list11.map(lambda i: np.where( (clus_data_arr.occ[i] <0.5),0.95,clus_data_arr['tau'][i]))
        clus_data_arr['tau'] = tem_list11.map(lambda i: np.where( (clus_data_arr.occ[i] >=0.75),
                     np.where(clus_data_arr.base[i]<=1000,1.10,1.10),clus_data_arr['tau'][i]))
        clus_data_arr['tau'] = tem_list11.map(lambda i: np.where( (clus_data_arr.occ[i] >=0.8),
                     np.where(clus_data_arr.base[i]<=1000,1.125,1.125),clus_data_arr['tau'][i]))
        clus_data_arr['tau'] = tem_list11.map(lambda i: np.where( (clus_data_arr.occ[i] >=0.9),
                     np.where(clus_data_arr.base[i]<=1000,1.15,1.15),clus_data_arr['tau'][i]))
        clus_data_arr['tau'] = tem_list11.map(lambda i: np.where(clus_data_arr.prop_age[i] =="new",1,clus_data_arr['tau'][i]))
        
        #clus_data_arr$tau = ifelse(loop %in% c('MUM651','KOL088','DEL889','PUN111','BLR699','MUM647','MUM602','NOD325','MUM640','PUN337','MUM280','MUM592','MUM661','MUM540','MUM017','BLR820','BLR775','BLR801','MUM631','PUN351'),pmax(1.15,clus_data_arr$tau),clus_data_arr$tau)
        #clus_data_arr$tau = ifelse(loop %in% c('MUM660'),pmax(1.25,clus_data_arr$tau),clus_data_arr$tau)
        
        clus_data_arr.prop_age = ''
        ############################### Calculating surge in arr for base (aggressive growth) ############################
        mu = 0
        
        ##Here we added base = Tau*base, Earlier the base was just the Day level last 4 week ARR
        clus_data_arr['base'] = clus_data_arr.tau * clus_data_arr['base'] + mu
        
        #All the nearbuy properties cluster
        cluster_data = cluster_data_new[cluster_data_new.oyo_id.isin(clus_hotel_list)]
        
        #Adding Days
        cluster_data['day'] = cluster_data.date.map(lambda x: x.weekday())
        
        #On day level urn for 30 days
        cluster_sum = cluster_data.groupby(by = ['day'],as_index = False).sum()  
        #occ number will tell us how many booking were made before the date of stay.
                             
        for x in range(31):
            cluster_sum['occ_'+str(x)] = 1-cluster_sum['urn_' + str(x)]/cluster_sum.cluster_urn
        
        
        tt02 = datetime.strftime(datetime.fromtimestamp(time.mktime(time.strptime(b,'%Y-%m-%d')))+timedelta(days =30), "%Y-%m-%d")          
        clus_occ = pd.DataFrame(pd.date_range(b,tt02,dtype = 'yyyy-mm-dd',freq = 'D'), columns = ['date'])
                  
        clus_occ['day'] = clus_occ.date.map(lambda x: x.weekday())
        
        clus_occ = pd.merge(clus_occ,cluster_sum,how = 'left',on = ['day'])
        clus_occ['occupancy'] = 0.0
        
        start_date = b
        end_date = datetime.strftime(datetime.fromtimestamp(time.mktime(time.strptime(b,'%Y-%m-%d')))+timedelta(days =30), "%Y-%m-%d")
        
        for i in list(clus_occ.index):
            date_loop = str(clus_occ.date[i])[:10]
            date1=time.strptime(date_loop,"%Y-%m-%d")
            date2=time.strptime(start_date,"%Y-%m-%d")
            date11 = datetime(date1[0],date1[1],date1[2])
            date22 = datetime(date2[0],date2[1],date2[2])
            diff = (date11 - date22).days
            clus_occ['occupancy'][i] =  clus_occ["occ_"+str(diff)][i]
        
        clus_occ_trend = clus_occ[[ 'date','day','occupancy' ]]
        clus_occ_trend = clus_occ_trend.rename(columns={'occupancy':'clus_occ'})
        clus_occ_trend.date = clus_occ_trend.date.map(lambda x: str(x)[:10])
        
        data_temp = hotel_data[['date','oyo_id','rem','occ','sellable_rooms']]
        data_temp = data_temp[data_temp.oyo_id == loop]
        data_temp.date = data_temp.date.map(lambda x: str(x)[:10])
        
        hotel_price_data = pd.merge(data_temp,clus_occ_trend[['date','day','clus_occ']],how = 'left',on = ['date'])
        hotel_price_data = pd.merge(hotel_price_data,clus_data_arr[['day','clus_arr','base', 'tau']],how = 'left',on = ['day'])
        
        ## Input Base ##
        if  input_base.shape[0] > 0:
            input_base.date = input_base.date.map(lambda x: str(x)[:10])
            hotel_price_data = pd.merge(hotel_price_data ,input_base,how = 'left',on = ['date','oyo_id'])
            tem_list22 = pd.Series(hotel_price_data.index)
            hotel_price_data['base'] = tem_list22.map(lambda i: np.where( pd.isnull(hotel_price_data.new_base[i]) == False,
                        hotel_price_data.new_base[i] , hotel_price_data.base[i]))
            hotel_price_data.new_base = ''
    
        hotel_price_data = pd.merge(hotel_price_data,hotel_past_occ,how = 'left',on = ['day'])
        hotel_price_data['occ_ftd_target'] = hotel_price_data.past_occ.map(lambda x:    min(max(x*1.2,x+0.1),0.8)  )
    
    
        #hotel_price_data$occ_ftd_target = pmin(pmax(hotel_price_data$past_occ*1.2,hotel_price_data$past_occ+0.15),1)
        hotel_price_data['tg_current'] = hotel_price_data.clus_occ*hotel_price_data.occ_ftd_target
        hotel_price_data['delta'] = hotel_price_data.occ-(hotel_price_data.clus_occ*hotel_price_data.occ_ftd_target)
        
        # Correction for Positive Increment in prices #
        
        hotel_price_data.delta = hotel_price_data.delta.map(lambda x: np.where(x >=0,1*x,x))
        
        #hotel_price_data$delta = ifelse(hotel_price_data$delta>=0.5 & hotel_price_data$date == b, 0.5,hotel_price_data$delta)
        tem_list33 = pd.Series(hotel_price_data.index)
        
        hotel_price_data.delta = tem_list33.map(lambda x: np.where(
                (hotel_price_data.delta[x] >=0.2) & (hotel_price_data.date[x] == b),0.2,hotel_price_data.delta[x]))
        
        #hotel_price_data$delta = ifelse(hotel_price_data$delta>=0.3 & hotel_price_data$date >= b+1, 0.3,hotel_price_data$delta)
        temp_b1 = datetime.strftime(datetime.fromtimestamp(time.mktime(time.strptime(b,'%Y-%m-%d')))+timedelta(days =1), "%Y-%m-%d")
        hotel_price_data.delta = tem_list33.map(lambda x: np.where(
                (hotel_price_data.delta[x] >=0.2) & (hotel_price_data.date[x] >= temp_b1 ),0.2,hotel_price_data.delta[x]))
        
        # Correction for Negative Increment in prices #
        #hotel_price_data$limit = ifelse(hotel_price_data$date == b,0.3,0.25)
        
        hotel_price_data['limit'] = hotel_price_data.date.map(lambda x: np.where(x == b ,0.15,0.15))
        
        temp_b2 = datetime.strftime(datetime.fromtimestamp(time.mktime(time.strptime(b,'%Y-%m-%d')))+timedelta(days =2), "%Y-%m-%d")
        
        hotel_price_data['limit'] = tem_list33.map(lambda x: np.where( hotel_price_data.date[x] >= temp_b2,
                                                                   0,hotel_price_data.limit[x]))
        
        hotel_price_data['delta'] = tem_list33.map(lambda x: np.where( hotel_price_data.delta[x] <= -hotel_price_data.limit[x],
                                    -hotel_price_data.limit[x],hotel_price_data.delta[x]))
        
        hotel_price_data['delta'] = tem_list33.map(lambda x: np.where( (hotel_price_data.rem[x] <= 1) & (hotel_price_data.date[x] == b),
                                    0.3,hotel_price_data.delta[x]))
        
        hotel_price_data['price'] = (1+hotel_price_data.delta)*hotel_price_data.base
    
        #hotel_price_data$delta = ifelse(hotel_price_data$delta>=hotel_price_data$limit,hotel_price_data$limit,hotel_price_data$delta)
      
        result_final = pd.concat([result_final, hotel_price_data[['date','oyo_id','price','clus_occ','occ','sellable_rooms','rem','delta','base',
                          'clus_arr','past_occ','past_arr','occ_ftd_target']]],ignore_index = True) 
    
        # result_final = rbind(result_final,select(hotel_price_data,date,oyo_id,city,price_new_single,clus_occ,occ,rem,delta,sellable_rooms,so_do_cat))
        complete_raw = pd.concat([complete_raw,hotel_price_data],ignore_index = True)
    
    test = result_final.copy()
    result_final = result_final[(result_final.price.isnull() == False) & (result_final.price != float('inf'))  ]
    result_final  = result_final.sort_values(axis=0,by=['date','oyo_id'],ascending=True)
    result_final['room_category_id'] = 4
    # In[9]
    
    
    tt04 = datetime.strftime(datetime.fromtimestamp(time.mktime(time.strptime( b ,'%Y-%m-%d')))+timedelta(days =1), "%Y-%m-%d")          
    query_3 = '''
    SELECT oyo_id
        ,ROOM_TYPE AS ROOM_TYPE
        ,c.name AS room_category
    FROM oyo_dw.DIM_BOOKING_ROOM a 
    INNER JOIN oyo_dw.dim_hotel b ON b.id = a.hotel_id
    INNER JOIN oyo_dw.dim_room_type c ON a.ROOM_TYPE = c.id
    WHERE a.create_time BETWEEN  SYSDATE - 15 AND SYSDATE +1
           and oyo_id in {0}
    GROUP BY b.oyo_id,a.ROOM_TYPE,c.name
      '''.format(str(smart_hotel_list))#,b, tt04)
    
    
    hotel_date_cat = pd.read_sql(query_3,con_devon)
    
    cat_multiplier = pd.read_sql('''select 
    "oyo_id",
    a.ROOM_TYPE,
    "category_multiplier",
    --b.name as room_category,
    b.code
    from OYO_DW_DEVON.CHINA_ROOM_CATEGORY_RATIOS a 
    LEFT JOIN oyo_dw_p.DIM_ROOM_CATEGORIES b ON a."room_category_id" = b.id''',
    con_devon)
    
    
    default_cat_multipliers = pd.DataFrame([38,39,40,41,42,43,44,45,46,47,48,49,50,20,26,27,28,29,30,31,32,33,34,36,37],columns = ['room_type'])
    default_cat_multipliers['default_multiplier'] = [0.9,0.95,1,1.4,1.4,1,1,1,1,1,1.2,1,1.3,1,1.2,1.3,1.3,1,1.3,1.6,1.5,1.3,1.3,0.9,0.9]
    
    
    
    hotel_date_cat = pd.merge(hotel_date_cat,cat_multiplier,how = 'left',on = ['oyo_id','room_type'])
    
    
    #hotel_date_cat = cat_multiplier.copy()
    hotel_date_cat = pd.merge(hotel_date_cat,
                              default_cat_multipliers,
                              how = 'left',
                              on = ['room_type'])
    
    tem_list44 = pd.Series(hotel_date_cat.index)
    hotel_date_cat['category_multiplier'] = tem_list44.map(lambda x: 
        np.where( pd.isnull( hotel_date_cat.category_multiplier[x]),    
                hotel_date_cat.default_multiplier[x],
                hotel_date_cat.category_multiplier[x]))
        
    hotel_date_cat.category_multiplier =  hotel_date_cat.category_multiplier.fillna(1.15)
    
    
    #result_final = result_final.rename(columns={'room_category':'room_category_id'})
    result_final_2 = result_final[result_final.date <= tt04 ]
    
    
    #del result_final_2['room_category'] 
    #hotel_date_cat.date = hotel_date_cat.date.map(lambda x: str(x)[:10] )
    
    result_final_2 = pd.merge(hotel_date_cat,
                              result_final_2,
                              how = 'inner',
                              on = ['oyo_id'])
    
    result_final_2  = result_final_2.sort_values(axis=0,by=['date', 'oyo_id','room_type'],ascending=True)
    
    
    
    result_final_2['final_price'] = round(result_final_2.price*result_final_2.category_multiplier,0)
    result_final_2['hourly_price'] = round(result_final_2.final_price*0.6,0)
    
    result_final_2.price = round(result_final_2.price,0)
    result_final_2.base = round(result_final_2.base,0)
    result_final_2.clus_arr = round(result_final_2.clus_arr,0)
    result_final_2.past_arr = round(result_final_2.past_arr,0)
    result_final_2.clus_occ = round(result_final_2.clus_occ,2)
    result_final_2.occ = round(result_final_2.occ,2)
    result_final_2.delta = round(result_final_2.delta,2)
    result_final_2.past_occ = round(result_final_2.past_occ,2)
    result_final_2.occ_ftd_target = round(result_final_2.occ_ftd_target,2)
    
    
    
    upload = result_final_2[['date','oyo_id','room_type','room_category','final_price','hourly_price']]
    
    
    
    
    # In[1]     # In[0]
    
    
    def send_mail(to_list, sub,file,attach_name):
        mail_user = conf.get('email', 'mail_user')  # 用户名 
        mail_pass = conf.get('email', 'mail_pass')  # 口令
        
        mail_host = "smtp.mxhichina.com"   # 设置服务器
        mail_postfix = "oyohotels.com"  # 发件箱的后缀
        me = "Big Data" + "<" + mail_user  + ">"
    
        msg = MIMEMultipart()
        msg['Subject'] = sub #邮件主题
        msg['From'] = me
        msg['To'] = ";".join(to_list)
    
        # 添加内容
        text = "Dear all!\n This is the dynamic pricing result for the first batch operated hotels."
        text_plain = MIMEText(text,'plain', 'utf-8')
        msg.attach(text_plain)
    
        # 添加 附加
        file.to_excel('log/'+attach_name)
        filename = 'log/'+attach_name
        att1 = MIMEText(open(filename, 'rb').read(), 'xls', 'gb2312')
        att1["Content-Type"] = 'application/octet-stream'
        att1["Content-Disposition"] = 'attachment;filename='+attach_name[-100:]
        msg.attach(att1)
        try:
            
            server=smtplib.SMTP_SSL(mail_host,465)
            server.set_debuglevel(0)
            server.connect(mail_host)
            server.login(mail_user, mail_pass)
            server.sendmail(me, to_list, msg.as_string())
            server.close()
            print ("邮件发送成功！！")
            return True
        except:
            print ("邮件发送失败！！")
            return False
    
    
    print('sql009 start')
    dbname= conf.get('Oracle_algo', 'dbname')
    host = conf.get('Oracle_algo', 'host')
    port=conf.get('Oracle_algo', 'port')
    user=conf.get('Oracle_algo', 'user')
    password=conf.get('Oracle_algo', 'password')
    Oracle_algo = 'oracle://'+user+':'+password+'@'+host+':'+port+'/'+dbname
    con22 = create_engine(Oracle_algo)
    sql009 = '''
    SELECT OYO_ID,UNIQUE_CODE, NAME AS hotel_name
    FROM OYO_DW.v_DIM_HOTEL
    WHERE status IN (2,3)
    GROUP BY OYO_ID,UNIQUE_CODE,NAME  '''
    Data_code = pd.read_sql(sql009,con22)
    print('sql009 end')
    
    upload = pd.merge(upload,Data_code,how = 'left',on = ['oyo_id'])    
    
    
    
    mailto_list = ['leon.yu@oyohotels.cn','Xiaolong.Chen@oyohotels.cn','qiusha.zhang@oyohotels.cn', 'yihao.li@oyohotels.cn',
                   'vicky.zhou@oyohotels.cn','amanda.pan@oyohotels.cn','eric.wang1@oyohotels.cn','himanshu.raghav@oyohotels.cn',
                   'Mridul@oyohotels.cn','Parmeshwar.jha@oyohotels.cn','devon.xiao@oyohotels.cn','shining.wu@oyohotels.cn',
                   'tom.chen@oyohotels.cn','yong.gao@oyohotels.cn','liang.hao@oyohotels.cn','yiren.fu@oyohotels.cn',
                   'qian.he@oyohotels.cn', 'haisong.ni@oyohotels.cn','kelly.zhu@oyohotels.cn','xuejing.xiao@oyohotels.cn',
                   'siwen.zhang@oyohotels.cn','dong.liang@oyohotels.cn','yuguang.zhang@oyohotels.cn','shushu.wang@oyohotels.cn',
                   'yingying.deng@oyohotels.cn','yetta.cui@oyohotels.cn','alex.lu@oyohotels.cn','renwen.xiao@oyohotels.cn',
                   'jonny.he@oyohotels.cn','pricechange.cst@oyohotels.cn','tom.chen@oyohotels.cn']


    file = upload.copy()
    #file = pd.read_excel('upload.xls')
    sub = "OM Dynamic Pricing "+ datetime.strftime(a, "%Y-%m-%d")             # 邮件主题
    attach_name = 'hotel_price_data'+datetime.strftime(a, "%Y-%m-%d %H_%M")+ '.xls'   # 附件文件名  （要加后缀)
    
    
    # send_mail(mailto_list,sub ,file,attach_name)
     
    return upload
    
    
    
    
    
    
    
        
        
        
        
