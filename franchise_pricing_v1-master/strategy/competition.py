#!/usr/bin/env python
# -*- coding:utf-8 -*-
import pandas as pd
import numpy as np
from sqlalchemy import create_engine

COMPETITION_PRICE_SQL = u"""
select
  oyo_id, c.competitor_ctrip_id, c.COMPETITOR_BASE_ROOM_TYPE_NAME, ROOM_TYPE_NAME, price, UPDATE_TIME, CHECKIN_DATE
from
  (select
     OYO_HOTEL_ID as oyo_id,
     COMPETITOR_CTRIP_ID,
     COMPETITOR_BASE_ROOM_TYPE_NAME
   from OYO_DW_PRICING.PRICING_COMPETITIONS where deleted_at is null
  ) c join
  (SELECT
     OTA_HOTEL_ID as competitor_ctrip_id,
     price,
     MAIN_ROOM_TYPE_NAME as ROOM_TYPE_NAME,
     UPDATE_TIME,
     to_char(trunc(checkin_date), 'yyyy-mm-dd') CHECKIN_DATE
   FROM OYO_DW_DEVON.DIM_PRICING_CTRIP_HOTEL_ROOM_PRICE_MONITOR p
   where
     IS_HOUR_ROOM = 'N'
     and room_type_name not like '%时间%'
     and trunc(current_date) = to_date(to_char(UPDATE_TIME, 'yyyy-mm-dd'), 'yyyy-mm-dd')
     and OTA_HOTEL_ID not in (1638329,6868410,4818774,914388,1701743,1333030,12257468,3267037,1914445,2221323,1419670,5090848,1744044,447388,5978394,25662783,23684057,994867,23840078,14003012,8550347,8426349,6868410,1676247,1691758,1424510,2917076,3074425,1196538,6310178,1262064,1700389,1305605,2205586,1971176,1757030,1027541,2000200,1638329,5381865,6820858,5246564,5512844,6150394,25733580,1720447,1735139,1520772,3423369) -- 剔除互为竞对的情况
     ) p
    on (c.competitor_ctrip_id = p.competitor_ctrip_id and c.COMPETITOR_BASE_ROOM_TYPE_NAME = trim(p.ROOM_TYPE_NAME))
"""


def calc_competition_price(conn, batch_date):
    """
    :param conn:
    :param batch_date: int, hour of the day (0-23)
    :return:
               oyo_id        date  competition_price
        0   CN_JIQ006  2019-02-01                 65
        1   CN_JIQ006  2019-02-02                 71
        2   CN_XGT002  2019-02-01                 85
        3   CN_XGT002  2019-02-02                 85
        4   CN_YNG011  2019-02-01                203
        5   CN_YNG011  2019-02-02                203
        6   CN_ZAN005  2019-02-01                106
    """
    conn = create_engine('oracle://oyo_dw_pricing:hiD(4e)i4)q/aZgjwSbK@10.200.71.247:1521/oyodw', encoding='utf8')
    sql = COMPETITION_PRICE_SQL.format(batch_date=batch_date)
    df = pd.read_sql(sql, conn)

    if len(df) == 0:
        raise Exception("crawler data is not ready")

    df['price'] = df['price'].astype(int)

    # """
    # find group (['oyo_id', 'competitor_ctrip_id']) median, and eliminate price outliers within the group
    # """
    df = df.groupby(['oyo_id', 'competitor_ctrip_id', 'checkin_date', 'room_type_name'])['price'].min().reset_index()
    # df = pd.merge(df, df_group_min, on=['oyo_id', 'competitor_ctrip_id', 'checkin_date'], how='left')
    # # if price < 0.6 * group median, treat as outlier
    # df = df.loc[df.price_x > df.price_y * 0.6]
    df.rename(columns={'price': 'competition_price'}, inplace=True)

    # """
    # Find lowest prices of competitors.
    # Use  90 percentile price (descending ordered) if count(competitors) >= 10,  or lowest price as competition_price
    # """
    # df_group_min = df.groupby(['oyo_id', 'competitor_ctrip_id', 'checkin_date'])['competition_price'].min().reset_index()

    hotel_price_by_day = list(df[['oyo_id', 'checkin_date']].groupby(['oyo_id', 'checkin_date']).groups.keys())

    ids = []
    dates = []
    competition_prices = []
    for oyo_id, date in hotel_price_by_day:
        prices = list(df.loc[(df.oyo_id == oyo_id) & (df.checkin_date == date)]['competition_price'])
        prices = sorted(prices, reverse=True)

        if len(prices) >= 10:
            price = prices[round(len(prices) * 0.9)]
        else:
            price = prices[-1]

        ids.append(oyo_id)
        dates.append(date)
        competition_prices.append(price)

    df_competition_price = pd.DataFrame.from_dict({'oyo_id': ids, 'date': dates, 'competition_price': competition_prices})

    return df_competition_price


def price_protection(base_price, competition_price):
    if np.isnan(competition_price):
        return np.NaN
    if competition_price < base_price * 0.7:
        return base_price * 0.7
    elif competition_price > base_price * 1.2:
        return base_price * 1.2
    else:
        return competition_price


def apply_competition_price(competition_price, old_strategy_price):
    if np.isnan(competition_price):
        return old_strategy_price
    return max(competition_price, old_strategy_price)
