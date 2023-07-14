import datetime as dt
import numpy as np
from common.util.utils import MiscUtil


def get_night_order_percent(smart_hotel_oyo_id_list, hour_offset, adb_query_manager):
    """
    get % of orders that happens after 9pm on biz_date in last 7 days

    :param all_hotel_oyo_id_str:
    :param adb_query_manager:
    :return:
        In [64]: df_night_order_pct
        Out[64]:
               oyo_id  night_pct
        0   CN_GGU004   0.145251
        1  CN_NAI1003   0.131868
        2   CN_DGN054   0.160121
        3  CN_ZHJ1001   0.166667
        4   CN_KMG045   0.269488
        5   CN_KMG020   0.167123
        6   CN_ZHJ010   0.123596
        7   CN_NAI018   0.275556
        8  CN_NAI1002   0.141667
        9  CN_QUA1018   0.292490
    """

    sql = """
        select oyo_id, sum(is_night) / count(1) as night_pct
        from (
          select
            oyo_id,
            case when timestampdiff(hour, date(arrival_date) + interval 1 day, source_create_time) >= -({}-1)
              then 1
            else 0 end as is_night
          from trade_booking tb
            join (select
                    id as hotel_id,
                    oyo_id
                  from product_hotel
                  where {}) h
              on (tb.hotel_id = h.hotel_id)
          where arrival_date between current_date - interval 8 day and current_date - interval 1 day
                and status in (0, 1, 2, 4)
                and is_deleted = 0
                and type <> 2
        ) group by oyo_id;
    """.format(hour_offset, MiscUtil.convert_to_oracle_query_oyo_id_list_str('oyo_id', smart_hotel_oyo_id_list, 2000))

    return adb_query_manager.read_sql(sql, 300)


def get_night_order_percent_wo_mm(smart_hotel_oyo_id_list, hour_offset, adb_query_manager, today=dt.datetime.now()):
    """
    get % of orders that happens after 9pm on biz_date in last 7 days

    :param all_hotel_oyo_id_str:
    :param adb_query_manager:
    :return:
        In [64]: df_night_order_pct
        Out[64]:
               oyo_id  night_pct
        0   CN_GGU004   0.145251
        1  CN_NAI1003   0.131868
        2   CN_DGN054   0.160121
        3  CN_ZHJ1001   0.166667
        4   CN_KMG045   0.269488
        5   CN_KMG020   0.167123
        6   CN_ZHJ010   0.123596
        7   CN_NAI018   0.275556
        8  CN_NAI1002   0.141667
        9  CN_QUA1018   0.292490
    """
    is_weekend = MiscUtil.is_weekend(today.weekday())

    mm_channel_ids = """
    select id
    from product_channel
    where is_valid = 1
    and is_deleted = 0
    and channel_name like 'MM%%'
    """

    long_stay_channels = list(adb_query_manager.read_sql(mm_channel_ids).id)

    # append walkin channel
    long_stay_channels.append(1)

    mm_channels = MiscUtil.convert_list_to_tuple_list_str(long_stay_channels)

    sql = """
        select oyo_id, sum(is_night) / count(1) as night_pct
        from (
          select
            oyo_id,
            case when timestampdiff(hour, date(arrival_date) + interval 1 day, source_create_time) >= -({hour_offset}-1)
              then 1
            else 0 end as is_night
          from trade_booking tb
            join (select
                    id as hotel_id,
                    oyo_id
                  from product_hotel
                  where {oyo_ids}) h
              on (tb.hotel_id = h.hotel_id)
          where arrival_date between current_date - interval 8 day and current_date - interval 1 day
                and status in (0, 1, 2, 4)
                and is_deleted = 0
                and type <> 2
                and not (
                -- exclude long stay brn
                ((source in {mm_channels}) and datediff(departure_date, arrival_date) >= 14) 
                or 
                (source = 10 and type = 3)
                )
                and dayofweek(arrival_date) in {weekday}
        ) group by oyo_id
    """.format(hour_offset=hour_offset,
               mm_channels=mm_channels,
               oyo_ids=MiscUtil.convert_to_oracle_query_oyo_id_list_str('oyo_id', smart_hotel_oyo_id_list, 2000),
               weekday='(6, 7)' if is_weekend else '(1, 2, 3, 4, 5)')

    return adb_query_manager.read_sql(sql, 300)


def apply_night_prediction(all_brn_with_srn_df, df_night_order_pct, start_time):
    """
    Raise current occ if hotel sells a sizable number of orders after 9pm, rule:

    1. current occ >= 30%, and
    2. date == today, and
    3. last 7 days % of night(>=9pm) orders >= 20%

    :param all_brn_with_srn_df:
    :param df_night_order_pct:
    :param start_time:
    :return:
    """
    df_night_order_pct['night_amplifier'] = df_night_order_pct['night_pct'] / (1 - df_night_order_pct['night_pct'])
    df_temp = all_brn_with_srn_df.merge(df_night_order_pct, on=['oyo_id'], how='left')

    # default night_pct and amplifier to 0, if no night data is found
    df_temp['night_pct'] = df_temp['night_pct'].replace([np.nan], 0)
    df_temp['night_amplifier'] = df_temp['night_amplifier'].replace([np.nan, np.inf], 0)
    df_temp['mm_occ'] = df_temp['mm_brn']/df_temp['srn']
    # exclude mm_occ from occ extrapolation
    df_temp['night_predicted_occ'] = df_temp['occ'] - df_temp['mm_occ']

    today = dt.datetime.strftime(start_time, "%Y-%m-%d")

    def night_prediction(row, date):
        if row['date'] == date:
            return min(max(row['night_predicted_occ'], 0.02) * (1 + row['night_amplifier']), 2)  # cap occ to 200%, if occ is 0, floor to 2%
        else:
            return row['night_predicted_occ']

    df_temp['night_predicted_occ'] = df_temp.apply(lambda row: night_prediction(row, today), axis=1)
    df_temp['night_predicted_occ'] = df_temp['night_predicted_occ'] + df_temp['mm_occ']
    df_temp['night_predicted_occ'] = df_temp['night_predicted_occ'].clip(upper=2.0, lower=0)
    df_temp['night_predicted_occ'] = df_temp['night_predicted_occ'].replace([np.nan], 0)

    df_temp = df_temp.rename(columns={
        'night_predicted_occ': 'occ',
        'occ': 'occ_wo_night_prediction',
        'night_pct': 'night_order_pct'}
    )

    return df_temp[list(all_brn_with_srn_df.columns.values) + ['night_order_pct', 'occ_wo_night_prediction']]
