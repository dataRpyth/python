#!/usr/bin/env python
# -*- coding:utf-8 -*-
import datetime as dt

import math
import numpy as np
import csv
import bisect

from common.util.utils import *
from pathlib import Path

base_path = Path(__file__)

LAST_MINUTE_SALE_BEGIN_HOUR = 19
LAST_MINUTE_SALE_OCC_THRESHOLD = 0.7
LAST_MINUTE_SALE_REM_THRESHOLD = 10

EVENT_ADJUSTMENT = {
    '2019-09-27': 0.8,
}

rule_based_occ_lookup_table_json = '{"day":{"0":8,"1":7,"2":6,"3":5,"4":4,"5":3,"6":2,"7":1,"8":1,"9":1,"10":1,"11":0,"12":0,"13":0,"14":0,"15":0},"start_hour":{"0":0,"1":0,"2":0,"3":0,"4":0,"5":0,"6":0,"7":0,"8":8,"9":15,"10":20,"11":0,"12":8,"13":12,"14":16,"15":20},"end_hour":{"0":24,"1":24,"2":24,"3":24,"4":24,"5":24,"6":24,"7":8,"8":15,"9":20,"10":24,"11":8,"12":12,"13":16,"14":20,"15":24},"10":{"0":-0.1,"1":-0.1,"2":-0.15,"3":-0.15,"4":-0.15,"5":-0.15,"6":-0.3,"7":-0.3,"8":-0.3,"9":-0.3,"10":-0.3,"11":-0.3,"12":-0.3,"13":-0.3,"14":-0.3,"15":-0.3},"20":{"0":0.0,"1":0.0,"2":-0.1,"3":-0.1,"4":-0.1,"5":-0.1,"6":-0.2,"7":-0.2,"8":-0.2,"9":-0.2,"10":-0.2,"11":-0.3,"12":-0.3,"13":-0.3,"14":-0.3,"15":-0.3},"30":{"0":0.3,"1":0.0,"2":0.0,"3":0.0,"4":-0.05,"5":-0.05,"6":-0.15,"7":-0.15,"8":-0.15,"9":-0.15,"10":-0.15,"11":-0.25,"12":-0.25,"13":-0.25,"14":-0.25,"15":-0.25},"40":{"0":0.3,"1":0.3,"2":0.0,"3":0.0,"4":0.0,"5":0.0,"6":-0.1,"7":-0.1,"8":-0.1,"9":-0.15,"10":-0.15,"11":-0.25,"12":-0.25,"13":-0.25,"14":-0.25,"15":-0.25},"50":{"0":0.7,"1":0.3,"2":0.2,"3":0.2,"4":0.2,"5":0.1,"6":0.0,"7":-0.1,"8":-0.1,"9":-0.1,"10":-0.1,"11":-0.2,"12":-0.2,"13":-0.2,"14":-0.2,"15":-0.2},"60":{"0":0.8,"1":0.3,"2":0.3,"3":0.3,"4":0.3,"5":0.3,"6":0.0,"7":0.0,"8":0.0,"9":0.0,"10":-0.1,"11":-0.1,"12":-0.1,"13":-0.1,"14":-0.2,"15":-0.2},"70":{"0":0.9,"1":0.4,"2":0.4,"3":0.4,"4":0.4,"5":0.3,"6":0.1,"7":0.1,"8":0.1,"9":0.1,"10":0.0,"11":0.0,"12":0.0,"13":-0.1,"14":-0.15,"15":-0.2},"80":{"0":0.9,"1":0.5,"2":0.5,"3":0.5,"4":0.5,"5":0.4,"6":0.3,"7":0.3,"8":0.3,"9":0.3,"10":0.3,"11":0.1,"12":0.1,"13":0.0,"14":0.0,"15":-0.1},"90":{"0":1.0,"1":0.8,"2":0.7,"3":0.6,"4":0.6,"5":0.5,"6":0.3,"7":0.55,"8":0.5,"9":0.45,"10":0.4,"11":0.25,"12":0.2,"13":0.15,"14":0.1,"15":0.0},"100":{"0":1.5,"1":1.2,"2":1.0,"3":0.8,"4":0.8,"5":0.8,"6":0.8,"7":0.8,"8":0.8,"9":0.8,"10":0.8,"11":0.6,"12":0.4,"13":0.2,"14":0.1,"15":0.1}}'
rule_based_occ_lookup_table_with_prediction_json_exp = '{"day":{"0":8,"1":7,"2":6,"3":5,"4":4,"5":3,"6":2,"7":1,"8":1,"9":1,"10":1,"11":0,"12":0,"13":0,"14":0,"15":0},"start_hour":{"0":0,"1":0,"2":0,"3":0,"4":0,"5":0,"6":0,"7":0,"8":8,"9":15,"10":20,"11":0,"12":8,"13":11,"14":16,"15":20},"end_hour":{"0":24,"1":24,"2":24,"3":24,"4":24,"5":24,"6":24,"7":8,"8":15,"9":20,"10":24,"11":8,"12":11,"13":16,"14":20,"15":24},"10":{"0":0.0,"1":0.0,"2":0.0,"3":0.0,"4":0.0,"5":0.0,"6":0.0,"7":-0.2,"8":-0.2,"9":-0.2,"10":-0.2,"11":-0.3,"12":-0.3,"13":-0.3,"14":-0.3,"15":-0.3},"20":{"0":0.0,"1":0.0,"2":0.0,"3":0.0,"4":0.0,"5":0.0,"6":0.0,"7":-0.2,"8":-0.2,"9":-0.2,"10":-0.2,"11":-0.3,"12":-0.3,"13":-0.3,"14":-0.3,"15":-0.3},"30":{"0":0.0,"1":0.0,"2":0.0,"3":0.0,"4":0.0,"5":0.0,"6":0.0,"7":-0.15,"8":-0.15,"9":-0.15,"10":-0.15,"11":-0.25,"12":-0.25,"13":-0.25,"14":-0.25,"15":-0.25},"40":{"0":0.1,"1":0.1,"2":0.0,"3":0.0,"4":0.0,"5":0.0,"6":0.0,"7":-0.1,"8":-0.1,"9":-0.1,"10":-0.1,"11":-0.25,"12":-0.25,"13":-0.25,"14":-0.25,"15":-0.25},"50":{"0":0.15,"1":0.1,"2":0.1,"3":0.0,"4":0.0,"5":0.0,"6":0.0,"7":0.0,"8":0.0,"9":0.0,"10":0.0,"11":-0.2,"12":-0.2,"13":-0.2,"14":-0.25,"15":-0.25},"60":{"0":0.15,"1":0.15,"2":0.1,"3":0.1,"4":0.0,"5":0.0,"6":0.0,"7":0.0,"8":0.0,"9":0.0,"10":0.0,"11":-0.1,"12":-0.15,"13":-0.15,"14":-0.2,"15":-0.25},"70":{"0":0.2,"1":0.15,"2":0.15,"3":0.1,"4":0.1,"5":0.0,"6":0.0,"7":0.0,"8":0.0,"9":0.0,"10":0.0,"11":-0.1,"12":-0.1,"13":-0.1,"14":-0.15,"15":-0.2},"80":{"0":0.2,"1":0.2,"2":0.15,"3":0.15,"4":0.1,"5":0.1,"6":0.0,"7":0.0,"8":0.0,"9":0.0,"10":0.0,"11":0.0,"12":0.0,"13":0.0,"14":0.0,"15":0.0},"90":{"0":0.25,"1":0.25,"2":0.2,"3":0.2,"4":0.15,"5":0.15,"6":0.1,"7":0.1,"8":0.1,"9":0.1,"10":0.1,"11":0.05,"12":0.05,"13":0.05,"14":0.05,"15":0.05},"100":{"0":0.3,"1":0.25,"2":0.25,"3":0.2,"4":0.2,"5":0.2,"6":0.2,"7":0.2,"8":0.2,"9":0.2,"10":0.2,"11":0.1,"12":0.1,"13":0.1,"14":0.1,"15":0.1},"120":{"0":0.3,"1":0.3,"2":0.3,"3":0.3,"4":0.3,"5":0.3,"6":0.3,"7":0.3,"8":0.3,"9":0.3,"10":0.3,"11":0.1,"12":0.1,"13":0.1,"14":0.1,"15":0.15},"150":{"0":0.3,"1":0.3,"2":0.3,"3":0.3,"4":0.3,"5":0.3,"6":0.3,"7":0.3,"8":0.3,"9":0.3,"10":0.3,"11":0.1,"12":0.1,"13":0.1,"14":0.15,"15":0.2},"180":{"0":0.3,"1":0.3,"2":0.3,"3":0.3,"4":0.3,"5":0.3,"6":0.3,"7":0.3,"8":0.3,"9":0.3,"10":0.3,"11":0.1,"12":0.1,"13":0.15,"14":0.2,"15":0.3},"200":{"0":0.3,"1":0.3,"2":0.3,"3":0.3,"4":0.3,"5":0.3,"6":0.3,"7":0.3,"8":0.3,"9":0.3,"10":0.3,"11":0.15,"12":0.15,"13":0.2,"14":0.25,"15":0.3}}'


df_occ_table_exp = pd.read_json(rule_based_occ_lookup_table_with_prediction_json_exp).sort_index()
occ_ladder = [int(n) for n in list(df_occ_table_exp.columns) if n.isnumeric()]


def load_strategy_data_csv_as_dict(file_name, col_1_type=str, col_2_type=int):
    with open((base_path / "../data/{}.csv".format(file_name)).resolve(), mode='r') as file:
        reader = csv.reader(file)
        return {col_1_type(rows[0]): col_2_type(rows[1]) for rows in reader}


geo_labels = load_strategy_data_csv_as_dict("geo_label")


def adjust_price_based_on_future_occ(smart_hotel_oyo_id, all_brn_with_srn_df, start_time,
                                     targeted_hotel_online_data_past_4_weeks_by_day,
                                     df_base_price_override_next_30d, hotel_ebase_price,
                                     occ_target_map, adjust_ratio_divisor_map, price_ec_map):
    """
    :param all_brn_with_srn_df:
        In [43]: hotel_price_data
        Out[43]:
               oyo_id        date  hotel_id  brn  srn       occ
        0   CN_DAI025  2019-05-15     42129    1   32  0.031250
        1   CN_DAI025  2019-05-16     42129    1   32  0.031250
        2   CN_DAI025  2019-05-17     42129    2   32  0.062500
        3   CN_DAI025  2019-05-18     42129    7   32  0.218750
        4   CN_DAI025  2019-05-19     42129   10   32  0.312500
        5   CN_DAI025  2019-05-20     42129    3   32  0.093750
        6   CN_DAI025  2019-05-21     42129    2   32  0.062500
        7   CN_DAI025  2019-05-22     42129    2   32  0.062500
    :param start_time:
    :param targeted_hotel_online_data_past_4_weeks_by_day:
        In [118]: targeted_hotel_online_data_past_4_weeks_by_day
        Out[118]:
           day  online_gmv  online_urn  4wk_online_arr  3wk_online_occ  3wk_online_arr    3wk_arr prop_age       base   tau  corrected_base
        0    0       870.5        14.0       62.178571        0.083333       63.875000  61.148148      old  63.875000  0.75       47.906250
        1    1       756.1        14.0       54.007143        0.125000       53.500000  60.666667      old  53.500000  0.80       42.800000
        2    2       925.4        13.0       71.184615        0.135417       71.184615  62.668750      old  71.184615  0.80       56.947692
        3    3      1113.5        18.0       61.861111        0.093750       64.055556  61.860000      old  64.055556  0.75       48.041667
        4    4      1679.2        26.0       64.584615        0.072917       65.857143  65.962963      old  65.857143  0.75       49.392857
        5    5       926.5        15.0       61.766667        0.062500       61.166667  58.130435      old  61.166667  0.75       45.875000
        6    6       875.5        16.0       54.718750        0.083333       51.125000  58.178571      old  51.125000  0.75       38.343750
    :return:
    In [117]: hotel_price_data
        Out[117]:
               oyo_id        date       occ  adjust_ratio  day       base   tau  corrected_base      price
        0   CN_DAI025  2019-05-15  0.031250         -0.30    2  71.184615  0.80       56.947692  39.863385
        1   CN_DAI025  2019-05-16  0.031250         -0.30    3  64.055556  0.75       48.041667  33.629167
        2   CN_DAI025  2019-05-17  0.062500         -0.30    4  65.857143  0.75       49.392857  34.575000
        3   CN_DAI025  2019-05-18  0.218750         -0.05    5  61.166667  0.75       45.875000  43.581250
        4   CN_DAI025  2019-05-19  0.312500          0.00    6  51.125000  0.75       38.343750  38.343750
        5   CN_DAI025  2019-05-20  0.093750         -0.15    0  63.875000  0.75       47.906250  40.720312
        6   CN_DAI025  2019-05-21  0.062500         -0.15    1  53.500000  0.80       42.800000  36.380000
        7   CN_DAI025  2019-05-22  0.062500         -0.10    2  71.184615  0.80       56.947692  51.252923
        8   CN_DAI025  2019-05-23  0.000000         -0.10    3  64.055556  0.75       48.041667  43.237500
        9   CN_SZX012  2019-05-15  0.107143         -0.30    2  71.184615  0.80       56.947692  39.863385
        10  CN_SZX012  2019-05-16  0.107143         -0.20    3  64.055556  0.75       48.041667  38.433333
    """
    all_hotel_price_data = all_brn_with_srn_df[all_brn_with_srn_df.oyo_id == smart_hotel_oyo_id]
    all_hotel_price_data['day'] = all_hotel_price_data['date'].apply(
        lambda d: (dt.datetime.strptime(d, '%Y-%m-%d').date() - start_time.date()).days)
    all_hotel_price_data['hour'] = start_time.hour
    all_hotel_price_data = pd.merge(all_hotel_price_data, df_occ_table_exp, how='left', on='day')

    hotel_price_data = all_hotel_price_data[(all_hotel_price_data.hour >= all_hotel_price_data.start_hour) & (
            all_hotel_price_data.hour < all_hotel_price_data.end_hour)]

    def _get_occ_target(oyo_id, date):
        occ_target = get_occ_target_from_map(oyo_id, date, occ_target_map)
        return 0.4 if occ_target is None else occ_target

    def _calc_occ_percent(occ_target, occ):
        return int(occ / occ_target * 100)

    hotel_price_data = DFUtil.apply_func_for_df(hotel_price_data, 'occ_target', ['oyo_id', 'date'], lambda values: _get_occ_target(*values))

    hotel_price_data = DFUtil.apply_func_for_df(hotel_price_data, 'occ_percent', ['occ_target', 'occ'],
                                                lambda values: _calc_occ_percent(*values))

    # Make sure occ_percent does not exceed range [0, 200]
    hotel_price_data['occ_percent'] = hotel_price_data.occ_percent.map(lambda x: np.where(x > 200, 200, x))

    hotel_price_data['adjust_ratio'] = hotel_price_data.apply(
        lambda row: row['' + str(occ_ladder[bisect.bisect_left(occ_ladder, row['occ_percent'])])],
        axis=1)

    min_deviation_factor = -0.2
    max_deviation_factor = 0.25

    def _replace_adjust_ratio_with_alpha(pred_occ, oyo_id, date_str, day, night_pct, adjust_ratio):
        occ_target = get_occ_target_from_map(oyo_id, date_str, occ_target_map)
        occ_target = 1 if occ_target is None else occ_target
        price_ec = get_price_ec_from_map(oyo_id, price_ec_map)
        b = max(0, min(1 - night_pct, 0.99))
        beta = pred_occ / occ_target
        alpha = (1 - beta * b) / (1 - b)
        alpha = max(0.01, min(alpha, 100))
        adjust_ratio = math.pow(alpha, 1 / price_ec) - 1
        adjust_ratio = min(max(adjust_ratio, min_deviation_factor), max_deviation_factor)
        future_adjust = 0
        if day >= 4:
            future_adjust = (day - 3) * 0.04
        adjust_ratio += future_adjust
        adjust_ratio = min(max(adjust_ratio, min_deviation_factor), max_deviation_factor)
        return adjust_ratio

    hotel_price_data = DFUtil.apply_func_for_df(hotel_price_data, 'adjust_ratio', ['occ', 'oyo_id', 'date', 'day', 'night_order_pct', 'adjust_ratio'],
                                                lambda values: _replace_adjust_ratio_with_alpha(*values))

    hotel_price_data = hotel_price_data[['oyo_id', 'date', 'occ', 'occ_target', 'adjust_ratio', 'night_order_pct', 'occ_wo_night_prediction']]

    hotel_price_data['day'] = hotel_price_data['date'].apply(lambda d: dt.datetime.strptime(d, '%Y-%m-%d').weekday())

    hotel_price_data = hotel_price_data.merge(
        targeted_hotel_online_data_past_4_weeks_by_day[['day', 'base', 'occ_diff', 'tau', 'corrected_base']], how='left',
        on=['day'])

    hotel_price_data = base_correction_with_ebase(hotel_price_data, hotel_ebase_price)

    # override base if override value is larger
    hotel_price_data = override_base_price(df_base_price_override_next_30d, hotel_price_data)

    hotel_price_data = extend_override_base_price(df_base_price_override_next_30d, hotel_price_data)

    # apply event ratio if necessary
    hotel_price_data = event_adjustment(hotel_price_data)

    def adjust_adjust_ratio(oyo_id, adjust_ratio):
        adjust_ratio_divisor = adjust_ratio_divisor_map.get(oyo_id)
        if adjust_ratio_divisor is None:
            adjust_ratio_divisor = 1.5
        adjust_ratio /= adjust_ratio_divisor
        return adjust_ratio

    DFUtil.apply_func_for_df(hotel_price_data, 'adjust_ratio', ['oyo_id', 'adjust_ratio'], lambda values: adjust_adjust_ratio(*values))

    hotel_price_data['price'] = hotel_price_data['corrected_base'] * (
            1 + hotel_price_data['adjust_ratio'])

    return hotel_price_data


def base_price_correction_overall_occ(overall_occ):
    if np.isnan(overall_occ):
        return 0.75
    elif 0 <= overall_occ < 0.1:
        return 0.75
    elif 0.1 <= overall_occ < 0.15:
        return 0.8
    elif 0.15 <= overall_occ < 0.3:
        return 0.85
    elif 0.3 <= overall_occ < 0.5:
        return 0.9
    elif 0.5 <= overall_occ < 0.7:
        return 0.95
    elif 0.7 <= overall_occ < 0.8:
        return 1.0
    elif 0.8 <= overall_occ < 0.9:
        return 1.10
    else:
        return 1.20


def base_price_correction_overall_with_moving_average_weekday(overall_occ):
    if np.isnan(overall_occ):
        return 0.75
    elif 0 <= overall_occ < 0.1:
        return 0.75
    elif 0.1 <= overall_occ < 0.15:
        return 0.75
    elif 0.15 <= overall_occ < 0.3:
        return 0.75
    elif 0.3 <= overall_occ < 0.5:
        return 0.75
    elif 0.5 <= overall_occ < 0.7:
        return 0.80
    elif 0.7 <= overall_occ < 0.8:
        return 0.90
    elif 0.8 <= overall_occ < 0.9:
        return 1.00
    else:
        return 1.15


def base_price_correction_overall_with_moving_average_weekend(overall_occ):
    if np.isnan(overall_occ):
        return 0.75
    elif 0 <= overall_occ < 0.1:
        return 0.75
    elif 0.1 <= overall_occ < 0.15:
        return 0.75
    elif 0.15 <= overall_occ < 0.3:
        return 0.75
    elif 0.3 <= overall_occ < 0.5:
        return 0.75
    elif 0.5 <= overall_occ < 0.7:
        return 0.80
    elif 0.7 <= overall_occ < 0.8:
        return 0.90
    elif 0.8 <= overall_occ < 0.9:
        return 1.00
    else:
        return 1.15


def base_price_correction(overall_occ):
    if np.isnan(overall_occ):
        return 0.8
    elif 0 <= overall_occ < 0.3:
        return 0.8
    elif 0.3 <= overall_occ < 0.5:
        return 0.8
    elif 0.5 <= overall_occ < 0.7:
        return 0.8
    elif 0.7 <= overall_occ < 0.8:
        return 0.85
    elif 0.8 <= overall_occ < 0.9:
        return 0.92
    elif 0.9 <= overall_occ < 1:
        return 1.08
    else:
        return 1.12


def get_occ_target_from_map(oyo_id, date_str, occ_target_map):
    date = datetime.datetime.strptime(date_str, '%Y-%m-%d')
    weekday = date.weekday()
    wky_wkd = 'WKD' if weekday in [4, 5] else 'WKY'
    hotel_map = occ_target_map.get(oyo_id)
    if hotel_map is None:
        return None
    occ_target = hotel_map.get(wky_wkd)
    return occ_target


def get_price_ec_from_map(oyo_id, price_ec_map):
    price_ec = price_ec_map.get(oyo_id, -2.5)
    price_ec = price_ec if not (np.isnan(price_ec) or np.isinf(price_ec)) else -2.5
    return price_ec


def base_price_correction_with_customized_target(overall_occ, oyo_id, date, occ_target_map, price_ec_map):
    geo_label = geo_labels.get(oyo_id)
    occ_target = get_occ_target_from_map(oyo_id, date.strftime('%Y-%m-%d'), occ_target_map)
    price_ec = get_price_ec_from_map(oyo_id, price_ec_map)

    if occ_target is not None:
        pass
    elif geo_label in [0, 1]:
        occ_target = 0.40
    else:
        occ_target = 0.40

    # if geo_label in [0, 1]:
    #     occ_target = 0.65
    # elif oyo_id in low_pe_hotels:
    #     occ_target = 0.65
    # elif oyo_id in personalized_target_occ:
    #     occ_target = personalized_target_occ[oyo_id]
    # else:
    #     occ_target = 0.9

    tau = 1
    if np.isnan(overall_occ) or overall_occ == 0:
        return tau
    elif 0 <= overall_occ < 0.7 * occ_target:
        tau = 0.8
    elif 0.7 * occ_target <= overall_occ < 0.8 * occ_target:
        tau = 0.85
    elif 0.8 * occ_target <= overall_occ < 0.9 * occ_target:
        tau = 0.90
    elif 0.9 * occ_target <= overall_occ < 0.95 * occ_target:
        tau = 0.95
    elif 0.95 * occ_target <= overall_occ < 1 * occ_target:
        tau = 1.03
    elif 1 * occ_target <= overall_occ < 1.1 * occ_target:
        tau = 1.08
    elif 1.1 * occ_target <= overall_occ < 1.2 * occ_target:
        tau = 1.12
    elif 1.2 * occ_target <= overall_occ:
        tau = 1.2

    # ec method 1: adjust tau with price ec
    # ec_tau = (tau - 1.05) * (-1.5 / price_ec) + 1.0

    # ec method 2: calculate tau with ec
    ec_tau = 1.0 + (occ_target / overall_occ - 1.0) / price_ec
    ec_tau = min(1.25, max(0.8, ec_tau))

    return ec_tau


def get_occ_target_by_geo_label(geo_label):
    if geo_label in [0, 1]:
        return 0.65
    return 1


def get_gear_factor(urn_completion_and_occ_delta):
    """
    :param urn_completion_and_occ_delta:
    :return:
    """
    if -1 <= urn_completion_and_occ_delta < -0.15:
        return 2.5
    elif -0.15 <= urn_completion_and_occ_delta < 0:
        return 2
    elif 0 <= urn_completion_and_occ_delta < 0.15:
        return 1
    elif 0.15 <= urn_completion_and_occ_delta < 0.3:
        return 1.05
    else:
        return 1.1


def get_occ_ftd_target(past_occ):
    return min(max(past_occ * 1.2, past_occ + 0.15), 0.8)


def correct_base_price_based_on_past_online_arr(all_hotel_data_past_4_weeks, arr_list, past_3wk_data_hotel_sum):
    """

    :param all_hotel_data_past_4_weeks:

    In [946]: all_hotel_data_past_4_weeks
    Out[946]:
             oyo_id       date  online_urn  total_urn  sellable_rooms  online_gmv total_gmv  day
    0     CN_SHA030 2019-03-27         9.0       37.0              57      836.50    3062.5    2
    1     CN_SHA030 2019-03-28        12.0       28.0              57     1362.20    2546.2    3
    2     CN_SHA030 2019-03-29        16.0       26.0              57     1660.48   2418.48    4
    3     CN_SHA030 2019-03-30        12.0       24.0              57     1161.52   2101.52    5
    4     CN_SHA030 2019-03-31         6.0       12.0              57      626.80     966.8    6
    5     CN_SHA030 2019-04-01        13.0       16.0              57     1431.20    1581.2    0
    6     CN_SHA030 2019-04-02         5.0       11.0              57      510.60     810.6    1
    7     CN_SHA030 2019-04-03        15.0       18.0              57     1422.38   1572.38    2
    8     CN_SHA030 2019-04-04        15.0       20.0              57     1596.50    1944.5    3
    9     CN_SHA030 2019-04-05        18.0       20.0              57     1858.40    2008.4    4
    10    CN_SHA030 2019-04-06        14.0       18.0              57     1485.46   1685.46    5

    :param arr_list: 'CN_SHA013'
    :param past_3wk_data_hotel_sum:

    In [948]: past_data_hotel_sum
    Out[948]:
       day  online_occ  online_arr prop_age
    0    1    0.014493   90.350000      old
    1    2    0.007246  106.200000      old
    2    3    0.028986  120.675000      old
    3    4    0.079710   76.590909      old
    4    5    0.050725   82.342857      old
    5    6    0.028986   96.175000      old
    6    0    0.000000    0.000000      old

    :return:
    """
    # Select the selected hotels. Hotel_data_all_new contains URN And GMV
    targeted_hotel_data_past_4_weeks = all_hotel_data_past_4_weeks[
        all_hotel_data_past_4_weeks.oyo_id.map(lambda x: x in arr_list)]

    # Added Days
    targeted_hotel_data_past_4_weeks['day'] = targeted_hotel_data_past_4_weeks.date.map(lambda x: x.weekday())

    # Day level Revenue and URN of a Hotel
    targeted_hotel_online_data_past_4_weeks_by_day = \
        targeted_hotel_data_past_4_weeks.groupby(by=['day'], as_index=False).sum()[['day', 'online_gmv', 'online_urn']]

    # Calculated the ARR
    targeted_hotel_online_data_past_4_weeks_by_day[
        '4wk_online_arr'] = targeted_hotel_online_data_past_4_weeks_by_day.online_gmv / targeted_hotel_online_data_past_4_weeks_by_day.online_urn
    targeted_hotel_online_data_past_4_weeks_by_day = targeted_hotel_online_data_past_4_weeks_by_day.replace(
        [np.inf, -np.inf], np.nan)

    # fill in zero values to avg of ARR:
    mean_online_arr = past_3wk_data_hotel_sum.iloc[past_3wk_data_hotel_sum['online_arr'].nonzero()].online_arr.mean()
    arr_calc_tmp = past_3wk_data_hotel_sum.copy()
    arr_calc_tmp['base'] = arr_calc_tmp['online_arr'].apply(
        lambda arr: mean_online_arr if (arr < 1 or np.isnan(arr)) else arr)

    # if online_arr is Nan, fill in total arr
    mean_arr = arr_calc_tmp.iloc[arr_calc_tmp['arr'].nonzero()].arr.mean()
    arr_calc_tmp['base'] = arr_calc_tmp.apply(
        lambda row: (row['arr'] if row['arr'] > 1 else mean_arr) if (row['base'] < 1 or np.isnan(row['base'])) else row[
            'base'], axis=1)

    # Joining 4 weeks average arr and occ with 21 days occ and age
    targeted_hotel_online_data_past_4_weeks_by_day = pd.merge(targeted_hotel_online_data_past_4_weeks_by_day,
                                                              arr_calc_tmp, how='left', on=['day'])
    targeted_hotel_online_data_past_4_weeks_by_day = targeted_hotel_online_data_past_4_weeks_by_day.rename(
        columns={'online_occ': '3wk_online_occ',
                 'online_arr': '3wk_online_arr',
                 'arr': '3wk_arr'}
    )

    # Correct base price using `tau` (base correction factor) based on prev 2 weeks online OCC
    targeted_hotel_online_data_past_4_weeks_by_day['tau'] = targeted_hotel_online_data_past_4_weeks_by_day.apply(
        lambda row: base_price_correction(row['3wk_online_occ']), axis=1)
    mu = 0
    targeted_hotel_online_data_past_4_weeks_by_day[
        'corrected_base'] = targeted_hotel_online_data_past_4_weeks_by_day.tau * \
                            targeted_hotel_online_data_past_4_weeks_by_day['base'] + mu

    targeted_hotel_online_data_past_4_weeks_by_day = targeted_hotel_online_data_past_4_weeks_by_day.replace(
        [np.inf, -np.inf, np.nan], 0)

    return targeted_hotel_online_data_past_4_weeks_by_day


def base_occ_calculation(past_n_week_online_data_smart_hotel):
    # calculate the MOST RECENT 3d moving average per period
    if past_n_week_online_data_smart_hotel.empty:
        return pd.DataFrame(columns=['oyo_id', 'period', 'base_occ'])
    past_n_week_online_data_smart_hotel_dropna = past_n_week_online_data_smart_hotel.replace(np.inf, np.nan).dropna(subset=['occ'])
    if past_n_week_online_data_smart_hotel_dropna.empty:
        return pd.DataFrame(columns=['oyo_id', 'period', 'base_occ'])
    df_most_recent_3d_data_per_period = past_n_week_online_data_smart_hotel_dropna.groupby(['oyo_id', 'period']).tail(3)

    df_most_recent_3d_mean_per_period = df_most_recent_3d_data_per_period.groupby(['oyo_id', 'period'])['occ'].mean()

    # calculate the same 1d of last week
    df_most_recent_1d_data_last_week = past_n_week_online_data_smart_hotel_dropna.groupby(['oyo_id', 'period'])['occ'].nth([-7])

    # base occ = mean(occ[-3:-1]) * alpha + occ[-7] * (1-alpha)
    alpha = 0.3
    df_base_occ = df_most_recent_3d_mean_per_period * alpha + df_most_recent_1d_data_last_week * (1. - alpha)

    # if occ is invalid (na, inf or >=2), then fall back to 3d average
    valid_occ_selector = df_base_occ < 2
    df_base_occ[~valid_occ_selector] = df_most_recent_3d_mean_per_period[~valid_occ_selector]
    df_base_occ = df_base_occ.reset_index().rename(columns={'occ': 'base_occ'})
    return df_base_occ


def base_price_calculation(past_n_week_online_data_smart_hotel):
    if past_n_week_online_data_smart_hotel.empty:
        return pd.DataFrame(columns=['oyo_id', 'period', 'base_price'])
    # calculate the MOST RECENT 3d moving average per period
    df_most_recent_3d_data_per_period = past_n_week_online_data_smart_hotel.replace(np.inf, np.nan) \
        .dropna(subset=['base']).groupby(['oyo_id', 'period']).tail(3)

    if df_most_recent_3d_data_per_period.empty:
        return pd.DataFrame(columns=['oyo_id', 'period', 'base_price'])

    df_most_recent_3d_mean_per_period = df_most_recent_3d_data_per_period.groupby(['oyo_id', 'period'])['base'].mean()

    # base occ = mean(base[-3:-1])
    df_base_price = df_most_recent_3d_mean_per_period.reset_index().rename(columns={'base': 'base_price'})
    return df_base_price


def prepare_base_correction_data(all_hotel_past_n_week_online_data):
    """
    :param all_hotel_past_n_week_online_data:
    :param smart_hotel_oyo_id:
    :return:

    In [357]: past_n_week_online_data_smart_hotel
    Out[357]:
             oyo_id       date  online_urn  total_urn  sellable_rooms  online_gmv total_gmv  day  online_occ  online_arr  arr  base
    1461  CN_ZHI029 2019-05-23         NaN        NaN              61         NaN      None    3         NaN         NaN  NaN    69
    1462  CN_ZHI029 2019-05-24         NaN        NaN              61         NaN      None    4         NaN         NaN  NaN    69
    1463  CN_ZHI029 2019-05-25         NaN        NaN              61         NaN      None    5         NaN         NaN  NaN    69
    1464  CN_ZHI029 2019-05-26         NaN        NaN              61         NaN      None    6         NaN         NaN  NaN    69
    1465  CN_ZHI029 2019-05-27         NaN        NaN              61         NaN      None    0         NaN         NaN  NaN    69
    1466  CN_ZHI029 2019-05-28         NaN        NaN              61         NaN      None    1         NaN         NaN  NaN    69
    1467  CN_ZHI029 2019-05-29         NaN        NaN              61         NaN      None    2         NaN         NaN  NaN    69
    """
    tt01 = dt.datetime.strftime(dt.datetime.fromtimestamp(time.time()), "%Y-%m-%d")
    tt02 = dt.datetime.strftime(dt.datetime.fromtimestamp(time.time()) + dt.timedelta(days=6), "%Y-%m-%d")
    past_data_hotel_sum_temp = pd.DataFrame(pd.date_range(tt01, tt02, freq='D'), columns=['date'])
    past_data_hotel_sum_temp['day'] = past_data_hotel_sum_temp.date.map(lambda x: x.weekday())

    all_hotel_past_n_week_online_data['period'] = all_hotel_past_n_week_online_data['day'].apply(
        lambda day: 'weekend' if day in [4, 5] else 'weekday')

    # adding Occ
    all_hotel_past_n_week_online_data[
        'occ'] = all_hotel_past_n_week_online_data.total_urn / all_hotel_past_n_week_online_data.sellable_rooms
    # adding ARR
    all_hotel_past_n_week_online_data[
        'online_arr'] = all_hotel_past_n_week_online_data.online_gmv / all_hotel_past_n_week_online_data.online_urn

    # if base is NaN, use overall arr
    all_hotel_past_n_week_online_data[
        'overall_arr'] = all_hotel_past_n_week_online_data.total_gmv / all_hotel_past_n_week_online_data.total_urn

    def _calc_base(overall_arr, online_arr):
        return overall_arr if np.isnan(online_arr) or online_arr < 1 else online_arr

    all_hotel_past_n_week_online_data = DFUtil.apply_func_for_df(all_hotel_past_n_week_online_data, 'base',
                                                                 ['overall_arr', 'online_arr'], lambda values: _calc_base(*values))

    # Experiment: use model pms price instead of arr
    all_hotel_past_n_week_online_data['use_pms_price_flag'] = True
    # Change all to use pms price
    all_hotel_past_n_week_online_data['base'] = all_hotel_past_n_week_online_data.pms_price

    past_n_week_online_data_smart_hotel = all_hotel_past_n_week_online_data.sort_values(by=['oyo_id', 'date'])

    # add period occ
    group_result = past_n_week_online_data_smart_hotel.groupby(['oyo_id', 'period']).sum().reset_index()
    group_columns = ['oyo_id', 'period', 'online_urn', 'total_urn', 'sellable_rooms', 'online_gmv', 'total_gmv']
    data_by_period = pd.DataFrame(columns=group_columns) if group_result.empty else group_result[group_columns]

    data_by_period['period_occ'] = data_by_period.total_urn / data_by_period.sellable_rooms

    past_n_week_online_data_smart_hotel = past_n_week_online_data_smart_hotel.merge(data_by_period[['oyo_id', 'period', 'period_occ']], on=['oyo_id', 'period'], how='left')

    # # old rule
    # #==================
    # # calculate the MOST RECENT 3d moving average per period
    # df_most_recent_3d_data_per_period = past_n_week_online_data_smart_hotel.groupby(['oyo_id', 'period']).tail(3)
    # df_most_recent_3d_groupby_period = df_most_recent_3d_data_per_period.groupby(['oyo_id', 'period'])['occ']
    #
    # # calculate the occ difference as: occ_diff = (last_day_of_window - base_occ)
    # df_3d_window_occ_diff_0 = (df_most_recent_3d_groupby_period.nth([-1]).replace(np.nan, 0) - df_most_recent_3d_groupby_period.mean().replace(np.nan, 0)).reset_index()
    # df_3d_window_occ_diff_0 = df_3d_window_occ_diff_0.rename(columns={'occ': 'occ_diff'})
    # df_most_recent_3d_data_per_period_avg_0 = df_most_recent_3d_data_per_period[
    #     ['oyo_id', 'period', 'base', 'occ']].groupby(['oyo_id', 'period']).mean().reset_index()
    # df_most_recent_3d_data_per_period_avg_0 = df_most_recent_3d_data_per_period_avg_0.rename(
    #     columns={'base': 'base_price', 'occ': 'base_occ'})
    # #==================


    df_base_price = base_price_calculation(past_n_week_online_data_smart_hotel)
    df_base_occ = base_occ_calculation(past_n_week_online_data_smart_hotel)
    df_3d_window_occ_diff = df_base_occ.merge(past_n_week_online_data_smart_hotel.groupby(['oyo_id', 'period']).nth([-1])[['occ']], on=['oyo_id', 'period'], how='left')
    df_3d_window_occ_diff['occ_diff'] = (df_3d_window_occ_diff['occ'] - df_3d_window_occ_diff['base_occ']).replace(np.nan, 0).replace(np.inf, 0)
    df_3d_window_occ_diff = pd.DataFrame(columns=['oyo_id', 'period', 'occ_diff']) if df_3d_window_occ_diff.empty else df_3d_window_occ_diff[['oyo_id', 'period', 'occ_diff']]

    df_most_recent_3d_data_per_period_avg = df_base_price.merge(df_base_occ, on=['oyo_id', 'period'], how='left')
    df_most_recent_3d_data_per_period_avg = df_most_recent_3d_data_per_period_avg.merge(df_3d_window_occ_diff, on=['oyo_id', 'period'], how='left')

    past_n_week_online_data_smart_hotel = past_n_week_online_data_smart_hotel.merge(
        df_most_recent_3d_data_per_period_avg, on=['oyo_id', 'period'], how='left')

    past_n_week_online_data_smart_hotel['corrected_base'] = past_n_week_online_data_smart_hotel['base']
    past_n_week_online_data_smart_hotel['tau'] = np.nan

    return past_n_week_online_data_smart_hotel


def base_correction_with_moving_average(oyo_id, past_n_week_online_data_smart_hotel, occ_target_map, price_ec_map):

    def _choose_base(base_price, base):
        return base if np.isnan(base_price) else base_price

    past_n_week_online_data_smart_hotel = DFUtil.apply_func_for_df(past_n_week_online_data_smart_hotel, 'base',
                                                                   ['base_price', 'base'],
                                                                   lambda values: _choose_base(*values))

    def _choose_base_occ(base_occ, occ):
        return occ if np.isnan(base_occ) else base_occ

    past_n_week_online_data_smart_hotel = DFUtil.apply_func_for_df(past_n_week_online_data_smart_hotel, 'base_occ',
                                                                   ['base_occ', 'occ'],
                                                                   lambda values: _choose_base_occ(*values))

    def _calc_tau(base_occ, oyo_id, date):
        return base_price_correction_with_customized_target(base_occ, oyo_id, date, occ_target_map, price_ec_map)

    past_n_week_online_data_smart_hotel = DFUtil.apply_func_for_df(past_n_week_online_data_smart_hotel, 'tau',
                                                                   ['base_occ', 'oyo_id', 'date'],
                                                                   lambda values: _calc_tau(*values))

    mu = 0

    # set tau to 1, if tau and occ_diff has different sign.
    # e.g tau is 1.15, occ_diff is -0.05: occ is decreasing, so stop increasing the price
    def _adjust_tau(tau, occ_diff):
        return 1 if (tau - 1) * occ_diff < 0 and abs(occ_diff) >= 0.10 else tau

    past_n_week_online_data_smart_hotel = DFUtil.apply_func_for_df(past_n_week_online_data_smart_hotel, 'tau',
                                                                   ['tau', 'occ_diff'],
                                                                   lambda values: _adjust_tau(*values))

    def _calc_corrected_base(tau, base):
        return tau * base + mu

    past_n_week_online_data_smart_hotel = DFUtil.apply_func_for_df(past_n_week_online_data_smart_hotel,
                                                                   'corrected_base', ['tau', 'base'],
                                                                   lambda values: _calc_corrected_base(*values))

    return past_n_week_online_data_smart_hotel


def base_correction_with_ebase(hotel_price_data, hotel_ebase_price):

    if hotel_ebase_price is None:
        # do nothing, but supplement as intermediate results
        hotel_price_data['ebase_price'] = ''
        return hotel_price_data

    hotel_price_data = hotel_price_data.merge(hotel_ebase_price, left_on=['oyo_id', 'date'],
                                              right_on=['oyo_id', 'sku_date'], how='left')

    def fallback_with_ebase(corrected_base, ebase_price):
        if MiscUtil.is_empty_value(ebase_price):
            return corrected_base
        return ebase_price

    hotel_price_data = DFUtil.apply_func_for_df(hotel_price_data, 'corrected_base',
                                                                  ['corrected_base', 'ebase_price'],
                                                                  lambda values: fallback_with_ebase(*values))
    return hotel_price_data


def base_correction_with_period_occ(past_n_week_online_data_smart_hotel):
    # Correct base price using `tau` (base correction factor) based on prev weeks online OCC
    past_n_week_online_data_smart_hotel['tau'] = past_n_week_online_data_smart_hotel.apply(
        lambda row: base_price_correction_overall_occ(row['period_occ']), axis=1)

    mu = 0

    past_n_week_online_data_smart_hotel[
        'corrected_base'] = past_n_week_online_data_smart_hotel.tau * \
                            past_n_week_online_data_smart_hotel['base'] + mu

    return past_n_week_online_data_smart_hotel


def get_hotel_urn_gmv_by_city(city_list, start_day_offset, end_day_offset, oracle_query_manager):
    """
    :param city_list:
    :param start_day_offset:
    :param end_day_offset:
    :param oracle_query_manager:
    :return:

             oyo_id       date  online_urn  total_urn  sellable_rooms  online_gmv total_gmv  day
    0     CN_SHA030 2019-03-27         9.0       37.0              57      836.50    3062.5    2
    1     CN_SHA030 2019-03-28        12.0       28.0              57     1362.20    2546.2    3
    2     CN_SHA030 2019-03-29        16.0       26.0              57     1660.48   2418.48    4
    3     CN_SHA030 2019-03-30        12.0       24.0              57     1161.52   2101.52    5
    4     CN_SHA030 2019-03-31         6.0       12.0              57      626.80     966.8    6
    5     CN_SHA030 2019-04-01        13.0       16.0              57     1431.20    1581.2    0
    6     CN_SHA030 2019-04-02         5.0       11.0              57      510.60     810.6    1
    7     CN_SHA030 2019-04-03        15.0       18.0              57     1422.38   1572.38    2
    8     CN_SHA030 2019-04-04        15.0       20.0              57     1596.50    1944.5    3
    9     CN_SHA030 2019-04-05        18.0       20.0              57     1858.40    2008.4    4
    10    CN_SHA030 2019-04-06        14.0       18.0              57     1485.46   1685.46    5
    11    CN_SHA030 2019-04-07        11.0       16.0              57     1108.80    1358.8    6
    12    CN_SHA030 2019-04-08        16.0       19.0              57     1602.06   1752.06    0
    13    CN_SHA030 2019-04-09        13.0       18.0              57     1317.60    1567.6    1
    14    CN_SHA030 2019-04-10        12.0       12.0              57     1285.60    1285.6    2
    15    CN_SHA030 2019-04-11        16.0       16.0              57     1696.20    1696.2    3
    16    CN_SHA030 2019-04-12        13.0        0.0              57        0.00         0    4
    17    CN_SHA030 2019-04-13        12.0       11.0              57      785.30     785.3    5
    18    CN_SHA030 2019-04-14        13.0       13.0              57     1204.86   1204.86    6
    19    CN_SHA030 2019-04-15        16.0       16.0              57     1681.40    1681.4    0
    20    CN_SHA030 2019-04-16        16.0       16.0              57     1755.06   1755.06    1
    21    CN_SHA030 2019-04-17        14.0       14.0              57     1480.54   1480.54    2
    22    CN_SHA030 2019-04-18        17.0       17.0              57     1873.84   1873.84    3
    23    CN_SHA030 2019-04-19        34.0       34.0              57     3872.36   3872.36    4
    24    CN_SHA030 2019-04-20        13.0       13.0              57     1215.22   1215.22    5
    25    CN_SHA030 2019-04-21         6.0        6.0              57      620.34    620.34    6
    26    CN_SHA030 2019-04-22         6.0        6.0              57      567.10     567.1    0
    27    CN_SHA029 2019-03-27         3.0       31.0              39      281.45   3085.45    2
    28    CN_SHA029 2019-03-28         2.0       30.0              39      214.80    3206.8    3
    29    CN_SHA029 2019-03-29         7.0       33.0              39      777.40    3501.4    4
    ...         ...        ...         ...        ...             ...         ...       ...  ...


    """
    # select crs_id as oyo_id,to_date(date_s,'yyyy-mm-dd') AS "date", online_brn as online_urn, tot_urn as total_urn, tot_srn AS sellable_rooms, app_gmv/0.93 + ota_gmv AS online_gmv, selling_price as total_gmv
    past_sql = '''
    select crs_id as oyo_id,to_date(date_s,'yyyy-mm-dd') AS "date", ota_brn as online_urn, tot_urn as total_urn, tot_srn AS sellable_rooms, ota_gmv AS online_gmv, selling_price as total_gmv
      from oyo_dw.V_RPT_HOTEL_SRN_URN a
      where to_date(date_s,'yyyy-mm-dd') between (current_date-{}) and (current_date -{})
        and  HOTEL_STATUS IN ('Live','Active')
        and a.crs_id in 
            (select oyo_id from
            OYO_DW.V_DIM_HOTEL h
            LEFT join OYO_DW.dim_zones ct
              on h.cluster_id = ct.id
            where ct.city_name in {})
        '''.format(start_day_offset, end_day_offset, city_list)
    begin = time.time()
    past_data = oracle_query_manager.read_sql(past_sql, 60)
    logger = LogUtil.get_cur_logger()
    logger.info('past_data cost: %0.2fs, records: %d', time.time() - begin, past_data.size)

    past_data['day'] = past_data.date.map(lambda x: x.weekday())
    logger.info('past_data process')
    past_data = past_data.replace([np.inf, -np.inf, np.nan, None], 0)
    past_data['total_gmv'] = past_data['total_gmv'].astype(float)
    past_data.online_urn = past_data.online_urn.map(lambda x: np.where(pd.isnull(x), 0, x))
    past_data.online_gmv = past_data.online_gmv.map(lambda x: np.where(pd.isnull(x), 0, x))
    past_data.total_urn = past_data.total_urn.map(lambda x: np.where(pd.isnull(x), 0, x))
    past_data.total_gmv = past_data.total_gmv.map(lambda x: np.where(pd.isnull(x), 0, x))

    return past_data


def get_urn_upstream_offset(start_date_str, mysql_query_mgr):
    date_format = '%Y-%m-%d'
    start_day = datetime.datetime.strptime(start_date_str, date_format)
    etl_most_recent_day = datetime.datetime.strptime(get_max_calc_day_from_etl_log('rpt_hotel_srn_urn', mysql_query_mgr), date_format)
    day_offset = (start_day - etl_most_recent_day).days
    # further fault tolerance
    day_offset = 0 if day_offset < 0 else day_offset
    LogUtil.get_cur_logger().info('get_urn_upstream_offset, start_day: {}, etl_most_recent_day: {}, day_delta: {}'.format(
        start_day, etl_most_recent_day, day_offset))
    return day_offset


def get_max_calc_day_from_etl_log(job_name, mysql_query_mgr):
    query = """
        select date_format(calc_day, '%%Y-%%m-%%d') as calc_day
        from oyo_pricing_offline.prod_etl_job_log
        where job_name = '{0}'
        order by calc_day desc limit 1
    """.format(job_name)
    df = mysql_query_mgr.read_sql(query, 60)
    return df['calc_day'].iloc[0]


def get_max_create_time(max_calc_date, table_name, mysql_query_mgr):
    query = """
    select created_at as created_at
    from {table_name}
    where calc_date = '{max_calc_date}'
    order by created_at desc limit 1
    """.format(max_calc_date=max_calc_date, table_name=table_name)
    df = mysql_query_mgr.read_sql(query, 240)
    return df['created_at'].iloc[0].strftime('%Y-%m-%d %H:%M:%S')


def get_hotel_urn_gmv_by_hotel(smart_hotel_oyo_id_list, start_day_offset, end_day_offset, calc_day, mysql_query_mgr):
    """
    :param all_hotels_str:
    :param start_day_offset:
    :param end_day_offset:
    :param mysql_query_mgr:
    :return:

             oyo_id       date  online_urn  total_urn  sellable_rooms  online_gmv total_gmv  day
    0     CN_SHA030 2019-03-27         9.0       37.0              57      836.50    3062.5    2
    1     CN_SHA030 2019-03-28        12.0       28.0              57     1362.20    2546.2    3
    2     CN_SHA030 2019-03-29        16.0       26.0              57     1660.48   2418.48    4
    3     CN_SHA030 2019-03-30        12.0       24.0              57     1161.52   2101.52    5
    4     CN_SHA030 2019-03-31         6.0       12.0              57      626.80     966.8    6
    5     CN_SHA030 2019-04-01        13.0       16.0              57     1431.20    1581.2    0
    6     CN_SHA030 2019-04-02         5.0       11.0              57      510.60     810.6    1
    7     CN_SHA030 2019-04-03        15.0       18.0              57     1422.38   1572.38    2
    8     CN_SHA030 2019-04-04        15.0       20.0              57     1596.50    1944.5    3
    9     CN_SHA030 2019-04-05        18.0       20.0              57     1858.40    2008.4    4
    10    CN_SHA030 2019-04-06        14.0       18.0              57     1485.46   1685.46    5
    11    CN_SHA030 2019-04-07        11.0       16.0              57     1108.80    1358.8    6
    12    CN_SHA030 2019-04-08        16.0       19.0              57     1602.06   1752.06    0
    13    CN_SHA030 2019-04-09        13.0       18.0              57     1317.60    1567.6    1
    14    CN_SHA030 2019-04-10        12.0       12.0              57     1285.60    1285.6    2
    15    CN_SHA030 2019-04-11        16.0       16.0              57     1696.20    1696.2    3
    16    CN_SHA030 2019-04-12        13.0        0.0              57        0.00         0    4
    17    CN_SHA030 2019-04-13        12.0       11.0              57      785.30     785.3    5
    18    CN_SHA030 2019-04-14        13.0       13.0              57     1204.86   1204.86    6
    19    CN_SHA030 2019-04-15        16.0       16.0              57     1681.40    1681.4    0
    20    CN_SHA030 2019-04-16        16.0       16.0              57     1755.06   1755.06    1
    21    CN_SHA030 2019-04-17        14.0       14.0              57     1480.54   1480.54    2
    22    CN_SHA030 2019-04-18        17.0       17.0              57     1873.84   1873.84    3
    23    CN_SHA030 2019-04-19        34.0       34.0              57     3872.36   3872.36    4
    24    CN_SHA030 2019-04-20        13.0       13.0              57     1215.22   1215.22    5
    25    CN_SHA030 2019-04-21         6.0        6.0              57      620.34    620.34    6
    26    CN_SHA030 2019-04-22         6.0        6.0              57      567.10     567.1    0
    27    CN_SHA029 2019-03-27         3.0       31.0              39      281.45   3085.45    2
    28    CN_SHA029 2019-03-28         2.0       30.0              39      214.80    3206.8    3
    29    CN_SHA029 2019-03-29         7.0       33.0              39      777.40    3501.4    4
    ...         ...        ...         ...        ...             ...         ...       ...  ...


    """
    past_sql = '''
    select oyo_id, date_s AS "date", calc_date, online_brn as online_urn, case when tot_long_private_urn is null then tot_urn when tot_long_private_urn > tot_urn then 0 else tot_urn - tot_long_private_urn end as total_urn, case when tot_long_private_urn is null then tot_srn when tot_long_private_urn >= tot_srn then 1 else tot_srn - tot_long_private_urn end AS sellable_rooms, app_pre_gmv + ota_pre_gmv AS online_gmv, tot_pre_gmv as total_gmv, created_at
    from oyo_pricing_offline.prod_rpt_hotel_srn_urn
    where date_s between date_format(date_sub(current_date, interval {start_day_offset} day), '%%Y%%m%%d') and date_format(date_sub(current_date, interval {end_day_offset} day), '%%Y%%m%%d')
      and oyo_id in {oyo_ids} and calc_date = '{calc_day}'
    order by oyo_id, date_s, calc_date, created_at
  '''.format(start_day_offset=start_day_offset, end_day_offset=end_day_offset,
             oyo_ids = MiscUtil.convert_list_to_tuple_list_str(smart_hotel_oyo_id_list),
             calc_day=calc_day)
    begin = time.time()
    past_data = mysql_query_mgr.read_sql(past_sql, 60)
    past_data = past_data.groupby(by=['oyo_id', 'date', 'calc_date'], as_index=False).tail(1)
    past_data.drop(columns=['calc_date', 'created_at'], inplace=True)
    logger = LogUtil.get_cur_logger()
    logger.info('past_data cost: %0.2fs, records: %d', time.time() - begin, past_data.size)

    past_data['date'] = past_data.date.map(lambda x: datetime.datetime.strptime(x, '%Y%m%d'))
    past_data['day'] = past_data.date.map(lambda x: x.weekday())
    past_data['date'] = past_data.date.map(lambda x: x.date())
    logger.info('past_data process')
    past_data = past_data.replace([np.inf, -np.inf, np.nan, None], 0)
    past_data['total_gmv'] = past_data['total_gmv'].astype(float)
    past_data.online_urn = past_data.online_urn.map(lambda x: np.where(pd.isnull(x), 0, x))
    past_data.online_gmv = past_data.online_gmv.map(lambda x: np.where(pd.isnull(x), 0, x))
    past_data.total_urn = past_data.total_urn.map(lambda x: np.where(pd.isnull(x), 0, x))
    past_data.total_gmv = past_data.total_gmv.map(lambda x: np.where(pd.isnull(x), 0, x))

    return past_data


def get_pms_price_from_pricing_context(smart_hotel_oyo_id_list, start_day_offset, end_day_offset, mysql_query_mgr):
    past_sql = '''
        select oyo_id, date, avg(pms_price) as pms_price
        from pricing_context
        where create_time between date_sub(current_date, interval {start} day) and date_sub(current_date, interval {end} day)
          and date between date_sub(current_date, interval {start} day) and date_sub(current_date, interval {end} day)
          and date = date(create_time)
          and {ids}
          and liquidation_sale_price is null
          and liquidation_strategy_type is null
        group by oyo_id, date
        order by oyo_id, date
    '''.format(start=start_day_offset, end=end_day_offset, ids=MiscUtil.convert_to_oracle_query_oyo_id_list_str('oyo_id', smart_hotel_oyo_id_list))
    begin = time.time()
    past_pms_price = mysql_query_mgr.read_sql(past_sql, 600)
    logger = LogUtil.get_cur_logger()
    logger.info('past_pms_price cost: %0.2fs, records: %d', time.time() - begin, past_pms_price.size)

    return past_pms_price


def get_hot_day_price_from_dingtalk(smart_hotel_oyo_id_list, start_day_offset, end_day_offset, mysql_query_mgr):
    hot_day_price_sql = """
    select oyo_id, date_format(start_date, '%%Y-%%m-%%d') as hot_day_start_date, date_format(end_date, '%%Y-%%m-%%d') as hot_day_end_date
    from ding_talk_approve
    where process_type = 0
      and ding_status = 'COMPLETED'
      and ding_result = 'agree'
      and {ids}
      and ((date_sub(current_date, interval {start} day) < start_date
      and date_sub(current_date, interval {end} day) >= start_date)
      or (date_sub(current_date, interval {start} day) >= start_date
        and date_sub(current_date, interval {end} day) <= end_date))
    """.format(start=start_day_offset,
               end=end_day_offset,
               ids=MiscUtil.convert_to_oracle_query_oyo_id_list_str('oyo_id', smart_hotel_oyo_id_list))
    begin = time.time()
    hot_day_price_df = mysql_query_mgr.read_sql(hot_day_price_sql, 600)
    logger = LogUtil.get_cur_logger()
    logger.info('get_hot_day_price_from_dingtalk cost: %0.2fs, records: %d', time.time() - begin, hot_day_price_df.size)

    return get_hotel_hot_day_map(hot_day_price_df)


def get_occ_target_map_from_pc(smart_hotel_oyo_id_list, mysql_query_mgr):
    occ_target_sql = """
    select * from hotel_pricing_occ_target
    where {ids}
    and is_delete = 0
    """.format(ids=MiscUtil.convert_to_oracle_query_oyo_id_list_str('oyo_id', smart_hotel_oyo_id_list))
    begin = time.time()
    occ_target_df = mysql_query_mgr.read_sql(occ_target_sql, 600)
    LogUtil.get_cur_logger().info('get_occ_target_from_pc cost: %0.2fs, records: %d', time.time() - begin, occ_target_df.size)
    occ_target_map = compose_occ_target_map_from_df(occ_target_df)
    return occ_target_map


def get_new_active_hotel_days(smart_hotel_oyo_id_list, start_date, days, mysql_query_mgr):
    ids = MiscUtil.convert_to_oracle_query_oyo_id_list_str('oyo_id', smart_hotel_oyo_id_list, 1000)
    hotel_calc_day = get_max_calc_day_from_etl_log('dim_china20_hotel', mysql_query_mgr)
    hotel_created_at = get_max_create_time(hotel_calc_day, 'oyo_pricing_offline.prod_dim_china20_hotel', mysql_query_mgr)
    resale_calc_day = get_max_calc_day_from_etl_log('upload_resale_hotel_detail', mysql_query_mgr)
    resale_created_at = get_max_create_time(resale_calc_day, 'oyo_pricing_offline.prod_upload_resale_hotel_detail', mysql_query_mgr)
    query_sql = """
    select *
    from (select oyo_id,
                 date_format(share_start_date, '%%Y-%%m-%%d') start_day,
                 datediff(curdate(), share_start_date) as  date_diff
          from oyo_pricing_offline.prod_dim_china20_hotel
          where life_cycle_status = 'VALID'
            and sub_template_code like '%%WIN%%'
            and share_start_date is not null
            and share_end_date is not null
            and share_start_date <= '{start_date}'
            and {ids}
            and created_at = '{hotel_created_at}'
            and share_start_date >= date_sub(str_to_date('{start_date}', '%%Y-%%m-%%d'), interval {days} day)
          union all
          select oyo_id,
                 date_add(date_s, interval 1 day)                      as start_day,
                 datediff(curdate(), date_add(date_s, interval 1 day)) as date_diff
          from oyo_pricing_offline.prod_upload_resale_hotel_detail
          where date_add(date_s, interval 1 day) <= '{start_date}'
            and {ids}
            and created_at = '{resale_created_at}'
            and date_add(date_s, interval 1 day) >= date_sub(str_to_date('{start_date}', '%%Y-%%m-%%d'), interval 3 day)) a
    where a.date_diff <= {days}
    """.format(ids=ids, start_date=start_date, days=days, hotel_created_at=hotel_created_at, resale_created_at=resale_created_at)
    return mysql_query_mgr.read_sql(query_sql, 120)


def get_hotel_pricing_start_day(smart_hotel_oyo_id_list, mysql_query_mgr):
    sql = """
    select oyo_id, date_format(pricing_start_date, '%%Y-%%m-%%d') as start_day from pricing_hotel 
    where oyo_id in {ids}
    and status = 1
    and deleted = 0
    """.format(ids=MiscUtil.convert_list_to_tuple_list_str(smart_hotel_oyo_id_list))
    return mysql_query_mgr.read_sql(sql, 120)


def get_adjust_ratio_divisor_map_from_oracle(smart_hotel_oyo_id_list, start_date, mysql_query_mgr):
    date_diff_df = get_new_active_hotel_start_days(smart_hotel_oyo_id_list, start_date, 6, mysql_query_mgr)
    return compose_adjust_ratio_divisor(date_diff_df)


def get_resale_hotel_count(start_date, mysql_query_mgr):
    resale_calc_day = get_max_calc_day_from_etl_log('upload_resale_hotel_detail', mysql_query_mgr)
    resale_created_at = get_max_create_time(resale_calc_day, 'oyo_pricing_offline.prod_upload_resale_hotel_detail',
                                            mysql_query_mgr)
    sql = """
    select count(*)
    from oyo_pricing_offline.prod_upload_resale_hotel_detail
    where date_add(date_s, interval 1 day) = '{start_date}'
      and created_at = '{resale_created_at}'
    """.format(start_date=start_date, resale_created_at=resale_created_at)
    df = mysql_query_mgr.read_sql(sql, 60)
    return df.iloc[0][0]


def get_new_active_hotel_start_days(smart_hotel_oyo_id_list, start_date, days, mysql_query_mgr):
    date_diff_df = get_new_active_hotel_days(smart_hotel_oyo_id_list, start_date, days, mysql_query_mgr)
    return date_diff_df.sort_values(['oyo_id', 'date_diff']).groupby('oyo_id').head(1)


def get_all_hotel_active_day(smart_hotel_oyo_id_list, start_date, days, mysql_query_mgr):
    new_active_hotel_days_df = get_new_active_hotel_start_days(smart_hotel_oyo_id_list, start_date, days, mysql_query_mgr)
    new_active_hotel_days_df = new_active_hotel_days_df[['oyo_id', 'start_day']]
    pricing_start_day_df = get_hotel_pricing_start_day(smart_hotel_oyo_id_list, mysql_query_mgr)
    all_hotel_active_day_df = pd.concat([new_active_hotel_days_df, pricing_start_day_df], ignore_index = True)
    result_df = all_hotel_active_day_df.sort_values(['oyo_id', 'start_day']).groupby('oyo_id').tail(1)
    return result_df


def compose_adjust_ratio_divisor(date_diff_df: pd.DataFrame):
    result_map = {}

    def date_diff_map_to_divisor(date_diff):
        if 0 <= date_diff <= 2:
            return 3
        elif 3 <= date_diff <= 6:
            return 2
        else:
            raise Exception('unexpected date_diff: {}'.format(date_diff))
    for index, row in date_diff_df.iterrows():
        oyo_id = row['oyo_id']
        divisor = date_diff_map_to_divisor(row['date_diff'])
        result_map[oyo_id] = divisor
    return result_map


def compose_occ_target_map_from_df(occ_target_df: pd.DataFrame):
    result_map = {}
    for index, row in occ_target_df.iterrows():
        oyo_id = row['oyo_id']
        weekend_weekday = row['weekday_weekend']
        occ_target = row['occ_target']
        hotel_map = result_map.get(oyo_id)
        if hotel_map is None:
            hotel_map = {}
            result_map[oyo_id] = hotel_map
        hotel_map[weekend_weekday] = occ_target
    return result_map


def get_hotel_hot_day_map(hot_day_df):
    hot_day_price_map = {}
    for index, row in hot_day_df.iterrows():
        oyo_id = row['oyo_id']
        start_date = row['hot_day_start_date']
        end_date = row['hot_day_end_date']
        date_range_lst = hot_day_price_map.get(oyo_id)
        if date_range_lst is None:
            date_range_lst = []
            hot_day_price_map[oyo_id] = date_range_lst
        date_range_lst.append((start_date, end_date))
    return hot_day_price_map


def get_mg_target_by_hotel(smart_hotel_oyo_id_list, today, oracle_query_manager):
    sql = '''
    select oyo_id, month, daily_revpar_target from OYO_DW.dim_china20_daily_revpar_target
        where {}
        and month in ({}, {}) '''.format(
        MiscUtil.convert_to_oracle_query_oyo_id_list_str('oyo_id', smart_hotel_oyo_id_list),
        today.month,
        today.month + 1
    )
    begin = time.time()
    mg_data = oracle_query_manager.read_sql(sql, 60)
    mg_data['month'] = mg_data['month'].astype(int)
    logger = LogUtil.get_cur_logger()
    logger.info('mg_data cost: %0.2fs, records: %d', time.time() - begin, mg_data.size)
    return mg_data


def get_price_lift_exp_data(smart_hotel_oyo_id_list, oracle_query_manager):
    sql = '''
        select crs_id as oyo_id, lift
        from OYO_ANALYZE.price_alot_test
        where {}'''.format(
        MiscUtil.convert_to_oracle_query_oyo_id_list_str('crs_id', smart_hotel_oyo_id_list)
    )
    begin = time.time()
    price_lift_data = oracle_query_manager.read_sql(sql, 60)
    logger = LogUtil.get_cur_logger()
    logger.info('price_lift_data cost: %0.2fs, records: %d', time.time() - begin, price_lift_data.size)
    return price_lift_data


def get_cluster_urn_completion_trend_line(start_date, city_total_urn_past_30d, clus_hotel_list):
    """

    :param city_total_urn_past_30d:

           date      oyo_id city  cluster_urn  urn_30  urn_29  urn_28  urn_27  urn_26  ...    urn_8  urn_7  urn_6  urn_5  urn_4  urn_3  urn_2  urn_1  urn_0
    0    2019-03-01   CN_SHA002                 4       4       4       4       4       4  ...        4      4      4      4      4      4      4      4      3
    1    2019-03-01   CN_SHA003                36      36      36      36      36      36  ...       35     35     35     35     35     35     34     34     25
    2    2019-03-01   CN_SHA004                57      57      57      57      57      57  ...       56     55     55     53     52     51     50     50     32
    3    2019-03-01   CN_SHA006                38      38      38      38      38      38  ...       37     36     35     35     35     33     31     29     20
    4    2019-03-01   CN_SHA007                11      11      11      11      11      11  ...       10     10     10     10     10     10     10     10      8
    5    2019-03-01   CN_SHA008                10      10      10      10      10      10  ...        9      9      9      9      9      9      9      9      1
    6    2019-03-01   CN_SHA009                29      26      26      26      26      26  ...       25     25     24     23     23     19     16     16      9
    7    2019-03-01   CN_SHA010                32      32      32      32      32      32  ...       32     32     32     32     32     32     32     32     22
    8    2019-03-01   CN_SHA011                28      28      28      28      28      28  ...       19     19     18     18     18     18     18     16     12

    :param clus_hotel_list:
    :return:

    In [952]: clus_urn_completion_trend
    Out[952]:
              date  day  urn_completion
    0   2019-04-19    4        0.543716
    1   2019-04-20    5        0.324176
    2   2019-04-21    6        0.369637
    3   2019-04-22    0        0.307443
    4   2019-04-23    1        0.263158
    5   2019-04-24    2        0.185596
    6   2019-04-25    3        0.163415
    7   2019-04-26    4        0.133880
    8   2019-04-27    5        0.131868
    9   2019-04-28    6        0.155116
    10  2019-04-29    0        0.142395
    11  2019-04-30    1        0.122807
    12  2019-05-01    2        0.105263
    13  2019-05-02    3        0.090244
    14  2019-05-03    4        0.068306
    15  2019-05-04    5        0.068681
    16  2019-05-05    6        0.079208
    17  2019-05-06    0        0.071197
    18  2019-05-07    1        0.058480
    19  2019-05-08    2        0.049861
    20  2019-05-09    3        0.041463
    21  2019-05-10    4        0.032787
    22  2019-05-11    5        0.032967
    23  2019-05-12    6        0.039604
    24  2019-05-13    0        0.038835
    25  2019-05-14    1        0.029240
    26  2019-05-15    2        0.024931
    27  2019-05-16    3        0.019512
    28  2019-05-17    4        0.008197
    29  2019-05-18    5        0.008242
    30  2019-05-19    6        0.009901


    """
    cluster_total_urn_past_30d = city_total_urn_past_30d[city_total_urn_past_30d.oyo_id.isin(clus_hotel_list)]
    cluster_total_urn_past_30d['day'] = cluster_total_urn_past_30d.date.map(lambda x: x.weekday())
    cluster_total_urn_past_30d_by_day = cluster_total_urn_past_30d.groupby(by=['day'], as_index=False).sum()

    # urn_completion number will tell us how many booking were made before the date of stay.
    for x in range(31):
        cluster_total_urn_past_30d_by_day['urn_completion_' + str(x)] = 1 - cluster_total_urn_past_30d_by_day[
            'urn_' + str(x)] / cluster_total_urn_past_30d_by_day.cluster_urn

    # construct calendar for next 30 days
    tt02 = dt.datetime.strftime(
        dt.datetime.fromtimestamp(time.mktime(time.strptime(start_date, '%Y-%m-%d'))) + dt.timedelta(days=30),
        "%Y-%m-%d")

    cluster_urn_completion_next30 = pd.DataFrame(pd.date_range(start_date, tt02, freq='D'), columns=['date'])
    cluster_urn_completion_next30['day'] = cluster_urn_completion_next30.date.map(lambda x: x.weekday())
    cluster_urn_completion_next30 = pd.merge(cluster_urn_completion_next30, cluster_total_urn_past_30d_by_day,
                                             how='left', on=['day'])
    cluster_urn_completion_next30['urn_completion'] = 0.0

    base_date = dt.datetime.strptime(start_date, "%Y-%m-%d")
    cluster_urn_completion_next30['look_up_idx'] = cluster_urn_completion_next30['date'].apply(
        lambda d: (d.date() - base_date.date()).days)
    cluster_urn_completion_next30['urn_completion'] = cluster_urn_completion_next30.apply(
        lambda row: row['urn_completion_' + str(row['look_up_idx'])], axis=1)

    clus_urn_completion_trend = cluster_urn_completion_next30[['date', 'day', 'urn_completion']]
    clus_urn_completion_trend.date = clus_urn_completion_trend.date.map(lambda x: str(x)[:10])

    return clus_urn_completion_trend


def get_base_price_override(smart_hotel_oyo_id_list, mysql_query_manager):
    base_price_query = '''
        select bp.oyo_id as oyo_id, bp.pricing_date as date, bp.base_price as base, h.pricing_start_date as price_start_date
        from oyo_pricing.hotel_base_price bp left join pricing_hotel h on bp.oyo_id = h.oyo_id
        where bp.pricing_date between current_date - interval 8 day and current_date + interval 30 day
          and bp.deleted = 0
          and bp.room_type_id = 20
          and bp.oyo_id in {0}
          and h.deleted = 0
          and h.status = 1
        '''.format(MiscUtil.convert_list_to_tuple_list_str(smart_hotel_oyo_id_list))
    begin = time.time()
    base_price_df = mysql_query_manager.read_sql(base_price_query)
    LogUtil.get_cur_logger().info('get_base_price_override cost: %0.2fs', time.time() - begin)

    base_price_df.columns = list(
        pd.Series(base_price_df.columns).map(lambda x: str(x).replace('base', 'override_base')))

    return base_price_df


def get_metrics_value(metrics_name: str, oyo_set: set,  data_date: str, mysql_query_manager):
    sql = '''
        select m.oyo_id, m.metrics_value
        from pricing_metrics  m
        left join pricing_metrics_config c
        on m.metrics_id = c.metrics_id
        where m.is_delete = 0
        and c.metrics_name = '{0}'
        and m.oyo_id in {1}
        and m.data_date = '{2}'
        and m.status = 1
        order by m.oyo_id, m.id asc;
        '''.format(metrics_name, MiscUtil.convert_set_to_tuple_list_str(oyo_set), data_date)
    begin = time.time()
    df_metrics = mysql_query_manager.read_sql(sql)
    LogUtil.get_cur_logger().info('get_metrics_value cost: %0.2fs', time.time() - begin)
    df_metrics.drop_duplicates(['oyo_id'], keep="last", inplace=True)
    df_metrics[metrics_name] = df_metrics['metrics_value']
    df_metrics_value = df_metrics[["oyo_id", metrics_name]]
    oyo_set_null = set(oyo_set) - set(df_metrics_value.oyo_id)
    if len(oyo_set_null) > 0:
        LogUtil.get_cur_logger().warn("get_metrics_value, metrics_name: %s, total: %d, empty: %d for oyo_id: %s",
                                      metrics_name, len(set(df_metrics_value.oyo_id)), len(oyo_set_null), oyo_set_null)
    return df_metrics_value


def get_price_ec_map(smart_hotel_oyo_id_list,  data_date, mysql_query_manager):
    _metrics_name = "Price_ec"
    df_metrics_value = get_metrics_value(_metrics_name, set(smart_hotel_oyo_id_list), data_date, mysql_query_manager)
    price_ec_map = df_metrics_value.set_index('oyo_id')[_metrics_name].to_dict()
    return price_ec_map


def get_metrics_json(metrics_name: str, oyo_set: set, data_date: str, mysql_query_manager):
    sql = '''
            select m.oyo_id, m.metrics_details
            from pricing_metrics  m
            left join pricing_metrics_config c
            on m.metrics_id = c.metrics_id
            where m.is_delete = 0
            and c.metrics_name = '{0}'
            and m.oyo_id in {1}
            and m.data_date = '{2}'
            and m.status = 1
            order by m.oyo_id, m.id asc;
            '''.format(metrics_name, MiscUtil.convert_set_to_tuple_list_str(oyo_set), data_date)
    begin = time.time()
    df_metrics = mysql_query_manager.read_sql(sql)
    LogUtil.get_cur_logger().info('get_metrics_details cost: %0.2fs', time.time() - begin)
    df_metrics.drop_duplicates(['oyo_id'], keep="last", inplace=True)
    df_metrics["metrics_json"] = df_metrics['metrics_details']
    df_metrics_json = df_metrics[["oyo_id", "metrics_json"]]
    oyo_set_null = set(oyo_set) - set(df_metrics_json.oyo_id)
    if len(oyo_set_null) > 0:
        LogUtil.get_cur_logger().warn("get_metrics_json, metrics_name: %s, total: %d, empty: %d for oyo_id: %s",
                                      metrics_name, len(set(df_metrics_json.oyo_id)), len(oyo_set_null), oyo_set_null)
    return df_metrics_json


def calculate_final_price(smart_hotel_oyo_id, all_smart_hotel_30d_full_channel_data, clus_urn_completion_trend,
                          targeted_hotel_online_data_past_4_weeks_by_day, df_base_price_override_next_30d):
    """
    :param smart_hotel_oyo_id:
    :param all_smart_hotel_30d_full_channel_data:

    In [953]: all_smart_hotel_30d_full_channel_data
    Out[953]:
             date     oyo_id cluster_name zone_name city_name  hub_name  used_rooms  sellable_rooms       occ  rem
    0  2019-04-19  CN_SHA013    Guangfeng      East  Shangrao  Shangrao           2              46  0.043478   44
    1  2019-04-20  CN_SHA013    Guangfeng      East  Shangrao  Shangrao           1              46  0.021739   45
    2  2019-04-21  CN_SHA013    Guangfeng      East  Shangrao  Shangrao           1              46  0.021739   45
    3  2019-04-22  CN_SHA013    Guangfeng      East  Shangrao  Shangrao           1              46  0.021739   45
    4  2019-04-23  CN_SHA013    Guangfeng      East  Shangrao  Shangrao           1              46  0.021739   45
    5  2019-04-24  CN_SHA013    Guangfeng      East  Shangrao  Shangrao           1              46  0.021739   45
    6  2019-04-25  CN_SHA013    Guangfeng      East  Shangrao  Shangrao           1              46  0.021739   45
    7  2019-04-26  CN_SHA013    Guangfeng      East  Shangrao  Shangrao           1              46  0.021739   45
    8  2019-04-27  CN_SHA013    Guangfeng      East  Shangrao  Shangrao           1              46  0.021739   45
    9  2019-04-28  CN_SHA013    Guangfeng      East  Shangrao  Shangrao           1              46  0.021739   45
    10 2019-04-29  CN_SHA013    Guangfeng      East  Shangrao  Shangrao           1              46  0.021739   45

    :param clus_urn_completion_trend:

    In [954]: clus_urn_completion_trend
    Out[954]:
              date  day  urn_completion
    0   2019-04-19    4        0.543716
    1   2019-04-20    5        0.324176
    2   2019-04-21    6        0.369637
    3   2019-04-22    0        0.307443
    4   2019-04-23    1        0.263158
    5   2019-04-24    2        0.185596
    6   2019-04-25    3        0.163415
    7   2019-04-26    4        0.133880
    8   2019-04-27    5        0.131868
    9   2019-04-28    6        0.155116
    10  2019-04-29    0        0.142395
    11  2019-04-30    1        0.122807
    12  2019-05-01    2        0.105263

    :param targeted_hotel_online_data_past_4_weeks_by_day:

    In [956]: targeted_hotel_online_data_past_4_weeks_by_day
    Out[956]:
       day  online_gmv  online_urn  4wk_online_arr        base  online_occ  3wk_online_arr prop_age   tau  corrected_base
    0    0         0.0         0.0             NaN   95.388961    0.000000        0.000000      old  0.75       71.541721
    1    1       180.7         2.0       90.350000   90.350000    0.014493       90.350000      old  0.75       67.762500
    2    2       212.4         2.0      106.200000  106.200000    0.007246      106.200000      old  0.75       79.650000
    3    3       482.7         4.0      120.675000  120.675000    0.028986      120.675000      old  0.75       90.506250
    4    4       842.5        11.0       76.590909   76.590909    0.079710       76.590909      old  0.75       57.443182
    5    5       576.4         7.0       82.342857   82.342857    0.050725       82.342857      old  0.75       61.757143
    6    6       384.7         4.0       96.175000   96.175000    0.028986       96.175000      old  0.75       72.131250

    :param df_base_price_override_next_30d:

    In [1191]: df_base_price_override_next_30d
    Out[1191]:
           oyo_id        date  new_base
    0   CN_SHA013  2019-05-07     168.0
    1   CN_SHA013  2019-05-08     168.0
    2   CN_SHA013  2019-05-09     168.0
    3   CN_SHA013  2019-05-10     178.0
    4   CN_SHA013  2019-05-11     178.0
    5   CN_SHA013  2019-05-12     168.0
    6   CN_SHA013  2019-05-13     168.0
    7   CN_SHA013  2019-05-14     168.0
    8   CN_SHA013  2019-05-15     168.0
    9   CN_SHA013  2019-05-16     168.0
    10  CN_SHA013  2019-05-17     178.0
    11  CN_SHA013  2019-05-18     178.0
    12  CN_SHA013  2019-05-19     168.0
    13  CN_SHA013  2019-05-20     168.0
    14  CN_SHA013  2019-05-21     168.0
    15  CN_SHA013  2019-05-22     168.0
    16  CN_SHA013  2019-05-23     168.0

    :return:
    """
    smart_hotel_full_channel_next_30d_data = all_smart_hotel_30d_full_channel_data[
        ['date', 'oyo_id', 'rem', 'occ', 'sellable_rooms']]
    smart_hotel_full_channel_next_30d_data = smart_hotel_full_channel_next_30d_data[
        smart_hotel_full_channel_next_30d_data.oyo_id == smart_hotel_oyo_id]
    smart_hotel_full_channel_next_30d_data.date = smart_hotel_full_channel_next_30d_data.date.map(lambda x: str(x)[:10])

    hotel_price_data = pd.merge(smart_hotel_full_channel_next_30d_data,
                                clus_urn_completion_trend[['date', 'day', 'urn_completion']], how='left', on=['date'])
    hotel_price_data = pd.merge(hotel_price_data, targeted_hotel_online_data_past_4_weeks_by_day[
        ['day', '3wk_online_arr', 'base', 'corrected_base', 'tau', '3wk_online_occ']], how='left',
                                on=['day'])

    # override base price with input from pricing center
    hotel_price_data = override_base_price(df_base_price_override_next_30d, hotel_price_data)

    hotel_price_data['occ_ftd_target'] = hotel_price_data.apply(lambda row: get_occ_ftd_target(row['3wk_online_occ']),
                                                                axis=1)
    hotel_price_data['delta'] = hotel_price_data.occ - (
            hotel_price_data.urn_completion * hotel_price_data.occ_ftd_target)
    hotel_price_data['gear_factor'] = hotel_price_data.apply(lambda row: get_gear_factor(row['delta']), axis=1)
    hotel_price_data['price_adj_ratio'] = hotel_price_data.apply(lambda row: 1 + (row['gear_factor'] * row['delta']),
                                                                 axis=1)
    hotel_price_data['price'] = hotel_price_data.apply(lambda row: int(row['corrected_base'] * row['price_adj_ratio']),
                                                       axis=1)

    return hotel_price_data


def override_base_price(df_base_price_override_next_30d, hotel_price_data):
    df_base_price_override_next_30d.date = df_base_price_override_next_30d.date.astype(str)
    today = datetime.datetime.now().date()
    df_base_price_override_next_30d['pricing_days'] = df_base_price_override_next_30d['price_start_date'].apply(lambda d: int((today - d).days + 1))
    hotel_price_data.date = hotel_price_data.date.astype(str)
    hotel_price_data = pd.merge(hotel_price_data, df_base_price_override_next_30d, how='left', on=['date', 'oyo_id'])

    def pick_price(override_base, corrected_base, pricing_days, tau):
        # if no valid corrected_base or price control under 7 days
        if np.isinf(corrected_base) or np.isnan(corrected_base) or pricing_days <= 7:
            return override_base

        # if no override base
        if np.isnan(override_base):
            return corrected_base
        else:
            return override_base
        #
        # if tau >= 1:
        #     return max(override_base, corrected_base)
        # else:
        #     return min(override_base, corrected_base)

    # hotel_price_data.corrected_base = hotel_price_data.apply(
    #     lambda row: row['corrected_base'] if (
    #             np.isnan(row['override_base']) or (not np.isinf(row['corrected_base']) and row['corrected_base'] >= row['override_base'] and row['pricing_days'] > 7))
    #     else row['override_base'], axis=1
    # )

    hotel_price_data.corrected_base = hotel_price_data.apply(lambda row: pick_price(row['override_base'], row['corrected_base'], row['pricing_days'], row['tau']), axis=1)

    return hotel_price_data


def extend_override_base_price(df_base_price_override_next_30d, hotel_price_data):
    df_base_price_override_next_30d = df_base_price_override_next_30d.rename(columns={'override_base': 'extend_override_base'})
    df_base_price_override_next_30d = DFUtil.apply_func_for_df(df_base_price_override_next_30d,'day', ['date'],
                                                               lambda values: datetime.datetime.strptime(values[0], '%Y-%m-%d').date().weekday())
    # fetch the newest only one X weekday
    df_base_price_override_next_30d = df_base_price_override_next_30d.sort_values(
        by=['oyo_id', 'date']).groupby(['oyo_id', 'day']).tail(1)
    hotel_price_data = hotel_price_data.merge(df_base_price_override_next_30d[['oyo_id', 'day', 'extend_override_base']],
                                              on=['oyo_id', 'day'], how='left')

    def _pick_price(corrected_base, extend_override_base):
        return corrected_base if MiscUtil.is_not_empty_value(corrected_base) else extend_override_base

    hotel_price_data = DFUtil.apply_func_for_df(hotel_price_data, 'corrected_base', ['corrected_base', 'extend_override_base'],
                                                lambda values: _pick_price(*values))
    return hotel_price_data


def event_adjustment(hotel_price_data):
    hotel_price_data['event_factor'] = hotel_price_data.apply(lambda row: EVENT_ADJUSTMENT.get(row['date'], 1), axis=1)
    hotel_price_data.corrected_base = hotel_price_data.corrected_base * hotel_price_data.event_factor
    return hotel_price_data


def get_total_urn_by_date_city(city_list, start_date, start_date_offset, end_date_offset, oracle_query_manager):
    """

    :param city_list: ('Shangrao', 'city_2')
    :param str start_date: '2019-04-19'
    :param start_date_offset:
    :param end_date_offset:
    :return:

    In [622]: city_online_urn_past_30d
    Out[622]:
               date      oyo_id      city  cluster_urn  urn_30  urn_29  urn_28  urn_27  urn_26  ...    urn_8  urn_7  urn_6  urn_5  urn_4  urn_3  urn_2  urn_1  urn_0
    0    2019-03-20   CN_SHA002  Shangrao           24      24      24      24      24      24  ...       24     24     24     24     24     24     24     24     22
    1    2019-03-20   CN_SHA003  Shangrao           30      30      30      30      30      30  ...       30     30     30     30     30     30     30     30     29
    2    2019-03-20   CN_SHA004  Shangrao           39      39      39      39      39      39  ...       38     38     38     38     38     38     37     21     18
    3    2019-03-20   CN_SHA006  Shangrao           25      25      25      25      25      25  ...       23     23     23     23     23     23     23     22     14
    4    2019-03-20   CN_SHA007  Shangrao           17      17      17      17      17      17  ...       17     17     17     17     17     17     17     16     13
    5    2019-03-20   CN_SHA008  Shangrao           19      19      19      19      19      19  ...       19     19     19     19     19     19     19     10      5
    6    2019-03-20   CN_SHA009  Shangrao           17      16      16      16      16      16  ...        7      7      5      5      4      4      4      4      3
    7    2019-03-20   CN_SHA010  Shangrao           38      38      38      38      38      38  ...       38     38     38     38     38     38     38     38     33
    8    2019-03-20   CN_SHA011  Shangrao           13      13      13      13      13      13  ...        9      9      9      9      9      9      9      9      7
    9    2019-03-20   CN_SHA012  Shangrao            9       9       9       9       9       9  ...        9      9      9      9      9      9      9      9      9
    10   2019-03-20   CN_SHA013  Shangrao           11       9       9       9       9       9  ...        9      8      8      8      8      8      8      8      4
    11   2019-03-20   CN_SHA015  Shangrao           27      26      26      26      26      26  ...       24     23     23     23     23     23     23     17     15
    12   2019-03-20   CN_SHA017  Shangrao           20      20      20      20      20      20  ...       20     20     20     20     20     20     20     10      7
    13   2019-03-20   CN_SHA019  Shangrao            7       7       7       7       7       7  ...        7      7      7      7      7      7      7      5      4
    14   2019-03-20   CN_SHA020  Shangrao            7       7       7       7       7       7  ...        7      7      7      7      7      7      7      7      5
    15   2019-03-20   CN_SHA021  Shangrao           10      10      10      10      10      10  ...       10     10     10     10     10     10     10     10      8

    """

    source_create_time_with_fallback_query = """
    case when fb.source_create_time is not null then fb.source_create_time else fb.create_time end
    """

    urn_city_agg_query = '''
        select ca."date", h.oyo_id as oyo_id, ct.city_name as city,
        SUM(case when b.status in (0,1,2,4) then 1 else 0 end) as cluster_urn
            ,sum(case when (b.status in (0,1,2,4) and trunc("date") - trunc({3}) <= 29) or (trunc("date") - trunc({3})= 30 and to_char({3},'hh24:mi') >= to_char(sysdate,'hh24:mi')) then 1 else 0 end) as urn_30
            ,sum(case when (b.status in (0,1,2,4) and trunc("date") - trunc({3}) <= 28) or (b.status in (0,1,2,4) and trunc("date") - trunc({3})= 29 and to_char({3},'hh24:mi') >= to_char(sysdate,'hh24:mi')) then 1 else 0 end) as urn_29
            ,sum(case when (b.status in (0,1,2,4) and trunc("date") - trunc({3}) <= 27) or (b.status in (0,1,2,4) and trunc("date") - trunc({3})= 28 and to_char({3},'hh24:mi') >= to_char(sysdate,'hh24:mi')) then 1 else 0 end) as urn_28
            ,sum(case when (b.status in (0,1,2,4) and trunc("date") - trunc({3}) <= 26) or (b.status in (0,1,2,4) and trunc("date") - trunc({3})= 27 and to_char({3},'hh24:mi') >= to_char(sysdate,'hh24:mi')) then 1 else 0 end) as urn_27
            ,sum(case when (b.status in (0,1,2,4) and trunc("date") - trunc({3}) <= 25) or (b.status in (0,1,2,4) and trunc("date") - trunc({3})= 26 and to_char({3},'hh24:mi') >= to_char(sysdate,'hh24:mi')) then 1 else 0 end) as urn_26
            ,sum(case when (b.status in (0,1,2,4) and trunc("date") - trunc({3}) <= 24) or (b.status in (0,1,2,4) and trunc("date") - trunc({3})= 25 and to_char({3},'hh24:mi') >= to_char(sysdate,'hh24:mi')) then 1 else 0 end) as urn_25
            ,sum(case when (b.status in (0,1,2,4) and trunc("date") - trunc({3}) <= 23) or (b.status in (0,1,2,4) and trunc("date") - trunc({3})= 24 and to_char({3},'hh24:mi') >= to_char(sysdate,'hh24:mi')) then 1 else 0 end) as urn_24
            ,sum(case when (b.status in (0,1,2,4) and trunc("date") - trunc({3}) <= 22) or (b.status in (0,1,2,4) and trunc("date") - trunc({3})= 23 and to_char({3},'hh24:mi') >= to_char(sysdate,'hh24:mi')) then 1 else 0 end) as urn_23
            ,sum(case when (b.status in (0,1,2,4) and trunc("date") - trunc({3}) <= 21) or (b.status in (0,1,2,4) and trunc("date") - trunc({3})= 22 and to_char({3},'hh24:mi') >= to_char(sysdate,'hh24:mi')) then 1 else 0 end) as urn_22
            ,sum(case when (b.status in (0,1,2,4) and trunc("date") - trunc({3}) <= 20) or (b.status in (0,1,2,4) and trunc("date") - trunc({3})= 21 and to_char({3},'hh24:mi') >= to_char(sysdate,'hh24:mi')) then 1 else 0 end) as urn_21
            ,sum(case when (b.status in (0,1,2,4) and trunc("date") - trunc({3}) <= 19) or (b.status in (0,1,2,4) and trunc("date") - trunc({3})= 20 and to_char({3},'hh24:mi') >= to_char(sysdate,'hh24:mi')) then 1 else 0 end) as urn_20
            ,sum(case when (b.status in (0,1,2,4) and trunc("date") - trunc({3}) <= 18) or (b.status in (0,1,2,4) and trunc("date") - trunc({3})= 19 and to_char({3},'hh24:mi') >= to_char(sysdate,'hh24:mi')) then 1 else 0 end) as urn_19
            ,sum(case when (b.status in (0,1,2,4) and trunc("date") - trunc({3}) <= 17) or (b.status in (0,1,2,4) and trunc("date") - trunc({3})= 18 and to_char({3},'hh24:mi') >= to_char(sysdate,'hh24:mi')) then 1 else 0 end) as urn_18
            ,sum(case when (b.status in (0,1,2,4) and trunc("date") - trunc({3}) <= 16) or (b.status in (0,1,2,4) and trunc("date") - trunc({3})= 17 and to_char({3},'hh24:mi') >= to_char(sysdate,'hh24:mi')) then 1 else 0 end) as urn_17
            ,sum(case when (b.status in (0,1,2,4) and trunc("date") - trunc({3}) <= 15) or (b.status in (0,1,2,4) and trunc("date") - trunc({3})= 16 and to_char({3},'hh24:mi') >= to_char(sysdate,'hh24:mi')) then 1 else 0 end) as urn_16
            ,sum(case when (b.status in (0,1,2,4) and trunc("date") - trunc({3}) <= 14) or (b.status in (0,1,2,4) and trunc("date") - trunc({3})= 15 and to_char({3},'hh24:mi') >= to_char(sysdate,'hh24:mi')) then 1 else 0 end) as urn_15
            ,sum(case when (b.status in (0,1,2,4) and trunc("date") - trunc({3}) <= 13) or (b.status in (0,1,2,4) and trunc("date") - trunc({3})= 14 and to_char({3},'hh24:mi') >= to_char(sysdate,'hh24:mi')) then 1 else 0 end) as urn_14
            ,sum(case when (b.status in (0,1,2,4) and trunc("date") - trunc({3}) <= 12) or (b.status in (0,1,2,4) and trunc("date") - trunc({3})= 13 and to_char({3},'hh24:mi') >= to_char(sysdate,'hh24:mi')) then 1 else 0 end) as urn_13
            ,sum(case when (b.status in (0,1,2,4) and trunc("date") - trunc({3}) <= 11) or (b.status in (0,1,2,4) and trunc("date") - trunc({3})= 12 and to_char({3},'hh24:mi') >= to_char(sysdate,'hh24:mi')) then 1 else 0 end) as urn_12
            ,sum(case when (b.status in (0,1,2,4) and trunc("date") - trunc({3}) <= 10) or (b.status in (0,1,2,4) and trunc("date") - trunc({3})= 11 and to_char({3},'hh24:mi') >= to_char(sysdate,'hh24:mi')) then 1 else 0 end) as urn_11
            ,sum(case when (b.status in (0,1,2,4) and trunc("date") - trunc({3}) <= 9 ) or (b.status in (0,1,2,4) and trunc("date") - trunc({3})= 10 and to_char({3},'hh24:mi') >= to_char(sysdate,'hh24:mi')) then 1 else 0 end) as urn_10
            ,sum(case when (b.status in (0,1,2,4) and trunc("date") - trunc({3}) <= 8 ) or (b.status in (0,1,2,4) and trunc("date") - trunc({3})= 9  and to_char({3},'hh24:mi') >= to_char(sysdate,'hh24:mi')) then 1 else 0 end) as urn_9 
            ,sum(case when (b.status in (0,1,2,4) and trunc("date") - trunc({3}) <= 7 ) or (b.status in (0,1,2,4) and trunc("date") - trunc({3})= 8  and to_char({3},'hh24:mi') >= to_char(sysdate,'hh24:mi')) then 1 else 0 end) as urn_8 
            ,sum(case when (b.status in (0,1,2,4) and trunc("date") - trunc({3}) <= 6 ) or (b.status in (0,1,2,4) and trunc("date") - trunc({3})= 7  and to_char({3},'hh24:mi') >= to_char(sysdate,'hh24:mi')) then 1 else 0 end) as urn_7 
            ,sum(case when (b.status in (0,1,2,4) and trunc("date") - trunc({3}) <= 5 ) or (b.status in (0,1,2,4) and trunc("date") - trunc({3})= 6  and to_char({3},'hh24:mi') >= to_char(sysdate,'hh24:mi')) then 1 else 0 end) as urn_6 
            ,sum(case when (b.status in (0,1,2,4) and trunc("date") - trunc({3}) <= 4 ) or (b.status in (0,1,2,4) and trunc("date") - trunc({3})= 5  and to_char({3},'hh24:mi') >= to_char(sysdate,'hh24:mi')) then 1 else 0 end) as urn_5 
            ,sum(case when (b.status in (0,1,2,4) and trunc("date") - trunc({3}) <= 3 ) or (b.status in (0,1,2,4) and trunc("date") - trunc({3})= 4  and to_char({3},'hh24:mi') >= to_char(sysdate,'hh24:mi')) then 1 else 0 end) as urn_4 
            ,sum(case when (b.status in (0,1,2,4) and trunc("date") - trunc({3}) <= 2 ) or (b.status in (0,1,2,4) and trunc("date") - trunc({3})= 3  and to_char({3},'hh24:mi') >= to_char(sysdate,'hh24:mi')) then 1 else 0 end) as urn_3 
            ,sum(case when (b.status in (0,1,2,4) and trunc("date") - trunc({3}) <= 1 ) or (b.status in (0,1,2,4) and trunc("date") - trunc({3})= 2  and to_char({3},'hh24:mi') >= to_char(sysdate,'hh24:mi')) then 1 else 0 end) as urn_2 
            ,sum(case when (b.status in (0,1,2,4) and trunc("date") - trunc({3}) <= 0 ) or (b.status in (0,1,2,4) and trunc("date") - trunc({3})= 1  and to_char({3},'hh24:mi') >= to_char(sysdate,'hh24:mi')) then 1 else 0 end) as urn_1 
            ,sum(case when b.status in (0,1,2,4) and  trunc("date") = trunc({3}) and to_char({3},'hh24:mi') >= to_char(sysdate,'hh24:mi') then 1 else 0 end) as urn_0
           from oyo_dw.fact_room_reservation  b
           INNER JOIN oyo_dw.fact_booking fb
             ON b.booking_id = fb.id
          inner join oyo_dw.v_dim_hotel  h
          on b.hotel_id = h.id --and h.status in (3) 
          LEFT join oyo_dw.dim_zones ct
          on h.cluster_id = ct.id
          cross join oyo_dw.dim_calendar  ca
          where trunc(ca."date") >= trunc(b.check_in)
          and trunc(ca."date") < trunc(b.check_out)
          and trunc(ca."date") >= to_date('{0}','yyyy-mm-dd') 
          and trunc(ca."date") <= to_date('{1}','yyyy-mm-dd') 
          and ct.city_name in {2}
          group by ca."date", h.oyo_id , ct.city_name
          order by 1,2
         '''.format(
        dt.datetime.strftime(dt.datetime.strptime(start_date, '%Y-%m-%d') - dt.timedelta(days=start_date_offset),
                             "%Y-%m-%d"),
        dt.datetime.strftime(dt.datetime.strptime(start_date, '%Y-%m-%d') - dt.timedelta(days=end_date_offset),
                             "%Y-%m-%d"),
        str(city_list), source_create_time_with_fallback_query)

    begin = time.time()

    time_out_in_seconds = 300

    city_online_urn_past_30d = oracle_query_manager.read_sql(urn_city_agg_query, time_out_in_seconds)
    logger = LogUtil.get_cur_logger()
    logger.info('cluster_data cost time %d  s rowsCount %d', time.time() - begin, city_online_urn_past_30d.size)
    logger.info('cluster_data process done')

    city_online_urn_past_30d.city = ''
    return city_online_urn_past_30d


def get_hotel_location(city_list, oracle_query_manager):
    """
    :param city_list: "('CN_SHA013')"
    :return:

    # In [13]: hotel_data_lat_long_all
    # Out[13]:
    # oyo_id   latitude   longitude      city
    # 0    CN_SHA034  29.264357  117.862950  Shangrao
    # 1    CN_SHA036  28.472898  117.979230  Shangrao
    # 2    CN_SHA014  28.674404  118.242235  Shangrao
    # 3    CN_SHA008  28.432830  118.186749  Shangrao
    # 4    CN_SHA015  28.452397  117.968477  Shangrao
    # 5    CN_SHA043  28.449820  118.193910  Shangrao
    # 6    CN_SHA045  28.701346  117.076280  Shangrao
    # 7    CN_SHA044  29.259813  117.872504  Shangrao
    # 8    CN_SHA042  28.429998  117.982087  Shangrao
    # 9    CN_SHA040  28.967218  118.042497  Shangrao
    # 10   CN_SHA046  28.447048  117.982089  Shangrao
    # 11   CN_SHA041  28.468421  117.944938  Shangrao
    # 12   CN_SHA021  28.688049  118.245950  Shangrao
    # 13   CN_SHA026  29.268509  117.865033  Shangrao
    # 14   CN_SHA009  28.439538  117.965218  Shangrao
    # 15   CN_SHA017  28.461203  117.903999  Shangrao
    """
    query = '''SELECT a.oyo_id
               ,a.latitude,a.longitude,b.city_name as city
               FROM oyo_dw.v_DIM_HOTEL a
               inner join oyo_dw.dim_zones b
               on a.cluster_id=b.id
               where city_name in {0}
         '''.format(str(city_list))
    begin = time.time()
    hotel_data_lat_long_all = oracle_query_manager.read_sql(query)
    LogUtil.get_cur_logger().info('hotel_data_lat_long_all cost time %d  s rowsCount %d', time.time() - begin,
                                  hotel_data_lat_long_all.size)

    hotel_data_lat_long_all.latitude = hotel_data_lat_long_all.latitude.astype('float64')
    hotel_data_lat_long_all.longitude = hotel_data_lat_long_all.longitude.astype('float64')

    return hotel_data_lat_long_all


# def get_future_urn():
#     # !!!!!!!! TODO use ADB data
#     query = '''
#         select ca."date",h.oyo_id as oyo_id,c.cluster_name,zone_name,city_name,hub_name
#         from oyo_dw.dim_calendar ca
#         cross join oyo_dw.v_dim_hotel h
#         left join oyo_dw.dim_zones c on c.id=h.cluster_id
#         where h.oyo_id in {2}
#         and ca."date" >= "TO_DATE"('{0}','YYYY-MM-DD' )
#         and ca."date" <= "TO_DATE"('{1}','YYYY-MM-DD' )
#         '''.format(str(start_date),
#                    dt.datetime.strftime(dt.datetime.strptime(start_date, '%Y-%m-%d') + dt.timedelta(days=29),
#                                         "%Y-%m-%d"),
#                    all_hotels_str)
#     begin = time.time()
#     hotel_data = pd.read_sql(query, OracleConf.get_engine())
#     logger.info('hotel_data cost time %d  s rowsCount %d', time.time() - begin, hotel_data.size)
#
#     hotel_data = pd.merge(hotel_data, srn_data, how='left', on=['date', 'oyo_id'])
#     hotel_data = hotel_data.fillna(0)
#
#     hotel_data['occ'] = hotel_data.used_rooms / hotel_data.sellable_rooms
#     hotel_data['rem'] = hotel_data.sellable_rooms - hotel_data.used_rooms
#
#     hotel_data.occ = hotel_data.occ.map(lambda x: np.where(pd.isnull(x) | (x == float("inf")), 0, x))
#     logger.info('hotel_data process done')

def get_cluster_hotel_list(smart_hotel_oyo_id, smart_hotel_data, hotel_data_lat_long_all_prop):
    """
    :param smart_hotel_oyo_id: 'CN_SHA013'
    :param smart_hotel_data:

    In [521]: smart_hotel_data
    Out[521]:
                date  hotel_id     oyo_id cluster_name      city valid_from valid_till  agreement_type  contracted_rooms
    25592 2019-04-20     39018  CN_SHA013    Guangfeng  Shangrao 2018-09-03 2019-09-03             6.0              46.0
    25593 2019-04-21     39018  CN_SHA013    Guangfeng  Shangrao 2018-09-03 2019-09-03             6.0              46.0
    25594 2019-04-22     39018  CN_SHA013    Guangfeng  Shangrao 2018-09-03 2019-09-03             6.0              46.0
    25595 2019-04-23     39018  CN_SHA013    Guangfeng  Shangrao 2018-09-03 2019-09-03             6.0              46.0
    25596 2019-04-24     39018  CN_SHA013    Guangfeng  Shangrao 2018-09-03 2019-09-03             6.0              46.0
    25597 2019-04-25     39018  CN_SHA013    Guangfeng  Shangrao 2018-09-03 2019-09-03             6.0              46.0
    25598 2019-04-26     39018  CN_SHA013    Guangfeng  Shangrao 2018-09-03 2019-09-03             6.0              46.0

    :param hotel_data_lat_long_all_prop:

    In [523]: hotel_data_lat_long_all
    Out[523]:
            oyo_id   latitude   longitude      city
    0   CN_SHA1001  28.451639  118.199450  Shangrao
    1   CN_SHA1003  28.697820  117.077703  Shangrao
    2    CN_SHA048  28.417152  117.615910  Shangrao
    3   CN_SHA1002  28.460229  117.993449  Shangrao
    4   CN_SHA1004  28.408664  117.438422  Shangrao
    5   CN_SHA1005  29.060116  117.714130  Shangrao
    6   CN_SHA1007  29.109360  117.932138  Shangrao
    7    CN_SHA044  29.259813  117.872504  Shangrao
    8    CN_SHA035  28.692738  117.080400  Shangrao
    9    CN_SHA038  28.906093  118.125688  Shangrao
    10   CN_SHA039  28.904713  118.134316  Shangrao
    11   CN_SHA042  28.429998  117.982087  Shangrao
    12   CN_SHA041  28.468421  117.944938  Shangrao
    13   CN_SHA045  28.701346  117.076280  Shangrao
    14   CN_SHA043  28.449820  118.193910  Shangrao
    15   CN_SHA046  28.447048  117.982089  Shangrao
    16   CN_SHA040  28.967218  118.042497  Shangrao
    17   CN_SHA029  28.471084  117.990257  Shangrao
    18   CN_SHA034  29.264357  117.862950  Shangrao
    19   CN_SHA036  28.472898  117.979230  Shangrao
    20   CN_SHA037  28.702210  117.075209  Shangrao
    21   CN_SHA030  28.456207  117.987514  Shangrao
    22   CN_SHA032  28.691024  117.081227  Shangrao

    :return: ['CN_SHA1001', 'CN_SHA1002', 'CN_SHA043', 'CN_SHA008', 'CN_SHA012', 'CN_SHA013']

    """
    city_name = list(smart_hotel_data[smart_hotel_data.oyo_id == smart_hotel_oyo_id].city.unique())
    # 用smart_hotel的城市 筛选数据
    hotel_data_lat_long = hotel_data_lat_long_all_prop[hotel_data_lat_long_all_prop.city.map(lambda x: x in city_name)]
    hotel_data_non_smart = hotel_data_lat_long[hotel_data_lat_long.oyo_id != smart_hotel_oyo_id]

    # Selected property Lat Long Details
    smart_lat = \
        hotel_data_lat_long_all_prop[hotel_data_lat_long_all_prop.oyo_id == smart_hotel_oyo_id].latitude.iloc[0]
    smart_long = \
        hotel_data_lat_long_all_prop[hotel_data_lat_long_all_prop.oyo_id == smart_hotel_oyo_id].longitude.iloc[0]

    smart_lat_rad = smart_lat * math.pi / 180
    smart_long_rad = smart_long * math.pi / 180

    # Dergree to radian for selected hotels
    hotel_data_non_smart['long_rad'] = hotel_data_non_smart.longitude * math.pi / 180
    hotel_data_non_smart['lat_rad'] = hotel_data_non_smart.latitude * math.pi / 180
    logger = LogUtil.get_cur_logger()
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

    # In [36]: hotel_data_non_smart
    # Out[36]:
    # oyo_id   latitude   longitude      city  long_rad   lat_rad        dist
    # 0    CN_SHA034  29.264357  117.862950  Shangrao  2.057097  0.510759   98.142862
    # 1    CN_SHA036  28.472898  117.979230  Shangrao  2.059126  0.496946   20.697973
    # 2    CN_SHA014  28.674404  118.242235  Shangrao  2.063716  0.500463   27.977467
    # 3    CN_SHA008  28.432830  118.186749  Shangrao  2.062748  0.496247    0.581602
    # 4    CN_SHA015  28.452397  117.968477  Shangrao  2.058938  0.496588   21.312592
    # 5    CN_SHA043  28.449820  118.193910  Shangrao  2.062873  0.496543    2.596352
    # 6    CN_SHA045  28.701346  117.076280  Shangrao  2.043367  0.500933  112.433285
    # 7    CN_SHA044  29.259813  117.872504  Shangrao  2.057263  0.510680   97.369807
    # 8    CN_SHA042  28.429998  117.982087  Shangrao  2.059176  0.496197   19.810957
    # 9    CN_SHA040  28.967218  118.042497  Shangrao  2.060230  0.505573   61.548575
    # 10   CN_SHA046  28.447048  117.982089  Shangrao  2.059176  0.496495   19.921579
    # 11   CN_SHA041  28.468421  117.944938  Shangrao  2.058528  0.496868   23.866507
    # 12   CN_SHA021  28.688049  118.245950  Shangrao  2.063781  0.500701   29.536631
    # 13   CN_SHA026  29.268509  117.865033  Shangrao  2.057133  0.510832   98.516009
    # 14   CN_SHA009  28.439538  117.965218  Shangrao  2.058881  0.496364   21.496987
    # 15   CN_SHA017  28.461203  117.903999  Shangrao  2.057813  0.496742   27.689750
    # 16   CN_SHA022  28.449129  117.980592  Shangrao  2.059150  0.496531   20.092798

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
    # List of hotels nearby
    logger.info('##########List of hotels nearby##########')
    clus_hotel_list = hotel_data_non_smart[hotel_data_non_smart.dist <= radius].oyo_id.tolist()

    # Adding the selected property in the list
    clus_hotel_list.append(smart_hotel_oyo_id)

    return clus_hotel_list


def default_final_price_to_mg_target(mg_target_data, smart_hotel_params_df):
    """
    Override final price to revpar_target if price is NAN.
    Usually happens for new hotel, where historical data is missing.
    :param mg_target_data:
    :param smart_hotel_params_df:
    :return:
    """
    smart_hotel_params_df['month'] = smart_hotel_params_df['date'].apply(
        lambda d_str: datetime.datetime.strptime(d_str, "%Y-%m-%d").month)
    smart_hotel_params_df = smart_hotel_params_df.merge(mg_target_data, on=['oyo_id', 'month'], how='left')

    def set_mg_target_if_price_null(row):
        if not np.isnan(row['price']):
            return row['price']
        else:
            return row['daily_revpar_target']

    smart_hotel_params_df['price'] = smart_hotel_params_df.apply(lambda row: set_mg_target_if_price_null(row), axis=1)
    return smart_hotel_params_df


def run_last_minute_sale(start_time, hotel_price_data):
    """

    :param start_time:  datetime.datetime(2019, 4, 23, 16, 46, 58, 644456)
    :param hotel_price_data:

    In [1061]: hotel_price_data
    Out[1061]:
              date     oyo_id  rem       occ  sellable_rooms  day    ...     occ_ftd_target     delta  gear_factor  price_adj_ratio  price  price_lm
    0   2019-04-19  CN_SHA013   44  0.043478              46    4    ...           0.229710 -0.087695            2         0.824610     47        47
    1   2019-04-20  CN_SHA013   45  0.021739              46    5    ...           0.200725 -0.043882            2         0.912235     56        56
    2   2019-04-21  CN_SHA013   45  0.021739              46    6    ...           0.178986 -0.044421            2         0.911159     65        65
    3   2019-04-22  CN_SHA013   45  0.021739              46    0    ...           0.150000 -0.024377            2         0.951245     68        68
    4   2019-04-23  CN_SHA013   45  0.021739              46    1    ...           0.164493 -0.021548            2         0.956903     19        19
    5   2019-04-24  CN_SHA013   45  0.021739              46    2    ...           0.157246 -0.007445            2         0.985110     78        78
    6   2019-04-25  CN_SHA013   45  0.021739              46    3    ...           0.178986 -0.007510            2         0.984981     89        89
    7   2019-04-26  CN_SHA013   45  0.021739              46    4    ...           0.229710 -0.009014            2         0.981971     56        56
    8   2019-04-27  CN_SHA013   45  0.021739              46    5    ...           0.200725 -0.004730            2         0.990540     61        61
    9   2019-04-28  CN_SHA013   45  0.021739              46    6    ...           0.178986 -0.006024            2         0.987951     71        71

    :return:
    """
    if start_time.hour < LAST_MINUTE_SALE_BEGIN_HOUR:
        return hotel_price_data

    def last_minute_sale(remaining_rooms, price, date):
        today = dt.datetime.strftime(start_time, "%Y-%m-%d")
        if date != today:
            return price

        if remaining_rooms < 5:
            return 30
        elif 5 <= remaining_rooms <= 15:
            return 25
        else:
            return 19

    hotel_price_data['price'] = hotel_price_data.apply(
        lambda row: last_minute_sale(row['rem'], row['price'], row['date']), axis=1)
    return hotel_price_data


def rearrange_past_data_to_days(smart_hotel_oyo_id, clus_hotel_list, past_3week_online_data,
                                prop_details):
    """

    :param smart_hotel_oyo_id: 'CN_SHA013'
    :param clus_hotel_list: ['CN_SHA1001', 'CN_SHA1002', 'CN_SHA043', 'CN_SHA008', 'CN_SHA012', 'CN_SHA013']
    :param past_3week_online_data_smart_hotel:

    In [839]: past_3week_online_data_smart_hotel
    Out[839]:
            oyo_id       date  online_urn  total_urn  sellable_rooms  online_gmv total_gmv  day
    210  CN_SHA013 2019-04-02         1.0        1.0              46        92.7      92.7    1
    211  CN_SHA013 2019-04-03         1.0        1.0              46       106.2     106.2    2
    212  CN_SHA013 2019-04-04         4.0       22.0              46       482.7    2286.7    3
    213  CN_SHA013 2019-04-05        11.0       27.0              46       842.5    3080.5    4
    214  CN_SHA013 2019-04-06         6.0       11.0              46       669.1    1085.1    5
    215  CN_SHA013 2019-04-07         1.0        3.0              46        92.7     172.7    6
    216  CN_SHA013 2019-04-08         0.0       10.0              46         0.0       850    0
    217  CN_SHA013 2019-04-09         0.0        3.0              46         0.0       120    1
    218  CN_SHA013 2019-04-10         0.0        3.0              46         0.0       120    2
    219  CN_SHA013 2019-04-11         0.0        2.0              46         0.0        80    3
    220  CN_SHA013 2019-04-12         0.0        5.0              46         0.0       350    4
    221  CN_SHA013 2019-04-13         1.0        2.0              46       -92.7     -12.7    5
    222  CN_SHA013 2019-04-14         3.0        5.0              46       292.0       372    6
    223  CN_SHA013 2019-04-15         0.0        2.0              46         0.0        80    0
    224  CN_SHA013 2019-04-16         1.0        4.0              46        88.0       248    1
    225  CN_SHA013 2019-04-17         0.0        2.0              46         0.0        80    2
    226  CN_SHA013 2019-04-18         0.0        2.0              46         0.0        80    3
    227  CN_SHA013 2019-04-19         0.0        2.0              46         0.0        80    4
    228  CN_SHA013 2019-04-20         0.0        1.0              46         0.0        40    5
    229  CN_SHA013 2019-04-21         0.0        1.0              46         0.0        40    6
    230  CN_SHA013 2019-04-22         0.0        2.0              46         0.0       120    0

    :param prop_details: In [530]: prop_details

    Out[530]:
          oyo_id cluster_name city_name  hub_name  prop_category
    0  CN_SHA013    GUANGFENG  SHANGRAO  SHANGRAO              0

    :return:

    In [123]: hotel_past_occ
    Out[123]:
    day  past_occ   past_arr
    0    4  0.119565  76.590909
    1    5  0.076087  82.342857
    2    6  0.043478  96.175000
    3    0  0.000000   0.000000
    4    1  0.010870  88.000000
    5    2  0.000000   0.000000
    6    3  0.000000   0.000000

    """
    ####Condition 1####
    # checking if property is active for last 21 days
    logger = LogUtil.get_cur_logger()
    logger.info('##########checking if property is active for last 21 days##########')

    past_3week_online_data_smart_hotel = past_3week_online_data[past_3week_online_data.oyo_id == smart_hotel_oyo_id]

    if past_3week_online_data_smart_hotel.shape[0] == 21:
        logger.info('########past_data_hotel.shape[0] == 21########')
        # arr_list means the selected property
        arr_list = smart_hotel_oyo_id
        # past_data_hotel_sum_temp is just givin us days
        tt01 = dt.datetime.strftime(dt.datetime.fromtimestamp(time.time()), "%Y-%m-%d")
        tt02 = dt.datetime.strftime(dt.datetime.fromtimestamp(time.time()) + dt.timedelta(days=6), "%Y-%m-%d")
        past_data_hotel_sum_temp = pd.DataFrame(pd.date_range(tt01, tt02, freq='D'), columns=['date'])
        past_data_hotel_sum_temp['day'] = past_data_hotel_sum_temp.date.map(lambda x: x.weekday())

        # In [120]: past_data_hotel_sum_temp
        # Out[120]:
        # date  day
        # 0 2019-04-19    4
        # 1 2019-04-20    5
        # 2 2019-04-21    6
        # 3 2019-04-22    0
        # 4 2019-04-23    1
        # 5 2019-04-24    2
        # 6 2019-04-25    3

        # del past_data_hotel_sum_temp['date']

        # In [115]: past_data_hotel
        # Out[115]:
        # oyo_id       date  used_rooms  sellable_rooms    gmv  day
        # 28  CN_SHA013 2019-04-05        11.0              46  842.5    4
        # 29  CN_SHA013 2019-04-06         6.0              46  669.1    5
        # 30  CN_SHA013 2019-04-07         1.0              46   92.7    6
        # 31  CN_SHA013 2019-04-08         0.0              46    0.0    0
        # 32  CN_SHA013 2019-04-09         0.0              46    0.0    1
        # 33  CN_SHA013 2019-04-10         0.0              46    0.0    2
        # 34  CN_SHA013 2019-04-11         0.0              46    0.0    3
        # 35  CN_SHA013 2019-04-12         0.0              46    0.0    4
        # 36  CN_SHA013 2019-04-13         1.0              46  -92.7    5
        # 37  CN_SHA013 2019-04-14         3.0              46  292.0    6
        # 38  CN_SHA013 2019-04-15         0.0              46    0.0    0
        # 39  CN_SHA013 2019-04-16         1.0              46   88.0    1
        # 40  CN_SHA013 2019-04-17         0.0              46    0.0    2
        # 41  CN_SHA013 2019-04-18         0.0              46    0.0    3

        # adding URN, SRN and GMV on the Days
        past_data_hotel_sum = past_3week_online_data_smart_hotel.groupby(by=['day', 'oyo_id'], as_index=False).sum()

        # In [117]: past_data_hotel_sum
        # Out[117]:
        # day     oyo_id  used_rooms  sellable_rooms    gmv
        # 0    0  CN_SHA013         0.0              92    0.0
        # 1    1  CN_SHA013         1.0              92   88.0
        # 2    2  CN_SHA013         0.0              92    0.0
        # 3    3  CN_SHA013         0.0              92    0.0
        # 4    4  CN_SHA013        11.0              92  842.5
        # 5    5  CN_SHA013         7.0              92  576.4
        # 6    6  CN_SHA013         4.0              92  384.7

        # arranging to days
        past_data_hotel_sum = pd.merge(past_data_hotel_sum_temp, past_data_hotel_sum, how='left', on=['day'])

        # adding Occ
        past_data_hotel_sum['online_occ'] = past_data_hotel_sum.online_urn / past_data_hotel_sum.sellable_rooms
        # adding ARR
        past_data_hotel_sum['online_arr'] = past_data_hotel_sum.online_gmv / past_data_hotel_sum.online_urn
        past_data_hotel_sum['arr'] = past_data_hotel_sum.total_gmv / past_data_hotel_sum.total_urn

        # In [107]: past_data_hotel_sum
        # Out[107]:
        # date  day     oyo_id  used_rooms  sellable_rooms    gmv       occ        arr
        # 0 2019-04-19    4  CN_SHA013        11.0              92  842.5  0.119565  76.590909
        # 1 2019-04-20    5  CN_SHA013         7.0              92  576.4  0.076087  82.342857
        # 2 2019-04-21    6  CN_SHA013         4.0              92  384.7  0.043478  96.175000
        # 3 2019-04-22    0  CN_SHA013         0.0              92    0.0  0.000000        NaN
        # 4 2019-04-23    1  CN_SHA013         1.0              92   88.0  0.010870  88.000000
        # 5 2019-04-24    2  CN_SHA013         0.0              92    0.0  0.000000        NaN
        # 6 2019-04-25    3  CN_SHA013         0.0              92    0.0  0.000000        NaN

        # Removing Errors
        past_data_hotel_sum.online_occ = past_data_hotel_sum.online_occ.map(lambda x: np.where(pd.isnull(x), 0, x))
        past_data_hotel_sum.online_arr = past_data_hotel_sum.online_arr.map(lambda x: np.where(pd.isnull(x), 0, x))
        past_data_hotel_sum.arr = past_data_hotel_sum.arr.map(lambda x: np.where(pd.isnull(x), 0, x))

        # last two weeks occ and ARR wrt days
        hotel_past_occ = past_data_hotel_sum[['day', 'online_occ', 'online_arr', 'arr']]
        # past_data_hotel_sum = select(past_data_hotel_sum,day,tau)
        # Tagging property age old becz it has data for last 14 days

        past_data_hotel_sum['prop_age'] = "old"
        # Selecting day,occ,age in this case age is old
        past_data_hotel_sum = past_data_hotel_sum[['day', 'online_occ', 'online_arr', 'arr', 'prop_age']]

        ####Condition 1 Result Day,Occ,Age####
    else:
        ####Condition 2 it means property is new. i.e not active for more than 21 days####

        # TODO fetch base from Pricing center
        logger.info(
            '########arr_list is all the properties which are close by to the selected property. Including the selected properties########')
        # here arr_list is all the properties which are close by to the selected property. Including the selected properties
        arr_list = clus_hotel_list

        # Category of the property
        loop_category = prop_details[prop_details.oyo_id == smart_hotel_oyo_id].prop_category.iloc[0]

        # Selecting the days of the week
        tt01 = dt.datetime.strftime(dt.datetime.fromtimestamp(time.time()), "%Y-%m-%d")
        tt02 = dt.datetime.strftime(dt.datetime.fromtimestamp(time.time()) + dt.timedelta(days=6), "%Y-%m-%d")

        past_data_hotel_sum = pd.DataFrame(pd.date_range(tt01, tt02, freq='D'), columns=['date'])

        past_data_hotel_sum['day'] = past_data_hotel_sum.date.map(lambda x: x.weekday())

        past_data_hotel_sum['tau'] = 1

        # In [48]: past_data_hotel_sum
        # Out[48]:
        # date  day  tau
        # 0 2019-04-19    4    1
        # 1 2019-04-20    5    1
        # 2 2019-04-21    6    1
        # 3 2019-04-22    0    1
        # 4 2019-04-23    1    1
        # 5 2019-04-24    2    1
        # 6 2019-04-25    3    1

        # Selecting URN, SRN and GMV of the properties which are nearby

        # In [49]: arr_list
        # Out[49]: ['CN_SHA008', 'CN_SHA043', 'CN_SHA012', 'CN_SHA1001', 'CN_SHA1002']
        past_data_cluster = past_3week_online_data[
            past_3week_online_data.oyo_id.isin(arr_list)]

        past_data_cluster_sum = past_data_cluster.groupby(by=['day'], as_index=False).sum()

        #             In [83]: past_data_cluster_sum
        # Out[83]:
        # day  used_rooms  SUM(A.BRN)  sellable_rooms      gmv
        # 0    0          14          14             114  1475.13
        # 1    1          12          12             114  1362.17
        # 2    2          16          16             117  1670.40
        # 3    3          12          12              79  1191.87
        # 4    4          18          22             190  2434.70
        # 5    5          29          29             190  3958.43
        # 6    6          12          14             152  1455.63

        # adding Occ
        past_data_cluster_sum['online_occ'] = past_data_cluster_sum.online_urn / past_data_cluster_sum.sellable_rooms
        # adding ARR
        past_data_cluster_sum['online_arr'] = past_data_cluster_sum.online_gmv / past_data_cluster_sum.online_urn
        past_data_cluster_sum['arr'] = past_data_cluster_sum.total_gmv / past_data_cluster_sum.total_urn

        past_data_cluster_sum.online_occ = past_data_cluster_sum.online_occ.map(lambda x: np.where(pd.isnull(x), 0, x))
        past_data_cluster_sum.online_arr = past_data_cluster_sum.online_arr.map(lambda x: np.where(pd.isnull(x), 0, x))
        past_data_cluster_sum.arr = past_data_cluster_sum.arr.map(lambda x: np.where(pd.isnull(x), 0, x))

        # In [85]: past_data_cluster_sum
        # Out[85]:
        # day  used_rooms  SUM(A.BRN)  sellable_rooms      gmv       occ         arr
        # 0    0          14          14             114  1475.13  0.122807  105.366429
        # 1    1          12          12             114  1362.17  0.105263  113.514167
        # 2    2          16          16             117  1670.40  0.136752  104.400000
        # 3    3          12          12              79  1191.87  0.151899   99.322500
        # 4    4          18          22             190  2434.70  0.094737  135.261111
        # 5    5          29          29             190  3958.43  0.152632  136.497586
        # 6    6          12          14             152  1455.63  0.078947  121.302500

        past_data_hotel_sum = pd.merge(past_data_hotel_sum,
                                       past_data_cluster_sum[['online_occ', 'online_arr', 'day', 'arr']],
                                       how='left', on=['day'])
        # Occ ARR Day of the properties nearby

        if loop_category == 2:
            past_data_hotel_sum.tau = 1.2
        # past_data_hotel_sum = select(past_data_hotel_sum,day,tau)
        # past_data_hotel_sum$occ = 1
        # Tagging Property AGE to be new
        past_data_hotel_sum['prop_age'] = "new"
        past_data_hotel_sum = past_data_hotel_sum[['day', 'online_occ', 'online_arr', 'prop_age', 'arr']]
        # Nearbuy Hotels Combines Property level day wise Occ and ARR with New age tag
        hotel_past_occ = past_data_hotel_sum.copy()
        # hotel_past_occ.prop_age = ''
        hotel_past_occ.tau = ''
        ####Condition 2 Ends with Nearbuy properties Day level occ,ARR for last 2 weeks.####

    # Last 3 weeks occ, ARR data of the hotel on day level
    hotel_past_occ = hotel_past_occ.rename(
        columns={'online_occ': 'past_3week_online_occ', 'online_arr': 'past_3week_online_arr',
                 'arr': 'past_3week_total_arr'})

    return hotel_past_occ, arr_list, past_data_hotel_sum
