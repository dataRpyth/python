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
from common.util.utils import *
from datetime import timedelta
import pandas as pd


def _create_or_get_map_from_map(map, key):
    key_map = map.get(key, None)
    if key_map is None:
        key_map = {}
        map[key] = key_map
    return key_map


def _create_or_get_list_from_map(map, key):
    key_map = map.get(key, None)
    if key_map is None:
        key_map = []
        map[key] = key_map
    return key_map


class BookingDateHourPrice:
    def __init__(self, booking_date, booking_hour, price, deprecated=False):
        self.booking_date = booking_date
        self.booking_hour = booking_hour
        self.price = price
        self.deprecated = deprecated


class PriceChangeHistory:
    def __init__(self, create_time, update_time, price):
        self.create_time_pdt = pd.to_datetime(create_time)
        self.update_time_pdt = pd.to_datetime(update_time)
        self.price = price

    def gen_price_list(self):
        range_list = []
        loop_start = self.create_time_pdt
        loop_end = self.update_time_pdt
        price = self.price
        loop = loop_start
        while loop < loop_end:
            booking_date = loop.strftime('%Y-%m-%d')
            booking_hour = loop.strftime('%-H')
            loop = loop + timedelta(hours=1)
            range_list.append(BookingDateHourPrice(booking_date, booking_hour, price))
        return range_list


def _record_price_change_history(price_change_history_map, hotel_id, room_type_id, sku_date, create_time,
                                 update_time, price):
    hotel_map = _create_or_get_map_from_map(price_change_history_map, hotel_id)
    room_type_map = _create_or_get_map_from_map(hotel_map, room_type_id)
    sku_date_list = _create_or_get_list_from_map(room_type_map, sku_date)
    price_change_history = PriceChangeHistory(create_time, update_time, price)
    sku_date_list.append(price_change_history)
    return sku_date_list


def _get_price_change_history_map(price_history_df):
    price_change_history_map = {}
    for index, row in price_history_df.iterrows():
        hotel_id = row['hotel_id']
        room_type_id = row['room_type_id']
        sku_date = row['sku_date']
        create_time = row['create_time']
        update_time = row['update_time']
        price = row['price']
        _record_price_change_history(price_change_history_map, hotel_id, room_type_id, sku_date,
                                     create_time, update_time, price)
    return price_change_history_map


def _gen_price_history_for_df(price_history_df, deduced_initial_price_map, booking_start_date, booking_end_date):
    price_change_history_map = _get_price_change_history_map(price_history_df)
    _clip_price_change_list_for_history_map(deduced_initial_price_map, price_change_history_map, booking_start_date,
                                            booking_end_date)
    price_history_map = _fill_hourly_booking_price_by_price_change_replay(price_change_history_map)
    return price_history_map


def _clip_price_change_list_for_history_map(deduced_initial_price_map, price_change_history_map, booking_start_date,
                                            booking_end_date):
    for hotel_id, hotel_map in price_change_history_map.items():
        for room_type_id, room_type_map in hotel_map.items():
            for sku_date, sku_date_list in room_type_map.items():
                new_price_change_list = _clip_price_change_list_for_sku_date(hotel_id, room_type_id, sku_date,
                                                                             deduced_initial_price_map, sku_date_list,
                                                                             booking_start_date, booking_end_date)
                room_type_map[sku_date] = new_price_change_list


def _get_deduced_price_for_sku_date(deduced_initial_price_map, hotel_id, room_type_id, sku_date):
    hotel_map = deduced_initial_price_map.get(hotel_id, None)
    if hotel_map is None:
        return None
    room_type_map = hotel_map.get(room_type_id, None)
    if room_type_map is None:
        return None
    return room_type_map.get(sku_date, None)


def _find_first_available_price_change_index(sku_date_price_change_list, start_time_pdt):
    list_sz = len(sku_date_price_change_list)
    for idx in range(list_sz):
        price_change = sku_date_price_change_list[idx]
        create_time_pdt = price_change.create_time_pdt
        update_time_pdt = price_change.update_time_pdt
        if create_time_pdt >= start_time_pdt or (create_time_pdt < start_time_pdt < update_time_pdt):
            return idx
    return list_sz


def _find_last_available_price_change_index(sku_date_price_change_list, end_time_pdt):
    list_sz = len(sku_date_price_change_list)
    for idx in range(list_sz - 1, -1, -1):
        price_change = sku_date_price_change_list[idx]
        create_time_pdt = price_change.create_time_pdt
        update_time_pdt = price_change.update_time_pdt
        if update_time_pdt < end_time_pdt or (update_time_pdt >= end_time_pdt > create_time_pdt):
            return idx
    return -1


def _clip_price_change_list_for_sku_date(hotel_id, room_type_id, sku_date, deduced_initial_price_map,
                                         sku_date_price_change_list, booking_start_date,
                                         booking_end_date):
    new_sku_date_price_change_list = list()
    list_sz = len(sku_date_price_change_list)
    if sku_date.strftime('%Y-%m-%d') < booking_start_date:
        return new_sku_date_price_change_list
    if list_sz == 0:
        LogUtil.get_cur_logger().warn(
            '_clip_price_change_list_for_sku_date, returning because sku_date_price_change_list size is 0')
        return new_sku_date_price_change_list
    booking_start_date_pdt = pd.to_datetime(booking_start_date)
    booking_end_date_pdt = pd.to_datetime(booking_end_date)
    sku_date_end_pdt = pd.to_datetime(sku_date) + timedelta(days=1)
    end_date_pdt = min(sku_date_end_pdt, booking_end_date_pdt)
    first_index = _find_first_available_price_change_index(sku_date_price_change_list, booking_start_date_pdt)
    if first_index >= list_sz:
        solo_day_create_time = booking_start_date_pdt
        solo_day_price = sku_date_price_change_list[list_sz - 1].price
        new_sku_date_price_change_list.append(
            PriceChangeHistory(solo_day_create_time,
                               _extend_booking_end_time_by_six_hours(sku_date_end_pdt, end_date_pdt),
                               solo_day_price))
        return new_sku_date_price_change_list
    last_index = _find_last_available_price_change_index(sku_date_price_change_list, end_date_pdt)
    if last_index < 0:
        solo_day_create_time = booking_start_date_pdt
        solo_day_price = _get_deduced_price_for_sku_date(deduced_initial_price_map, hotel_id, room_type_id, sku_date)
        new_sku_date_price_change_list.append(
            PriceChangeHistory(solo_day_create_time,
                               _extend_booking_end_time_by_six_hours(sku_date_end_pdt, end_date_pdt),
                               solo_day_price))
        return new_sku_date_price_change_list
    first_price_change = sku_date_price_change_list[first_index]
    first_create_time_date_with_hour = first_price_change.create_time_pdt.strftime('%Y-%m-%d-%H')
    if first_create_time_date_with_hour <= booking_start_date_pdt.strftime('%Y-%m-%d-%H'):
        # 第一个change slice横跨start time，只需更改create time即可
        first_price_change.create_time_pdt = booking_start_date_pdt
    else:
        # 否则需要加入一个假的时间片，从当天0点开始
        new_first_create_time = max(booking_start_date_pdt.strftime('%Y-%m-%d'),
                                    first_price_change.create_time_pdt.strftime('%Y-%m-%d'))
        new_first_update_time = first_price_change.create_time_pdt
        new_first_price = _get_deduced_price_for_sku_date(deduced_initial_price_map, hotel_id, room_type_id, sku_date)
        new_first_price_change = PriceChangeHistory(new_first_create_time, new_first_update_time, new_first_price)
        new_sku_date_price_change_list.append(new_first_price_change)
    for idx in range(first_index, last_index + 1, 1):
        new_sku_date_price_change_list.append(sku_date_price_change_list[idx])
    last_price_change = sku_date_price_change_list[last_index]
    last_update_time_date_with_an_hour_inc = (last_price_change.update_time_pdt + timedelta(hours=1)).strftime(
        '%Y-%m-%d-%H')
    if last_update_time_date_with_an_hour_inc >= end_date_pdt.strftime('%Y-%m-%d-%H'):
        # 最后一个change slice横跨end time，只需更改update time即可
        last_price_change.update_time_pdt = _extend_booking_end_time_by_six_hours(sku_date_end_pdt,
                                                                                  end_date_pdt)
    else:
        # 否则需要加入一个假的时间片，直到当天结束
        new_last_create_time = last_price_change.update_time_pdt
        new_last_update_time = _extend_booking_end_time_by_six_hours(sku_date_end_pdt, end_date_pdt)
        new_last_price = last_price_change.price
        new_sku_date_price_change_list.append(
            PriceChangeHistory(new_last_create_time, new_last_update_time, new_last_price))
    return new_sku_date_price_change_list


def _extend_booking_end_time_by_six_hours(sku_date_end_pdt, booking_end_date_pdt):
    dt_check = booking_end_date_pdt.strftime('%H:%M:%S')
    if dt_check != '00:00:00':
        LogUtil.get_cur_logger().warning(
            'wrong booking_end_date_pdt: {}'.format(booking_end_date_pdt.strftime('%Y-%m-%d %H:%M:%S')))
    if sku_date_end_pdt > booking_end_date_pdt:
        # booking_end_date 限定，只有sku date小于booking end date的日期才可以延展
        return booking_end_date_pdt
    # sku当天的第二天凌晨可以下单，扩展到第二天凌晨6点
    return booking_end_date_pdt + timedelta(hours=6)


def _gen_date_list_from_booking_start_and_end_date(booking_start_date, booking_end_date):
    booking_start_date_pdt = pd.to_datetime(booking_start_date)
    booking_end_date_pdt = pd.to_datetime(booking_end_date)
    loop_booking_pdt = booking_start_date_pdt
    booking_dates = []
    while loop_booking_pdt < booking_end_date_pdt:
        booking_date = loop_booking_pdt.strftime('%Y-%m-%d')
        booking_dates.append(booking_date)
    return booking_dates


def _fill_hourly_booking_price_by_price_change_replay(price_change_history_map):
    price_history_map = {}
    for hotel_id, hotel_map in price_change_history_map.items():
        ph_hotel_map = {}
        price_history_map[hotel_id] = ph_hotel_map
        for room_type_id, room_type_map in hotel_map.items():
            ph_room_type_map = {}
            ph_hotel_map[room_type_id] = ph_room_type_map
            for sku_date, sku_date_list in room_type_map.items():
                price_list = _gen_price_history_from_price_change_list(sku_date_list)
                ph_room_type_map[sku_date] = price_list
    return price_history_map


def _gen_price_history_from_price_change_list(price_change_list):
    temp_price_list = []
    temp_map = {}
    for price_change in price_change_list:
        booking_date_hour_list = price_change.gen_price_list()
        temp_price_list.extend(booking_date_hour_list)
        for booking_date_hour in booking_date_hour_list:
            booking_date = booking_date_hour.booking_date
            booking_hour = booking_date_hour.booking_hour
            key = booking_date + '-' + booking_hour
            old = temp_map.get(key, None)
            if old is not None:
                old.deprecated = True
            temp_map[key] = booking_date_hour
    ret_price_list = []
    for booking_date_hour in temp_price_list:
        if not booking_date_hour.deprecated:
            ret_price_list.append(booking_date_hour)
    return ret_price_list


def _put_deduced_price_to_map(deduced_price_map, hotel_id, room_type_id, sku_date, price):
    deduced_price_hotel_map = _create_or_get_map_from_map(deduced_price_map, hotel_id)
    deduced_price_room_type_map = _create_or_get_map_from_map(deduced_price_hotel_map, room_type_id)
    existing_price = deduced_price_room_type_map.get(sku_date, None)
    if existing_price is not None:
        LogUtil.get_cur_logger().warn(
            'error, price exists for hotel_id: {0}, '
            'room_type_id: {1}, '
            'sku_date: {2}, '
            'existing_price: {3}, '
            'new_price: {4}'.format(hotel_id, room_type_id, sku_date, existing_price, price))
    deduced_price_room_type_map[sku_date] = price


def _calc_deduced_price_map_with_all_price_history_df(all_price_history_df, booking_start_date):
    deduced_price_map = {}
    booking_start_date_pdt = pd.to_datetime(booking_start_date)
    price_change_history_map = {}
    for index, row in all_price_history_df.iterrows():
        hotel_id = row['hotel_id']
        room_type_id = row['room_type_id']
        sku_date = row['sku_date']
        create_time = row['create_time']
        update_time = row['update_time']
        price = row['price']
        hotel_map = _create_or_get_map_from_map(price_change_history_map, hotel_id)
        room_type_map = _create_or_get_map_from_map(hotel_map, room_type_id)
        sku_date_list = _create_or_get_list_from_map(room_type_map, sku_date)
        sku_date_list.append(PriceChangeHistory(create_time, update_time, price))
    for hotel_id, hotel_map in price_change_history_map.items():
        for room_type_id, room_type_map in hotel_map.items():
            for sku_date, sku_date_list in room_type_map.items():
                lst_index = 0
                for price_change_history in sku_date_list:
                    create_time_pdt = price_change_history.create_time_pdt
                    update_time_pdt = price_change_history.update_time_pdt
                    price = price_change_history.price
                    if booking_start_date_pdt <= create_time_pdt:
                        # 第一条改价记录晚于booking date, 认为价格为空
                        _put_deduced_price_to_map(deduced_price_map, hotel_id, room_type_id, sku_date, price)
                        break
                    if create_time_pdt <= booking_start_date_pdt < update_time_pdt:
                        # booking_start_date刚好介于一条记录的 create_time & update_time 之间，记录对应price即为结果
                        _put_deduced_price_to_map(deduced_price_map, hotel_id, room_type_id, sku_date, price)
                        break
                    next_price_change_history = _get_item_from_list_with_fallback(sku_date_list, lst_index + 1, None)
                    if next_price_change_history is not None:
                        next_create_time_pdt = next_price_change_history.create_time_pdt
                        next_update_time_pdt = next_price_change_history.update_time_pdt
                        next_price = next_price_change_history.price
                        if update_time_pdt <= booking_start_date_pdt < next_create_time_pdt:
                            # 两条记录create_time1 <-> update_time1 <=> create_time2 <-> update_time2
                            # update_time1 和 create_time2 之间未能完全衔接，有缝隙，且booking_start_date刚好介于两者间
                            # 则认为推导price为第一条记录的price
                            _put_deduced_price_to_map(deduced_price_map, hotel_id, room_type_id, sku_date, price)
                            break
                        elif next_create_time_pdt <= booking_start_date_pdt < next_update_time_pdt:
                            # booking_start_date介于下一条记录的create_time & update_time 之间，下一条记录的price为结果，短路返回
                            _put_deduced_price_to_map(deduced_price_map, hotel_id, room_type_id, sku_date, next_price)
                            break
                    else:
                        # 遍历全部，booking_start_date仍然不在任意一区间，则最后一条记录的price为结果
                        _put_deduced_price_to_map(deduced_price_map, hotel_id, room_type_id, sku_date, price)
                    lst_index += 1
    return deduced_price_map


def _get_item_from_list_with_fallback(lst, index, fallback):
    if index > len(lst) - 1:
        return fallback
    return lst[index]


class PriceHistoryJob(JobBase):

    def __init__(self, job_config):
        JobBase.__init__(self, job_config)

    def get_job_name(self):
        return 'BaseDataJob'

    def run(self):
        logger = LogUtil.get_cur_logger()

        config = self.get_job_config()

        DateUtil.init_preset_time()

        all_hotels_list = ['CN_HUI033',
                           'CN_ZHJ1002',
                           'CN_NAP1004',
                           'CN_GGU045',
                           'CN_FZH1013',
                           'CN_KMG057',
                           'CN_ZHJ1006',
                           'CN_BAI022',
                           'CN_GGU004',
                           'CN_ZHJ1007',
                           'CN_KMG020',
                           'CN_DGN054',
                           'CN_KMG031',
                           'CN_NAI018',
                           'CN_NIN010',
                           'CN_XMN001',
                           'CN_XMN1026',
                           'CN_KMG045',
                           'CN_HUI003',
                           'CN_DGN1014',
                           'CN_DGN1015',
                           'CN_NAI1003',
                           'CN_HKU072',
                           'CN_XMN015',
                           'CN_QUA1017',
                           'CN_NAP002',
                           'CN_NAP1007',
                           'CN_BEI1031',
                           'CN_XMN1017',
                           'CN_ZNG1001',
                           'CN_GGU1039',
                           'CN_QUA1018',
                           'CN_SZX198',
                           'CN_FZH053',
                           'CN_KMG043',
                           'CN_HKU001',
                           'CN_SZX1069',
                           'CN_DNZ006',
                           'CN_LIJ015',
                           'CN_NAI1002',
                           'CN_HKU032',
                           'CN_KMG1045',
                           'CN_SZX012',
                           'CN_HKU1017',
                           'CN_SHN008',
                           'CN_SHN004',
                           'CN_XMN1027',
                           'CN_KMG1040',
                           'CN_KMG1041',
                           'CN_YNU1056',
                           'CN_HKU1019',
                           'CN_HKU1020',
                           'CN_QUA019',
                           'CN_GGU044',
                           'CN_XAN1098',
                           'CN_KMG1049',
                           'CN_XAN1095',
                           'CN_GGU149',
                           'CN_BEI1034',
                           'CN_JIL026',
                           'CN_HFI087',
                           'CN_SNA1018',
                           'CN_ZNG1005',
                           'CN_KMG091',
                           'CN_HEY1007',
                           'CN_XAN157',
                           'CN_NJG093',
                           'CN_WZH1029',
                           'CN_KMG1043',
                           'CN_CHA1002',
                           'CN_HFI1036',
                           'CN_XAN060',
                           'CN_HFI1034',
                           'CN_NJG076',
                           'CN_BAH1001',
                           'CN_QIA009',
                           'CN_WZH033',
                           'CN_LUY027',
                           'CN_HOH1003',
                           'CN_XAN164',
                           'CN_KMG003',
                           'CN_SUZ1015',
                           'CN_NCH072',
                           'CN_CHA1014',
                           'CN_CHO002',
                           'CN_GGU132',
                           'CN_XHU036',
                           'CN_QAX1003',
                           'CN_CGD1033',
                           'CN_XAN1108',
                           'CN_LIA025',
                           'CN_ANK1010',
                           'CN_HUZ009',
                           'CN_CHO001',
                           'CN_TNJ1017',
                           'CN_XAN1125',
                           'CN_DLI1047',
                           'CN_CCN075',
                           'CN_HEN1015',
                           'CN_HFI1035',
                           'CN_SNA1019',
                           'CN_WZH005',
                           'CN_XAN1090',
                           'CN_YNG015',
                           'CN_LOG1002',
                           'CN_YNU015',
                           'CN_BAY1003',
                           'CN_GUA013',
                           'CN_LNZ036',
                           'CN_CGD1030',
                           'CN_XHU011',
                           'CN_XAN018',
                           'CN_TAN013',
                           'CN_XAN1114',
                           'CN_HUZ012',
                           'CN_CCN1052',
                           'CN_QIH003',
                           'CN_TNJ1016',
                           'CN_SZX104',
                           'CN_WZH040',
                           'CN_WZH039',
                           'CN_TNJ1010',
                           'CN_ZHI1005',
                           'CN_GUI1026',
                           'CN_XAN1007',
                           'CN_JGD1004',
                           'CN_JIX1015',
                           'CN_YUL011',
                           'CN_CHA1015',
                           'CN_TNJ1026',
                           'CN_WUH002',
                           'CN_ZGU098',
                           'CN_NJG1020',
                           'CN_HAZ1008',
                           'CN_XAN014',
                           'CN_NCH1013',
                           'CN_JGD003',
                           'CN_LNZ1020',
                           'CN_BEI063',
                           'CN_KMG002',
                           'CN_XAN1116',
                           'CN_XNG1013',
                           'CN_FOS070',
                           'CN_YNU1042',
                           'CN_QGO1035',
                           'CN_XAN183',
                           'CN_XAN070',
                           'CN_QIN1001',
                           'CN_LOG1001',
                           'CN_JIU019',
                           'CN_BAT1012',
                           'CN_KMG035',
                           'CN_BEI1035',
                           'CN_QUA053',
                           'CN_XMN067',
                           'CN_SHU1014',
                           'CN_QGO1029',
                           'CN_NJG067',
                           'CN_LOG1007',
                           'CN_YNG014',
                           'CN_NJG083',
                           'CN_NJG034',
                           'CN_FUS1021',
                           'CN_HUB1002',
                           'CN_FUS1013',
                           'CN_NJG108',
                           'CN_NJG099',
                           'CN_YNU1060',
                           'CN_HFI021',
                           'CN_ANQ013',
                           'CN_NAI015',
                           'CN_NCH1007',
                           'CN_XAN1128',
                           'CN_HUH1006',
                           'CN_CAO1007',
                           'CN_JIZ020',
                           'CN_KMG087',
                           'CN_WEH1023',
                           'CN_PIN1011',
                           'CN_LOU002',
                           'CN_SZU1026',
                           'CN_NCH028',
                           'CN_LNZ1019',
                           'CN_ZHG1010',
                           'CN_SZX032',
                           'CN_NAI012',
                           'CN_LIN013',
                           'CN_BOD1012',
                           'CN_FUZ1004',
                           'CN_SHA1021',
                           'CN_HFI1039',
                           'CN_CCN1056',
                           'CN_HUZ1011',
                           'CN_QGO073',
                           'CN_YNG1011',
                           'CN_FUY1023',
                           'CN_ZHA1039',
                           'CN_TNJ053',
                           'CN_HUI017',
                           'CN_NBO1006',
                           'CN_SNA041',
                           'CN_TOG004',
                           'CN_YNU1059',
                           'CN_SHU1010',
                           'CN_LNZ037',
                           'CN_URU014',
                           'CN_ANQ006',
                           'CN_LIN032',
                           'CN_XGT005',
                           'CN_HIN1003',
                           'CN_BAN025',
                           'CN_BEI062',
                           'CN_GUY1021',
                           'CN_URU1011',
                           'CN_YNU1064',
                           'CN_JIZ1011',
                           'CN_HFI090',
                           'CN_GUY1027',
                           'CN_PUY1005',
                           'CN_WHN1059',
                           'CN_HFI078',
                           'CN_BIN1015',
                           'CN_XAN012',
                           'CN_CNG1044',
                           'CN_JIX1016',
                           'CN_LVL1005',
                           'CN_TNJ1014',
                           'CN_HLU1012',
                           'CN_GUY1028',
                           'CN_KMG1052',
                           'CN_FOS1020',
                           'CN_MAO009',
                           'CN_ZHA1041',
                           'CN_QIH1117',
                           'CN_HEB1005',
                           'CN_WEH1045',
                           'CN_YUL1021',
                           'CN_ANQ002',
                           'CN_WUH007',
                           'CN_YNU1058',
                           'CN_BIN1007',
                           'CN_BIN1004',
                           'CN_XMN1029',
                           'CN_NCH014',
                           'CN_SHU1024',
                           'CN_BIN1006',
                           'CN_BIN1009',
                           'CN_PAN1003',
                           'CN_NJG1005',
                           'CN_LUY008',
                           'CN_JGD1005',
                           'CN_TIZ024',
                           'CN_MAN004',
                           'CN_HGU1067',
                           'CN_LNZ028',
                           'CN_XIA1019',
                           'CN_FOS1005',
                           'CN_YNC017',
                           'CN_ZIB004',
                           'CN_QIH001',
                           'CN_JIZ017',
                           'CN_NJG1007',
                           'CN_WHN033',
                           'CN_HOH1005',
                           'CN_HEB1006',
                           'CN_JIZ1014',
                           'CN_ANQ009',
                           'CN_DAQ011',
                           'CN_SZU064',
                           'CN_SHA021',
                           'CN_WZH1035',
                           'CN_TIZ025',
                           'CN_WZH041',
                           'CN_SYG010',
                           'CN_EDO1006',
                           'CN_XAN027',
                           'CN_HAN010',
                           'CN_CCN029',
                           'CN_QGO1056',
                           'CN_SNQ1019',
                           'CN_JNG028',
                           'CN_SNQ1018',
                           'CN_TIZ012',
                           'CN_LNG1011',
                           'CN_CHA1005',
                           'CN_TNJ052',
                           'CN_WZH1033',
                           'CN_YAN058',
                           'CN_KMG085',
                           'CN_LOU1007',
                           'CN_LUY055',
                           'CN_SAO004',
                           'CN_HUZ1002',
                           'CN_WHI005',
                           'CN_BAT1007',
                           'CN_JIL1021',
                           'CN_ZHI018',
                           'CN_XIA1018',
                           'CN_NBO081',
                           'CN_XNA1008',
                           'CN_ZGU020',
                           'CN_FZH040',
                           'CN_QUA063',
                           'CN_BEI021',
                           'CN_CNG1132',
                           'CN_TIN008',
                           'CN_YUL1007',
                           'CN_ZIB013',
                           'CN_SAM1010',
                           'CN_ZIB001',
                           'CN_ZIB018',
                           'CN_WEH1004',
                           'CN_XAN013',
                           'CN_DAI1031',
                           'CN_GUY1031',
                           'CN_CNG1061',
                           'CN_NBO1003',
                           'CN_YAN1043',
                           'CN_HEZ007',
                           'CN_QIH059',
                           'CN_DEZ1020',
                           'CN_HEZ1017',
                           'CN_WZH029',
                           'CN_JIU005',
                           'CN_BEN1006',
                           'CN_XIG1002',
                           'CN_HFI1037',
                           'CN_BEN1005',
                           'CN_CHI1009',
                           'CN_ZHN012',
                           'CN_JIL1031',
                           'CN_DGN1011',
                           'CN_XII1007',
                           'CN_ZHA1040',
                           'CN_BAN1015',
                           'CN_DAI1032',
                           'CN_SHU1056',
                           'CN_CNG1042',
                           'CN_TOG010',
                           'CN_CCN1026',
                           'CN_NNJ1006',
                           'CN_YNG1013',
                           'CN_XIG1006',
                           'CN_SON1010',
                           'CN_SON1008',
                           'CN_HRB1050',
                           'CN_CHG005',
                           'CN_XNA002',
                           'CN_FUS1011',
                           'CN_ANS1005',
                           'CN_HOH1016',
                           'CN_TIN1014',
                           'CN_XAN1129',
                           'CN_FUX1002',
                           'CN_HEY1009',
                           'CN_TIZ1005',
                           'CN_GUI1041',
                           'CN_KMG1026',
                           'CN_CHO1009',
                           'CN_GUI1012',
                           'CN_HUZ014',
                           'CN_UIA002',
                           'CN_TIZ022',
                           'CN_CNG1137',
                           'CN_WEF008',
                           'CN_GUA012',
                           'CN_TNJ029',
                           'CN_JIU1013',
                           'CN_QIA014',
                           'CN_JIQ1005',
                           'CN_DLI068',
                           'CN_QIN1005',
                           'CN_WEH036',
                           'CN_QIN1004',
                           'CN_XNA1009',
                           'CN_BOD011',
                           'CN_TIN019',
                           'CN_WUZ001',
                           'CN_XHU1018',
                           'CN_HEY1008',
                           'CN_XMN1028',
                           'CN_CNG098',
                           'CN_CNG1131',
                           'CN_QGO104',
                           'CN_HEZ008',
                           'CN_NBO1009',
                           'CN_BEI1002',
                           'CN_WHN014',
                           'CN_SNA071']

        hotels_in_hive_list = ['CN_BAY1003',
                               'CN_LIJ1033',
                               'CN_WUH002',
                               'CN_FOS070',
                               'CN_XAN164',
                               'CN_XNA1005',
                               'CN_LOU002',
                               'CN_XMN015',
                               'CN_LUY037',
                               'CN_KMG057',
                               'CN_QUA1016',
                               'CN_HUI033',
                               'CN_YNG1005',
                               'CN_LIJ023',
                               'CN_NJG076',
                               'CN_LIJ1037',
                               'CN_KMG003',
                               'CN_ZNG1005',
                               'CN_NCH1013',
                               'CN_ZHJ1008',
                               'CN_SZX032',
                               'CN_NAI018',
                               'CN_ZHJ1006',
                               'CN_NAP1007',
                               'CN_KMG020',
                               'CN_LIJ046',
                               'CN_CHA1014',
                               'CN_ZHG1010',
                               'CN_BEI1035',
                               'CN_CCN1056',
                               'CN_WEH1023',
                               'CN_XMN1026',
                               'CN_WZH040',
                               'CN_ANK1010',
                               'CN_HRB1067',
                               'CN_NJG1020',
                               'CN_XHU011',
                               'CN_NEI001',
                               'CN_NAP002',
                               'CN_HFI1035',
                               'CN_XAN1125',
                               'CN_QUA1013',
                               'CN_YNU1060',
                               'CN_HFI1036',
                               'CN_ZHJ1007',
                               'CN_TNJ1010',
                               'CN_LOG1001',
                               'CN_ZHA1039',
                               'CN_ZNG1001',
                               'CN_CCN1052',
                               'CN_GGU149',
                               'CN_CHA1002',
                               'CN_XMN1027',
                               'CN_KMG1049',
                               'CN_HUH1006',
                               'CN_BAI022',
                               'CN_NCH1007',
                               'CN_NJG093',
                               'CN_YNU015',
                               'CN_GGU123',
                               'CN_NJG099',
                               'CN_WZH039',
                               'CN_XAN1116',
                               'CN_HFI1034',
                               'CN_YNU1059',
                               'CN_HUZ012',
                               'CN_XAN001',
                               'CN_LNZ037',
                               'CN_CHA1015',
                               'CN_LNZ1015',
                               'CN_XMN001',
                               'CN_DLI1047',
                               'CN_HKU001',
                               'CN_HKU1020',
                               'CN_CCN018',
                               'CN_GGU004',
                               'CN_XAN1090',
                               'CN_SHN008',
                               'CN_SNA041',
                               'CN_TNJ1017',
                               'CN_HFI087',
                               'CN_YNG015',
                               'CN_CGD1033',
                               'CN_FZH1013',
                               'CN_HFI1039',
                               'CN_ANQ006',
                               'CN_YNU1056',
                               'CN_YNU1042',
                               'CN_XAN1098',
                               'CN_YIB008',
                               'CN_ZHJ010',
                               'CN_QGO1029',
                               'CN_YNG014',
                               'CN_XAN183',
                               'CN_KMG1041',
                               'CN_SNA1019',
                               'CN_TNJ052',
                               'CN_GGU044',
                               'CN_NAP1008',
                               'CN_SZX1069',
                               'CN_KMG091',
                               'CN_HEY1007',
                               'CN_KMG1043',
                               'CN_JIL026',
                               'CN_LIN013',
                               'CN_SUZ1015',
                               'CN_ANQ013',
                               'CN_NAP1004',
                               'CN_PIN1011',
                               'CN_XAN060',
                               'CN_CHO002',
                               'CN_ZHJ1002',
                               'CN_BOD1012',
                               'CN_JGD003',
                               'CN_SZX198',
                               'CN_BAT1012',
                               'CN_SHN004',
                               'CN_CGD1030',
                               'CN_XAN1114',
                               'CN_JIZ020',
                               'CN_WZH1033',
                               'CN_SZX015',
                               'CN_QGO1035',
                               'CN_QIA009',
                               'CN_TOG004',
                               'CN_XAN157',
                               'CN_KMG1045',
                               'CN_LIA025',
                               'CN_TNJ053',
                               'CN_KMG043',
                               'CN_LVL1001',
                               'CN_SNA1018',
                               'CN_NJG034',
                               'CN_DGN1014',
                               'CN_HUI017',
                               'CN_KMG031',
                               'CN_URU014',
                               'CN_LNZ036',
                               'CN_XMN1017',
                               'CN_FZH053',
                               'CN_WZH033',
                               'CN_XHU036',
                               'CN_HUZ009',
                               'CN_GUA013',
                               'CN_HKU032',
                               'CN_QGO1038',
                               'CN_DGN1015',
                               'CN_YNG1011',
                               'CN_HKU072',
                               'CN_BAH1001',
                               'CN_NAI1002',
                               'CN_NCH028',
                               'CN_SHU1010',
                               'CN_HOH1003',
                               'CN_QGO1014',
                               'CN_QAX1003',
                               'CN_SHU1014',
                               'CN_NAI015',
                               'CN_HUZ1011',
                               'CN_KMG035',
                               'CN_GGU1039',
                               'CN_WZH005',
                               'CN_HFI021',
                               'CN_NIN010',
                               'CN_BEI1031',
                               'CN_QIN1001',
                               'CN_NAI012',
                               'CN_BEI063',
                               'CN_NAI1003',
                               'CN_KMG087',
                               'CN_NJG108',
                               'CN_HUL1013',
                               'CN_LIJ1032',
                               'CN_QUA053',
                               'CN_XAN1108',
                               'CN_DNZ006',
                               'CN_XAN014',
                               'CN_TNJ1026',
                               'CN_XAN1095',
                               'CN_FUS1013',
                               'CN_CCN075',
                               'CN_SAN017',
                               'CN_NJG083',
                               'CN_XAN018',
                               'CN_HAZ1008',
                               'CN_NCH072',
                               'CN_GUI1026',
                               'CN_XAN1007',
                               'CN_LIJ1036',
                               'CN_HKU1019',
                               'CN_FUZ1004',
                               'CN_SZX104',
                               'CN_CHA1005',
                               'CN_WEI1007',
                               'CN_TNJ1016',
                               'CN_WZH1029',
                               'CN_ZGU098',
                               'CN_SZU1026',
                               'CN_YUI1005',
                               'CN_XMN067',
                               'CN_HUB1002',
                               'CN_JIX1015',
                               'CN_LOG1002',
                               'CN_HUI003',
                               'CN_XNG1013',
                               'CN_JGD1004',
                               'CN_CAO1007',
                               'CN_QIH003',
                               'CN_LIJ1035',
                               'CN_KMG045',
                               'CN_LNZ1019',
                               'CN_SHA1021',
                               'CN_HEN1015',
                               'CN_CHO001',
                               'CN_QUA1018',
                               'CN_SZX012',
                               'CN_TAN013',
                               'CN_XAN1128',
                               'CN_FUY1023',
                               'CN_ZHI1005',
                               'CN_QGO073',
                               'CN_YUL011',
                               'CN_BEI1034',
                               'CN_NJG067',
                               'CN_JIU019',
                               'CN_QUA019',
                               'CN_HKU1017',
                               'CN_XNG1012',
                               'CN_FUS1021',
                               'CN_XAN070',
                               'CN_DGN054',
                               'CN_KMG1040',
                               'CN_XHU052',
                               'CN_LIJ015',
                               'CN_LIJ048',
                               'CN_LOG1007',
                               'CN_KMG002',
                               'CN_YAG014',
                               'CN_GGU132',
                               'CN_NBO1026',
                               'CN_LIN032',
                               'CN_NBO1006',
                               'CN_QUA1017',
                               'CN_GGU045',
                               'CN_LNZ1020',
                               'CN_LUY027']

        all_hotels_set = set(all_hotels_list)

        hotels_in_hive_set = set(hotels_in_hive_list)

        remaining_hotels = all_hotels_set.difference(hotels_in_hive_set)

        oyo_id_list = list(remaining_hotels)

        oyo_id_series = pd.DataFrame({'oyo_id': oyo_id_list}).oyo_id

        all_hotels_str = MiscUtil.convert_list_to_tuple_list_str(oyo_id_list)

        min_create_time_query = """
            select oyo_id, date_format(min_create_time, '%%Y-%%m-%%d') as min_create_date
            from (select hotel_id, min(create_time) as min_create_time
                  from price_list_rate
                  where hotel_id in (select id as hotel_id
                                     from product_hotel
                                     where oyo_id in {0})
                  group by hotel_id) hotel_with_min_create_time
                   join (select id as hotel_id, oyo_id
                         from product_hotel
                         where oyo_id in {0}) hotel_id_with_oyo_id
                        on hotel_with_min_create_time.hotel_id = hotel_id_with_oyo_id.hotel_id
        """.format(all_hotels_str)

        min_create_time_df = self.get_adb_query_manager().read_sql(min_create_time_query)

        booking_end_date = '2019-07-10'

        for index, row in min_create_time_df.iterrows():
            oyo_id = row['oyo_id']
            booking_start_date = row['min_create_date']
            self.write_price_history_to_csv_for_hotel(oyo_id, booking_start_date, booking_end_date)

    def write_price_history_to_csv_for_hotel(self, oyo_id, booking_start_date, booking_end_date):
        LogUtil.get_cur_logger().info(
            'write_price_history_to_csv_for_hotel, start processing hotel: {0}, '
            'start_date: {1}, '
            'end_date: {2}'.format(oyo_id, booking_start_date, booking_end_date))

        deduce_initial_price_history_query = """
            select hotel_id, room_type_id, rate_date as sku_date, create_time, update_time, is_deleted, rate as price
            from price_list_rate
            where hotel_id in (select id as hotel_id from product_hotel where oyo_id = '{0}')
             and (
               (date_format(create_time, '%%Y-%%m-%%d %%H-%%i-%%S') >= '{1}'
                 and date_format(create_time, '%%Y-%%m-%%d %%H-%%i-%%S') < '{2}')
               or (date_format(update_time, '%%Y-%%m-%%d %%H-%%i-%%S') > '{1}'))
            order by hotel_id
                  , room_type_id
                  , rate_date
                  , create_time
                  , update_time
                """.format(oyo_id, booking_start_date, booking_end_date)

        deduce_initial_price_history_df = self.get_adb_query_manager().read_sql(deduce_initial_price_history_query, 200)

        deduced_initial_price_map = _calc_deduced_price_map_with_all_price_history_df(deduce_initial_price_history_df,
                                                                                      booking_start_date)

        booking_date_price_history_query = """
                    select hotel_id, room_type_id, rate_date as sku_date, create_time, update_time, is_deleted, rate as price
                    from price_list_rate
                    where hotel_id in (select id as hotel_id from product_hotel where oyo_id = '{0}')
                      and (
                        (date_format(update_time, '%%Y-%%m-%%d %%H-%%i-%%S') <= '{1} 00-00-00'
                          and is_deleted = 0)
                        or
                        (date_format(create_time, '%%Y-%%m-%%d %%H-%%i-%%S') <= '{1} 00-00-00'
                          and date_format(update_time, '%%Y-%%m-%%d %%H-%%i-%%S') > '{1} 00-00-00')
                        or
                        (date_format(create_time, '%%Y-%%m-%%d %%H-%%i-%%S') > '{1} 00-00-00'
                          and date_format(update_time, '%%Y-%%m-%%d %%H-%%i-%%S') < '{2} 00-00-00')
                        or
                        (date_format(create_time, '%%Y-%%m-%%d %%H-%%i-%%S') < '{2} 00-00-00'
                          and date_format(update_time, '%%Y-%%m-%%d %%H-%%i-%%S') > '{2} 00-00-00'))
                    order by hotel_id
                           , room_type_id
                           , rate_date
                           , create_time
                           , update_time
                """.format(oyo_id, booking_start_date, booking_end_date)

        price_history_df = self.get_adb_query_manager().read_sql(booking_date_price_history_query, 200)

        price_history_map = _gen_price_history_for_df(price_history_df, deduced_initial_price_map, booking_start_date,
                                                      booking_end_date)

        LogUtil.get_cur_logger().info('price history map composed for oyo_id: {0}'.format(oyo_id))

        create_time_str = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')

        price_history_df = _compose_price_history_map_to_df(price_history_map, create_time_str)

        oyo_id_query = """
            select id as hotel_id, oyo_id
            from product_hotel
            where oyo_id = '{0}'
        """.format(oyo_id)

        oyo_id_df = self.get_adb_query_manager().read_sql(oyo_id_query)

        price_history_df = pd.merge(price_history_df, oyo_id_df, how='left', on=['hotel_id'])

        price_history_df = price_history_df[
            ['hotel_id', 'sku_date', 'price', 'create_time', 'oyo_id', 'room_type_id', 'booking_date', 'booking_hour']]

        LogUtil.get_cur_logger().info('price history data frame generated for oyo_id: {0}'.format(oyo_id))

        file_end_date = (pd.to_datetime(booking_end_date) - timedelta(days=1)).strftime('%Y-%m-%d')

        output_file_name = '{0}_{1}_{2}.csv'.format(oyo_id, booking_start_date, file_end_date)

        file_path = join_path(cur_path, 'log', output_file_name)

        price_history_df.to_csv(file_path, index=False)

        LogUtil.get_cur_logger().info(
            'price history file: {0} generated for oyo_id: {1}'.format(output_file_name, oyo_id))


def _compose_price_history_map_to_df(price_history_map, create_time_str):
    hotel_id_lst = []
    room_type_lst = []
    sku_date_lst = []
    booking_date_lst = []
    booking_hour_lst = []
    price_lst = []
    create_time_lst = []
    for hotel_id, hotel_map in price_history_map.items():
        for room_type_id, room_type_map in hotel_map.items():
            for sku_date, history_list in room_type_map.items():
                for price in history_list:
                    booking_date = price.booking_date
                    booking_hour = price.booking_hour
                    price = price.price
                    if price is not None:
                        hotel_id_lst.append(hotel_id)
                        room_type_lst.append(room_type_id)
                        sku_date_lst.append(sku_date)
                        booking_date_lst.append(booking_date)
                        booking_hour_lst.append(booking_hour)
                        price_lst.append(price)
                        create_time_lst.append(create_time_str)
                    else:
                        LogUtil.get_cur_logger().info(
                            'null price for hotel_id: {0}, '
                            'room_type_id: {1}, '
                            'sku_date: {2}, '
                            'booking_date: {3}, '
                            'booking_hour: {4}'.format(hotel_id, room_type_id, sku_date, booking_date, booking_hour))
    return pd.DataFrame({'hotel_id': hotel_id_lst,
                         'room_type_id': room_type_lst,
                         'sku_date': sku_date_lst,
                         'booking_date': booking_date_lst,
                         'booking_hour': booking_hour_lst,
                         'price': price_lst,
                         'create_time': create_time_lst})
