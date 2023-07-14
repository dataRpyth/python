#!/usr/bin/env python
# -*- coding:utf-8 -*-
import os
import sys
import warnings
from math import ceil
from os.path import join as join_path


cur_path = os.path.dirname(os.path.realpath(__file__))
sys.path.append(join_path(cur_path, '..'))
warnings.filterwarnings("ignore")

from Liquidation.configuration import LIQUIDATION_BATCH_49, LIQUIDATION_BATCH_39, LIQUIDATION_BATCH_29_2, \
    LIQUIDATION_BATCH_29_1
from egm_liquidation.configuration import EGM_LIQUIDATION_BATCH_49, EGM_LIQUIDATION_BATCH_39, \
    EGM_LIQUIDATION_BATCH_29_2, EGM_LIQUIDATION_BATCH_29_1

from pandas import DataFrame
from common.util.utils import MiscUtil


class LiqStrategy:
    def __init__(self):
        pass

    def calc_sale_price_for_liquidation_rate(self, df_for_room_liq, batch):
        if df_for_room_liq.empty:
            return df_for_room_liq
        df_for_room_liq = df_for_room_liq[
            df_for_room_liq.base.apply(lambda x: MiscUtil.is_not_empty_value(x)) &
            df_for_room_liq.oyo_id.apply(lambda x: MiscUtil.is_not_empty_value(x)) &
            df_for_room_liq.arr.apply(lambda x: MiscUtil.is_not_empty_value(x)) &
            df_for_room_liq.pricing_date.apply(lambda x: MiscUtil.is_not_empty_value(x))]
        df_for_room_liq = df_for_room_liq.sort_values(
            ['oyo_id', 'pricing_date', 'price_delta', "price_multiplier", "room_type_id"], ascending=True).reset_index(
            drop=True)
        base_pms_price0 = 29
        price_delta0 = 0
        oyo_id0 = None
        date0 = None
        floor_price = self.get_floor_price(batch)
        for index, row in df_for_room_liq.iterrows():
            rate = self.get_rate(batch, row.get("arr"))
            if row.get("oyo_id") != oyo_id0 or row.get("pricing_date") != date0:
                # 最差第一个房型base_pms_price
                base_pms_price0 = (row.get("base") + row.get("price_delta")) * rate
                base_pms_price0 = self.get_base(base_pms_price0, floor_price)
                base_pms_price = base_pms_price0
                oyo_id0 = row.get("oyo_id")
                date0 = row.get("pricing_date")
                price_delta0 = row.get("price_delta")
            else:
                # 基于第一个base_pms_price
                base_pms_price = base_pms_price0 + ceil((row.get("price_delta") - price_delta0) * rate)
            df_for_room_liq.at[index, 'sale_price'] = self.decorate_end_digit(base_pms_price, 4)
        return df_for_room_liq

    def decorate_end_digit(self, base, end_digit, off_set=1):
        return (base + off_set) if base % 10 == end_digit else base

    def get_floor_price(self, batch):
        if batch == LIQUIDATION_BATCH_29_1 or batch == LIQUIDATION_BATCH_29_2:
            return 29
        elif batch == LIQUIDATION_BATCH_39:
            return 39
        elif batch == LIQUIDATION_BATCH_49:
            return 49
        raise Exception('unknown batch: {}'.format(batch))

    def get_base(self, base, floor_price):
        base_9 = round(self.decorate_end_digit(base, 5) * 0.1) * 10 - 1
        base_9 = max(base_9, floor_price)
        base_9 = min(base_9, 499)
        return base_9

    def get_rate(self, batch, arr):
        if batch == LIQUIDATION_BATCH_49 or batch == LIQUIDATION_BATCH_39:
            rate = 0.55 if arr >= 100 else 0.6
        elif batch == LIQUIDATION_BATCH_29_1 or batch == LIQUIDATION_BATCH_29_2:
            rate = 0.5
        else:
            raise Exception('unknown batch: {}'.format(batch))
        return rate

    def calc_sale_price_for_liquidation(self, prices_for_room_type_df, base_liquidation_price=29):
        prices_for_room_type_df = prices_for_room_type_df.sort_values(
            ['oyo_id', 'pricing_date', 'price_delta', "price_multiplier"], ascending=True).reset_index(drop=True)
        base_pms_price = 0
        oyo_id = None
        price_delta = None
        date = None
        for index, row in prices_for_room_type_df.iterrows():
            if row.get("oyo_id") != oyo_id or row.get("pricing_date") != date:
                base_pms_price = base_liquidation_price
                oyo_id = row.get("oyo_id")
                date = row.get("pricing_date")
            else:
                if price_delta == row.get("price_delta"):
                    base_pms_price = base_pms_price - 10
            price_delta = row.get("price_delta")
            prices_for_room_type_df.at[index, 'sale_price'] = 35 if base_pms_price == 29 else base_pms_price
            base_pms_price = base_pms_price + 10
        return prices_for_room_type_df

    def get_hotel_target_occ(self, weekday_number):
        return 0.8 if MiscUtil.is_weekend(weekday_number) else 0.7

    # 甩卖酒店分类
    def categorize_hotels(self, hotel_batch_df):
        df_category_1 = DataFrame()
        df_category_2 = DataFrame()
        df_category_3 = DataFrame()
        df_category_4 = DataFrame()
        df_category_5 = DataFrame()
        # 1. df_ytd_revpar_achived
        df_ytd_revpar_achived = hotel_batch_df[hotel_batch_df.ytd_revpar_achived >= 0]
        if not df_ytd_revpar_achived.empty:
            # 1.1. df_ytd_revpar_achived & mtd_occ >= 0.8
            df_category_1 = df_ytd_revpar_achived[df_ytd_revpar_achived.mtd_occ >= 0.8]
            # 1.2. df_ytd_revpar_achived & mtd_occ < 0.8
            df_category_2 = df_ytd_revpar_achived[df_ytd_revpar_achived.mtd_occ < 0.8]
        # 2. df_ytd_revpar_not_achived
        df_ytd_revpar_not_achived = hotel_batch_df[hotel_batch_df.ytd_revpar_achived < 0]
        if not df_ytd_revpar_not_achived.empty:
            # 2.1. df_ytd_revpar_not_achived & mtd_occ >= 0.8
            df_category_3 = df_ytd_revpar_not_achived[df_ytd_revpar_not_achived.mtd_occ >= 0.8]
            df_ytd_revpar_not_achived_occ = df_ytd_revpar_not_achived[df_ytd_revpar_not_achived.mtd_occ < 0.8]
            if not df_ytd_revpar_not_achived_occ.empty:
                # 2.2. df_ytd_revpar_not_achived & mtd_occ < 0.8 & mtd_ota_arr >= 0.7
                df_category_4 = df_ytd_revpar_not_achived_occ[df_ytd_revpar_not_achived_occ.mtd_ota_arr >= 0.7]
                # 2.3. df_ytd_revpar_not_achived & mtd_occ < 0.8 & mtd_ota_arr < 0.7
                df_category_5 = df_ytd_revpar_not_achived_occ[df_ytd_revpar_not_achived_occ.mtd_ota_arr < 0.7]
        return df_category_1, df_category_2, df_category_3, df_category_4, df_category_5


class LiqEgmStrategy(LiqStrategy):
    def __init__(self):
        super(LiqEgmStrategy, self).__init__()

    def get_floor_price(self, batch):
        if batch == EGM_LIQUIDATION_BATCH_29_1 or batch == EGM_LIQUIDATION_BATCH_29_2:
            return 29
        elif batch == EGM_LIQUIDATION_BATCH_39:
            return 39
        elif batch == EGM_LIQUIDATION_BATCH_49:
            return 49
        raise Exception('unknown batch: {}'.format(batch))

    def get_rate(self, batch, arr):
        if batch == EGM_LIQUIDATION_BATCH_49 or batch == EGM_LIQUIDATION_BATCH_39:
            rate = 0.55 if arr >= 100 else 0.6
        elif batch == EGM_LIQUIDATION_BATCH_29_1 or batch == EGM_LIQUIDATION_BATCH_29_2:
            rate = 0.5
        else:
            raise Exception('unknown batch: {}'.format(batch))
        return rate
