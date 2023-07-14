#!/usr/bin/env python
# -*- coding:utf-8 -*-

import os
import sys
from os.path import join as join_path

cur_path = os.path.dirname(os.path.realpath(__file__))
sys.path.append(join_path(cur_path, '..'))

from common.util.utils import *
from common.sendmail.mail_sender import MailAttachment
from common.sendmail.mail_sender import MailSender as ms
from common.dingtalk_sdk.dingtalk_py_cmd import DingTalkPy

_JOIN_ON_COLUMN_OYO_ID_ROOM_TYPE_ID = ['oyo_id', 'room_type_id']

_QUHUHU_PRICE_RAISE_RATIO = 1.05

OTA_CHANNEL_NAME_CTRIP = 'ctrip'
OTA_CHANNEL_NAME_MEITUAN = 'meituan'
OTA_CHANNEL_NAME_FLIGGY = 'fliggy'
OTA_CHANNEL_NAME_ELONG = 'elong'
OTA_CHANNEL_NAME_QUNAR = 'qunar'
OTA_CHANNEL_NAME_QUHUHU = 'quhuhu'
OTA_CHANNEL_NAME_HUAMIN = 'huamin'

OTA_CHANNEL_CN_NAME_CTRIP = '携程'
OTA_CHANNEL_CN_NAME_MEITUAN = '美团'
OTA_CHANNEL_CN_NAME_FLIGGY = '飞猪'
OTA_CHANNEL_CN_NAME_ELONG = '艺龙'
OTA_CHANNEL_CN_NAME_QUNAR = '去哪儿'
OTA_CHANNEL_CN_NAME_QUHUHU = '去呼呼'
OTA_CHANNEL_CN_NAME_HUAMIN = '华闽'

OTA_CHANNEL_ID_CTRIP = 1
OTA_CHANNEL_ID_MEITUAN = 2
OTA_CHANNEL_ID_FLIGGY = 3
OTA_CHANNEL_ID_ELONG = 4
OTA_CHANNEL_ID_QUNAR = 5
OTA_CHANNEL_ID_QUHUHU = 6
OTA_CHANNEL_ID_HUAMIN = 7

SEVEN_CHANNELS_CN_NAME_MAP = {
    OTA_CHANNEL_NAME_CTRIP: OTA_CHANNEL_CN_NAME_CTRIP,
    OTA_CHANNEL_NAME_MEITUAN: OTA_CHANNEL_CN_NAME_MEITUAN,
    OTA_CHANNEL_NAME_FLIGGY: OTA_CHANNEL_CN_NAME_FLIGGY,
    OTA_CHANNEL_NAME_ELONG: OTA_CHANNEL_CN_NAME_ELONG,
    OTA_CHANNEL_NAME_QUNAR: OTA_CHANNEL_CN_NAME_QUNAR,
    OTA_CHANNEL_NAME_QUHUHU: OTA_CHANNEL_CN_NAME_QUHUHU,
    OTA_CHANNEL_NAME_HUAMIN: OTA_CHANNEL_CN_NAME_HUAMIN
}

SEVEN_CHANNELS_MAP = {
    OTA_CHANNEL_NAME_CTRIP: OTA_CHANNEL_ID_CTRIP,
    OTA_CHANNEL_NAME_MEITUAN: OTA_CHANNEL_ID_MEITUAN,
    OTA_CHANNEL_NAME_FLIGGY: OTA_CHANNEL_ID_FLIGGY,
    OTA_CHANNEL_NAME_ELONG: OTA_CHANNEL_ID_ELONG,
    OTA_CHANNEL_NAME_QUNAR: OTA_CHANNEL_ID_QUNAR,
    OTA_CHANNEL_NAME_QUHUHU: OTA_CHANNEL_ID_QUHUHU,
    OTA_CHANNEL_NAME_HUAMIN: OTA_CHANNEL_ID_HUAMIN
}

FIVE_CHANNELS_MAP = {
    OTA_CHANNEL_NAME_CTRIP: OTA_CHANNEL_ID_CTRIP,
    OTA_CHANNEL_NAME_MEITUAN: OTA_CHANNEL_ID_MEITUAN,
    OTA_CHANNEL_NAME_FLIGGY: OTA_CHANNEL_ID_FLIGGY,
    OTA_CHANNEL_NAME_ELONG: OTA_CHANNEL_ID_ELONG,
    OTA_CHANNEL_NAME_QUNAR: OTA_CHANNEL_ID_QUNAR
}


class PricingPipeline:

    @staticmethod
    def has_ota(ota_channel_id, ota_channel_map):
        if ota_channel_id in ota_channel_map.values():
            return True
        return False

    @staticmethod
    def compose_real_table_name(table_prefix, table_name):
        if table_prefix is None:
            return table_name
        return table_prefix + '_' + table_name

    @staticmethod
    def gen_ota_plugin_data_process_slice(prices_df, ota_channel_map, ota_room_type_merged_df, ota_net_price_commission_merged_df,
                                          ota_top_rating_discount_activity_merged_df, promo_merged_dfs):
        prices_df = pd.merge(prices_df, ota_room_type_merged_df, how='left',
                             on=_JOIN_ON_COLUMN_OYO_ID_ROOM_TYPE_ID)

        prices_df = pd.merge(prices_df, ota_net_price_commission_merged_df, how='left',
                             on=_JOIN_ON_COLUMN_OYO_ID_ROOM_TYPE_ID)

        prices_df = pd.merge(prices_df, ota_top_rating_discount_activity_merged_df, how='left',
                             on=_JOIN_ON_COLUMN_OYO_ID_ROOM_TYPE_ID)

        prices_df = pd.merge(prices_df, promo_merged_dfs, how='left', on=_JOIN_ON_COLUMN_OYO_ID_ROOM_TYPE_ID)

        PricingPipelineUtil.calc_prices_for_df(prices_df, ota_channel_map)

        if prices_df.empty:
            return prices_df

        if PricingPipeline.has_ota(OTA_CHANNEL_ID_CTRIP, ota_channel_map):
            # 携程预付底价
            prices_df['ctrip_pre_net_price'] = prices_df.ctrip_net_price
            # 携程预付卖价
            prices_df['ctrip_pre_sell_price'] = prices_df.ctrip_price_after_ota_top_rating
            # 携程现付卖价
            prices_df['ctrip_post_sell_price'] = prices_df.ctrip_pre_sell_price.map(
                lambda price: PriceUtil.calc_post_price_from_pre_price(price))

            PricingPipelineUtil.calc_promo_for_df(prices_df, OTA_CHANNEL_NAME_CTRIP,
                                                  ['ctrip_pre_net_price', 'ctrip_pre_sell_price',
                                                   'ctrip_post_sell_price'])

            PriceUtil.round_digits_for_df_columns(prices_df, ['ctrip_pre_net_price'], 2)

            PriceUtil.commission_ratio_to_percentile_for_df_columns(prices_df, ['ctrip_pre_commission',
                                                                                'ctrip_post_commission'],
                                                                    1)

        if PricingPipeline.has_ota(OTA_CHANNEL_ID_MEITUAN, ota_channel_map):
            # 美团预付底价
            prices_df['meituan_pre_net_price'] = prices_df.meituan_net_price
            # 美团预付卖价
            prices_df['meituan_pre_sell_price'] = prices_df.meituan_price_after_ota_top_rating
            # 美团现付卖价
            prices_df['meituan_post_sell_price'] = prices_df.meituan_pre_sell_price.map(
                lambda price: PriceUtil.calc_post_price_from_pre_price(price))

            PricingPipelineUtil.calc_promo_for_df(prices_df, OTA_CHANNEL_NAME_MEITUAN,
                                                  ['meituan_pre_net_price', 'meituan_pre_sell_price',
                                                   'meituan_post_sell_price'])

            PriceUtil.commission_ratio_to_percentile_for_df_columns(prices_df, ['meituan_pre_commission',
                                                                                'meituan_post_commission'],
                                                                    1)

        if PricingPipeline.has_ota(OTA_CHANNEL_ID_FLIGGY, ota_channel_map):
            # 飞猪预付卖价
            prices_df['fliggy_pre_sell_price'] = prices_df.fliggy_price_after_ota_top_rating

            PricingPipelineUtil.calc_promo_for_df(prices_df, OTA_CHANNEL_NAME_FLIGGY,
                                                  ['fliggy_pre_sell_price'])

            PriceUtil.commission_ratio_to_percentile_for_df_columns(prices_df, ['fliggy_pre_commission',
                                                                                'fliggy_post_commission'],
                                                                    1)

        if PricingPipeline.has_ota(OTA_CHANNEL_ID_ELONG, ota_channel_map):
            # 艺龙预付底价
            prices_df['elong_pre_net_price'] = prices_df.elong_net_price
            # 艺龙预付卖价
            prices_df['elong_pre_sell_price'] = prices_df.elong_price_after_ota_top_rating
            # 艺龙现付卖价
            prices_df['elong_post_sell_price'] = prices_df.elong_pre_sell_price.map(
                lambda price: PriceUtil.calc_post_price_from_pre_price(price))

            PricingPipelineUtil.calc_promo_for_df(prices_df, OTA_CHANNEL_NAME_ELONG,
                                                  ['elong_pre_net_price', 'elong_pre_sell_price',
                                                   'elong_post_sell_price'])

            PriceUtil.round_digits_for_df_columns(prices_df, ['elong_pre_net_price'], 2)

            PriceUtil.commission_ratio_to_percentile_for_df_columns(prices_df, ['elong_pre_commission',
                                                                                'elong_post_commission'],
                                                                    1)

        if PricingPipeline.has_ota(OTA_CHANNEL_ID_QUNAR, ota_channel_map):
            # 去哪儿预付卖价
            prices_df['qunar_pre_sell_price'] = prices_df.qunar_price_after_ota_top_rating
            # 去哪儿现付卖价
            prices_df['qunar_post_sell_price'] = prices_df.qunar_pre_sell_price.map(
                lambda price: PriceUtil.calc_post_price_from_pre_price(price))

            PricingPipelineUtil.calc_promo_for_df(prices_df, OTA_CHANNEL_NAME_QUNAR,
                                                  ['qunar_pre_sell_price', 'qunar_post_sell_price'])

            PriceUtil.commission_ratio_to_percentile_for_df_columns(prices_df, ['qunar_pre_commission',
                                                                                'qunar_post_commission'],
                                                                    1)

        PricingPipelineUtil.wrap_ota_columns_to_null_for_df(prices_df, ota_channel_map)

        # TODO(yry) sort
        return prices_df

    @staticmethod
    def pipe_join_ota_for_pms_prices(prices_df, mysql_query_manager, ota_channel_map, pool, table_prefix=None,
                                     all_hotels_str=None):

        ota_room_type_merged_df = PricingPipelineUtil.calc_merged_ota_commission_df(
            PricingPipeline.compose_real_table_name(table_prefix, 'hotel_ota_room_type'), ota_channel_map,
            mysql_query_manager, _JOIN_ON_COLUMN_OYO_ID_ROOM_TYPE_ID, all_hotels_str)

        ota_net_price_commission_merged_df = PricingPipelineUtil.calc_merged_ota_net_commission_df(
            PricingPipeline.compose_real_table_name(table_prefix, 'ota_net_price_commission'), ota_channel_map,
            mysql_query_manager, _JOIN_ON_COLUMN_OYO_ID_ROOM_TYPE_ID, all_hotels_str)

        ota_top_rating_discount_activity_merged_df = PricingPipelineUtil.calc_merged_ota_top_rating_discount_activity_df(
            PricingPipeline.compose_real_table_name(table_prefix, 'ota_top_rating_discount_activity'), ota_channel_map,
            mysql_query_manager, _JOIN_ON_COLUMN_OYO_ID_ROOM_TYPE_ID, all_hotels_str)

        promo_merged_dfs = PricingPipelineUtil.calc_merged_ota_promo_df(
            PricingPipeline.compose_real_table_name(table_prefix, 'ota_promo'), ota_channel_map, mysql_query_manager,
            _JOIN_ON_COLUMN_OYO_ID_ROOM_TYPE_ID, all_hotels_str)

        set_oyo_id = set(prices_df.oyo_id)
        hotel_size = len(set_oyo_id)
        cpu_count = multiprocessing.cpu_count()
        group_size = MiscUtil.split_into_groups(len(set_oyo_id), cpu_count)
        LogUtil.get_cur_logger().info('pipe_join_ota_for_pms_prices, hotel_size: {}, cpu_count: {}, group_size: {}'.format(hotel_size, cpu_count, group_size))
        oyo_id_group_lst = MiscUtil.group_by_list(list(set_oyo_id), group_size)
        prices_df_slice_lst = list()
        ota_channel_map_lst = list()
        ota_room_type_merged_df_lst = list()
        ota_net_price_commission_merged_df_lst = list()
        ota_top_rating_discount_activity_merged_df_lst = list()
        promo_merged_dfs_lst = list()
        for oyo_id_group in oyo_id_group_lst:
            prices_df_slice_lst.append(prices_df[prices_df.oyo_id.isin(oyo_id_group)])
            ota_channel_map_lst.append(ota_channel_map)
            ota_room_type_merged_df_lst.append(ota_room_type_merged_df[ota_room_type_merged_df.oyo_id.isin(oyo_id_group)])
            ota_net_price_commission_merged_df_lst.append(ota_net_price_commission_merged_df[ota_net_price_commission_merged_df.oyo_id.isin(oyo_id_group)])
            ota_top_rating_discount_activity_merged_df_lst.append(ota_top_rating_discount_activity_merged_df[ota_top_rating_discount_activity_merged_df.oyo_id.isin(oyo_id_group)])
            promo_merged_dfs_lst.append(promo_merged_dfs[promo_merged_dfs.oyo_id.isin(oyo_id_group)])
        prices_df_lst = pool.map(PricingPipeline.gen_ota_plugin_data_process_slice, prices_df_slice_lst, ota_channel_map_lst,
                                 ota_room_type_merged_df_lst, ota_net_price_commission_merged_df_lst,
                                 ota_top_rating_discount_activity_merged_df_lst, promo_merged_dfs_lst)
        prices_df = pd.concat(prices_df_lst, ignore_index=True)
        # TODO(yry) sort
        return prices_df

    @staticmethod
    def wrap_to_ota_plugin_percent_for_data_frame(df, price_column_name):
        df[price_column_name] = df[price_column_name].map(
            lambda value: MiscUtil.wrap_to_ota_plugin_percent_values(value))

    @staticmethod
    def wrap_to_ota_plugin_percent_for_data_frame_columns(df, price_column_name_list):
        for price_column_name in price_column_name_list:
            PricingPipeline.wrap_to_ota_plugin_percent_for_data_frame(df, price_column_name)

    @staticmethod
    def compose_ota_plugin_v1_data_from_pms_prices(prices_df):

        prices_df['ota_plugin_date'] = prices_df['date'].map(
            lambda date: pd.to_datetime(str(date)).strftime('%Y/%-m/%-d'))

        ota_pre_prices_df = prices_df[
            ['ota_plugin_date', 'oyo_id', 'room_type_id', 'room_type_name', 'pms_price', 'hourly_price', 'hotel_name',
             'meituan_pre_commission', 'ctrip_pre_commission', 'elong_pre_commission', 'fliggy_pre_commission',
             'qunar_pre_commission']]

        ota_pre_prices_df.rename(columns={'ota_plugin_date': 'date'}, inplace=True)

        ota_post_prices_df = prices_df[
            ['ota_plugin_date', 'oyo_id', 'room_type_id', 'room_type_name', 'pms_price', 'hourly_price', 'hotel_name',
             'meituan_post_commission', 'ctrip_post_commission', 'elong_post_commission',
             'qunar_post_commission']]

        ota_post_prices_df.rename(columns={'ota_plugin_date': 'date'}, inplace=True)

        MiscUtil.set_columns_to_value(ota_pre_prices_df,
                                      ['ctrip_post_commission', 'meituan_post_commission', 'fliggy_post_commission',
                                       'elong_post_commission', 'qunar_post_commission'], DEFAULT_NULL_VALUE)

        MiscUtil.set_columns_to_value(ota_post_prices_df,
                                      ['ctrip_pre_commission', 'meituan_pre_commission', 'fliggy_pre_commission',
                                       'elong_pre_commission', 'qunar_pre_commission', 'fliggy_post_commission'],
                                      DEFAULT_NULL_VALUE)
        # 重排顺序
        ota_pre_prices_df = ota_pre_prices_df[
            ['date', 'oyo_id', 'room_type_id', 'room_type_name', 'pms_price', 'hourly_price', 'hotel_name',
             'meituan_pre_commission', 'meituan_post_commission', 'ctrip_pre_commission', 'ctrip_post_commission',
             'elong_pre_commission', 'elong_post_commission', 'fliggy_pre_commission', 'fliggy_post_commission',
             'qunar_pre_commission', 'qunar_post_commission']]

        # 重排顺序
        ota_post_prices_df = ota_post_prices_df[
            ['date', 'oyo_id', 'room_type_id', 'room_type_name', 'pms_price', 'hourly_price', 'hotel_name',
             'meituan_pre_commission', 'meituan_post_commission', 'ctrip_pre_commission', 'ctrip_post_commission',
             'elong_pre_commission', 'elong_post_commission', 'fliggy_pre_commission', 'fliggy_post_commission',
             'qunar_pre_commission', 'qunar_post_commission']]

        ota_post_prices_df['pms_price'] = ota_post_prices_df['pms_price'].map(
            lambda price: round(price * POST_PRE_PRICING_RATIO), 0)

        PricingPipeline.wrap_to_ota_plugin_percent_for_data_frame_columns(ota_pre_prices_df,
                                                                          ['meituan_pre_commission',
                                                                           'meituan_post_commission',
                                                                           'ctrip_pre_commission',
                                                                           'ctrip_post_commission',
                                                                           'elong_pre_commission',
                                                                           'elong_post_commission',
                                                                           'fliggy_pre_commission',
                                                                           'fliggy_post_commission',
                                                                           'qunar_pre_commission',
                                                                           'qunar_post_commission'])

        PricingPipeline.wrap_to_ota_plugin_percent_for_data_frame_columns(ota_post_prices_df,
                                                                          ['meituan_pre_commission',
                                                                           'meituan_post_commission',
                                                                           'ctrip_pre_commission',
                                                                           'ctrip_post_commission',
                                                                           'elong_pre_commission',
                                                                           'elong_post_commission',
                                                                           'fliggy_pre_commission',
                                                                           'fliggy_post_commission',
                                                                           'qunar_pre_commission',
                                                                           'qunar_post_commission'])

        ota_pre_prices_df = ota_pre_prices_df.rename(columns={
            'room_type_id': 'room_type',
            'room_type_name': 'room_category',
            'pms_price': 'final_price',
            'meituan_pre_commission': 'PRE_MEITUAN_COMMISSION%',
            'meituan_post_commission': 'POST_MEITUAN_COMMISSION%',
            'ctrip_pre_commission': 'PRE_CTRIP_COMMISSION%',
            'ctrip_post_commission': 'POST_CTRIP_COMMISSION%',
            'elong_pre_commission': 'PRE_YILONG_COMMISSION%',
            'elong_post_commission': 'POST_YILONG_COMMISSION%',
            'fliggy_pre_commission': 'PRE_FEIZHU_COMMISSION%',
            'fliggy_post_commission': 'POST_FEIZHU_COMMISSION%',
            'qunar_pre_commission': 'PRE_QUNAER_COMMISSION%',
            'qunar_post_commission': 'POST_QUNAER_COMMISSION%'
        })

        ota_post_prices_df = ota_post_prices_df.rename(columns={
            'room_type_id': 'room_type',
            'room_type_name': 'room_category',
            'pms_price': 'final_price',
            'meituan_pre_commission': 'PRE_MEITUAN_COMMISSION%',
            'meituan_post_commission': 'POST_MEITUAN_COMMISSION%',
            'ctrip_pre_commission': 'PRE_CTRIP_COMMISSION%',
            'ctrip_post_commission': 'POST_CTRIP_COMMISSION%',
            'elong_pre_commission': 'PRE_YILONG_COMMISSION%',
            'elong_post_commission': 'POST_YILONG_COMMISSION%',
            'fliggy_pre_commission': 'PRE_FEIZHU_COMMISSION%',
            'fliggy_post_commission': 'POST_FEIZHU_COMMISSION%',
            'qunar_pre_commission': 'PRE_QUNAER_COMMISSION%',
            'qunar_post_commission': 'POST_QUNAER_COMMISSION%'
        })

        return ota_pre_prices_df, ota_post_prices_df

    @staticmethod
    def _filter_rows(pre_commission, post_commission):
        return MiscUtil.is_not_empty_value(pre_commission) or MiscUtil.is_not_empty_value(post_commission)

    @staticmethod
    def compose_ota_plugin_v2_data_from_pms_prices(prices_df, ebk_ota_room_type_mapping_df, pool):

        if prices_df.empty:
            return prices_df

        prices_df_copy = prices_df[
            ['date', 'oyo_id', 'hotel_name', 'room_type_id', 'room_type_name', 'hourly_price',
             'ctrip_pre_sell_price', 'ctrip_post_sell_price', 'ctrip_pre_commission', 'ctrip_post_commission',
             'meituan_pre_sell_price', 'meituan_post_sell_price', 'meituan_pre_commission', 'meituan_post_commission',
             'fliggy_pre_sell_price', 'fliggy_pre_commission', 'fliggy_post_commission',
             'elong_pre_sell_price', 'elong_post_sell_price', 'elong_pre_commission', 'elong_post_commission',
             'qunar_pre_sell_price', 'qunar_post_sell_price', 'qunar_pre_commission', 'qunar_post_commission'
             ]]

        prices_df_copy['pricing_date'] = prices_df_copy['date'].map(
            lambda date: pd.to_datetime(str(date)).strftime('%Y-%m-%d'))
        PricingPipeline.wrap_to_ota_plugin_percent_for_data_frame_columns(prices_df_copy,
                                                                          ['meituan_pre_commission',
                                                                           'meituan_post_commission',
                                                                           'ctrip_pre_commission',
                                                                           'ctrip_post_commission',
                                                                           'elong_pre_commission',
                                                                           'elong_post_commission',
                                                                           'fliggy_pre_commission',
                                                                           'fliggy_post_commission',
                                                                           'qunar_pre_commission',
                                                                           'qunar_post_commission'])
        ota_ctrip_prices_df = prices_df_copy
        ota_meituan_prices_df = prices_df_copy.copy()
        ota_fliggy_prices_df = prices_df_copy.copy()
        ota_elong_prices_df = prices_df_copy.copy()
        ota_qunar_prices_df = prices_df_copy.copy()

        # ---------ctrip
        ota_ctrip_prices_df['ota_channel_id'] = OTA_CHANNEL_ID_CTRIP
        ota_ctrip_prices_df['pre_commission'] = ota_ctrip_prices_df.apply(
            lambda row: DFUtil.get_item_from_row(row, 'ctrip_pre_commission'), axis=1)
        ota_ctrip_prices_df['post_commission'] = ota_ctrip_prices_df.apply(
            lambda row: DFUtil.get_item_from_row(row, 'ctrip_post_commission'), axis=1)
        ota_ctrip_prices_df['pre_sell_price'] = ota_ctrip_prices_df.apply(
            lambda row: DFUtil.get_item_from_row(row, 'ctrip_pre_sell_price'), axis=1)
        ota_ctrip_prices_df['post_sell_price'] = ota_ctrip_prices_df.apply(
            lambda row: DFUtil.get_item_from_row(row, 'ctrip_post_sell_price'), axis=1)
        ota_ctrip_prices_df['pre_breakfast_price'] = DEFAULT_NULL_VALUE
        ota_ctrip_prices_df['post_breakfast_price'] = DEFAULT_NULL_VALUE

        ota_ctrip_prices_df_filtered = ota_ctrip_prices_df[
            ota_ctrip_prices_df[['pre_commission', 'post_commission']].apply(
                lambda values: PricingPipeline._filter_rows(*values), axis=1)]

        # ---------meituan
        ota_meituan_prices_df['ota_channel_id'] = OTA_CHANNEL_ID_MEITUAN
        ota_meituan_prices_df['pre_commission'] = ota_meituan_prices_df.apply(
            lambda row: DFUtil.get_item_from_row(row, 'meituan_pre_commission'), axis=1)
        ota_meituan_prices_df['post_commission'] = ota_meituan_prices_df.apply(
            lambda row: DFUtil.get_item_from_row(row, 'meituan_post_commission'), axis=1)
        ota_meituan_prices_df['pre_sell_price'] = ota_meituan_prices_df.apply(
            lambda row: DFUtil.get_item_from_row(row, 'meituan_pre_sell_price'), axis=1)
        ota_meituan_prices_df['post_sell_price'] = ota_meituan_prices_df.apply(
            lambda row: DFUtil.get_item_from_row(row, 'meituan_post_sell_price'), axis=1)
        ota_meituan_prices_df['pre_breakfast_price'] = DEFAULT_NULL_VALUE
        ota_meituan_prices_df['post_breakfast_price'] = DEFAULT_NULL_VALUE

        ota_meituan_prices_df_filtered = ota_meituan_prices_df[
            ota_meituan_prices_df[['pre_commission', 'post_commission']].apply(
                lambda values: PricingPipeline._filter_rows(*values), axis=1)]

        # ---------fliggy
        ota_fliggy_prices_df['ota_channel_id'] = OTA_CHANNEL_ID_FLIGGY
        ota_fliggy_prices_df['pre_commission'] = ota_fliggy_prices_df.apply(
            lambda row: DFUtil.get_item_from_row(row, 'fliggy_pre_commission'), axis=1)
        ota_fliggy_prices_df['post_commission'] = ota_fliggy_prices_df.apply(
            lambda row: DFUtil.get_item_from_row(row, 'fliggy_post_commission'), axis=1)
        ota_fliggy_prices_df['pre_sell_price'] = ota_fliggy_prices_df.apply(
            lambda row: DFUtil.get_item_from_row(row, 'fliggy_pre_sell_price'), axis=1)
        ota_fliggy_prices_df['post_sell_price'] = ota_fliggy_prices_df.apply(
            lambda row: DFUtil.get_item_from_row(row, 'fliggy_post_sell_price'), axis=1)
        ota_fliggy_prices_df['pre_breakfast_price'] = DEFAULT_NULL_VALUE
        ota_fliggy_prices_df['post_breakfast_price'] = DEFAULT_NULL_VALUE

        ota_fliggy_prices_df_filtered = ota_fliggy_prices_df[
            ota_fliggy_prices_df[['pre_commission', 'post_commission']].apply(
                lambda values: PricingPipeline._filter_rows(*values), axis=1)]

        # ---------elong
        ota_elong_prices_df['ota_channel_id'] = OTA_CHANNEL_ID_ELONG
        ota_elong_prices_df['pre_commission'] = ota_elong_prices_df.apply(
            lambda row: DFUtil.get_item_from_row(row, 'elong_pre_commission'), axis=1)
        ota_elong_prices_df['post_commission'] = ota_elong_prices_df.apply(
            lambda row: DFUtil.get_item_from_row(row, 'elong_post_commission'), axis=1)
        ota_elong_prices_df['pre_sell_price'] = ota_elong_prices_df.apply(
            lambda row: DFUtil.get_item_from_row(row, 'elong_pre_sell_price'), axis=1)
        ota_elong_prices_df['post_sell_price'] = ota_elong_prices_df.apply(
            lambda row: DFUtil.get_item_from_row(row, 'elong_post_sell_price'), axis=1)
        ota_elong_prices_df['pre_breakfast_price'] = DEFAULT_NULL_VALUE
        ota_elong_prices_df['post_breakfast_price'] = DEFAULT_NULL_VALUE

        ota_elong_prices_df_filtered = ota_elong_prices_df[
            ota_elong_prices_df[['pre_commission', 'post_commission']].apply(
                lambda values: PricingPipeline._filter_rows(*values), axis=1)]

        # ---------qunar
        ota_qunar_prices_df['ota_channel_id'] = OTA_CHANNEL_ID_QUNAR
        ota_qunar_prices_df['pre_commission'] = ota_qunar_prices_df.apply(
            lambda row: DFUtil.get_item_from_row(row, 'qunar_pre_commission'), axis=1)
        ota_qunar_prices_df['post_commission'] = ota_qunar_prices_df.apply(
            lambda row: DFUtil.get_item_from_row(row, 'qunar_post_commission'), axis=1)
        ota_qunar_prices_df['pre_sell_price'] = ota_qunar_prices_df.apply(
            lambda row: DFUtil.get_item_from_row(row, 'qunar_pre_sell_price'), axis=1)
        ota_qunar_prices_df['post_sell_price'] = ota_qunar_prices_df.apply(
            lambda row: DFUtil.get_item_from_row(row, 'qunar_post_sell_price'), axis=1)
        ota_qunar_prices_df['pre_breakfast_price'] = DEFAULT_NULL_VALUE
        ota_qunar_prices_df['post_breakfast_price'] = DEFAULT_NULL_VALUE

        ota_qunar_prices_df_filtered = ota_qunar_prices_df[
            ota_qunar_prices_df[['pre_commission', 'post_commission']].apply(
                lambda values: PricingPipeline._filter_rows(*values), axis=1)]

        ota_prices_df_lst = [ota_ctrip_prices_df_filtered,
                             ota_meituan_prices_df_filtered,
                             ota_fliggy_prices_df_filtered,
                             ota_elong_prices_df_filtered,
                             ota_qunar_prices_df_filtered]

        ota_prices_df = pd.concat(ota_prices_df_lst, ignore_index=True)

        ota_prices_df = ota_prices_df[
            ['pricing_date', 'oyo_id', 'hotel_name', 'room_type_id', 'room_type_name', 'ota_channel_id',
             'pre_sell_price', 'post_sell_price', 'hourly_price', 'pre_commission', 'post_commission',
             'pre_breakfast_price', 'post_breakfast_price']]

        ota_prices_df = ota_prices_df.merge(ebk_ota_room_type_mapping_df, on=['oyo_id', 'room_type_id', 'ota_channel_id'])

        ota_prices_df.drop(columns=['ota_room_type_name'], inplace=True)

        return ota_prices_df

    @staticmethod
    def send_ota_plugin_v1_dfs_by_mail(pre_prices_df, post_prices_df, local_file_folder_path, file_date_str, is_preset,
                                       mail_user, mail_password, mail_receivers, batch_order, business_mode_name):
        pre_price_local_file_name = (
            '{0}_ota_plugin_pre_prices_{1}.xls' if not is_preset else '{0}_ota_plugin_test_preset_pre_prices_{1}.xls').format(
            business_mode_name, file_date_str)

        post_price_local_file_name = (
            '{0}_ota_plugin_post_prices_{1}.xls' if not is_preset else '{0}_ota_plugin_test_preset_post_prices_{1}.xls').format(
            business_mode_name, file_date_str)

        ota_pre_prices_local_file_path = join_path(local_file_folder_path, pre_price_local_file_name)

        pre_prices_df.to_excel(ota_pre_prices_local_file_path)

        ota_post_prices_local_file_path = join_path(local_file_folder_path, post_price_local_file_name)

        post_prices_df.to_excel(ota_post_prices_local_file_path)

        ota_pre_prices_attachment_name = (
            '{0}_ota_plugin_pre_prices_{1}.xls' if not is_preset else '{0}_ota_plugin_test_preset_pre_prices_{1}.xls').format(
            business_mode_name, file_date_str)

        ota_post_prices_attachment_name = (
            '{0}_ota_plugin_post_prices_{1}.xls' if not is_preset else '{0}_ota_plugin_test_preset_post_prices_{1}.xls').format(
            business_mode_name, file_date_str)

        ma1 = MailAttachment(ota_pre_prices_local_file_path, ota_pre_prices_attachment_name)

        ma2 = MailAttachment(ota_post_prices_local_file_path, ota_post_prices_attachment_name)

        ota_ma_list = [ma1, ma2]

        mail_sub = '({0}_OTA_PLUGIN) OTA plugin results for batch {1}'.format(
            business_mode_name,
            batch_order) if not is_preset else '({0}_OTA_PLUGIN) OTA plugin preset results'.format(
            business_mode_name)

        mail_content = ' Dear all, this is the results for hotels({0}_OTA_PLUGIN) for batch: {1}'.format(
            business_mode_name,
            batch_order) if not is_preset else '({0}_OTA_PLUGIN) Dear all, this is the preset results for {0} hotels'.format(
            business_mode_name)

        ms.send_mail_with_attachment_list(mail_user, mail_password, mail_receivers, mail_sub,
                                          mail_content.format(batch_order), ota_ma_list)

    @staticmethod
    def send_ota_plugin_v2_dfs_by_mail_and_ddt_robot(ota_prices_df, local_file_folder_path, file_date_str, mail_user,
                                                     mail_password, mail_receivers, batch_order, business_mode_name,
                                                     biz_model_id, ddt_robot_token, ddt_env, preset_time,
                                                     toggle_on_robot_send):
        pre_price_local_file_name = '{0}_ota_plugin_prices_{1}.xlsx'.format(business_mode_name, file_date_str)

        prices_local_file_path = join_path(local_file_folder_path, pre_price_local_file_name)

        ota_prices_df.sort_values(axis=0, by=['oyo_id', 'pricing_date', 'room_type_id'], ascending=True, inplace=True)

        ota_prices_df.to_excel(prices_local_file_path, index=False)

        ma1 = MailAttachment(prices_local_file_path, pre_price_local_file_name)

        ota_ma_list = [ma1]

        mail_sub = '({0}_OTA_PLUGIN) OTA plugin results for batch {1}'.format(business_mode_name, batch_order)

        head = 'Dear all, this is the results for ({0}_OTA_PLUGIN) hotels for batch: {1}'.format(
            business_mode_name, batch_order)
        mail_content = DFUtil.gen_excel_content_by_html(head)

        ms.send_mail_with_attachment_list(mail_user, mail_password, mail_receivers, mail_sub,
                                          mail_content.format(batch_order), ota_ma_list)
        if toggle_on_robot_send:
            DingTalkPy().robot_send(ddt_robot_token, business_mode_name, biz_model_id, preset_time,
                                  prices_local_file_path,
                                  'OTA plugin data from {0}'.format(business_mode_name), ddt_env)

    @staticmethod
    def ota_prices_attach_file_rename(ota_final_prices_df):
        ota_final_prices_df.rename(columns={
            'date': '改价日期',
            'oyo_id': 'OYO ID',
            'hotel_name': '酒店名称',
            'room_type_id': '房型ID',
            'zone_name': '区域',
            'room_type_name': 'PMS房型',
            'pms_price': 'PMS房价',
            'ctrip_room_type_name': '携程房型',
            'ctrip_post_sell_price': '携程现付卖价',
            'ctrip_post_commission': '携程现付佣金%',
            'ctrip_pre_sell_price': '携程预付卖价',
            'ctrip_pre_net_price': '携程预付底价',
            'ctrip_pre_commission': '携程预付佣金%',
            'meituan_room_type_name': '美团房型',
            'meituan_post_sell_price': '美团现付卖价',
            'meituan_pre_sell_price': '美团预付卖价',
            'meituan_pre_net_price': '美团预付底价',
            'fliggy_room_type_name': '飞猪房型',
            'fliggy_pre_sell_price': '飞猪预付卖价',
            'elong_room_type_name': '艺龙房型',
            'elong_post_sell_price': '艺龙现付卖价',
            'elong_pre_sell_price': '艺龙预付卖价',
            'elong_pre_net_price': '艺龙预付底价',
            'elong_pre_commission': '艺龙佣金%',
            'qunar_room_type_name': '去哪儿房型',
            'qunar_post_sell_price': '去哪儿现付卖价',
            'qunar_pre_sell_price': '去哪儿预付卖价',
            'qunar_pre_commission': '去哪儿佣金%',
            'quhuhu_room_type_name': '去呼呼房型',
            'quhuhu_pre_sell_price': '去呼呼预付卖价',
            'huamin_room_type_name': '华闽房型',
            'huamin_pre_net_price': '华闽预付底价'}, inplace=True)
        return ota_final_prices_df
