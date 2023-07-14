import time

import pandas as pd

from common.job_common.job_source import JobSource
from common.priceop.api_base import ApiBase
from common.priceop.marking_price_to_channel import MarkingPrice
from common.priceop.price_log import PriceLog
from common.priceop.price_to_crs import PriceInsert
from common.priceop.price_to_ota import OtaPriceUpload
from common.pricing_pipeline.pipeline import PricingPipeline, FIVE_CHANNELS_MAP, PricingPipelineUtil
from common.util.utils import DateUtil, LogUtil, MiscUtil
from strategy.china_rebirth import get_all_hotel_active_day
from datetime import datetime, timedelta

class JobSinker:

    def __init__(self, job_source: JobSource):
        self.job_source = job_source

    def get_ota_price_diff_df(self, prices_df, new_activated_hotels, ota_code):

        if prices_df.empty:
            return pd.DataFrame(columns=['oyo_id', 'room_type_id', 'date'])

        def is_different(row):
            oyo_id = row.get('oyo_id')
            ota_recent_price = row.get('ota_recent_price')
            ota_pre_sell_price_row = row.get("ota_code") + '_pre_sell_price'
            ota_pre_sell_price = row.get(ota_pre_sell_price_row)

            if MiscUtil.is_empty_value(ota_pre_sell_price):
                return False
            if MiscUtil.is_empty_value(ota_recent_price):
                return True
            return ota_recent_price != ota_pre_sell_price

        prices_df['diff_flag'] = prices_df.apply(lambda row: is_different(row), axis=1)
        prices_df_diff = prices_df[prices_df.diff_flag == True]
        if prices_df_diff.empty:
            return pd.DataFrame(columns=['oyo_id', 'room_type_id', 'date'])
        LogUtil.get_cur_logger().info("ota_prices_diff_%s, before: %d, after: %d, diff-percent: %0.2f", ota_code,
                                      len(prices_df), len(prices_df_diff),
                                      (0 if len(prices_df) == 0 else len(prices_df_diff) / len(prices_df)))
        return prices_df_diff[['oyo_id', 'room_type_id', 'date']]

    def pms_prices_to_crs(self, config, pms_prices, start_date, pricing_end_date, new_activated_hotels):
        try:
            pms_prices = pms_prices[
                ['date', 'hotel_name', 'hotel_id', 'unique_code', 'oyo_id', 'room_type_id', 'room_type_name',
                 'pms_price', 'hourly_price']]
            # add crs_recent_price
            pms_prices = self.get_df_with_crs_recent_price(pms_prices, start_date, pricing_end_date)
            pms_prices["create_time"] = DateUtil.stamp_to_date_format(time.time())
            LogUtil.get_cur_logger().info('prepare data send mail start')
            pms_prices["strategy_type"] = config.get_job().get_job_name()
            pms_mail_prices_df = pms_prices.copy()
            pms_mail_prices_df.drop(['hotel_id', 'crs_recent_price'], axis=1, inplace=True)
            config.get_mail_send().send_mail_for_pms_price(config, pms_mail_prices_df)
            # batch_insert_pms_price_to_crs_and_send_mail
            insert_to_pms_df = self.get_df_for_crs_recent_price_diff(pms_prices, new_activated_hotels, config.disable_crs_price_diff())
            PriceLog.report_price_and_send_mail(config, insert_to_pms_df)
            insert_to_pms_df = insert_to_pms_df.rename(
                columns={'hotel_id': 'id', 'room_type_id': 'room_type', 'pms_price': 'final_price'})
            LogUtil.get_cur_logger().info('start inserting pms prices to crs, size:{}'.format(len(insert_to_pms_df)))
            PriceInsert.batch_insert_pms_price_to_crs_and_send_mail(config, insert_to_pms_df)
            LogUtil.get_cur_logger().info('end inserting pms prices to crs')
        except Exception:
            ApiBase.send_stack_trace("pms_prices_to_crs", config)

    def marking_prices_to_channel(self, config, marking_price_df):
        try:
            if marking_price_df.empty:
                return
            marking_price_df = marking_price_df[['oyo_id', 'room_type_id', 'price', 'hotel_id']]
            LogUtil.get_cur_logger().info('start post marking prices to channel, size:{}'.format(len(marking_price_df)))
            MarkingPrice(config).set_marking_price(marking_price_df)
            LogUtil.get_cur_logger().info('end post marking prices to channel')
        except Exception:
            ApiBase.send_stack_trace("marking_prices_to_channel", config)

    def get_df_with_crs_recent_price(self, prices_for_all_room_type_df, start_date, end_date):
        set_oyo_id = set(prices_for_all_room_type_df.oyo_id)
        df_crs_recent_price = MiscUtil.wrap_read_adb_df(self.job_source.get_df_for_crs_recent_price, set_oyo_id,
                                                        start_date, end_date)
        prices_for_all_room_type_df = pd.merge(prices_for_all_room_type_df, df_crs_recent_price, how='left',
                                               on=['oyo_id', 'room_type_id', 'date'])
        return prices_for_all_room_type_df

    def get_df_for_crs_recent_price_diff(self, prices_df, new_activated_hotels, disable_price_diff):
        if disable_price_diff == True:
            return prices_df

        def is_different(row):
            oyo_id = row['oyo_id']
            pms_price = row['pms_price']
            crs_recent_price = row['crs_recent_price']
            if MiscUtil.is_empty_value(pms_price):
                return False
            if MiscUtil.is_empty_value(crs_recent_price):
                return True
            return pms_price != crs_recent_price

        prices_df_diff = prices_df.copy()
        prices_df_diff['diff_flag'] = prices_df_diff.apply(lambda row: is_different(row), axis=1)
        prices_df_diff = prices_df_diff[prices_df_diff.diff_flag == True]
        LogUtil.get_cur_logger().info("get_df_for_crs_recent_price_diff, before: %d, after: %d, diff-percent: %0.2f",
                                      prices_df.shape[0], prices_df_diff.shape[0],
                                      (0 if prices_df.shape[0] == 0 else prices_df_diff.shape[0] / prices_df.shape[0]))
        prices_df_diff.drop(['crs_recent_price'], axis=1, inplace=True)
        return prices_df_diff

    @staticmethod
    def compose_ebk_ota_room_type_mapping_filter_list(ebk_ota_room_type_mapping_df):
        ebk_ota_room_type_mapping_filter_lst = list()
        for index, row in ebk_ota_room_type_mapping_df.iterrows():
            oyo_id = row['oyo_id']
            room_type_id = row['room_type_id']
            ota_channel_id = row['ota_channel_id']
            ebk_ota_room_type_mapping_filter_lst.append(PricingPipelineUtil.compose_row_id(oyo_id, room_type_id, ota_channel_id))
        return ebk_ota_room_type_mapping_filter_lst

    def pms_prices_to_ota_and_plugin(self, config, prices_for_all_room_type_df, set_oyo_id, start_stamp, date_end,
                                     new_activated_hotels, pool):
        try:
            ota_prices_df = self.get_ota_prices_df(config, start_stamp, date_end, prices_for_all_room_type_df, pool,
                                                   set_oyo_id, new_activated_hotels)

            if ota_prices_df.empty:
                return

            set_oyo_id = set(ota_prices_df.oyo_id)

            ebk_ota_room_type_mapping_df = PricingPipelineUtil.calc_fold_ota_room_type_df(self.job_source.mysql_query_mgr,
                                                                                          list(set_oyo_id))

            ebk_ota_room_type_mapping_filter_lst = JobSinker.compose_ebk_ota_room_type_mapping_filter_list(ebk_ota_room_type_mapping_df)

            PriceLog.report_ota_price_and_send_mail(config, ota_prices_df, ebk_ota_room_type_mapping_filter_lst)

            OtaPriceUpload.ota_price_upload_and_mail_send(config, ota_prices_df, ebk_ota_room_type_mapping_filter_lst)

            self.send_for_ota_plugin(config, start_stamp, ota_prices_df, ebk_ota_room_type_mapping_df, pool)
        except Exception:
            ApiBase.send_stack_trace("pms_prices_to_ota_and_plugin", config)

    def pms_prices_to_ota_plugin(self, config, prices_for_all_room_type_df, set_oyo_id, start_stamp, date_end, new_activated_hotels, pool):
        ota_prices_df = self.get_ota_prices_df(config, start_stamp, date_end, prices_for_all_room_type_df, pool,
                                               set_oyo_id, new_activated_hotels)
        if ota_prices_df.empty:
            return
        set_oyo_id = set(ota_prices_df.oyo_id)

        ebk_ota_room_type_mapping_df = PricingPipelineUtil.calc_fold_ota_room_type_df(self.job_source.mysql_query_mgr,
                                                                                      list(set_oyo_id))

        ebk_ota_room_type_mapping_filter_lst = self.compose_ebk_ota_room_type_mapping_filter_list(
            ebk_ota_room_type_mapping_df)

        PriceLog.report_ota_price_and_send_mail(config, ota_prices_df, ebk_ota_room_type_mapping_filter_lst)

        self.send_for_ota_plugin(config, start_stamp, ota_prices_df, ebk_ota_room_type_mapping_df, pool)

    def get_ota_prices_df(self, config, start_stamp, date_end, prices_for_all_room_type_df, pool, set_oyo_id, new_activated_hotels):
        logger = LogUtil.get_cur_logger()

        date_start = DateUtil.stamp_to_date_format0(start_stamp)

        all_smart_hotels_str = MiscUtil.convert_set_to_tuple_list_str(set_oyo_id)

        logger.info('start pipe join ota for pms prices')

        ota_prices_df = PricingPipeline.pipe_join_ota_for_pms_prices(prices_for_all_room_type_df,
                                                                     self.job_source.mysql_query_mgr,
                                                                     FIVE_CHANNELS_MAP, pool, None,
                                                                     all_smart_hotels_str)


        ota_prices_df["strategy_type"] = config.get_job().get_job_name()

        # 筛除实时价相同的ota_price
        ota_prices_df = self.get_df_for_ota_recent_price_diff(ota_prices_df, date_start, date_end, new_activated_hotels,
                                                              config.disable_ota_price_diff())
        return ota_prices_df

    def send_for_ota_plugin(self, config, start_stamp, ota_prices_df, ebk_ota_room_type_mapping_df, pool):
        logger = LogUtil.get_cur_logger()

        job_start_time_str = DateUtil.stamp_to_date_format2(start_stamp)

        logger.info('start composing ota plugin manual prices')

        ota_plugin_df = PricingPipeline.compose_ota_plugin_v2_data_from_pms_prices(ota_prices_df,
                                                                                   ebk_ota_room_type_mapping_df, pool)


        logger.info('end composing ota plugin manual prices')

        logger.info(
            'start sending ota plugin manual prices with mail, ota_plugin-size: {}'.format(len(ota_plugin_df)))

        config.get_mail_send().send_mail_for_ota_plugin_v2_result(config, ota_plugin_df, job_start_time_str, 1)

    def get_df_for_ota_recent_price_diff(self, prices_df, start_date, end_date, new_activated_hotels, disable_ota_price_diff):
        if disable_ota_price_diff == True:
            return prices_df

        def add_ota_filter(row, ota_code_lst):
            for code in ota_code_lst:
                row_name = code + "_pre_sell_price"
                if MiscUtil.is_not_empty_value(row.get(row_name)):
                    return True
            return False

        prices_df["filter_flag"] = prices_df.apply(lambda x: add_ota_filter(x, ["meituan", "ctrip"]), axis=1)
        prices_df = prices_df[prices_df.filter_flag == True]
        ota_prices_df_meituan, ota_prices_df_ctrip = self.get_df_with_ota_recent_price(prices_df.copy(), start_date,
                                                                                       end_date)
        prices_diff_meituan = self.get_ota_price_diff_df(ota_prices_df_meituan, new_activated_hotels, "meituan")

        prices_diff_ctrip = self.get_ota_price_diff_df(ota_prices_df_ctrip, new_activated_hotels, "ctrip")

        prices_df_diff = pd.merge(prices_diff_meituan, prices_diff_ctrip, how='outer',
                                  on=['oyo_id', 'room_type_id', 'date'])
        prices_df_diff['diff_flag'] = True
        ota_prices_df_diff = prices_df
        if not prices_df_diff.empty:
            prices_df = pd.merge(prices_df, prices_df_diff, how='left', on=['oyo_id', 'room_type_id', 'date'])
            ota_prices_df_diff = prices_df[prices_df.diff_flag == True]
        LogUtil.get_cur_logger().info("get_df_for_ota_recent_price_diff, before: %d, after: %d, diff-percent: %0.2f",
                                      prices_df.shape[0], ota_prices_df_diff.shape[0],
                                      (0 if prices_df.shape[0] == 0 else ota_prices_df_diff.shape[0] / prices_df.shape[
                                          0]))
        return ota_prices_df_diff

    def get_df_with_ota_recent_price(self, ota_prices_df, start_date, end_date):
        oyo_id_list = list(set(ota_prices_df.oyo_id))
        df_ota_recent_price = self.job_source.get_df_for_ota_recent_price(oyo_id_list, start_date, end_date)
        # ota_prices_df_meituan
        ota_prices_df_meituan = ota_prices_df[['oyo_id', 'room_type_id', 'date', 'meituan_pre_sell_price']]
        ota_prices_df_meituan["ota_code"] = "meituan"
        ota_prices_df_meituan = ota_prices_df_meituan[
            ota_prices_df_meituan.meituan_pre_sell_price.apply(lambda x: MiscUtil.is_not_empty_value(x))]
        # ota_prices_df_ctrip
        ota_prices_df_ctrip = ota_prices_df[['oyo_id', 'room_type_id', 'date', 'ctrip_pre_sell_price']]
        ota_prices_df_ctrip["ota_code"] = "ctrip"
        ota_prices_df_ctrip = ota_prices_df_ctrip[
            ota_prices_df_ctrip.ctrip_pre_sell_price.apply(lambda x: MiscUtil.is_not_empty_value(x))]

        if df_ota_recent_price.empty:
            return ota_prices_df_meituan, ota_prices_df_ctrip
        ota_prices_df_meituan = pd.merge(ota_prices_df_meituan, df_ota_recent_price, how='left',
                                         on=['oyo_id', 'room_type_id', 'date', 'ota_code'])
        ota_prices_df_ctrip = pd.merge(ota_prices_df_ctrip, df_ota_recent_price, how='left',
                                       on=['oyo_id', 'room_type_id', 'date', 'ota_code'])
        return ota_prices_df_meituan, ota_prices_df_ctrip
