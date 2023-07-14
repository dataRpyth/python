#!/usr/bin/env python
# -*- coding:utf-8 -*-

import os
import sys
import warnings
from os.path import join as join_path

cur_path = os.path.dirname(os.path.realpath(__file__))
sys.path.append(join_path(cur_path, '..'))
warnings.filterwarnings("ignore")

from common.util.utils import *
from strategy.liquidation import LiqEgmStrategy
from egm_liquidation.configuration import EGM_LIQUIDATION_BATCH_29_2
from strategy.common import *
from Liquidation.liq_job_runner import LiquidationJob


class EgmLiqJob(LiquidationJob):

    def __init__(self, job_config):
        super(EgmLiqJob, self).__init__(job_config)

    def get_job_name(self):
        return "EgmLiquidation"

    def run(self):
        logger = LogUtil.get_cur_logger()
        config = self.get_job_config()
        job_name = self.get_job_name()
        pred_occ = config.get_pred_occ()
        job_begin = time.time()

        start_stamp = job_begin
        start_date = DateUtil.stamp_to_date_format0(start_stamp)
        logger.info('*********************{} run begin********************'.format(job_name))

        # Step1 ############ hotels from center ##############
        set_oyo_id = self.job_source.get_hotel_batch_oyo_set_from_center(start_time_stamp=start_stamp, model_id=6)

        if config.get_do_exclude() == 1:
            set_exclude = set(config.get_exclude_oyo_ids())
            set_oyo_id = set_oyo_id.difference(set_exclude)
            logger.info("after do_exclude, hotel-size: %d, set_exclude: %s", len(set_oyo_id), set_exclude)

        if config.get_do_special_batch() == 1:
            set_oyo_id = set(config.get_special_batch_oyo_ids())
            logger.info("do_special_batch: %s, after this, hotel-size: %d", set_oyo_id, len(set_oyo_id))

        # Step2 ############ hotels filter ##############
        # Filter1 ############ hotels without white list ##############
        set_oyo_id = self.get_hotels_whitelist_excluded(set_oyo_id, start_date, config.get_liq_batch())
        # Filter2 ############ Predicted occ #############
        df_predicted_low_occ = self.get_predicted_low_occ(set_oyo_id, start_stamp, pred_occ)

        # Step3 ############ hotels room-type-liquidation ##############
        df_room_type_liq = self.get_df_for_room_type_liquidation(df_predicted_low_occ, start_stamp)

        # Step3.1 ############ hotels for pc special sale ##############
        df_for_special_sale = self.get_df_for_special_sale(config, df_room_type_liq)
        self.set_for_pc_special_sale(config, df_for_special_sale.copy())

        # Step3.2 ############ hotels for ump api ##############
        self.liquidation_to_ump(config, df_for_special_sale.copy())

        logger.info('*******************job run end, %.2f elapsed *********************', time.time() - job_begin)

    def calc_pms_price(self, batch, df_for_pc_liq):
        df_for_pc_liq = LiqEgmStrategy().calc_sale_price_for_liquidation_rate(df_for_pc_liq, batch)
        return df_for_pc_liq

    def get_oyo_start_with(self, batch):
        # 丽江，西宁全城酒店加入白名单
        tuple_start_with = ["CN_LIJ", "CN_XNG"]
        if batch != EGM_LIQUIDATION_BATCH_29_2:
            # 业务指定除21：00批次的时差酒店加入白名单-20191119
            tuple_start_with.extend(["CN_BAY", "CN_URU"])
        return tuple_start_with

    def my_white_list(self, set_liquidation_base, white_list_oyo_id_set):
        # 排除过去7天平均，晚9pm后发生的订单占全天订单的20%以上
        # do not exclude this at this time
        night_order_pct_exclude_set, df_night_order_pct = self.get_night_order_pct_exclude_set_egm(set_liquidation_base)
        LogUtil.get_cur_logger().info("night_order_pct_exclude_set:{0}".format(night_order_pct_exclude_set))
        white_list_oyo_id_set = white_list_oyo_id_set.union(night_order_pct_exclude_set)
        return white_list_oyo_id_set

    # 排除过去7天平均，晚9pm后发生的订单占全天订单的20%以上
    def get_night_order_pct_exclude_set_egm(self, oyo_id_set):
        night_hour = 21
        # todo: get_night_order_percent_wo_mm or get_night_order_percent_for_liquidation ？
        df_night_order_pct = get_night_order_percent_wo_mm(oyo_id_set, 24 - night_hour, self.get_adb_query_manager())

        night_order_pct_exclude_set = set()
        for index, row in df_night_order_pct.iterrows():
            oyo_id = row['oyo_id']
            night_pct = row['night_pct']
            if night_pct > 0.2:
                night_order_pct_exclude_set.add(oyo_id)

        return night_order_pct_exclude_set, df_night_order_pct
