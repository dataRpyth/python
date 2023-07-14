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
from common.job_base.job_base import JobBase
from common.job_common.job_source import JobSource
from common.priceop.special_sale_to_ump import LmsToUmp
from common.priceop.special_sale_to_pc import SpecialSale
from Liquidation.liquidation_price_rule_search import *
from strategy.common import *
from strategy.liquidation import LiqStrategy

from Liquidation.configuration import LIQUIDATION_BATCH_29_2


def _get_hotel_lms_target_map(df):
    hotel_lms_target_map = {row['oyo_id']: row['total_lms_target'] for index, row in df.iterrows()}
    return hotel_lms_target_map


def _try_sell_lms_from_target_map(hotel_lms_target_map, oyo_id, lms_target):
    target_left = hotel_lms_target_map.get(oyo_id, None)
    if target_left is None or target_left == 0:
        return 0
    lms_result = min(target_left, lms_target)
    target_left -= lms_result
    hotel_lms_target_map[oyo_id] = target_left
    return lms_result


def _calc_lms_for_room_types(hotel_lms_target_map, hotel_room_type_rem_df):
    hotel_map = {}
    for index, row in hotel_room_type_rem_df.iterrows():
        oyo_id = row['oyo_id']
        room_type_id = row['room_type_id']
        room_type_target = row['lms_room_type_target']
        lms_result = _try_sell_lms_from_target_map(hotel_lms_target_map, oyo_id, room_type_target)
        if lms_result <= 0:
            continue
        room_type_map = hotel_map.get(oyo_id, None)
        if room_type_map is None:
            room_type_map = {}
            hotel_map[oyo_id] = room_type_map
        room_type_map[room_type_id] = lms_result
    return hotel_map


def _compose_data_frame_from_room_type_lms_map(room_type_lms):
    oyo_id_lst = list()
    room_type_lst = list()
    lms_room_number_lst = list()
    for oyo_id, lms_room_type_map in room_type_lms.items():
        for room_type_id, lms_room_number in lms_room_type_map.items():
            oyo_id_lst.append(oyo_id)
            room_type_lst.append(room_type_id)
            lms_room_number_lst.append(lms_room_number)
    return pd.DataFrame({'oyo_id': oyo_id_lst,
                         'room_type_id': room_type_lst,
                         'lms_room_number': lms_room_number_lst
                         })


class LiquidationJob(JobBase):

    def __init__(self, job_config):
        super(LiquidationJob, self).__init__(job_config)
        self.job_source = JobSource(self.get_adb_query_manager(), self.get_mysql_query_manager(),
                                    self.get_oracle_query_manager())

    def get_job_name(self):
        return 'LiquidationV1'

    def get_algorithm_model(self):
        return 'LmsAlgChinaRebirthV1'

    def run(self):
        logger = LogUtil.get_cur_logger()
        config = self.get_job_config()

        batch_order = config.get_batch_order()

        job_begin = time.time()

        start_stamp = job_begin
        start_date = DateUtil.stamp_to_date_format0(start_stamp)

        if '2020-01-31' <= start_date <= '2020-03-08':
            logger.info('*********************returning because 2019 SARS********************')
            return

        logger.info('*********************{} run begin********************'.format(self.get_job_name()))

        # Step1 ############ hotels from center ##############
        set_oyo_id = self.job_source.get_hotel_batch_oyo_set_from_center(batch_orders_str=batch_order,
                                                                         start_time_stamp=start_stamp)
        # 筛除hotel_tagged
        if config.is_tagged():
            set_oyo_id = set_oyo_id - self.job_source.get_hotel_tagged_oyo_id('pricing-China2.0-high-star')
            logger.info("after excluded hotel-tagged, hotel-size: {0}".format(len(set_oyo_id)))

        # Step2 ############ hotels filter ##############
        # Filter1 ############ hotels without white list ##############
        set_oyo_id = self.get_hotels_whitelist_excluded(set_oyo_id, start_date, config.get_liq_batch())
        # Filter2 ############ Predicted occ #############
        df_predicted_low_occ = self.get_predicted_low_occ(set_oyo_id, start_stamp, config.get_pred_occ())

        # Step3 ############ hotels room-type-liquidation ##############
        df_room_type_liq = self.get_df_for_room_type_liquidation(df_predicted_low_occ, start_stamp)

        # Step3.1 ############ hotels for pc special sale ##############
        df_for_special_sale = self.get_df_for_special_sale(config, df_room_type_liq)
        self.set_for_pc_special_sale(config, df_for_special_sale.copy())

        # Step3.2 ############ hotels for ump api ##############
        self.liquidation_to_ump(config, df_for_special_sale.copy())

        logger.info('*******************job run end, %.2fs elapsed *********************', time.time() - job_begin)

    def get_hotels_whitelist_excluded(self, set_oyo_id, start_date, batch):
        set_oyo_for_white_list = self.get_oyo_id_set_for_white_list(set_oyo_id, start_date, batch)
        set_liq_white_list_excluded = set_oyo_id.difference(set_oyo_for_white_list)
        LogUtil.get_cur_logger().info("filtered_white_list, size: %d", len(set_liq_white_list_excluded))
        return set_liq_white_list_excluded

    def add_to_white_list(self, white_list_oyo_id_set, set_oyo_id_base, tuple_start_with):
        existed_oyo_set = {i for s in tuple_start_with for i in set_oyo_id_base if i.startswith(s)}
        return white_list_oyo_id_set.union(existed_oyo_set)

    def liquidation_to_ump(self, config, liquidation_to_ump_df):
        DFUtil.print_data_frame(liquidation_to_ump_df, "liquidation_to_ump_df_before", True)
        if EnvUtil.is_local_env(config.get_config_env()) or EnvUtil.is_dev_env(config.get_config_env()):
            hotel_excel = pd.read_excel(cur_path + '/doc/hotel_dev.xlsx')
        elif EnvUtil.is_test_env(config.get_config_env()):
            hotel_excel = pd.read_excel(cur_path + '/doc/hotel_test.xlsx')
        elif EnvUtil.is_uat_env(config.get_config_env()):
            hotel_excel = pd.read_excel(cur_path + '/doc/hotel_uat.xlsx')
        if not EnvUtil.is_prod_env(config.get_config_env()):
            liquidation_to_ump_df = liquidation_to_ump_df.drop_duplicates(['oyo_id', 'room_type_id'], keep="last").drop(
                ['hotel_id', 'hotel_name', 'room_type_id'], axis=1)
            liquidation_to_ump_df = pd.concat([liquidation_to_ump_df, hotel_excel], axis=1, sort=True)
        liquidation_to_ump_df = liquidation_to_ump_df[pd.notna(liquidation_to_ump_df.oyo_id)]
        liquidation_to_ump_df["oyo_subsidy"] = 0
        liquidation_to_ump_df["owner_subsidy"] = DEFAULT_NULL_VALUE
        DFUtil.print_data_frame(liquidation_to_ump_df,
                                "liquidation_to_ump_df_after_【%s】" % config.get_config_env(), True)
        LmsToUmp.special_sale_to_ump_and_mail_send(config, liquidation_to_ump_df)

    def limit_liq_inventory_for_high_avg_occ_hotels(self, df_room_type_srn_with_brn, df_hotel_srn, job_begin):
        # 获取酒店名单
        oyo_id_set = set(df_room_type_srn_with_brn.oyo_id)
        # 计算T-7 ~ T-1的OCC
        start_time = DateUtil.stamp_to_date_format0(job_begin)
        urn_for_prev_seven_days = MiscUtil.wrap_read_adb_df(self.job_source.get_df_for_urn_with_pre_7d, oyo_id_set,
                                                            start_time)
        df_avg_occ = pd.merge(urn_for_prev_seven_days, df_hotel_srn, how='left', on='oyo_id')
        df_avg_occ['past_occ'] = df_avg_occ.urn / df_avg_occ.hotel_srn
        df_avg_occ = df_avg_occ.groupby(by=['oyo_id'], as_index=False).past_occ.agg({'mean_occ': np.mean})
        # 如果过去7天平均OCC >= 0.8，则对甩卖库存做特殊限制。限制规则为：min((1-mean_occ) * srn, 7)
        df_avg_occ_high = df_avg_occ[df_avg_occ.mean_occ >= 0.8]
        df_avg_occ_high_with_srn = pd.merge(df_avg_occ_high, df_hotel_srn, how='left', on='oyo_id')
        df_avg_occ_high_with_srn['override_total_lms_target'] = (
                                                                        1 - df_avg_occ_high_with_srn.mean_occ) * df_avg_occ_high_with_srn.hotel_srn

        def get_override_target(original_target):
            return 0 if original_target <= 0 else min(ceil(original_target), 7)

        df_avg_occ_high_with_srn['override_total_lms_target'] = df_avg_occ_high_with_srn.override_total_lms_target.map(
            lambda target: get_override_target(target))
        df_avg_occ_high_with_srn.drop(columns=['hotel_srn'], inplace=True)
        df_avg_occ_high_with_srn = df_avg_occ_high_with_srn[df_avg_occ_high_with_srn.override_total_lms_target > 0]
        df_room_type_srn_with_brn = pd.merge(df_room_type_srn_with_brn, df_avg_occ_high_with_srn, how='left',
                                             on='oyo_id')
        df_room_type_srn_with_brn["override_total_lms_target"] = pd.to_numeric(df_room_type_srn_with_brn["override_total_lms_target"])
        df_room_type_srn_with_brn.total_lms_target = np.where(
            np.isnan(df_room_type_srn_with_brn.override_total_lms_target),
            df_room_type_srn_with_brn.total_lms_target,
            df_room_type_srn_with_brn.override_total_lms_target)
        return df_room_type_srn_with_brn

    def get_df_for_room_type_liquidation(self, df_low_predicted_occ, start_stamp):
        hotel_set = set(df_low_predicted_occ.oyo_id)
        df_room_type_srn = MiscUtil.wrap_read_adb_df(self.job_source.get_df_for_room_type_srn, hotel_set)
        df_room_type_brn = MiscUtil.wrap_read_adb_df(self.job_source.get_df_for_room_type_brn, hotel_set, start_stamp)
        df_hotel_brn = df_room_type_brn.groupby(by='oyo_id', as_index=False).sum().rename(columns={'brn': 'hotel_brn'})[
            ['oyo_id', 'hotel_brn']]
        df_hotel_srn = df_room_type_srn.groupby(by='oyo_id', as_index=False).sum().rename(columns={'srn': 'hotel_srn'})[
            ['oyo_id', 'hotel_srn']]
        df_hotel_srn_with_brn = pd.merge(df_hotel_brn, df_hotel_srn, how='left', on=['oyo_id'])
        df_hotel_srn_with_brn['hotel_occ'] = df_hotel_srn_with_brn.hotel_brn / df_hotel_srn_with_brn.hotel_srn

        df_room_type_srn_with_brn = pd.merge(df_room_type_srn, df_room_type_brn, how='left',
                                             on=['oyo_id', 'room_type_id'])
        df_room_type_srn_with_brn = pd.merge(df_room_type_srn_with_brn,
                                             df_low_predicted_occ, how='left', on='oyo_id', suffixes=['', '_y'])
        df_room_type_srn_with_brn = df_room_type_srn_with_brn[pd.notna(df_room_type_srn_with_brn.pred_occ)]
        df_room_type_srn_with_brn['brn'] = df_room_type_srn_with_brn.brn.fillna(0)
        df_room_type_srn_with_brn['occ'] = df_room_type_srn_with_brn.brn / df_room_type_srn_with_brn.srn
        df_room_type_srn_with_brn = pd.merge(df_room_type_srn_with_brn, df_hotel_srn_with_brn, on='oyo_id', how='left')
        df_room_type_srn_with_brn['total_lms_target'] = round(
            df_room_type_srn_with_brn.hotel_srn * (1 - df_room_type_srn_with_brn.pred_occ), 0)
        df_room_type_srn_with_brn = self.limit_liq_inventory_for_high_avg_occ_hotels(df_room_type_srn_with_brn,
                                                                                     df_hotel_srn, start_stamp)
        srn_with_total_lms_target = df_room_type_srn_with_brn[['oyo_id', 'total_lms_target']]
        hotel_lms_target_map = _get_hotel_lms_target_map(srn_with_total_lms_target)
        # step 2, calculate rem for each room type
        df_room_type_srn_with_brn['rem'] = df_room_type_srn_with_brn.srn - df_room_type_srn_with_brn.brn

        srn_with_brn_df = df_room_type_srn_with_brn

        srn_with_brn_df['lms_room_type_target'] = srn_with_brn_df.rem

        df_room_type_diff = self.job_source.get_room_type_diff_df(hotel_set)

        df_room_type_diff = self.job_source.get_available_room_type_df(df_room_type_diff, hotel_set)

        df_room_type_diff = df_room_type_diff[
            ["oyo_id", "room_type_id", "difference_type", "price_delta", "price_multiplier"]]
        srn_brn_with_room_type_diff = pd.merge(srn_with_brn_df, df_room_type_diff, how='left',
                                               on=['oyo_id', 'room_type_id'])
        DFUtil.print_data_frame(srn_brn_with_room_type_diff[srn_brn_with_room_type_diff.difference_type.isnull()]
                                , "srn_brn_with_room_type_diff_non_difference_type", True)
        srn_brn_with_room_type_diff = srn_brn_with_room_type_diff[~srn_brn_with_room_type_diff.difference_type.isnull()]

        srn_brn_with_room_type_diff = srn_brn_with_room_type_diff.sort_values(axis=0, by=['oyo_id', 'price_delta',
                                                                                          'price_multiplier'],
                                                                              ascending=True)

        srn_brn_with_room_type_diff['price_delta'] = srn_brn_with_room_type_diff.price_delta.fillna(0)

        srn_brn_with_room_type_diff['price_multiplier'] = srn_brn_with_room_type_diff.price_multiplier.fillna(1)

        lms_room_type_map = _calc_lms_for_room_types(hotel_lms_target_map, srn_brn_with_room_type_diff[
            ['oyo_id', 'room_type_id', 'lms_room_type_target']])

        lms_room_type_df = _compose_data_frame_from_room_type_lms_map(lms_room_type_map)

        room_type_name_df = self.job_source.get_room_type_name_df()

        srn_brn_with_room_type_diff = pd.merge(srn_brn_with_room_type_diff, room_type_name_df, how='left',
                                               on=['room_type_id'])
        lms_room_type_df = pd.merge(lms_room_type_df, srn_brn_with_room_type_diff, how='left',
                                    on=['oyo_id', 'room_type_id'])
        lms_room_type_df = lms_room_type_df[pd.notna(lms_room_type_df.room_type_id)]
        return lms_room_type_df[
            ['oyo_id', 'hotel_id', 'hotel_name', 'room_type_id', 'room_type_name', 'hotel_brn', 'hotel_srn',
             'hotel_occ', 'brn', 'srn', 'occ', 'total_count', 'rem', 'lms_room_number', "price_delta",
             "price_multiplier"]]

    def get_df_for_special_sale(self, config, df_for_pc_liq):
        if df_for_pc_liq.empty:
            LogUtil.get_cur_logger().warn('df_for_pc_liquidation is empty, would not post to pricing center!')
            return
        df_for_hotel_daily_prepare = self.job_source.get_df_for_hotel_daily_prepare(
            config.get_hotel_daily_prepare_table(), set(df_for_pc_liq.oyo_id))
        df_for_pc_liq = pd.merge(df_for_pc_liq, df_for_hotel_daily_prepare, how='left', on=['oyo_id'])
        df_for_pc_liq = df_for_pc_liq[df_for_pc_liq.base.apply(lambda x: MiscUtil.is_not_empty_value(x))]
        df_for_pc_liq["pricing_date"] = DateUtil.stamp_to_date_format1(config.get_job_preset_time())
        df_for_pc_liq["act_type"] = 1
        batch = config.get_liq_batch()
        df_for_pc_liq["strategy_type"] = self.get_job_config().get_liq_batch_strategy_dict().get(batch)
        # calculate pms_price
        df_for_pc_liq = self.calc_pms_price(batch, df_for_pc_liq)

        def add_rule_id_for_sale_price(sale_price, rule_id_search_map):
            return rule_id_search_map.get(config.get_config_env()).get(sale_price)

        df_for_pc_liq = DFUtil.apply_func_for_df(df_for_pc_liq, "rule_id", ["sale_price"],
                                                 lambda values: add_rule_id_for_sale_price(*values,
                                                                                           LIQ_PRICE_RULE_SEARCH_MAP))

        def get_period_time(_config, _batch, _today_time):
            env = _config.get_config_env()
            _begin_period_time = _today_time + LIQ_START_END_TIME_MAP.get(env).get(_batch)[0].get(
                "start_hour_minute")
            _end_period_time = _today_time + LIQ_START_END_TIME_MAP.get(env).get(_batch)[0].get(
                "end_hour_minute")
            return _begin_period_time, _end_period_time

        today_time = int(time.mktime(datetime.date.today().timetuple()))
        begin_period_time, end_period_time = get_period_time(config, batch, today_time)
        df_for_pc_liq["begin_period"] = DateUtil.stamp_to_date_format3(begin_period_time)
        df_for_pc_liq["end_period"] = DateUtil.stamp_to_date_format3(end_period_time)
        df_for_pc_liq["sale_start_time"] = df_for_pc_liq["begin_period"]
        df_for_pc_liq["sale_end_time"] = df_for_pc_liq["end_period"]
        df_for_pc_liq["room_type_id"] = df_for_pc_liq["room_type_id"].map(lambda x: str(x))
        df_for_pc_liq = self.get_df_for_hotel_city(df_for_pc_liq)
        df_for_pc_liq = df_for_pc_liq[
            ["city_cnname", "oyo_id", 'hotel_id', "hotel_name", "room_type_id", "room_type_name", 'base', 'arr',
             'price_delta', "price_multiplier", 'hotel_brn', 'hotel_srn', 'hotel_occ', "total_count", "srn", "brn",
             "occ", "rem", "lms_room_number", "sale_start_time", "sale_end_time", "sale_price", "rule_id",
             "strategy_type", "act_type", "begin_period", "end_period", "pricing_date"]]
        df_for_pc_liq.sort_values(axis=0, by=["city_cnname", "oyo_id", 'room_type_id'], ascending=True)
        DFUtil.print_data_frame(df_for_pc_liq, 'df_for_pc_special_sale', True)
        return df_for_pc_liq

    def calc_pms_price(self, batch, df_for_pc_liq):
        df_for_pc_liq = LiqStrategy().calc_sale_price_for_liquidation_rate(df_for_pc_liq, batch)
        return df_for_pc_liq

    def set_for_pc_special_sale(self, config, df_for_pc_liq):
        if df_for_pc_liq.empty:
            LogUtil.get_cur_logger().warn('data is empty, would not post special sale to pricing center!')
            return
        df_for_pc_liq["ump_price"] = df_for_pc_liq["sale_price"]
        df_for_pc_liq.sale_price = df_for_pc_liq.sale_price.map(lambda x: max(x, 35))
        df_for_pc_liq = df_for_pc_liq[
            ["city_cnname", "oyo_id", 'hotel_id', "hotel_name", "room_type_id", "room_type_name", 'base', 'arr',
             'price_delta', "price_multiplier", 'hotel_brn', 'hotel_srn', 'hotel_occ', "total_count", "srn", "brn",
             "occ", "rem", "sale_price", "lms_room_number", "sale_start_time", "sale_end_time", "ump_price", "rule_id",
             "strategy_type", "act_type", "begin_period", "end_period", "pricing_date"]]
        SpecialSale.set_hotel_special_sale(config, df_for_pc_liq.copy())
        df_for_pc_liq.rename(inplace=True, columns={'hotel_id': '酒店ID', 'oyo_id': 'CRS_ID',
                                                    'city_cnname': '城市', 'hotel_name': '酒店名称'})
        config.get_mail_send().send_mail_for_liquidation_to_pc(config, df_for_pc_liq)

    def get_df_for_hotel_city(self, df_for_pc_liquidation):
        hotel_city_df = MiscUtil.wrap_read_adb_df(self.job_source.get_city_cnname_df, set(df_for_pc_liquidation.oyo_id))
        df_for_pc_liquidation = pd.merge(df_for_pc_liquidation, hotel_city_df, how='left', on='oyo_id')
        return df_for_pc_liquidation

    def get_oyo_id_set_for_white_list(self, set_liquidation_base, start_date, batch):
        oyo_id_tuple_list_str = MiscUtil.convert_set_to_tuple_list_str(set_liquidation_base)
        # 加入业主或poc要求不能甩的酒店
        batch_dict = self.get_job_config().get_liq_batch_strategy_dict()
        white_list_oyo_id_set = self.job_source.get_white_or_black_list_set(start_date, batch_dict.get(batch))
        # 开始控价不足8天，加入甩房白名单
        new_pricing_start_hotels_set = self.job_source.get_new_pricing_hotels_set(start_date)
        white_list_oyo_id_set = white_list_oyo_id_set.union(new_pricing_start_hotels_set)
        # 城市级过滤
        tuple_start_with = self.get_oyo_start_with(batch)
        white_list_oyo_id_set = self.add_to_white_list(white_list_oyo_id_set, set_liquidation_base, tuple_start_with)
        # 如果有更高优先级的floor price override，则该酒店不参与尾房甩卖（加入白名单）
        override_floor_price_hotels_df = self.job_source.get_df_for_override_floor_price_hotels(oyo_id_tuple_list_str,
                                                                                                start_date)
        white_list_oyo_id_set.update(set(override_floor_price_hotels_df.oyo_id))
        white_list_oyo_id_set = self.biz_white_list(set_liquidation_base, white_list_oyo_id_set)
        return white_list_oyo_id_set

    def get_oyo_start_with(self, batch):
        # 丽江，西宁全城酒店加入白名单
        tuple_start_with = ["CN_LIJ", "CN_XNG"]
        if batch != LIQUIDATION_BATCH_29_2:
            # 业务指定除21：00批次的时差酒店加入白名单-20191119
            tuple_start_with.extend(["CN_BAY", "CN_URU"])
        return tuple_start_with

    def biz_white_list(self, set_liquidation_base, white_list_oyo_id_set):
        return white_list_oyo_id_set

    def get_predicted_low_occ(self, set_oyo_id, start_stamp, target_occ):
        LogUtil.get_cur_logger().info("start get_predicted_low_occ ...")
        start_time = dt.datetime.fromtimestamp(start_stamp)
        df_night_order_pct = MiscUtil.wrap_read_adb_df(get_night_order_percent, list(set_oyo_id),
                                                       24 - start_time.hour, self.get_adb_query_manager())
        # use actual occ after 6pm
        if start_time.hour >= 18:
            df_night_order_pct['night_amplifier'] = 0
        else:
            df_night_order_pct['night_amplifier'] = df_night_order_pct['night_pct'] / (
                    1 - df_night_order_pct['night_pct'])

        df_hotel_occ = self.job_source.get_df_for_hotel_occ(set_oyo_id, start_stamp)
        df_temp = df_hotel_occ.merge(df_night_order_pct, on=['oyo_id'], how='left')

        # default night_pct and amplifier to 0, if no night data is found
        df_temp['night_pct'] = df_temp['night_pct'].replace([np.nan], 0)
        df_temp['night_amplifier'] = df_temp['night_amplifier'].replace([np.nan], 0)
        df_temp['night_amplifier'] = df_temp['night_amplifier'].replace([np.inf], 100)
        df_temp['pred_occ'] = df_temp.apply(lambda row: max(row['occ'], 0.02) * (1 + row['night_amplifier']), axis=1)
        df_temp = df_temp[df_temp.pred_occ <= target_occ]
        set_predicted_occ = set(df_temp.oyo_id).intersection(set_oyo_id)
        LogUtil.get_cur_logger().info("filtered_predicted_occ, size: %d", len(set_predicted_occ))
        df_low_predicted_occ = df_temp[df_temp.oyo_id.isin(set_oyo_id)]
        return df_low_predicted_occ
