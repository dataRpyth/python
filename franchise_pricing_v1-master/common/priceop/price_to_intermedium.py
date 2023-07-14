#!/usr/bin/env python
# -*- coding:utf-8 -*-
from common.priceop.api_base import ApiBase
from common.pricing_pipeline.pipeline_pojo import JSONSerializable
from common.sendmail.mail_sender import MailSender
from common.util.utils import *

JSON_HEADER = {'Content-Type': 'application/json; charset=utf-8'}
SOURCE_BIG_DATA = 0
INT_MAX = 2147483647


class IntermediumHandler(ApiBase):

    @staticmethod
    def post_intermedium_for_china_rebirth(config, df_intermedium):
        try:
            LogUtil.get_cur_logger().info("start post intermedium for china rebirth ")
            if df_intermedium.empty:
                LogUtil.get_cur_logger().warn("intermedium for china rebirth is empty")
                return
            df_intermedium.rename(columns={'ebase_price': 'estimated_base'}, inplace=True)
            df_intermedium = df_intermedium[['oyo_id', 'room_type_name', 'room_type_id', 'price_delta',
                                             'price_multiplier', 'difference_type', 'date', 'occ', 'adjust_ratio',
                                             'night_order_pct', 'occ_wo_night_prediction', 'day', 'base', 'occ_diff',
                                             'tau', 'corrected_base', 'override_base', 'estimated_base', 'occ_target', 'price_start_date',
                                             'pricing_days', 'event_factor', 'price', 'pms_price', 'hourly_price',
                                             'floor_price_type', 'floor_price', 'ceiling_price_type',
                                             'ceiling_price', 'floor_override_price', 'ceiling_override_price',
                                             'sale_price', 'strategy_type']]
            df_intermedium = df_intermedium[df_intermedium.room_type_id == 20]
            df_intermedium.rename(columns={
                'sale_price': 'liquidation_sale_price',
                'strategy_type': 'liquidation_strategy_type'
            }, inplace=True)
            LogUtil.get_cur_logger().info("start post prices for intermedium, size: %s", df_intermedium.shape[0])
            DFUtil.print_data_frame(df_intermedium, "df_intermedium", False)
            # 1. 发送邮件
            send_mail_for_final_result(config, df_intermedium)
            if not config.get_toggle_on_price_to_intermedium():
                return
            # 2. 上传中间结果到pc
            post_to_pc_for_intermedium(config, df_intermedium)
        except Exception:
            ApiBase.send_stack_trace("post prices for intermedium", config)


def send_mail_for_final_result(config, df_intermedium):
    df_final_result = df_intermedium[
        ['oyo_id', 'date', 'occ', 'adjust_ratio', 'night_order_pct',
         'occ_wo_night_prediction', 'day', 'base', 'occ_diff', 'tau', 'corrected_base', 'override_base',
         'estimated_base', 'occ_target', 'event_factor', 'floor_price', 'ceiling_price',
         'floor_override_price', 'ceiling_override_price', 'price', 'pms_price',
         'liquidation_sale_price', 'liquidation_strategy_type']]
    config.get_mail_send().send_mail_for_final_result(config, df_final_result)


def post_to_pc_for_intermedium(config, df_intermedium):
    batch_size = config.get_intermedium_batch_size()
    # 1. 准备数据
    pojo_chunk_list, prices_len = df_to_pojo_chunk_list(df_intermedium, batch_size)

    # 2. POST数据
    succeeded_pojo_chunk_list, failed_pojo_chunk_list = price_to_intermedium_post(config, pojo_chunk_list,
                                                                                  prices_len, batch_size)
    # 3. 调API结束，准备邮件逻辑
    send_mail_for_intermedium_monitor(config, succeeded_pojo_chunk_list, failed_pojo_chunk_list)


def send_mail_for_intermedium_monitor(config, succeeded_pojo_chunk_list, failed_pojo_chunk_list):
    logger = LogUtil.get_cur_logger()
    try:
        biz_name = config.get_job().get_job_name()
        robot_token = config.get_robot_send_token()
        begin_time = time.time()
        time_finished = DateUtil.stamp_to_date_format3(begin_time)
        time_file_name = DateUtil.stamp_to_date_format(begin_time, "%Y_%m_%d_%H_%M_%S")
        fail_count = CommonUtil.get_count_for_chunk_list_attr(failed_pojo_chunk_list, "params")
        suc_count = CommonUtil.get_count_for_chunk_list_attr(succeeded_pojo_chunk_list, "params")

        succeeded_report_df = get_report_df_from_pojo_chunk_list(succeeded_pojo_chunk_list)
        failed_report_df = get_report_df_from_pojo_chunk_list(failed_pojo_chunk_list)
        multi_sheets = [DataFrameWithSheetName(succeeded_report_df, '上传成功'),
                        DataFrameWithSheetName(failed_report_df, '上传失败')]

        attach_name = '{0}_price_to_intermedium_report_{1}.xlsx'.format(biz_name, time_file_name)
        sub = "{0} price to intermedium post report {1}".format(biz_name, time_file_name)
        if fail_count > 0:
            DdtUtil.robot_send_ddt_msg(
                '业务线: {0}, 中间价格上传出现异常, 成功:{1}, 失败:{2}, 时间:{3}'.format(biz_name, suc_count, fail_count,
                                                                      time_finished), robot_token)
            failed_report_df.reset_index(drop=True, inplace=True)
        head = "业务线: {0}, 中间价格上传完成, 共:{1}, 成功:{2}, 失败:{3}, 时间:{4}".format(
            biz_name, str(suc_count + fail_count), str(suc_count), str(fail_count), time_finished)
        mail_content = DFUtil.gen_excel_content_by_html(head, failed_report_df)
        MailSender.send_for_multi_sheet(config, config.get_price_log_post_result_receivers(), sub, mail_content,
                                        multi_sheets, attach_name, head)
    except Exception:
        logger.exception("unable to send_mail_for_price_to_intermedium_monitor")


def get_report_df_from_pojo_chunk_list(pojo_chunk_list):
    if len(pojo_chunk_list) == 0:
        return pd.DataFrame()
    row_dict_list = list()
    for pojo_chunk in pojo_chunk_list:
        cr_intermediums = pojo_chunk.params
        for item in cr_intermediums:
            row_dict = {"oyoId": item.oyoId, "date": item.date, "roomTypeId": item.roomTypeId,
                        "roomTypeName": item.roomTypeName,
                        "occ": item.occ, "occDiff": item.occDiff, "adjustRatio": item.adjustRatio,
                        "nightOrderPct": item.nightOrderPct,
                        "occWoNightPrediction": item.occWoNightPrediction, "base": item.base, "day": item.day,
                        "tau": item.tau, "eventFactor": item.eventFactor, "differenceType": item.differenceType,
                        "correctedBase": item.correctedBase, "overrideBase": item.overrideBase, "eBase": item.estimatedBase,
                        "occTarget": item.occTarget, "priceDelta": item.priceDelta, "priceMultiplier": item.priceMultiplier,
                        "floorPrice": item.floorPrice, "floorOverridePrice": item.floorOverridePrice,
                        "floorPriceType": item.floorPriceType, "ceilingPrice": item.ceilingPrice,
                        "ceilingOverridePrice": item.ceilingOverridePrice, "cellingPriceType": item.cellingPriceType,
                        "price": item.price, "pmsPrice": item.pmsPrice, "hourlyPrice": item.hourlyPrice,
                        "liquidationSalePrice": item.liquidationSalePrice,
                        "liquidationStrategyType": item.liquidationStrategyType}
            row_dict_list.append(row_dict)
    report_df = pd.DataFrame(row_dict_list)
    return report_df


def price_to_intermedium_post(config, pojo_chunk_list, prices_len, slices_size):
    logger = LogUtil.get_cur_logger()
    begin = time.time()
    size_failed = 0
    size_suc = 0
    failed_pojo_chunk_list = list()
    succeeded_pojo_chunk_list = list()
    num_post = 0
    url = config.get_price_to_intermedium_url()
    for pojo_chunk in pojo_chunk_list:
        num_post += 1
        __begin0 = time.time()
        post_result = post_json_chunk_with_retry(pojo_chunk, url, 3)
        if post_result[0] == -2 or post_result[0] == '999':
            failed_pojo_chunk_list.append(pojo_chunk)
            size_failed += len(pojo_chunk.params)
            JsonUtil.print_json_file("./price-to-intermedium-errorParamsJson-{0}.json".format(num_post),
                                     pojo_chunk)
        else:
            size_suc += len(pojo_chunk.params)
            succeeded_pojo_chunk_list.append(pojo_chunk)
        logger.info("price-to-intermedium-%s, size: %d, code: %s, cost: %0.2fs", num_post, len(pojo_chunk.params),
                    post_result[0], (time.time() - __begin0))
    logger.info('price-to-intermedium totalSize: %s, batchSize: %s, succeeded: %s, failed: %s, cost: %0.2fs',
                prices_len, slices_size, size_suc, size_failed, (time.time() - begin))
    return succeeded_pojo_chunk_list, failed_pojo_chunk_list


def post_pojo_chunk_to_remote(pojo_chunk, url):
    try:
        result = requests.post(url=url, headers=JSON_HEADER, data=pojo_chunk.to_json().encode("utf-8"), timeout=5)
        json_result = json.loads(result.text, encoding="utf-8")
        code = json_result['code']
        msg = json_result['msg']
        data = json_result['data']
        return code, msg, data
    except Exception as e:
        LogUtil.get_cur_logger().exception(
            "price-to-intermedium Exception, e: %s", str(e))
        return -1, str(e)


def post_json_chunk_with_retry(pojo_chunk, url, retry_times):
    post_result = None
    for retry_count in range(retry_times):
        post_result = post_pojo_chunk_to_remote(pojo_chunk, url)
        if post_result[0] == '000':
            break
        elif post_result[0] == '999':
            time.sleep(10)
            continue
        else:
            LogUtil.get_cur_logger().exception(
                "price-to-intermedium Exception, url: %s, errorInfo: %s, paramsJson: %s", url, post_result,
                pojo_chunk.to_json()[0:500])
            return -2, post_result[1]
    return post_result


def value_trim(row, param, default=None):
    if row.get(param, default) == float("-inf"):
        return -INT_MAX
    if row.get(param, default) == float("inf"):
        return INT_MAX
    return row.get(param, default)


def value_arr_trim(row, param, default=None):
    value = row.get(param, default)
    return value if None else value.split(",")


def df_to_pojo_chunk_list(df_price_to_intermedium, batch_size):
    cr_intermediums = list()
    pojo_chunk_list = list()
    for index, row in df_price_to_intermedium.iterrows():
        cr_intermedium = ChinaRebirthIntermedium(
            value_trim(row, "oyo_id"),
            value_trim(row, "date"),
            value_trim(row, "room_type_id"),
            value_trim(row, "room_type_name"),
            value_trim(row, "occ"),
            value_trim(row, "occ_diff"),
            value_trim(row, "adjust_ratio"),
            value_trim(row, "night_order_pct"),
            value_trim(row, "occ_wo_night_prediction"),
            value_trim(row, "base"),
            value_trim(row, "day"),
            value_trim(row, "tau"),
            value_trim(row, "event_factor"),
            value_trim(row, "difference_type"),
            value_trim(row, "corrected_base"),
            value_trim(row, "override_base"),
            value_trim(row, "estimated_base"),
            value_trim(row, 'occ_target'),
            value_trim(row, "price_delta"),
            value_trim(row, "price_multiplier"),
            value_trim(row, "floor_price"),
            value_trim(row, "floor_override_price"),
            value_trim(row, "floor_price_type"),
            value_trim(row, "ceiling_price"),
            value_trim(row, "ceiling_override_price"),
            value_trim(row, "celling_price_type"),
            value_trim(row, "price"),
            value_trim(row, "pms_price"),
            value_trim(row, "hourly_price"),
            value_trim(row, "liquidation_sale_price"),
            value_trim(row, "liquidation_strategy_type"))
        cr_intermediums.append(cr_intermedium)

    # prices数组切片
    prices_len = len(cr_intermediums)
    slices_len = int(prices_len / batch_size)
    if (prices_len % batch_size) != 0:
        slices_len += 1
    for slice_index in range(slices_len):
        right_max = min((slice_index + 1) * batch_size, prices_len)
        pojo_chunk_list.append(ChinaRebirthIntermediumParams(cr_intermediums[slice_index * batch_size:right_max]))
    return pojo_chunk_list, prices_len


# {
#     "params": [{
#         "oyoId": "str",
#         "date": "str",
#         "roomTypeId": 0,
#         "roomTypeName": "str",
#         "occ": "BigDecimal",
#         "occDiff": "BigDecimal",
#         "adjustRatio": "BigDecimal",
#         "nightOrderPct": "BigDecimal",
#         "occWoNightPrediction": "BigDecimal",
#         "base": "BigDecimal",
#         "day": 0,
#         "tau": "BigDecimal",
#         "eventFactor": "str",
#         "differenceType": 0,
#         "correctedBase": "BigDecimal",
#         "estimatedBase": "BigDecimal",
#         "occTarget": "BigDecimal",
#         "overrideBase": "BigDecimal",
#         "priceDelta": "BigDecimal",
#         "priceMultiplier": "BigDecimal",
#         "floorPrice": "BigDecimal",
#         "floorOverridePrice": "BigDecimal",
#         "floorPriceType": "str",
#         "ceilingPrice": "BigDecimal",
#         "ceilingOverridePrice": "BigDecimal",
#         "cellingPriceType": "str",
#         "price": "BigDecimal",
#         "pmsPrice": "BigDecimal",
#         "hourlyPrice": "BigDecimal",
#         "liquidationSalePrice": "BigDecimal",
#         "liquidationStrategyType": "str"
#     }]
# }

class ChinaRebirthIntermediumParams(JSONSerializable):
    def __init__(self, params):
        self.params = params


class ChinaRebirthIntermedium(JSONSerializable):
    def __init__(self, oto_id, date, room_type_id, room_type_name, occ, occ_diff, adjust_ratio, night_order_pct,
                 occ_wo_night_pred, base, day, tau, event_factor, difference_type, corrected_base, override_base,
                 estimated_base, occ_target, price_delta, price_multiplier, floor_price, floor_override_price, floor_price_type,
                 ceiling_price, ceiling_override_price, celling_price_type, price, pms_price, hourly_price,
                 liquidation_sale_price, liquidation_strategy_type):
        self.oyoId = oto_id  # Long
        self.date = date  # yyyy-MM-dd
        self.roomTypeId = room_type_id
        self.roomTypeName = room_type_name
        self.occ = occ
        self.occDiff = occ_diff
        self.adjustRatio = adjust_ratio
        self.nightOrderPct = night_order_pct
        self.occWoNightPrediction = occ_wo_night_pred
        self.base = base
        self.day = day
        self.tau = tau
        self.eventFactor = event_factor
        self.differenceType = difference_type
        self.correctedBase = corrected_base
        self.estimatedBase = estimated_base
        self.occTarget = occ_target
        self.overrideBase = override_base
        self.priceDelta = price_delta
        self.priceMultiplier = price_multiplier
        self.floorPrice = floor_price
        self.floorOverridePrice = floor_override_price
        self.floorPriceType = floor_price_type
        self.ceilingPrice = ceiling_price
        self.ceilingOverridePrice = ceiling_override_price
        self.cellingPriceType = celling_price_type
        self.price = price
        self.pmsPrice = pms_price
        self.hourlyPrice = hourly_price
        self.liquidationSalePrice = liquidation_sale_price
        self.liquidationStrategyType = liquidation_strategy_type
