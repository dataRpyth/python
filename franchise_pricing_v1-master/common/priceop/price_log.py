import datetime as dt

from common.priceop.api_base import ApiBase
from common.pricing_pipeline.pipeline_pojo import JSONSerializable
from common.util.utils import *
from common.pricing_pipeline.pipeline import OTA_CHANNEL_ID_CTRIP, OTA_CHANNEL_ID_MEITUAN, OTA_CHANNEL_ID_FLIGGY,\
    OTA_CHANNEL_ID_ELONG, OTA_CHANNEL_ID_QUNAR, OTA_CHANNEL_CN_NAME_QUHUHU, OTA_CHANNEL_CN_NAME_HUAMIN

JSON_HEADER = {'Content-Type': 'application/json; charset=utf-8'}


def is_nan(x):
    return isinstance(x, float) and math.isnan(x)


def trans_nan(v):
    return 0.0 if (is_nan(v)) else v


def trans_n(v):
    return 0.0 if MiscUtil.is_empty_value(v) else v


def is_date(x):
    return isinstance(x, dt.date)


def strf_time(v):
    return v.strftime("%Y-%m-%d") if (is_date(v)) else v


def get_preset_days(sub_prefix, config):
    return int(sub_prefix) if sub_prefix == "2" or sub_prefix == "30" \
        else config.get_preset_days()


# 一. insertFranchiseV1InterResult-保存 result_final.xls 接口
def occ_inter_result_report_and_mail_send(config, result_final):
    if not config.get_toggle_on_pricing_log():
        return
    logger = LogUtil.get_cur_logger()
    mail_send = config.get_mail_send()
    _job_name = config.get_job().get_job_name()
    __batch_size = config.get_pricing_log_insert_batch_size()
    _batch_time = config.get_job_preset_time()
    # 1. 结果集准备
    report_data_to_insert = result_final
    # 2. 结果集准备结束，开始调API插入
    begin = time.time()
    columns = ["date", "oyo_id", "price", "clus_occ", "occ", "sellable_rooms",
               "rem", "delta", "base", "clus_arr", "past_occ", "past_arr",
               "occ_ftd_target", "preset_days", "fail_code", "fail_message"]
    failed_series_list = list()
    json_data_list = list()
    preset_days = get_preset_days(_job_name, config)
    for index, row in report_data_to_insert.iterrows():
        d = {"date": strf_time(row['date']),
             "oyoId": row['oyo_id'],
             "price": trans_nan(row['price']),
             "clusOcc": trans_nan(row['clus_occ']),
             "occ": trans_nan(row['occ']),
             "sellableRooms": row['sellable_rooms'],
             "rem": trans_nan(row['rem']),
             "delta": row['delta'],
             "base": trans_nan(row['base']),
             "clusArr": trans_nan(row['clus_arr']),
             "pastOcc": trans_nan(row['past_occ']),
             "pastArr": trans_nan(row['past_arr']),
             "occFtdTarget": trans_nan(row['occ_ftd_target']),
             "strategyType": row['strategy_type'],
             "batchTime": _batch_time,
             "presetDays": preset_days}
        json_data_list.append(d)

    json_data_list = list(filter(None, json_data_list))
    json_data_len = len(json_data_list)
    slices_len = int(json_data_len / __batch_size)
    if (json_data_len % __batch_size) != 0:
        slices_len += 1
    size_suc = 0
    size_failed = 0
    for index in range(slices_len):
        right_max = min((index + 1) * __batch_size, json_data_len)
        data_list = json_data_list[index * __batch_size:right_max]
        code = None
        msg = None
        begin0 = time.time()
        url = config.get_report_insert_franchise_v1_inter_result_url()
        data_req = {"reportFinalResults": data_list}
        try:
            result = requests.post(url=url, headers=JSON_HEADER, json=data_req)
            json_result = json.loads(result.text)
            code = json_result['code']
            msg = json_result['msg']
            if code == '000':
                logger.info("batchInsertFranchiseV1InterResultLog-%s, size: %d, code: %s, cost: %0.2fs",
                            index + 1, len(data_list), code, (time.time() - begin0))
                size_suc += len(data_list)
            else:
                logger.error(
                    "batchInsertFranchiseV1InterResultLog  Exception, url:%s, code:%s, msg:%s, paramsJson:%s",
                    url, code, msg, str(data_req)[0:100])
                size_failed += len(data_list)
                JsonUtil.print_json_str_file("./batchInsertPmsPriceLog-errorParamsJson-{0}.json".format(index + 1),
                                             data_req)
                build_failed_series_list_for_occ_inter_result(failed_series_list, data_list, columns, code, msg)
        except Exception as e:
            JsonUtil.print_json_str_file(
                "./batchInsertFranchiseV1InterResultLog-errorParams-{0}.json".format(index + 1), data_req)
            logger.error("batchInsertFranchiseV1InterResultLog  Exception:%s, url:%s, paramsJson:%s",
                         str(e), url, str(data_req)[0:100])
            size_failed += len(data_list)
            build_failed_series_list_for_occ_inter_result(failed_series_list, data_list, columns, code, msg)
    logger.info(
        'batchInsertFranchiseV1InterResultLog totalSize: %d, batchSize: %d, succeeded: %s, failed: %s, cost: %0.2fs',
        json_data_len, __batch_size, size_suc, size_failed, (time.time() - begin))
    # 3. 调API结束，准备邮件逻辑
    attach_name, failed_df, mail_content = build_mail_content(failed_series_list, columns, json_data_len)
    # 3.1. 开始发送邮件
    mail_send.send_mail_for_report_franchise_v1_inter_result(config, mail_content, failed_df, attach_name)


def build_failed_series_list_for_occ_inter_result(failed_series_list, data_list, columns, code, msg):
    logger = LogUtil.get_cur_logger()
    for e in data_list:
        try:
            failed_series_list.append(
                pd.Series([e.get("date"), e.get("oyoId"), e.get("price"), e.get("clusOcc"),
                           e.get("occ"), e.get("sellableRooms"), e.get("rem"), e.get("delta"),
                           e.get("base"), e.get("clusArr"), e.get("pastOcc"), e.get("pastArr"),
                           e.get("occFtdTarget"), e.get("presetDays"), code, msg], index=columns))
        except Exception as e:
            logger.error("build_failed_series_list1 error,  %s", str(e))


# 二. insertPriceReportAndSendMail-保存 MG_hotel_price.xls 接口
def pms_price_report_and_mail_send(config, df_data):
    if not config.get_toggle_on_pricing_log():
        return
    logger = LogUtil.get_cur_logger()
    mail_send = config.get_mail_send()
    _job_name = config.get_job().get_job_name()
    __batch_size = config.get_pricing_log_insert_batch_size()
    _batch_time = config.get_job_preset_time()
    # 1. 结果集准备
    report_data_to_insert = df_data
    report_data_to_insert = report_data_to_insert.rename(columns={
        'room_type': 'room_type_id',
        'room_category': 'room_type_name',
        'final_price': 'pms_price'
    })
    # 2. 结果集准备结束，开始调API插入
    begin = time.time()
    columns = ["date", "oyo_id", "room_type", "room_category", "final_price", "hourly_price",
               "unique_code", "hotel_name", "zone_name", "preset_days", "fail_code", "fail_message"]
    failed_series_list = list()
    json_data_list = list()
    preset_days = get_preset_days(_job_name, config)
    slices_len = 0
    json_data_len = 0
    for index, row in report_data_to_insert.iterrows():
        d = {"date": strf_time(row['date']),
             "oyoId": row['oyo_id'],
             "roomType": row['room_type_id'],
             "roomCategory": row['room_type_name'],
             "finalPrice": row['pms_price'],
             "hourlyPrice": 0 if MiscUtil.is_empty_value(row['hourly_price']) else row['hourly_price'],
             "uniqueCode": row['unique_code'],
             "hotelName": row['hotel_name'],
             "zoneName": DFUtil.get_item_from_row(row, 'zone_name', ''),
             "strategyType": row['strategy_type'],
             "batchTime": _batch_time,
             "presetDays": preset_days}
        json_data_list.append(d)
        json_data_list = list(filter(None, json_data_list))
        json_data_len = len(json_data_list)
        slices_len = int(json_data_len / __batch_size)
        if (json_data_len % __batch_size) != 0:
            slices_len += 1
    size_suc = 0
    size_failed = 0
    for index in range(slices_len):
        right_max = min((index + 1) * __batch_size, json_data_len)
        data_list = json_data_list[index * __batch_size:right_max]
        code = None
        msg = None
        begin0 = time.time()
        url = config.get_report_insert_price_url()
        data_req = {"reportHotelPrices": data_list}
        try:
            result = requests.post(url=url, headers=JSON_HEADER, json=data_req)
            json_result = json.loads(result.text)
            code = json_result['code']
            msg = json_result['msg']
            if code == '000':
                logger.info("batchInsertPmsPriceLog-%s, size: %d, code: %s, timeCost: %0.2fs", index + 1,
                            len(data_list), code, (time.time() - begin0))
                size_suc += len(data_list)
            else:
                logger.error("batchInsertPmsPriceLog Exception, url:%s, code:%s, msg:%s, paramsJson:%s", url, code, msg,
                             str(data_req)[0:100])
                size_failed += len(data_list)
                JsonUtil.print_json_str_file("./batchInsertPmsPriceLog-errorParamsJson-{0}.json".format(index + 1),
                                             data_req)
                build_failed_series_list_for_pms_price(failed_series_list, data_list, columns, code, msg)
        except Exception as e:
            logger.error("batchInsertPmsPriceLog  Exception: %s, url:%s, paramsJson:%s", str(e), url,
                         str(data_req)[0:100])
            size_failed += len(data_list)
            JsonUtil.print_json_str_file("./batchInsertPmsPriceLog-errorParamsJson-{0}.json".format(index + 1),
                                         data_req)
            build_failed_series_list_for_pms_price(failed_series_list, data_list, columns, code, msg)
    logger.info('batchInsertPmsPriceLog totalSize: %d, batchSize: %d, succeeded: %s, failed: %s, cost: %0.2fs',
                json_data_len, __batch_size, size_suc, size_failed, (time.time() - begin))
    # 3. 调API结束，准备邮件逻辑
    attach_name, failed_df, mail_content = build_mail_content(failed_series_list, columns, json_data_len)
    # 3.1. 开始发送邮件
    mail_send.send_mail_for_report_price_insert(config, mail_content, failed_df, attach_name)


def build_failed_series_list_for_pms_price(failed_series_list, data_list, columns, code, msg):
    logger = LogUtil.get_cur_logger()
    for e in data_list:
        try:
            failed_series_list.append(
                pd.Series([e.get("date"), e.get("oyoId"), e.get("roomType"), e.get("roomCategory"),
                           e.get("finalPrice"), e.get("hourlyPrice"), e.get("uniqueCode"), e.get("hotelName"),
                           e.get("zoneName"), e.get("presetDays"), code, msg], index=columns))
        except Exception as e:
            logger.error("build_failed_series_list2 error,  %s", str(e))


# 三. insertOMV1InterResultAndSendMail-保存 MG_hotel_price.xls 接口
def om_v1_inter_result_report_and_mail_send(config, file_data):
    if not config.get_toggle_on_pricing_log():
        return
    logger = LogUtil.get_cur_logger()
    mail_send = config.get_mail_send()
    _job_name = config.get_job().get_job_name()
    __batch_size = config.get_pricing_log_insert_batch_size()
    _batch_time = config.get_job_preset_time()
    # 1. 结果集准备
    report_data_to_insert = file_data
    # 2. 结果集准备结束，开始调API插入
    begin = time.time()
    columns = ["date", "oyo_id", "month", "weekday_name", "weekday_weekend", "urn", "srn", "occ",
               "scaled_occ", "occ_left_threshold", "occ_right_threshold", "pricing_ratio", "base_price",
               "dynamic_price", "floor_price", "price", "is_special", "preset_days", "fail_code", "fail_message"]
    json_data_list = list()
    failed_series_list = list()
    preset_days = get_preset_days(_job_name, config)
    for index, row in report_data_to_insert.iterrows():
        d = {"date": strf_time(row['date']),
             "oyoId": row['oyo_id'],
             "month": row['month'],
             "weekdayName": row['weekday_name'],
             "weekdayWeekend": row['weekday_weekend'],
             "urn": row['urn'],
             "srn": row['srn'],
             "occ": row['occ'],
             "scaledOcc": row['scaled_occ'],
             "occLeftThreshold": row['occ_left_threshold'],
             "occRightThreshold": row['occ_right_threshold'],
             "pricingRatio": row['pricing_ratio'],
             "basePrice": row['base_price'],
             "dynamicPrice": row['dynamic_price'],
             "floorPrice": row['floor_price'],
             "price": row['price'],
             "isSpecial": row['is_special'],
             "strategyType": row['strategy_type'],
             "batchTime": _batch_time,
             "presetDays": preset_days}
        json_data_list.append(d)
    json_data_list = list(filter(None, json_data_list))
    json_data_len = len(json_data_list)
    slices_len = int(json_data_len / __batch_size)
    if (json_data_len % __batch_size) != 0:
        slices_len += 1
    size_suc = 0
    size_failed = 0
    for index in range(slices_len):
        right_max = min((index + 1) * __batch_size, json_data_len)
        data_list = json_data_list[index * __batch_size:right_max]
        code = None
        msg = None
        begin0 = time.time()
        url = config.get_report_insert_om_v1_inter_result_url()
        data_req = {"reportInterResults": data_list}
        try:
            result = requests.post(url=url, headers=JSON_HEADER, json=data_req)
            json_result = json.loads(result.text)
            code = json_result['code']
            msg = json_result['msg']
            if code == '000':
                logger.info("batchInsertOMV1InterResultLog-%s, size: %d, code: %s, cost: %0.2fs",
                            index + 1, len(data_list), code, (time.time() - begin0))
                size_suc += len(data_list)
            else:
                logger.error("batchInsertOMV1InterResultLog Exception, url:%s, code:%s, msg:%s, paramsJson:%s",
                             url, code, msg, str(data_req)[0:100])
                size_failed += len(data_list)
                JsonUtil.print_json_str_file("./batchInsertOMV1InterResultLog-errorParams-{0}.json".format(index + 1),
                                             data_req)
                build_failed_series_list_for_om_v1_inter_result(failed_series_list, data_list, columns, code, msg)
        except Exception as e:
            size_failed += len(data_list)
            logger.error("batchInsertOMV1InterResultLog  Exception:%s, url:%s, paramsJson:%s", str(e), url,
                         str(data_req)[0:100])
            JsonUtil.print_json_str_file("./batchInsertOMV1InterResultLog-errorParams-{0}.json".format(index + 1),
                                         data_req)
            build_failed_series_list_for_om_v1_inter_result(failed_series_list, data_list, columns, code, msg)
    logger.info('batchInsertOMV1InterResultLog totalSize: %d, batchSize: %d, succeeded: %s, failed: %s, cost: %0.2fs',
                json_data_len, __batch_size, size_suc, size_failed, (time.time() - begin))
    # 3. 调API结束，准备邮件逻辑
    attach_name, failed_df, mail_content = build_mail_content(failed_series_list, columns, json_data_len)
    # 3.1. 开始发送邮件
    mail_send.send_mail_for_report_om_v1_inter_result_insert(config, mail_content, failed_df, attach_name)
    return


def build_failed_series_list_for_om_v1_inter_result(failed_series_list, data_list, columns, code, msg):
    logger = LogUtil.get_cur_logger()
    for e in data_list:
        try:
            failed_series_list.append(
                pd.Series(
                    [e.get("date"), e.get("oyoId"), e.get("month"), e.get("weekdayName"),
                     e.get("weekdayWeekend"),
                     e.get("urn"), e.get("srn"), e.get("occ"), e.get("scaledOcc"), e.get("occLeftThreshold"),
                     e.get("occRightThreshold"), e.get("pricingRatio"), e.get("basePrice"),
                     e.get("dynamicPrice"),
                     e.get("floorPrice"), e.get("price"), e.get("isSpecial"), e.get("presetDays"), code, msg],
                    index=columns))
        except Exception as e:
            logger.error("build_failed_series_list3 error,  %s", str(e))


# 四. ota_price_report_and_mail_send-保存 MG_hotel_price.xls 接口
def ota_price_report_and_mail_send(config, file_data, ebk_ota_room_type_mapping_filter_lst):
    if not config.get_toggle_on_pricing_log():
        return
    logger = LogUtil.get_cur_logger()
    mail_send = config.get_mail_send()
    __batch_size = config.get_pricing_log_insert_batch_size()
    sub_prefix = config.get_job().get_job_name()
    batch_time = config.get_job_preset_time()
    # 1. 结果集准备
    report_data_to_insert = file_data

    # 2. 结果集准备结束，开始调API插入
    begin = time.time()
    columns = ["date", "oyo_id", "room_type", "channel_id", "post_sell_price", "pre_sell_price",
               "pre_net_price", "post_commission", "pre_commission", "preset_days", "fail_code", "fail_message"]
    json_data_list = list()
    failed_series_list = list()
    preset_days = get_preset_days(sub_prefix, config)
    for index, row in report_data_to_insert.iterrows():
        try:
            date_str = strf_time(row['date'])
            strategy_type = row['strategy_type']
            json_data_list.append(
                compose_ota_record(ebk_ota_room_type_mapping_filter_lst, date_str,
                                   row, 'ctrip_room_type_name', OTA_CHANNEL_ID_CTRIP,
                                   'ctrip_post_sell_price',
                                   'ctrip_pre_sell_price', 'ctrip_pre_net_price',
                                   'ctrip_post_commission', 'ctrip_pre_commission',
                                   preset_days, batch_time, strategy_type))

            json_data_list.append(
                compose_ota_record(ebk_ota_room_type_mapping_filter_lst, date_str, row, 'meituan_room_type_name',
                                   OTA_CHANNEL_ID_MEITUAN, 'meituan_post_sell_price', 'meituan_pre_sell_price',
                                   'meituan_pre_net_price', 'meituan_post_commission', 'meituan_pre_commission',
                                   preset_days, batch_time, strategy_type))

            json_data_list.append(
                compose_ota_record(ebk_ota_room_type_mapping_filter_lst, date_str, row, 'fliggy_room_type_name',
                                   OTA_CHANNEL_ID_FLIGGY, 'fliggy_post_sell_price', 'fliggy_pre_sell_price',
                                   'fliggy_pre_net_price', 'fliggy_post_commission', 'fliggy_pre_commission',
                                   preset_days, batch_time, strategy_type))

            json_data_list.append(
                compose_ota_record(ebk_ota_room_type_mapping_filter_lst, date_str, row, 'elong_room_type_name',
                                   OTA_CHANNEL_ID_ELONG, 'elong_post_sell_price', 'elong_pre_sell_price',
                                   'elong_pre_net_price',
                                   'elong_post_commission', 'elong_pre_commission',
                                   preset_days, batch_time, strategy_type))

            json_data_list.append(
                compose_ota_record(ebk_ota_room_type_mapping_filter_lst, date_str, row, 'qunar_room_type_name',
                                   OTA_CHANNEL_ID_QUNAR, 'qunar_post_sell_price',
                                   'qunar_pre_sell_price', 'qunar_pre_net_price',
                                   'qunar_post_commission', 'qunar_pre_commission',
                                   preset_days, batch_time, strategy_type))

            json_data_list.append(
                compose_ota_record(ebk_ota_room_type_mapping_filter_lst, date_str, row, 'quhuhu_room_type_name',
                                   OTA_CHANNEL_CN_NAME_QUHUHU, 'quhuhu_post_sell_price',
                                   'quhuhu_pre_sell_price', 'quhuhu_pre_net_price', 'quhuhu_post_commission',
                                   'quhuhu_pre_commission', preset_days, batch_time, strategy_type))

            json_data_list.append(
                compose_ota_record(ebk_ota_room_type_mapping_filter_lst, date_str, row, 'huamin_room_type_name',
                                   OTA_CHANNEL_CN_NAME_HUAMIN, 'huamin_post_sell_price',
                                   'huamin_pre_sell_price', 'huamin_pre_net_price', 'huamin_post_commission',
                                   'huamin_pre_commission', preset_days, batch_time, strategy_type))
        except Exception:
            logger.exception("unable to start thread")
    json_data_list = list(filter(None, json_data_list))
    json_data_len = len(json_data_list)
    slices_len = int(json_data_len / __batch_size)
    if (json_data_len % __batch_size) != 0:
        slices_len += 1
    size_suc = 0
    size_failed = 0
    for index in range(slices_len):
        right_max = min((index + 1) * __batch_size, json_data_len)
        data_list = json_data_list[index * __batch_size:right_max]
        code = None
        msg = None
        begin0 = time.time()
        url = config.get_report_insert_ota_price_url()
        try:
            data_req = {"reportHotelChannelPrices": data_list}
            result = requests.post(url=url, headers=JSON_HEADER, json=data_req)
            json_result = json.loads(result.text)
            code = json_result['code']
            msg = json_result['msg']
            if code == '000':
                logger.info("batchInsertOtaPriceLog-%s, size: %d, code: %s, cost: %0.2fs", index + 1,
                            len(data_list), code, (time.time() - begin0))
                size_suc += len(data_list)
            else:
                logger.error("batchInsertOtaPriceLog Exception, url: %s, code: %s, msg: %s, paramsJson: %s", url, code,
                             msg, str(data_req)[0:100])
                size_failed += len(data_list)
                JsonUtil.print_json_str_file("./batchInsertOtaPriceLog-errorParams-{0}.json".format(index + 1),
                                             data_req)
                build_failed_series_list_for_ota_price(failed_series_list, data_list, columns, code, msg)
        except Exception as e:
            size_failed += len(data_list)
            logger.error("batchInsertOtaPriceLog  Exception: %s, url: %s,  paramsJson:%s", str(e), url,
                         str(data_req)[0:100])
            JsonUtil.print_json_str_file("./batchInsertOtaPriceLog-errorParams-{0}.json".format(index + 1), data_req)
            build_failed_series_list_for_ota_price(failed_series_list, data_list, columns, code, msg)
    logger.info('batchInsertOtaPriceLog totalSize: %d, batchSize: %d, succeeded: %s, failed: %s, cost: %0.2fs',
                json_data_len, __batch_size, size_suc, size_failed, (time.time() - begin))

    # 3. 调API结束，准备邮件逻辑
    attach_name, failed_df, mail_content = build_mail_content(failed_series_list, columns, json_data_len)
    # 3.1. 开始发送邮件
    mail_send.send_mail_for_report_ota_price_insert(config, mail_content, failed_df, attach_name)


def build_failed_series_list_for_ota_price(failed_series_list, data_list, columns, code, msg):
    logger = LogUtil.get_cur_logger()
    for e in data_list:
        try:
            failed_series_list.append(
                pd.Series([e.get("date"), e.get("oyoId"), e.get("roomType"), e.get("channelId"),
                           e.get("postSellPrice"), e.get("preSellPrice"), e.get("preNetPrice"),
                           e.get("postCommission"), e.get("preCommission"), e.get("presetDays"), code, msg],
                          index=columns))
        except Exception as e:
            logger.error("build_failed_series_list4 error,  %s", str(e))


def compose_ota_record(ebk_ota_room_type_mapping_filter_lst, date_str, row, room_type_column_name, channel_id,
                       post_sell_price_column_name, pre_sell_price_column_name, pre_net_price_column_name,
                       post_commission_column_name, pre_commission_name, preset_days, batch_time, strategy_type):
    if room_type_column_name not in row.index or pd.isnull(row[room_type_column_name]):
        return None
    oyo_id = row['oyo_id']
    room_type_id = row['room_type_id']
    row_id = PricingPipelineUtil.compose_row_id(oyo_id, room_type_id, channel_id)
    if row_id not in ebk_ota_room_type_mapping_filter_lst:
        return None
    ota_record = {
        "channelId": channel_id,
        "date": date_str,
        "oyoId": row['oyo_id'],
        "roomType": row['room_type_id'],
        "postSellPrice": trans_n(DFUtil.get_item_from_row(row, post_sell_price_column_name)),
        "preSellPrice": trans_n(DFUtil.get_item_from_row(row, pre_sell_price_column_name)),
        "preNetPrice": trans_n(DFUtil.get_item_from_row(row, pre_net_price_column_name)),
        "postCommission": PriceUtil.scale_commission001(DFUtil.get_item_from_row(row, post_commission_column_name)),
        "preCommission": PriceUtil.scale_commission001(DFUtil.get_item_from_row(row, pre_commission_name)),
        "strategyType": strategy_type,
        "presetDays": preset_days,
        "batchTime": batch_time
    }
    return ota_record


def build_mail_content(failed_series_list, columns, total_count):
    fail_count = len(failed_series_list)
    failed_df = None
    attach_name = ""
    if fail_count == 0:
        mail_content = "All data insert succeed, number of succeeded records: " + str(total_count)
    else:
        failed_df = pd.DataFrame(failed_series_list, columns=columns)
        mail_content = ("Not all data insert succeeded, succeeded count: {0}, failed count: {1}, "
                        "please refer to attachment for more details".format(str(total_count - fail_count),
                                                                             str(fail_count)))
        start_time = dt.datetime.fromtimestamp(time.time())
        attach_name = "insertOtaPriceReportAndSendMail_" + dt.datetime.strftime(start_time, "%Y-%m-%d-%H") + ".xls"
    return attach_name, failed_df, mail_content


class PriceLog(ApiBase):

    @staticmethod
    def report_franchise_v1_inter_result_and_send_mail(config, df_result_final):
        try:
            occ_inter_result_report_and_mail_send(config, df_result_final)
        except Exception:
            ApiBase.send_stack_trace("occ_inter_result_report_and_mail_send", config)

    @staticmethod
    def report_price_and_send_mail(config, df_result_final):
        try:
            pms_price_report_and_mail_send(config, df_result_final)
        except Exception:
            ApiBase.send_stack_trace("pms_price_report_and_mail_send", config)

    @staticmethod
    def report_om_v1_inter_result_and_send_mail(config, df_result_final):
        try:
            om_v1_inter_result_report_and_mail_send(config, df_result_final)
        except Exception:
            ApiBase.send_stack_trace("om_v1_inter_result_report_and_mail_send", config)

    @staticmethod
    def report_ota_price_and_send_mail(config, df_result_final, ebk_ota_room_type_mapping_filter_lst):
        try:
            ota_price_report_and_mail_send(config, df_result_final, ebk_ota_room_type_mapping_filter_lst)
        except Exception:
            ApiBase.send_stack_trace("ota_price_report_and_mail_send", config)


class PmsPriceReport(JSONSerializable):
    # {"date": strf_time(row['date']),
    #  "oyoId": row['oyo_id'],
    #  "roomType": row['room_type_id'],
    #  "roomCategory": row['room_type_name'],
    #  "finalPrice": row['pms_price'],
    #  "hourlyPrice": row['hourly_price'],
    #  "uniqueCode": '',
    #  "hotelName": row['hotel_name'],
    #  "zoneName": '',
    #  "strategyType": row['strategy_type'],
    #  "presetDays": preset_days}
    # -----
    # PmsPriceReport(
    #     strf_time(row['date']), row['oyo_id'], row['room_type_id'], row['room_type_name'], row['pms_price'],
    #     row['hourly_price'], '', row['hotel_name'], '', row['strategy_type'], preset_days).to_json())
    # json_data_list = list(filter(None, json_data_list)
    def __init__(self, date, oyoId, roomType, roomCategory, finalPrice, hourlyPrice, uniqueCode, hotelName, zoneName,
                 strategyType, presetDays):
        self.date = date
        self.oyoId = oyoId
        self.roomType = roomType
        self.roomCategory = roomCategory
        self.finalPrice = finalPrice
        self.hourlyPrice = hourlyPrice
        self.uniqueCode = uniqueCode
        self.hotelName = hotelName
        self.zoneName = zoneName
        self.strategyType = strategyType
        self.presetDays = presetDays


class OccPriceReport(JSONSerializable):
    # {"date": strf_time(row['date']),
    #  "oyoId": row['oyo_id'],
    #  "price": trans_nan(row['price']),
    #  "clusOcc": trans_nan(row['clus_occ']),
    #  "occ": trans_nan(row['occ']),
    #  "sellableRooms": row['sellable_rooms'],
    #  "rem": trans_nan(row['rem']),
    #  "delta": row['delta'],
    #  "base": trans_nan(row['base']),
    #  "clusArr": trans_nan(row['clus_arr']),
    #  "pastOcc": trans_nan(row['past_occ']),
    #  "pastArr": trans_nan(row['past_arr']),
    #  "occFtdTarget": trans_nan(row['occ_ftd_target']),
    #  "strategyType": row['strategy_type'],
    #  "presetDays": preset_days}
    def __init__(self, date, oyoId, price, clusOcc, occ, sellableRooms, rem, delta, base, clusArr, pastOcc, pastArr,
                 occFtdTarget, strategyType, presetDays):
        self.date = date
        self.oyoId = oyoId
        self.price = price
        self.clusOcc = clusOcc
        self.occ = occ
        self.sellableRooms = sellableRooms
        self.rem = rem
        self.delta = delta
        self.base = base
        self.clusArr = clusArr
        self.pastOcc = pastOcc
        self.pastArr = pastArr
        self.occFtdTarget = occFtdTarget
        self.strategyType = strategyType
        self.presetDays = presetDays
