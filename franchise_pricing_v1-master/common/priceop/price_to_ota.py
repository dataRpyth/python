import datetime as dt

from common.dingtalk_sdk.dingtalk_py_cmd import DingTalkPy
from common.priceop.api_base import ApiBase
from common.pricing_pipeline.pipeline import OTA_CHANNEL_NAME_CTRIP, OTA_CHANNEL_NAME_MEITUAN, OTA_CHANNEL_NAME_ELONG, \
    OTA_CHANNEL_NAME_FLIGGY, OTA_CHANNEL_NAME_QUNAR, SEVEN_CHANNELS_CN_NAME_MAP, OTA_CHANNEL_ID_CTRIP, \
    OTA_CHANNEL_ID_MEITUAN, OTA_CHANNEL_ID_FLIGGY, OTA_CHANNEL_ID_ELONG, OTA_CHANNEL_ID_QUNAR
from common.pricing_pipeline.pipeline_pojo import OtaPricing, Prices, ChangePriceOta
from common.util.utils import *

JSON_HEADER = {'Content-Type': 'application/json; charset=utf-8', 'client_id': 'ota-biz', 'client_type': 'web'}


def ota_price_upload_and_send_mail(config, all_prices_df, ebk_ota_room_type_mapping_filter_lst):
    # DFUtil.print_data_frame(all_prices_df, 'result_final_otaPriceUpload', True)
    columns = ["date", "oyo_id", "room_type_id", "room_type_name", "hotel_name", "hourly_price",
               "final_price", "fail_code", "fail_message"]
    slices_size = config.get_ota_pricing_slices_size()
    temp_file_folder = config.get_local_file_folder()
    biz_name = config.get_job().get_job_name()
    ddt_env = config.get_ddt_env()
    job_preset_time = config.get_job_preset_time()
    # 1. 准备数据
    pojo_chunk_list, prices_len = df_to_pojo_chunk_list(all_prices_df, slices_size, job_preset_time, ebk_ota_room_type_mapping_filter_lst)

    robot_token = config.get_robot_send_token()

    robot_send_biz_model_id = config.get_robot_send_biz_model_id()

    upload_start_time = DateUtil.stamp_to_date_format3(time.time())

    DdtUtil.robot_send_ddt_msg('业务线: {0} 开始上传OTA改价数据, 时间: {1}'.format(biz_name, upload_start_time), robot_token)

    # 2. 调取API
    succeeded_pojo_chunk_list, failed_pojo_chunk_list = ota_pricing_upload(config, pojo_chunk_list, prices_len,
                                                                           slices_size)

    fail_count = CommonUtil.get_count_for_chunk_list_attr(failed_pojo_chunk_list, "prices")
    suc_count = CommonUtil.get_count_for_chunk_list_attr(succeeded_pojo_chunk_list, "prices")

    if fail_count > 0:
        DdtUtil.robot_send_ddt_msg(
            '业务线: {0}, OTA改价数据上传出现异常, 成功:{1}, 失败:{2}, 时间: {3}'.format(biz_name, suc_count, fail_count,
                                                                      upload_start_time), robot_token)

    upload_finish_readable_time = DateUtil.stamp_to_date_format3(time.time())

    upload_finish_file_name_time = DateUtil.stamp_to_date_format(time.time(), "%Y_%m_%d_%H_%M_%S")

    report_excel = compose_upload_report(temp_file_folder, succeeded_pojo_chunk_list, failed_pojo_chunk_list, biz_name,
                                         upload_finish_readable_time, upload_finish_file_name_time)
    if config.get_toggle_on_robot_send():
        DingTalkPy().robot_send(robot_token, biz_name, robot_send_biz_model_id, config.get_job_preset_time(),
                              report_excel,
                              '业务线: {0}, OTA改价数据上传完成, 成功:{1}, 失败:{2}, 时间: {3}'.format(biz_name, suc_count, fail_count,
                                                                                      upload_finish_readable_time),
                              ddt_env)

    # 3. 调API结束，准备邮件逻辑
    mail_send_for_ota_price_upload(config, failed_pojo_chunk_list, columns, prices_len)


def compose_upload_report(temp_file_folder, succeeded_pojo_chunk_list, failed_pojo_chunk_list, biz_name,
                          upload_finish_readable_time, upload_finish_file_name_time):
    succeeded_report_df = get_report_df_from_pojo_chunk_list(biz_name, succeeded_pojo_chunk_list,
                                                             upload_finish_readable_time)
    failed_report_df = get_report_df_from_pojo_chunk_list(biz_name, failed_pojo_chunk_list, upload_finish_readable_time)
    df_with_sheet_names = [DataFrameWithSheetName(succeeded_report_df, '上传成功'),
                           DataFrameWithSheetName(failed_report_df, '上传失败')]
    excel_file_path = join_path(temp_file_folder,
                                '{0}_ota_upload_report_{1}.xlsx'.format(biz_name, upload_finish_file_name_time))
    excel_file = DFUtil.write_multiple_df_to_excel(excel_file_path, df_with_sheet_names)
    return excel_file


def compose_ota_concatenated_cn_names(change_price_otas):
    ota_cn_name_list = list()
    for change_price_ota in change_price_otas:
        ota_channel_name = change_price_ota.otaChannelName
        ota_channel_cn_name = SEVEN_CHANNELS_CN_NAME_MAP[ota_channel_name]
        ota_cn_name_list.append(ota_channel_cn_name)

    ota_cn_name_list_len = len(ota_cn_name_list)
    if ota_cn_name_list_len == 0:
        return ''

    concatenated_cn_names = ''
    for idx in range(ota_cn_name_list_len):
        if idx == 0:
            concatenated_cn_names += ota_cn_name_list[idx]
        else:
            concatenated_cn_names += ',' + ota_cn_name_list[idx]
    return concatenated_cn_names


def get_report_df_from_pojo_chunk_list(biz_name, pojo_chunk_list, upload_time):
    if len(pojo_chunk_list) == 0:
        return pd.DataFrame()
    ota_cn_name_cache = {}
    row_dict_list = list()
    for pojo_chunk in pojo_chunk_list:
        prices = pojo_chunk.prices
        for price in prices:
            oyo_id = price.oyoId
            ota_concatenated_cn_names = ota_cn_name_cache.get(oyo_id, None)
            if ota_concatenated_cn_names is None:
                ota_concatenated_cn_names = compose_ota_concatenated_cn_names(price.changePriceOtas)
                ota_cn_name_cache[oyo_id] = ota_concatenated_cn_names
            row_dict = {'bizName': biz_name, 'oyoId': price.oyoId, 'hotelName': price.hotelName,
                        'otaNames': ota_concatenated_cn_names, 'count': 1, 'uploadTime': upload_time}
            row_dict_list.append(row_dict)
    report_df = pd.DataFrame(row_dict_list)
    grouped_df = report_df.groupby(by=['bizName', 'oyoId', 'hotelName', 'otaNames', 'uploadTime'], as_index=False)[
        'count'].sum()
    grouped_df.rename(
        columns={'bizName': '项目名称', 'oyoId': 'CRS ID', 'hotelName': '酒店名称', 'otaNames': '改价平台', 'count': '上传数据条数',
                 'uploadTime': '导入时间'}, inplace=True)
    return grouped_df


def get_breakfast_price(price):
    if MiscUtil.is_empty_value(price):
        return None
    return round(float(price)) + 10


def has_ota_mapping(row, ota_channel_id, ebk_ota_room_type_mapping_filter_set):
    oyo_id = row['oyo_id']
    room_type_id = row['room_type_id']
    row_id = PricingPipelineUtil.compose_row_id(oyo_id, room_type_id, ota_channel_id)
    return row_id in ebk_ota_room_type_mapping_filter_set


def df_to_pojo_chunk_list(all_prices_df, slices_size, job_preset_time, ebk_ota_room_type_mapping_filter_set):
    prices = list()
    for index, row in all_prices_df.iterrows():
        change_price_otas = []
        is_filter = True
        if (MiscUtil.is_not_empty_value(row["ctrip_pre_commission"]) or MiscUtil.is_not_empty_value(
                row["ctrip_post_commission"])) and has_ota_mapping(row, OTA_CHANNEL_ID_CTRIP,
                                                                   ebk_ota_room_type_mapping_filter_set):
            is_filter = False
            change_price_otas.append(
                ChangePriceOta(OTA_CHANNEL_NAME_CTRIP,
                               row["ctrip_pre_commission"],
                               row["ctrip_post_commission"],
                               row["ctrip_pre_sell_price"],
                               row["ctrip_post_sell_price"],
                               row["hourly_price"],
                               get_breakfast_price(row["ctrip_pre_sell_price"]),
                               get_breakfast_price(row["ctrip_post_sell_price"])))
        if (MiscUtil.is_not_empty_value(row["meituan_pre_commission"]) or MiscUtil.is_not_empty_value(
                row["meituan_post_commission"])) and has_ota_mapping(row, OTA_CHANNEL_ID_MEITUAN,
                                                                     ebk_ota_room_type_mapping_filter_set):
            is_filter = False
            change_price_otas.append(
                ChangePriceOta(OTA_CHANNEL_NAME_MEITUAN,
                               row["meituan_pre_commission"],
                               row["meituan_post_commission"],
                               row["meituan_pre_sell_price"],
                               row["meituan_post_sell_price"],
                               row["hourly_price"],
                               get_breakfast_price(row["meituan_pre_sell_price"]),
                               get_breakfast_price(row["meituan_post_sell_price"])))
        if (MiscUtil.is_not_empty_value(row["fliggy_pre_commission"]) or MiscUtil.is_not_empty_value(
                row["fliggy_post_commission"])) and has_ota_mapping(row, OTA_CHANNEL_ID_FLIGGY,
                                                                   ebk_ota_room_type_mapping_filter_set):
            is_filter = False
            change_price_otas.append(
                ChangePriceOta(OTA_CHANNEL_NAME_FLIGGY,
                               row["fliggy_pre_commission"],
                               row["fliggy_post_commission"],
                               row["fliggy_pre_sell_price"],
                               None,
                               row["hourly_price"],
                               get_breakfast_price(row["fliggy_pre_sell_price"]),
                               None))
        if (MiscUtil.is_not_empty_value(row["elong_pre_commission"]) or MiscUtil.is_not_empty_value(
                row["elong_post_commission"])) and has_ota_mapping(row, OTA_CHANNEL_ID_ELONG,
                                                                   ebk_ota_room_type_mapping_filter_set):
            is_filter = False
            change_price_otas.append(
                ChangePriceOta(OTA_CHANNEL_NAME_ELONG,
                               row["elong_pre_commission"],
                               row["elong_post_commission"],
                               row["elong_pre_sell_price"],
                               row["elong_post_sell_price"],
                               row["hourly_price"],
                               get_breakfast_price(row["elong_pre_sell_price"]),
                               get_breakfast_price(row["elong_post_sell_price"])))
        if (MiscUtil.is_not_empty_value(row["qunar_pre_commission"]) or MiscUtil.is_not_empty_value(
                row["qunar_post_commission"])) and has_ota_mapping(row, OTA_CHANNEL_ID_QUNAR,
                                                                   ebk_ota_room_type_mapping_filter_set):
            is_filter = False
            change_price_otas.append(
                ChangePriceOta(OTA_CHANNEL_NAME_QUNAR,
                               row["qunar_pre_commission"],
                               row["qunar_post_commission"],
                               row["qunar_pre_sell_price"],
                               row["qunar_post_sell_price"],
                               row["hourly_price"],
                               get_breakfast_price(row["qunar_pre_sell_price"]),
                               get_breakfast_price(row["qunar_post_sell_price"])))
        if is_filter:
            continue
        price = Prices(date_format(row["date"]), row["oyo_id"], row["hotel_name"], row["room_type_id"],
                       row["room_type_name"], change_price_otas)
        prices.append(price)

    # prices数组切片
    prices_len = len(prices)
    pojo_chunk_list = list()
    slices_len = int(prices_len / slices_size)
    if (prices_len % slices_size) != 0:
        slices_len += 1
    for slice_index in range(slices_len):
        right_max = min((slice_index + 1) * slices_size, prices_len)
        pojo_chunk_list.append(OtaPricing(prices[slice_index * slices_size:right_max], str(job_preset_time)))
    return pojo_chunk_list, prices_len


def post_pojo_chunk_to_remote(pojo_chunk, url):
    try:
        result = requests.post(url=url, headers=JSON_HEADER, data=pojo_chunk.to_json().encode("utf-8"))
        json_result = json.loads(result.text, encoding="utf-8")
        code = json_result['code']
        msg = json_result['msg']
        return code, msg
    except Exception as e:
        return -1, str(e)


def post_json_chunk_with_retry(pojo_chunk, url, retry_times):
    post_result = None
    for retry_count in range(retry_times):
        post_result = post_pojo_chunk_to_remote(pojo_chunk, url)
        if post_result[0] == '000':
            break
        elif post_result[0] == '999':
            LogUtil.get_cur_logger().warn('otaPriceUpload, retrying count: {0}'.format(retry_count))
            time.sleep(10)
            continue
        else:
            LogUtil.get_cur_logger().exception("otaPriceUpload Exception, code: %s ,msg: %s", post_result[0],
                                               post_result[1])
            return -2, post_result[1]
    return post_result


def ota_pricing_upload(config, pojo_chunk_list, prices_len, slices_size):
    logger = LogUtil.get_cur_logger()
    begin = time.time()
    size_failed = 0
    size_suc = 0
    failed_pojo_chunk_list = list()
    succeeded_pojo_chunk_list = list()
    num_post = 0
    url = config.get_ota_pricing_upload_url()
    for pojo_chunk in pojo_chunk_list:
        num_post += 1
        __begin0 = time.time()
        post_result = post_json_chunk_with_retry(pojo_chunk, url, 3)
        pojo_chunk_len = len(pojo_chunk.prices)
        if not post_result[0] == '000':
            JsonUtil.print_json_file("./otaPriceUpload-errorParamsJson-{0}.json".format(num_post), pojo_chunk)
            failed_pojo_chunk_list.append(pojo_chunk)
            size_failed += pojo_chunk_len
            LogUtil.get_cur_logger().exception(
                "otaPriceUpload-%s, Exception, url:%s, code:%s, msg:%s", num_post, url, post_result[0], post_result[1])
        else:
            succeeded_pojo_chunk_list.append(pojo_chunk)
            size_suc += pojo_chunk_len
        logger.info("otaPriceUpload-%s, size: %d, code: %s, cost: %0.2fs", num_post, pojo_chunk_len, post_result[0],
                    (time.time() - __begin0))
    logger.info('otaPriceUpload actual uploaded totalSize: %s, batchSize: %s, succeeded: %s, failed: %s, cost: %0.2fs', prices_len,
                slices_size, size_suc, size_failed, (time.time() - begin))
    return succeeded_pojo_chunk_list, failed_pojo_chunk_list


def compose_failed_series_list(failed_series_list, post_result, pojo_chunk, columns):
    prices_data = pojo_chunk.get("prices")
    for price in prices_data:
        failed_series_list.append(
            pd.Series([price.get("date"), price.get("oyoId"), price.get("roomTypeId"),
                       price.get("roomTypeName"), price.get("hotelName"), price.get("hourlyPrice"),
                       price.get("finalPrice"), post_result[0], post_result[1]], index=columns))


def mail_send_for_ota_price_upload(config, failed_series_list, columns, total_count):
    logger = LogUtil.get_cur_logger()
    try:
        fail_count = len(failed_series_list)
        mail_send = config.get_mail_send()
        attach_name = ""
        failed_df = None
        if fail_count == 0:
            mail_content = "All data otaPriceUpload succeed, number of succeeded records: " + str(total_count)
        else:
            failed_list = list()
            for failed_series in failed_series_list:
                for price in failed_series.prices:
                    failed_list.append({
                        "date": price.date,
                        "oyo_id": price.oyoId,
                        "room_type_id": price.roomTypeId,
                        "room_type_name": price.roomTypeName,
                        "hotel_name": price.hotelName,
                        "hourly_price": price.changePriceOtas[0].hourlyPrice,
                        "final_price": price.changePriceOtas[0].preSellPrice,
                        "fail_code": "",
                        "fail_msg": " "
                    })
            failed_df = pd.DataFrame(failed_list, columns=columns)
            failed_df.reset_index(drop=True, inplace=True)
            head = "Not all data otaPriceUpload succeeded, succeeded count: {0}, failed count: {1}, please refer to attachment for more details".format(
                str(total_count - fail_count), str(fail_count))
            mail_content = DFUtil.gen_excel_content_by_html(head, failed_df)

            start_time = dt.datetime.fromtimestamp(time.time())
            attach_name = "otaPriceUpload_result_" + date_format(start_time) + ".xls"
        mail_send.send_mail_for_ota_price_upload(config, mail_content, failed_df, attach_name)
    except Exception:
        logger.exception("unable to send_mail_for_ota_price_upload")


class OtaPriceUpload(ApiBase):

    @staticmethod
    def ota_price_upload_and_mail_send(config, ota_prices_df, ebk_ota_room_type_mapping_filter_lst):
        try:
            if config.get_toggle_on_ota_pricing_upload():
                LogUtil.get_cur_logger().info(
                    'start uploading ota prices to qitian, ota_prices-size before ota mapping filter: {}'.format(len(ota_prices_df)))
                ota_price_upload_and_send_mail(config, ota_prices_df, ebk_ota_room_type_mapping_filter_lst)
                LogUtil.get_cur_logger().info('end uploading ota prices to qitian')
        except Exception:
            ApiBase.send_stack_trace("ota_price_upload_and_mail_send", config)


def date_format(date_time):
    if isinstance(date_time, dt.date):
        return dt.datetime.strftime(date_time, "%Y-%m-%d-%H")
    return date_time
