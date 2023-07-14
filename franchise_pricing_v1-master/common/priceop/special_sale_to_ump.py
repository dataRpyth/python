from common.dingtalk_sdk.dingtalk_py_cmd import DingTalkPy
from common.priceop.api_base import ApiBase
from common.pricing_pipeline.pipeline import SEVEN_CHANNELS_CN_NAME_MAP
from common.pricing_pipeline.pipeline_pojo import JSONSerializable
from common.util.utils import *

JSON_HEADER = {'Content-Type': 'application/json; charset=utf-8'}


class LmsInfo(JSONSerializable):
    def __init__(self, data_time, batch, act_type, begin_period, end_period, algorithm_model, data):
        self.dataTime = data_time
        self.batch = batch
        self.actType = act_type
        self.beginPeriod = begin_period
        self.endPeriod = end_period
        self.algorithm_model = algorithm_model
        self.data = data


class DiscHotelRoomRulesDTO(JSONSerializable):
    def __init__(self, hotel_name, hotel_id, room_type_name, room_type_id, rules_id,
                 rate_oyo, rate_owner, total_qty, share_type):
        self.hotelName = hotel_name
        self.hotelId = hotel_id
        self.roomTypeName = room_type_name
        self.roomTypeId = room_type_id
        self.rulesId = rules_id
        self.rateOyo = rate_oyo
        self.rateOwner = rate_owner
        self.totalQty = total_qty
        self.shareType = share_type


class LmsToUmp(ApiBase):

    @staticmethod
    def special_sale_to_ump_and_mail_send(config, data_frame):
        try:
            if config.get_toggle_on_lms_to_ump():
                special_sale_to_ump_and_send_mail(config, data_frame)
        except Exception:
            ApiBase.send_stack_trace("special_sale_to_ump_and_mail_send", config)


def special_sale_to_ump_and_send_mail(config, all_prices_df):
    # 1. 准备数据
    pojo_chunk_list, rules_len = df_to_pojo_chunk_list(config, all_prices_df)

    # 2. 调取API
    succeeded_pojo_chunk_list, failed_pojo_chunk_list = liquidation_to_ump(config, pojo_chunk_list, rules_len)

    # 3. send  mail
    send_mail_for_lms_monitor(config, failed_pojo_chunk_list, succeeded_pojo_chunk_list)


def send_mail_for_lms_monitor(config, failed_pojo_chunk_list, succeeded_pojo_chunk_list):
    finish_readable_time = DateUtil.stamp_to_date_format3(time.time())
    finish_file_name_time = DateUtil.stamp_to_date_format(time.time(), "%Y_%m_%d_%H_%M_%S")
    temp_file_folder = config.get_local_file_folder()
    biz_name = config.get_job().get_job_name()
    ddt_env = config.get_ddt_env()
    robot_token = config.get_robot_send_token()
    biz_model_id = config.get_robot_send_biz_model_id()
    batch = config.get_liq_batch()

    succeeded_report_df = get_df_from_pojo_chunk_list(biz_name, succeeded_pojo_chunk_list, finish_readable_time)
    failed_report_df = get_df_from_pojo_chunk_list(biz_name, failed_pojo_chunk_list, finish_readable_time)
    len_succ = 0
    len_failed = 0
    for item in succeeded_pojo_chunk_list:
        len_succ += len(item.data)
    for item in failed_pojo_chunk_list:
        len_failed += len(item.data)

    if not failed_report_df.empty:
        DdtUtil.robot_send_ddt_msg(
            '业务线: {0}, 尾房甩卖数据上传UMP异常, 批次:{1}, 成功:{2}, 失败:{3}, 时间: {4}'.format(biz_name, batch, len_succ,
                                                                              len_failed, finish_file_name_time),
            robot_token)
    df_with_sheet_names = [DataFrameWithSheetName(succeeded_report_df, '上传成功'),
                           DataFrameWithSheetName(failed_report_df, '上传失败')]
    excel_file_path = join_path(temp_file_folder, '{0}_special_sale_to_ump_report_{1}.xlsx'
                                .format(biz_name, finish_file_name_time))
    excel_file = DFUtil.write_multiple_df_to_excel(excel_file_path, df_with_sheet_names)
    if config.get_toggle_on_robot_send():
        DingTalkPy().robot_send(robot_token, biz_name, biz_model_id, config.get_job_preset_time(), excel_file,
                                '业务线: {0}, 尾房甩卖数据上传UMP完成, 批次:{1}, 成功:{2}, 失败:{3}, 时间: {4}'.format(biz_name,
                                                                                                  batch, len_succ,
                                                                                                  len_failed,
                                                                                                  finish_readable_time),
                                ddt_env)


def get_df_from_pojo_chunk_list(biz_name, pojo_chunk_list, upload_time):
    if len(pojo_chunk_list) == 0:
        return pd.DataFrame()
    row_dict_list = list()
    for pojo_chunk in pojo_chunk_list:
        datas = pojo_chunk.data
        for data in datas:
            row_dict = {'bizName': biz_name, 'hotelId': data.hotelId, 'hotelName': data.hotelName,
                        'roomTypeId': data.roomTypeId, 'roomTypeName': data.roomTypeName, 'rulesId': data.rulesId,
                        'rateOyo': data.rateOyo, 'rateOwner': data.rateOwner, 'totalQty': data.totalQty,
                        'shareType': data.shareType, "uploadTime": upload_time}
            row_dict_list.append(row_dict)
    report_df = pd.DataFrame(row_dict_list)
    grouped_df = report_df.groupby(by=['bizName', 'hotelId', 'roomTypeId', 'rulesId', 'uploadTime'], as_index=False)[
        'totalQty'].sum()
    grouped_df.rename(
        columns={'bizName': '项目名称', 'hotelId': 'CRS ID', 'hotelName': '酒店名称', 'roomTypeId': '房型ID',
                 'roomTypeName': '房型名称', 'rulesId': '规则code', 'rateOyo': 'oyo补贴', 'rateOwner': '业主补贴',
                 'totalQty': '每日活动库存', 'shareType': '分摊类型', 'uploadTime': '更新时间'}, inplace=True)
    return grouped_df


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


def df_to_pojo_chunk_list(config, lms_to_ump_df):
    # 配置管理，11点到12点，14点到16，18到21点
    # act_type = 1
    # begin_period = "2019-07-15 20:10:00"
    # end_period = "2019-07-15 21:10:00"
    act_type = list(lms_to_ump_df.act_type)[0]
    begin_period = list(lms_to_ump_df.begin_period)[0]
    end_period = list(lms_to_ump_df.end_period)[0]
    disc_hotel_room_rules_list = list()
    for index, row in lms_to_ump_df.iterrows():
        disc_hotel_room_rules_list.append(
            DiscHotelRoomRulesDTO(
                row["hotel_name"],
                row["hotel_id"],
                row["room_type_name"],
                row["room_type_id"],
                row["rule_id"],
                row["oyo_subsidy"],
                row["owner_subsidy"],
                row["lms_room_number"],
                1
            ))
    # disc_hotel_room_rules_list切片
    rules_len = len(disc_hotel_room_rules_list)
    pojo_chunk_list = list()
    batch_size = config.get_lms_to_ump_batch_size()
    slices_len = int(rules_len / batch_size)
    if rules_len % batch_size != 0:
        slices_len += 1
    job_preset_time = DateUtil.stamp_to_date_format(config.get_job_preset_time(), "%Y-%m-%d")
    for slice_idx in range(slices_len):
        right_max = min((slice_idx + 1) * batch_size, rules_len)
        pojo_chunk_list.append(
            LmsInfo(job_preset_time, (slice_idx + 1), act_type, begin_period, end_period,
                    config.get_job().get_algorithm_model(),
                    disc_hotel_room_rules_list[slice_idx * batch_size:right_max])
        )
    return pojo_chunk_list, rules_len


def liquidation_to_ump(config, pojo_chunk_list, rules_len):
    logger = LogUtil.get_cur_logger()
    robot_token = config.get_robot_send_token()
    biz_name = config.get_job().get_job_name()
    begin = time.time()
    upload_start_time = DateUtil.stamp_to_date_format3(begin)

    DdtUtil.robot_send_ddt_msg('业务线: {0}, 开始上传尾房甩卖数据, 时间: {1}'.format(biz_name, upload_start_time),
                               robot_token, None, True)
    size_failed = 0
    size_suc = 0
    failed_pojo_chunk_list = list()
    succeeded_pojo_chunk_list = list()
    num_post = 0
    url = config.get_lms_to_ump_url()
    batch_size = config.get_lms_to_ump_batch_size()
    for pojo_chunk in pojo_chunk_list:
        num_post += 1
        __begin0 = time.time()
        post_result = post_json_chunk_with_retry(pojo_chunk, url, 3)
        if not post_result[0] == '000':
            JsonUtil.print_json_file("./liquidation-to-ump-errorParams-{0}.json".format(num_post), pojo_chunk)
            failed_pojo_chunk_list.append(pojo_chunk)
            size_failed += len(pojo_chunk.data)
            logger.exception("liquidation_to_ump Exception, url: %s, code: %s, msg: %s, paramsJson: %s",
                             url, post_result[0], post_result[1], pojo_chunk.to_json()[0:1000])
        else:
            size_suc += len(pojo_chunk.data)
            succeeded_pojo_chunk_list.append(pojo_chunk)
        logger.info("liquidation_to_ump-%s, size: %d, code: %s, cost: %0.2fs", num_post, len(pojo_chunk.data),
                    post_result[0], (time.time() - __begin0))
    logger.info('liquidation_to_ump totalSize: %s, batchSize: %s, succeeded: %s, failed: %s, cost: %0.2fs', rules_len,
                batch_size, size_suc, size_failed, (time.time() - begin))
    return succeeded_pojo_chunk_list, failed_pojo_chunk_list


def post_json_chunk_with_retry(pojo_chunk, url, retry_times):
    post_result = None
    for retry_count in range(retry_times):
        post_result = post_pojo_chunk_to_remote(pojo_chunk, url)
        if post_result[0] == '000':
            break
        elif post_result[0] == '999':
            LogUtil.get_cur_logger().warn('post_json_chunk_with_retry, retrying count: {0}'.format(retry_count))
            time.sleep(10)
            continue
        else:
            LogUtil.get_cur_logger().exception(
                "post_json_chunk_with_retry Exception, errorInfo code: %s ,msg: %s ,paramsJson: %s",
                post_result[0], post_result[1], pojo_chunk.to_json()[0:1000])
            return -2, post_result[1]
    return post_result


def post_pojo_chunk_to_remote(pojo_chunk, url):
    try:
        result = requests.post(url=url, headers=JSON_HEADER, data=pojo_chunk.to_json().encode("utf-8"))
        json_result = json.loads(result.text, encoding="utf-8")
        code = json_result['code']
        msg = json_result['msg']
        data = json_result['data']
        return code, msg, data
    except Exception as e:
        return -1, str(e)
