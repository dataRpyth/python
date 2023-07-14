#!/usr/bin/env python
# -*- coding:utf-8 -*-
from common.priceop.api_base import ApiBase
from common.pricing_pipeline.pipeline_pojo import JSONSerializable
from common.util.utils import *

JSON_HEADER = {'Content-Type': 'application/json; charset=utf-8'}


class SpecialSale(ApiBase):
    OPERATE_TYPE_INSERT = 1
    OPERATE_TYPE_UPDATE = 2
    OPERATE_TYPE_DELETE = 3

    @staticmethod
    def set_hotel_special_sale(config, df_liquidation_srn_with_brn):
        try:
            df_liquidation_srn_with_brn["channel_ids"] = "-1"
            df_liquidation_srn_with_brn["sale_type"] = 1
            df_liquidation_srn_with_brn["remark"] = "尾房甩卖"
            df_liquidation_srn_with_brn["enabled"] = 1
            func_hotel_special_sale(config, df_liquidation_srn_with_brn, SpecialSale.OPERATE_TYPE_INSERT)
        except Exception:
            ApiBase.send_stack_trace("set_hotel_special_sale", config)

    @staticmethod
    def update_hotel_special_sale(config, df_liquidation_srn_with_brn):
        try:
            func_hotel_special_sale(config, df_liquidation_srn_with_brn, SpecialSale.OPERATE_TYPE_UPDATE)
        except Exception:
            ApiBase.send_stack_trace("update_hotel_special_sale", config)

    @staticmethod
    def relieve_hotel_special_sale(config, df_liquidation_srn_with_brn):
        try:
            df_liquidation_srn_with_brn["enabled"] = 0
            DFUtil.apply_func_for_df(df_liquidation_srn_with_brn, "pricing_date", ["pricing_date"], lambda x: str(*x))
            func_hotel_special_sale(config, df_liquidation_srn_with_brn, SpecialSale.OPERATE_TYPE_UPDATE)
        except Exception:
            ApiBase.send_stack_trace("relieve_hotel_special_sale", config)


def func_hotel_special_sale(config, df_hotel_special_sale, operate_type):
    # 1. 准备数据
    slices_size = config.get_hotel_special_sale_slices_size()
    pojo_chunk_list, prices_len = df_to_pojo_chunk_list(df_hotel_special_sale, slices_size, operate_type)

    # 2. POST数据
    succeeded_pojo_chunk_list, failed_pojo_chunk_list = hotel_special_sale_upload(config, pojo_chunk_list, prices_len,
                                                                                  slices_size)
    # 3. send mail
    config.get_mail_send().send_mail_for_hotel_special_sale_monitor(config, failed_pojo_chunk_list,
                                                                    succeeded_pojo_chunk_list)


def hotel_special_sale_upload(config, pojo_chunk_list, prices_len, slices_size):
    logger = LogUtil.get_cur_logger()
    begin = time.time()
    size_failed = 0
    size_suc = 0
    failed_pojo_chunk_list = list()
    succeeded_pojo_chunk_list = list()
    num_post = 0
    url = config.get_special_sale_to_pc_url()
    for pojo_chunk in pojo_chunk_list:
        num_post += 1
        __begin0 = time.time()
        post_result = post_json_chunk_with_retry(pojo_chunk, url, 3)
        if not post_result[0] == '000':
            failed_pojo_chunk_list.append(pojo_chunk)
            size_failed += len(pojo_chunk.items)
            logger.exception("special_sale_to_pc Exception, url: %s, code: %s, msg: %s, paramsJson: %s",
                             url, post_result[0], post_result[1], pojo_chunk.to_json()[0:100])
            JsonUtil.print_json_file("./special-sale-to-pc-errorParamsJson-{0}.json".format(num_post), pojo_chunk)
        else:
            size_suc += len(pojo_chunk.items)
            succeeded_pojo_chunk_list.append(pojo_chunk)
        logger.info("special_sale_to_pc-%s, size: %d, code: %s, cost: %0.2fs", num_post, len(pojo_chunk.items),
                    post_result[0], (time.time() - __begin0))
    logger.info('special_sale_to_pc totalSize: %s, batchSize: %s, succeeded: %s, failed: %s, cost: %0.2fs', prices_len,
                slices_size, size_suc, size_failed, (time.time() - begin))
    return succeeded_pojo_chunk_list, failed_pojo_chunk_list


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
            LogUtil.get_cur_logger().warn('post_json_chunk_with_retry, retrying count: {0}'.format(retry_count))
            time.sleep(10)
            continue
        else:
            LogUtil.get_cur_logger().exception(
                "special_sale_to_pc Exception, errorInfo code: %s ,msg: %s ,paramsJson: %s",
                post_result[0], post_result[1], pojo_chunk.to_json()[0:1000])
            return -2, post_result[1]
    return post_result


def value_trim(row, param, default=None):
    return row.get(param, default)


def df_to_pojo_chunk_list(df_hotel_special_sale, slices_size, operate_type):
    prices = list()
    pojo_chunk_list = list()
    for index, row in df_hotel_special_sale.iterrows():
        special_sale = HotelSpecialSale(
            value_trim(row, "oyo_id"),
            value_trim(row, "room_type_id"),
            value_trim(row, "pricing_date"),
            value_trim(row, "sale_start_time"),
            value_trim(row, "sale_end_time"),
            value_trim(row, "channel_ids"),
            value_trim(row, "lms_room_number"),
            value_trim(row, "sale_price"),
            value_trim(row, "sale_type"),
            value_trim(row, "strategy_type"),
            value_trim(row, "remark"),
            value_trim(row, "enabled"))
        prices.append(special_sale)

    # prices数组切片
    prices_len = len(prices)
    slices_len = int(prices_len / slices_size)
    if (prices_len % slices_size) != 0:
        slices_len += 1
    for slice_index in range(slices_len):
        right_max = min((slice_index + 1) * slices_size, prices_len)
        pojo_chunk_list.append(HotelSpecialSales(operate_type, prices[slice_index * slices_size:right_max]))
    return pojo_chunk_list, prices_len


# {
#     "operateType": "UPDATE",
#     "items":
#             "oyoId": "CN_CHA010",
#              "roomTypeId": 20,
#              "pricingDate": 2019-03-05,
#              "saleStartTime": 2019-03-05 16:30:00,
#              "saleEndTime": 2019-03-05 16:30:00,
#              "channelIds": "2,3,4",
#              "saleNumber": 20,
#              "salePrice": 200,
#              "saleType": 20,
#              "strategyType": "china2.0",
#              "remark": "尾房甩卖",
#              "enabled": 20
#         }
#     ]
# }

class HotelSpecialSale(JSONSerializable):
    def __init__(self, oyoId, roomTypeId, pricingDate, saleStartTime, saleEndTime, channelIds, saleNumber, salePrice,
                 saleType, strategyType, remark, enabled):
        self.oyoId = oyoId
        self.roomTypeId = roomTypeId
        self.pricingDate = pricingDate
        self.saleStartTime = saleStartTime
        self.saleEndTime = saleEndTime
        self.channelIds = channelIds
        self.saleNumber = saleNumber
        self.salePrice = salePrice
        self.saleType = saleType
        self.strategyType = strategyType
        self.remark = remark
        self.enabled = enabled


class HotelSpecialSales(JSONSerializable):
    def __init__(self, operate_type, items):
        self.operateType = operate_type  # 1.insert 2.update 3.delete
        self.items = items
