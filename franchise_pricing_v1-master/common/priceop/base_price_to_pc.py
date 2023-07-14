#!/usr/bin/env python
# -*- coding:utf-8 -*-

from common.priceop.api_base import ApiBase
from common.pricing_pipeline.pipeline_pojo import JSONSerializable
from common.util.utils import DFUtil


class HotelBasePrice(ApiBase):

    def set_hotel_base_price(self, config, df_hotel_with_base):
        try:
            DFUtil.print_data_frame(df_hotel_with_base, 'df_hotel_with_base', True)
            self.post_hotel_base_price(config, df_hotel_with_base)
        except Exception:
            self.send_stack_trace("price_to_channel_rate", config)

    def post_hotel_base_price(self, config, df_hotel_with_base):
        # 1. 准备数据
        slices_size = config.get_base_price_change_batch_size()
        pojo_chunk_list, prices_len = df_to_pojo_chunk_list(df_hotel_with_base, slices_size)

        # 2. POST数据
        succeeded_pojo_chunk_list, failed_pojo_chunk_list = self.http_request(config.get_base_price_change_url(),
                                                                              pojo_chunk_list, prices_len,
                                                                              slices_size, "hotel_base_price_to_pc",
                                                                              "basePrices")
        # 3. send mail
        config.get_mail_send().send_mail_for_holiday_base_price(config, df_hotel_with_base)
        config.get_mail_send().send_mail_for_holiday_base_price_monitor(config, failed_pojo_chunk_list,
                                                                        succeeded_pojo_chunk_list)


def value_trim(row, param, default=None):
    return row.get(param, default)


def df_to_pojo_chunk_list(df_hotel_with_base, slices_size):
    prices = list()
    pojo_chunk_list = list()
    for index, row in df_hotel_with_base.iterrows():
        special_sale = BasePrice(
            value_trim(row, "oyo_id"),
            value_trim(row, "room_type_id"),
            value_trim(row, "date"),
            value_trim(row, "base_override"),
            value_trim(row, "reason_id"))
        prices.append(special_sale)

    # prices数组切片
    prices_len = len(prices)
    slices_len = int(prices_len / slices_size)
    if (prices_len % slices_size) != 0:
        slices_len += 1
    for slice_index in range(slices_len):
        right_max = min((slice_index + 1) * slices_size, prices_len)
        pojo_chunk_list.append(HotelBase(prices[slice_index * slices_size:right_max]))
    return pojo_chunk_list, prices_len


# {
#     "basePrices":[
#         "oyoId": "CN_CHA010",
#          "roomTypeId": 20,
#          "pricingDate": 2019-10-05,
#          "basePrice": 100,
#          "reasonId": 1
#     ]
# }

class BasePrice(JSONSerializable):
    def __init__(self, oyo_id, room_type_id, pricing_date, base_price, reason_id):
        self.oyoId = oyo_id
        self.roomTypeId = room_type_id
        self.pricingDate = pricing_date
        self.basePrice = base_price
        self.reasonId = reason_id


class HotelBase(JSONSerializable):
    def __init__(self, base_prices):
        self.basePrices = base_prices
