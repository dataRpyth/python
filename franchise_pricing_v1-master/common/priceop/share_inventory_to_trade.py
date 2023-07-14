#!/usr/bin/env python
# -*- coding:utf-8 -*-
from pandas import DataFrame

from common.enum.pricing_enum import PriceChannel
from common.priceop.api_base import ApiBase
from common.pricing_pipeline.pipeline_pojo import JSONSerializable
from common.util.utils import DFUtil


class ShareInventorApi(ApiBase):

    def __init__(self, config):
        self.__config = config
        self.__succeeded_pojo_chunk_list = list()
        self.__failed_pojo_chunk_list = list()
        self.__df_share_inventor = DataFrame()

    def set_share_inventor(self, df_share_inventor, price_channel: PriceChannel):
        try:
            DFUtil.print_data_frame(df_share_inventor, 'df_share_inventor', True)
            df_share_inventor["channel_list"] = price_channel.value
            df_share_inventor.sort_values(axis=0, by=['hotel_id', 'room_type_id'], ascending=True)
            self.__df_share_inventor = df_share_inventor

            self.post_share_inventor(df_share_inventor)
            self.send_mail_for_share_inventory()
            self.send_mail_for_monitor()
        except Exception:
            self.send_stack_trace("share_inventor_to_share_inventor", self.__config)

    def post_share_inventor(self, df_share_inventor):
        # 1. 准备数据
        batch_size = self.__config.get_share_inventory_batch_size()
        pojo_chunk_list, items_len, slices_size = self.get_pojo_chunk_list(df_share_inventor, batch_size,
                                                                           'post_share_inventor')

        # 2. POST数据
        self.__succeeded_pojo_chunk_list, self.__failed_pojo_chunk_list = self.http_request_post(
            self.__config.get_share_inventory_to_trade_url(), pojo_chunk_list, items_len, batch_size,
            biz="share_inventory_to_trade", item1="shareInventoryList")

    def send_mail_for_share_inventory(self):
        self.__config.get_mail_send().send_mail_for_share_inventory(self.__config, self.__df_share_inventor)

    def send_mail_for_monitor(self):
        self.__config.get_mail_send().send_mail_for_share_inventory_monitor(self.__config,
                                                                            self.__failed_pojo_chunk_list,
                                                                            self.__succeeded_pojo_chunk_list)

    def df_to_pojo_chunk_list(self, df_room_marking_price, batch_size):
        share_inventor_infos = list()
        for index, row in df_room_marking_price.iterrows():
            share_inventor_info = ShareInventorInfo(
                self.value_trim(row, "hotel_id"),
                self.value_trim(row, "room_type_id"),
                self.value_trim(row, "share_type"),
                self.value_trim(row, "share_room_type_id"),
                self.value_trim(row, "share_count"),
                self.value_trim(row, "is_valid"),
                self.value_trim(row, "valid_from"),
                self.value_trim(row, "valid_to"),
                self.value_trim(row, "channel_list").split(","))
            share_inventor_infos.append(share_inventor_info)

        # prices数组切片
        items_len = len(share_inventor_infos)
        slices_len = int(items_len / batch_size) if (items_len % batch_size) == 0 else int(items_len / batch_size) + 1
        pojo_chunk_list = [
            ShareInventoryList(share_inventor_infos[idx * batch_size:min((idx + 1) * batch_size, items_len)])
            for idx in range(slices_len)]
        return pojo_chunk_list, items_len, batch_size


# {
#     "shareInventoryList": [
#         {
#             "hotelId": 265582,
#             "isValid": 1,
#             "roomTypeId": "20",
#             "shareCount": 1,
#             "shareRoomTypeId": "26",
#             "shareType": 1,
#             "validFrom": "2019-10-30",
#             "validTo": "2019-11-06"
#             "channelList": [ "8", "9", "24"],
#         }
# }

class ShareInventorInfo(JSONSerializable):
    def __init__(self, hotel_id, room_type_id, share_type, share_room_type_id, share_count, is_valid, valid_from,
                 valid_to, channel_list):
        self.hotelId = hotel_id
        self.roomTypeId = room_type_id
        self.shareType = share_type
        self.shareRoomTypeId = share_room_type_id
        self.shareCount = share_count
        self.isValid = is_valid
        self.validFrom = valid_from
        self.validTo = valid_to
        self.channelList = channel_list


class ShareInventoryList(JSONSerializable):
    def __init__(self, share_inventory_list):
        self.shareInventoryList = share_inventory_list
