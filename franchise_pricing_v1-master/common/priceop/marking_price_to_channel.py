#!/usr/bin/env python
# -*- coding:utf-8 -*-
import time

from pandas import DataFrame

from common.enum.pricing_enum import PriceChannel, Job
from common.priceop.api_base import ApiBase
from common.pricing_pipeline.pipeline_pojo import JSONSerializable
from common.util.utils import DFUtil, DateUtil, MiscUtil


class MarkingPrice(ApiBase):

    def __init__(self, config):
        self.__config = config
        self.__succeeded_pojo_chunk_list = list()
        self.__failed_pojo_chunk_list = list()
        self.__df_room_marking_price = DataFrame()

    def set_marking_price(self, df_room_marking_price, price_channel=PriceChannel.CHANNEL_IDS_APP):
        try:
            DFUtil.print_data_frame(df_room_marking_price, 'df_room_marking_price', True)
            df_room_marking_price["channel_ids"] = price_channel.value
            df_room_marking_price["checkin_types"] = "1"
            df_room_marking_price["date"] = DateUtil.stamp_to_date_format0(time.time())
            self.__df_room_marking_price = df_room_marking_price
            self.post_room_type_marking_price()
            self.mail_send()
            self.send_mail_for_monitor()
        except Exception:
            self.send_stack_trace("price_to_channel_rate", self.__config)

    def post_room_type_marking_price(self):
        pojo_chunk_list, prices_len, slices_size = self.get_pojo_chunk_list(self.__df_room_marking_price,
                                                                            self.__config.get_marking_price_change_batch_size(),
                                                                            biz="marking_price_post")

        # 2. POST数据
        self.__succeeded_pojo_chunk_list, self.__failed_pojo_chunk_list = self.http_request_post(
            self.__config.get_marking_price_post_url(), pojo_chunk_list, prices_len, slices_size,
            biz="marking_price_post", item1="channelThroughHotelInfos")

    # 3. send mail
    def mail_send(self):
        self.__config.get_mail_send().send_mail_for_marking_price(self.__config, self.__df_room_marking_price)

    # 4. send_mail_for_monitor
    def send_mail_for_monitor(self):
        self.__config.get_mail_send().send_mail_for_marking_price_monitor(self.__config, self.__failed_pojo_chunk_list,
                                                                          self.__succeeded_pojo_chunk_list)

    def df_to_pojo_chunk_list(self, df_room_marking_price, batch_size):
        operator_id = Job.OPERATOR_ID.value
        source = ApiBase.SOURCE_BIG_DATA
        hotel_infos = list()
        for index, row in df_room_marking_price.iterrows():
            condition_through_prices = list()
            condition_through_price = ConditionThroughPrice(
                self.value_trim(row, "price"),
                [int(x) for x in self.value_trim(row, "channel_ids").split(",")],
                [int(x) for x in self.value_trim(row, "checkin_types").split(",")],
                self.value_trim(row, "hour_room_duration"))
            condition_through_prices.append(condition_through_price)
            hotel_id = self.value_trim(row, "hotel_id")
            room_type_id = self.value_trim(row, "room_type_id")
            date = self.value_trim(row, "date")
            if MiscUtil.is_empty_value(hotel_id) or MiscUtil.is_empty_value(room_type_id) or MiscUtil.is_empty_value(
                    date):
                continue
            channel_through_hotel_info = ChannelThroughHotelInfo(
                self.value_trim(row, "hotel_id"),
                self.value_trim(row, "room_type_id"),
                self.value_trim(row, "date"),
                condition_through_prices)
            hotel_infos.append(channel_through_hotel_info)

        # prices数组切片
        items_len = len(hotel_infos)
        slices_len = int(items_len / batch_size) if (items_len % batch_size) == 0 else int(
            items_len / batch_size) + 1
        pojo_chunk_list = [
            MarkingPriceInfo(operator_id, source,
                             hotel_infos[idx * batch_size:min((idx + 1) * batch_size, items_len)])
            for idx in range(slices_len)]
        return pojo_chunk_list, items_len, batch_size


# {
#     "basePrices":[
#         "oyoId": "CN_CHA010",
#          "roomTypeId": 20,
#          "pricingDate": 2019-10-05,
#          "basePrice": 100,
#          "reasonId": 1
#     ]
# }

class ConditionThroughPrice(JSONSerializable):
    def __init__(self, price, channel_ids, checkin_types, hour_room_duration):
        self.price = price
        self.channelIds = channel_ids
        self.checkinTypes = checkin_types
        self.hourRoomDuration = hour_room_duration


class ChannelThroughHotelInfo(JSONSerializable):
    def __init__(self, hotel_id, room_type_id, date, condition_through_prices):
        self.hotelId = hotel_id
        self.roomTypeId = room_type_id
        self.date = date
        self.conditionThroughPrices = condition_through_prices


class MarkingPriceInfo(JSONSerializable):
    def __init__(self, operator_id, source, channel_through_hotel_infos):
        self.operatorId = operator_id
        self.source = source
        self.channelThroughHotelInfos = channel_through_hotel_infos
