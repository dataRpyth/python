#!/usr/bin/env python
# -*- coding:utf-8 -*-
from pandas import DataFrame

from common.enum.pricing_enum import Job, PriceChannel
from common.priceop.api_base import ApiBase
from common.pricing_pipeline.pipeline_pojo import JSONSerializable
from common.sendmail.mail_sender import MailSender
from common.util.utils import *

SOURCE_BIG_DATA = 0


class ChannelRate(ApiBase):

    def __init__(self, config):
        self.config = config
        self.__df_price_to_channel_rate = DataFrame()
        self.__succeeded_pojo_chunk_list = list()
        self.__failed_pojo_chunk_list = list()

    @staticmethod
    def price_to_channel_rate(config, df_price_to_channel_rate, price_channel: PriceChannel):
        try:
            if not config.get_toggle_on_price_to_channel_rate() or df_price_to_channel_rate.empty:
                return
            LogUtil.get_cur_logger().info("start post prices to channel rate, channel_ids: %s, size: %s",
                                          price_channel.value, df_price_to_channel_rate.shape[0])
            df_price_to_channel_rate["channel_ids"] = price_channel.value
            df_price_to_channel_rate["pay_types"] = "0,1,2"
            df_price_to_channel_rate["control_type"] = 1
            df_price_to_channel_rate["priority"] = 0
            df_price_to_channel_rate["checkin_types"] = "1"
            channel_rate = ChannelRate(config)
            channel_rate.__df_price_to_channel_rate = df_price_to_channel_rate
            channel_rate.post_price_to_channel_rate()
            channel_rate.send_mail_for_monitor()
        except Exception:
            ApiBase.send_stack_trace("price_to_channel_rate", config)

    @staticmethod
    def hourly_room_price_to_channel_rate(config, df_price_to_channel_rate, price_channel=PriceChannel):
        try:
            if not config.get_toggle_on_price_to_channel_rate() or df_price_to_channel_rate.empty:
                return
            LogUtil.get_cur_logger().info("start post hourly_room prices to channel rate, channel_ids: %s, size: %s",
                                          price_channel.value, df_price_to_channel_rate.shape[0])
            df_price_to_channel_rate["channel_ids"] = price_channel.value
            df_price_to_channel_rate["pay_types"] = "0,1,2"
            df_price_to_channel_rate["control_type"] = 1
            df_price_to_channel_rate["priority"] = 0
            df_price_to_channel_rate["checkin_types"] = "2"
            channel_rate = ChannelRate(config)
            channel_rate.__df_price_to_channel_rate = df_price_to_channel_rate
            channel_rate.post_price_to_channel_rate()
            channel_rate.send_mail_for_monitor()
        except Exception:
            ApiBase.send_stack_trace("price_to_channel_rate", config)

    def post_price_to_channel_rate(self):
        # 1. 准备数据
        pojo_chunk_list, prices_len, slices_size = self.get_pojo_chunk_list(self.__df_price_to_channel_rate,
                                                                            self.config.get_channel_hotel_infos_batch_size(),
                                                                            biz="price_to_channel_rate_post")
        # 2. POST数据
        self.__succeeded_pojo_chunk_list, self.__failed_pojo_chunk_list = self.http_request_post(
            self.config.get_price_to_channel_rate_url(), pojo_chunk_list, prices_len,
            slices_size, biz="price_to_channel_rate", item1="channelHotelInfos")

    def send_mail_for_monitor(self):
        self.send_mail_for_price_to_channel_rate_monitor(self.config, self.__succeeded_pojo_chunk_list,
                                                         self.__failed_pojo_chunk_list)

    def df_to_pojo_chunk_list(self, df_special_sale, batch_size):
        prices = list()
        channel_hotel_infos = list()
        channel_rate_info_chunk_list = list()
        sku = None
        channel_ids_len = 1
        df_special_sale.sort_values(axis=0, by=['date', 'oyo_id', 'room_type_id'], inplace=True)
        df_special_sale.reset_index(drop=True, inplace=True)
        len_df_hotel_special_sale = len(df_special_sale)
        idx = 0
        while idx <= len_df_hotel_special_sale:
            if idx == len_df_hotel_special_sale:
                self.pre_channel_hotel_info_add(channel_hotel_infos, df_special_sale, idx, prices)
                break
            row = df_special_sale.loc[idx]
            hotel_id = int(self.value_trim(row, "hotel_id"))
            room_type_id = int(self.value_trim(row, "room_type_id"))
            date = self.value_trim(row, "date")
            channel_ids = self.value_arr_trim(row, "channel_ids")
            channel_ids_len = len(channel_ids)
            sku0 = join_path(str(hotel_id), str(room_type_id), str(date))
            sku = (sku0 if sku is None else None)
            if sku != sku0:
                self.pre_channel_hotel_info_add(channel_hotel_infos, df_special_sale, idx, prices)
                prices.clear()
                sku = sku0

            price = ConditionPrice(
                self.value_trim(row, "pms_price"),
                channel_ids,
                self.value_arr_trim(row, "checkin_types"),
                self.value_arr_trim(row, "pay_types"),
                int(self.value_trim(row, "control_type")),
                int(self.value_trim(row, "priority")),
                self.value_trim(row, "hour_room_duration"))
            prices.append(price)
            idx += 1

        # channel_hotel_infos切片
        channel_hotel_infos_len = len(channel_hotel_infos)
        operator_id = Job.OPERATOR_ID.value
        source = SOURCE_BIG_DATA
        slices_size = int(batch_size / channel_ids_len)
        slices_len = int(channel_hotel_infos_len / slices_size) + 1 \
            if (channel_hotel_infos_len % slices_size) != 0 else int(channel_hotel_infos_len / slices_size)
        for slice_index in range(slices_len):
            right_max = min((slice_index + 1) * slices_size, channel_hotel_infos_len)
            channel_rate_info_chunk_list.append(
                ChannelRateInfo(operator_id, source, channel_hotel_infos[slice_index * slices_size:right_max]))
        return channel_rate_info_chunk_list, channel_hotel_infos_len, slices_size

    def pre_channel_hotel_info_add(self, channel_hotel_infos, df_hotel_special_sale, idx, prices):
        row = df_hotel_special_sale.loc[idx - 1]
        hotel_id = int(self.value_trim(row, "hotel_id"))
        room_type_id = int(self.value_trim(row, "room_type_id"))
        date = self.value_trim(row, "date")
        channel_hotel_infos.append(
            ChannelHotelInfo(hotel_id, room_type_id, date, prices.copy()))

    def send_mail_for_price_to_channel_rate_monitor(self, config, succeeded_pojo_chunk_list, failed_pojo_chunk_list):
        logger = LogUtil.get_cur_logger()
        try:
            biz_name = config.get_job().get_job_name()
            robot_token = config.get_robot_send_token()
            begin_time = time.time()
            time_finished = DateUtil.stamp_to_date_format3(begin_time)
            time_file_name = DateUtil.stamp_to_date_format(begin_time, "%Y_%m_%d_%H_%M_%S")
            fail_count = CommonUtil.get_count_for_chunk_list_attr(failed_pojo_chunk_list, "channelHotelInfos")
            suc_count = CommonUtil.get_count_for_chunk_list_attr(succeeded_pojo_chunk_list, "channelHotelInfos")

            succeeded_report_df = self.get_report_df_from_pojo_chunk_list(biz_name, succeeded_pojo_chunk_list,
                                                                          time_finished)
            failed_report_df = self.get_report_df_from_pojo_chunk_list(biz_name, failed_pojo_chunk_list, time_finished)
            multi_sheets = [DataFrameWithSheetName(succeeded_report_df, '上传成功'),
                            DataFrameWithSheetName(failed_report_df, '上传失败')]

            attach_name = '{0}_price_to_channel_rate_report_{1}.xlsx'.format(biz_name, time_file_name)
            sub = "{0} price data to channel rate report {1}".format(biz_name, time_file_name)
            if fail_count > 0:
                DdtUtil.robot_send_ddt_msg(
                    '业务线: {0}, 渠道价数据导入出现异常, 成功:{1}, 失败:{2}, 时间:{3}'.format(biz_name, suc_count, fail_count,
                                                                           time_finished), robot_token)
                failed_report_df.reset_index(drop=True, inplace=True)
            head = "业务线: {0}, 渠道价数据导入完成, 共:{1}, 成功:{2}, 失败:{3}, 时间:{4}".format(
                biz_name, str(suc_count + fail_count), str(suc_count), str(fail_count), time_finished)
            mail_content = DFUtil.gen_excel_content_by_html(head, failed_report_df)
            MailSender.send_for_multi_sheet(config, config.get_price_log_post_result_receivers(), sub, mail_content,
                                            multi_sheets, attach_name, head, True)
        except Exception:
            logger.exception("unable to send_mail_for_price_to_channel_rate_monitor")

    def get_report_df_from_pojo_chunk_list(self, biz_name, pojo_chunk_list, upload_time):
        if len(pojo_chunk_list) == 0:
            return pd.DataFrame()
        row_dict_list = list()
        for pojo_chunk in pojo_chunk_list:
            channel_hotel_infos = pojo_chunk.channelHotelInfos
            for hotel in channel_hotel_infos:
                for price in hotel.conditionPrices:
                    row_dict = {'bizName': biz_name, 'hotelId': hotel.hotelId, 'roomTypeId': hotel.roomTypeId,
                                'date': hotel.date, 'price': price.price, 'channelIds': price.channelIds,
                                'checkinTypes': price.checkinTypes, 'payTypes': price.payTypes,
                                'controlType': price.controlType, 'priority': price.priority, 'uploadTime': upload_time}
                    row_dict_list.append(row_dict)
        report_df = pd.DataFrame(row_dict_list)
        report_df.rename(
            columns={'bizName': '项目名称', 'oyoId': 'CRS ID', 'hotelName': '酒店名称', 'otaNames': '改价平台',
                     'uploadTime': '导入时间'}, inplace=True)
        return report_df

    def get_post_timeout(self):
        return 60


# {
#     "channelHotelInfos": [
#         {
#             "hotelId": 1,
#             "roomTypeId": 1,
#             "date": "2019-06-07",
#             "conditionPrices": [
#                 {
#                     "priority": 0,
#                     "controlType":0,
#                     "price":7,
#                     "channelIds":[1,2,3],
#                     "checkinTypes":[1],
#                     "payTypes":[1]
#                 }
#             ]
#         }
#     ],
#     "operatorId": 111,
#     "source": 0
# }

class ConditionPrice(JSONSerializable):
    def __init__(self, price, channel_ids, checkin_types, pay_types, control_type, priority, hour_room_duration):
        self.price = price
        self.channelIds = channel_ids  # 渠道id list
        self.checkinTypes = checkin_types  # 入住类型list  1:全天房  2:钟点房
        self.payTypes = pay_types  # 支付类型list  0：现付  ﻿1：预付  2：信用住
        self.controlType = control_type  # 控制类型Integer   0: 渠道基准价  1: 销售终端价格
        self.priority = priority  # 价格优先级Integer  大数据:0
        self.hourRoomDuration = hour_room_duration  # 钟点房持续时间(单位：h)+当checkinTypes有2时必传


class ChannelHotelInfo(JSONSerializable):
    def __init__(self, hotel_id, room_type_id, date, condition_prices):
        self.hotelId = hotel_id  # Long
        self.roomTypeId = room_type_id  # Long
        self.date = date  # yyyy-MM-dd
        self.conditionPrices = condition_prices  # List


class ChannelRateInfo(JSONSerializable):
    def __init__(self, operator_id, source, channel_hotel_infos):
        self.operatorId = operator_id  # Long 操作人ID
        self.source = source  # 0. 大数据 1. OTA
        self.channelHotelInfos = channel_hotel_infos  # List
