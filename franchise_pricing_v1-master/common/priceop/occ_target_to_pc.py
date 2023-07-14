#!/usr/bin/env python
# -*- coding:utf-8 -*-

from pandas import DataFrame

from common.priceop.api_base import ApiBase
from common.pricing_pipeline.pipeline_pojo import JSONSerializable
from common.util.utils import DFUtil, MiscUtil


class OccTarget(ApiBase):
    SYSTEM_CODE = 2

    def __init__(self, config):
        self.__config = config
        self.__succeeded_pojo_chunk_list = list()
        self.__failed_pojo_chunk_list = list()
        self.__df_occ_target = DataFrame()

    def set_occ_target(self, df_occ_target):
        try:
            DFUtil.print_data_frame(df_occ_target, 'df_occ_target', True)
            self.__df_occ_target = df_occ_target
            self.post_occ_target()
            self.mail_send()
            self.send_mail_for_monitor()
        except Exception:
            self.send_stack_trace("occ_target_to_pc", self.__config)

    def post_occ_target(self):
        pojo_chunk_list, prices_len, slices_size = self.get_pojo_chunk_list(self.__df_occ_target,
                                                                            self.__config.get_occ_target_batch_size(),
                                                                            biz="occ_target_post")

        # 2. POST数据
        self.__succeeded_pojo_chunk_list, self.__failed_pojo_chunk_list = self.http_request_post(
            self.__config.get_occ_target_insert_url(), pojo_chunk_list, prices_len, slices_size,
            biz="occ_target_post", item1="items")

    # 3. send mail
    def mail_send(self):
        self.__config.get_mail_send().send_mail_for_occ_target(self.__config, self.__df_occ_target)

    # 4. send_mail_for_monitor
    def send_mail_for_monitor(self):
        self.__config.get_mail_send().send_mail_for_occ_target_monitor(self.__config, self.__failed_pojo_chunk_list,
                                                                       self.__succeeded_pojo_chunk_list)

    def df_to_pojo_chunk_list(self, df_occ_target, batch_size):
        occ_target_infos = list()
        # prices数组切片
        for index, row in df_occ_target.iterrows():
            hotel_id = self.value_trim(row, "oyo_id")
            if MiscUtil.is_empty_value(hotel_id):
                continue
            channel_through_hotel_info = OccTargetInfo(
                self.value_trim(row, "oyo_id"),
                self.value_trim(row, "week"),
                self.value_trim(row, "occ_target"))
            occ_target_infos.append(channel_through_hotel_info)

        items_len = len(occ_target_infos)
        slices_len = int(items_len / batch_size) if (items_len % batch_size) == 0 else int(
            items_len / batch_size) + 1
        pojo_chunk_list = [
            OccTargets(self.SYSTEM_CODE, occ_target_infos[idx * batch_size:min((idx + 1) * batch_size, items_len)])
            for idx in range(slices_len)]
        return pojo_chunk_list, items_len, batch_size


# {
#     "systemCode": 2,
#     "items": [
#         {
#             "oyoId": "CN_CHA010",
#              "weekdayWeekend": "WKY",
#             "occTarget": 0.75
#         },
#         {
#             "oyoId": "CN_CHA020",
#              "weekdayWeekend": "WKY",
#             "occTarget": 0.95
#         }
#     ]
# }

class OccTargetInfo(JSONSerializable):
    def __init__(self, oyo_id, weekday_weekend, occ_target):
        self.oyoId = oyo_id
        self.weekdayWeekend = weekday_weekend
        self.occTarget = occ_target


class OccTargets(JSONSerializable):
    def __init__(self, system_code, occ_target_info):
        self.systemCode = system_code
        self.items = occ_target_info
