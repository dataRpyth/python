#!/usr/bin/env python
# -*- coding:utf-8 -*-

from pandas import DataFrame

from common.priceop.api_base import ApiBase
from common.pricing_pipeline.pipeline_pojo import JSONSerializable
from common.util.utils import DFUtil, MiscUtil


class MetricsTarget(ApiBase):
    SYSTEM_CODE = 2

    def __init__(self, config):
        self.__config = config
        self.__succeeded_pojo_chunk_list = list()
        self.__failed_pojo_chunk_list = list()
        self.__df_metrics_target = DataFrame()

    def set_and_post_metrics_target(self, df_metrics_target):
        try:
            DFUtil.print_data_frame(df_metrics_target, 'df_metrics_target', True)
            self.__df_metrics_target = df_metrics_target
            self.post_metrics_target()
        except Exception:
            self.send_stack_trace("metrics_target_to_pc", self.__config)

    def post_metrics_target(self):
        pojo_chunk_list, prices_len, slices_size = self.get_pojo_chunk_list(self.__df_metrics_target,
                                                                            self.__config.get_metrics_target_batch_size(),
                                                                            biz="metrics_target_post")

        # 2. POST数据
        self.__succeeded_pojo_chunk_list, self.__failed_pojo_chunk_list = self.http_request_post(
            self.__config.get_metrics_target_insert_url(), pojo_chunk_list, prices_len, slices_size,
            biz="metrics_target_post", item1="items")

    # 3. send mail
    def mail_send(self):
        self.__config.get_mail_send().send_mail_for_metrics_target(self.__config, self.__df_metrics_target)

    # 4. send_mail_for_monitor
    def send_mail_for_monitor(self):
        self.__config.get_mail_send().send_mail_for_metrics_target_monitor(self.__config, self.__failed_pojo_chunk_list,
                                                                           self.__succeeded_pojo_chunk_list)

    def df_to_pojo_chunk_list(self, df_metrics_target, batch_size):
        metrics_target_infos = list()
        # prices数组切片
        for index, row in df_metrics_target.iterrows():
            hotel_id = self.value_trim(row, "oyo_id")
            if MiscUtil.is_empty_value(hotel_id):
                continue
            channel_through_hotel_info = MetricsTargetInfo(
                self.value_trim(row, "metrics_value"),
                self.value_trim(row, "oyo_id"),
                self.value_trim(row, "data_date"),
                self.value_trim(row, "hour"),
                self.value_trim(row, "metrics_id"),
                self.value_trim(row, "metrics_source_id"),
                self.value_trim(row, "metrics_details"),
                self.value_trim(row, "last_update"),
                self.value_trim(row, "priority"))
            metrics_target_infos.append(channel_through_hotel_info)

        items_len = len(metrics_target_infos)
        slices_len = int(items_len / batch_size) if (items_len % batch_size) == 0 else int(
            items_len / batch_size) + 1
        pojo_chunk_list = [
            MetricsTargets(self.SYSTEM_CODE,
                           metrics_target_infos[idx * batch_size:min((idx + 1) * batch_size, items_len)])
            for idx in range(slices_len)]
        return pojo_chunk_list, items_len, batch_size

    def get_df_metrics_target(self):
        return self.__df_metrics_target

    def set_df_metrics_target(self, df_metrics_target):
        self.__df_metrics_target = df_metrics_target

    def get_sued_pojo_chunk_list(self):
        return self.__succeeded_pojo_chunk_list

    def set_sued_pojo_chunk_list(self, succeeded_pojo_chunk_list):
        self.__succeeded_pojo_chunk_list = succeeded_pojo_chunk_list

    def get_failed_pojo_chunk_list(self):
        return self.__failed_pojo_chunk_list

    def set_failed_pojo_chunk_list(self, failed_pojo_chunk_list):
        self.__failed_pojo_chunk_list = failed_pojo_chunk_list


# 1）请求items.size()不能大于500
# 2）一次请求dataDate、 hour、metricsId值一定是同一个值

# {
#     "systemCode": 2,
#     "items":[
#         {
#         "metricsValue":0.75,
#         "oyoId":"CN_CHA010",
#         "dataDate":2020-03-07,
#         "hour":10,
#         "metricsId":1,
#         "metricsSourceId":"metricsSourceId"
#         "metricsDetails":"价格弹性"
#         "lastUpdate":"2020-03-07"
#         "priority":1
#         }
#     ]
# }

class MetricsTargetInfo(JSONSerializable):
    def __init__(self, metrics_value, oyo_id, data_date, hour, metrics_id, metrics_source_id, metrics_details, last_update, priority):
        self.metricsValue = metrics_value
        self.oyoId = oyo_id
        self.dataDate = data_date
        self.hour = hour
        self.metricsId = metrics_id
        self.metricsSourceId = metrics_source_id
        self.metricsDetails = metrics_details
        self.lastUpdate = last_update
        self.priority = priority


class MetricsTargets(JSONSerializable):
    def __init__(self, system_code, metrics_target_info):
        self.systemCode = system_code
        self.items = metrics_target_info
