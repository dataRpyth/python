#!/usr/bin/env python
# -*- coding:utf-8 -*-

import os
import sys
from os.path import join as join_path

cur_path = os.path.dirname(os.path.realpath(__file__))
sys.path.append(join_path(cur_path, '..'))

from common.util.utils import LogUtil
from common.dingtalk_sdk.sdk_core.config import DdtDiskSdkConf
from common.dingtalk_sdk.jvm_wrapper import DdtJvmWrapper


def init_jvm_for_ddt_disk():
    DdtJvmWrapper.init_clazz_map(DdtDiskSdkConf.get_jar_path(), DdtDiskSdkConf.get_class_list())
    LogUtil.get_cur_logger().info('jvm for ddt disk initialized')


class DingTalkPy:

    def __init__(self):
        self.ddt_jvm_wrapper = DdtJvmWrapper(DdtDiskSdkConf.get_jar_path(), DdtDiskSdkConf.get_class_list())

    def cspace_add_to_singlechat(self, receiver_code, _file_path, _env):
        '''
            OtaPrices(2011, "OtaPrices"),
            FinalResult(2012, "FinalResult"),
            FinalResultReceiversDept(2013, "FinalResultReceiversDept"),
            HotelPriceData(2014, "HotelPriceData"),
            PriceInsertResult(2015, "PriceInsertResult"),
            PriceLogPostResult(2016, "PriceLogPostResult"),
            PmsMail(2017, "PmsMail"),
            IntermediateResult(2018, "IntermediateResult"),
            CcResult(2019, "CcResult"),
            PmsInsertResult(2020, "PmsInsertResult"),
            OtaPluginResult(2021, "OtaPluginResult"),
            OtaPluginPmsResult(2022, "OtaPluginPmsResult"),
            OtaPluginIntermediateResult(2023, "OtaPluginIntermediateResult"),
            OtaPluginCcResult(2024, "OtaPluginCcResult"),
            OtaPluginOtaPrices(2025, "OtaPluginOtaPrices"),
            OtaPluginFinalResult(2026, "OtaPluginFinalResult"),
            OtaPluginHotelPriceData(2027, "OtaPluginHotelPriceData"),
            FranchiseUpdatingPrice(2028, "FranchiseUpdatingPrice");
            --------------
            Dev("dev"),
            Local("local"),
            Test("test"),
            Uat("uat"),
            Prod("prod");
        '''
        if _file_path is None or receiver_code is None:
            return
        try:
            self.ddt_jvm_wrapper.get_class(DdtDiskSdkConf.get_class_cspace_app()).cspaceAddToSingleChat(receiver_code,
                                                                                                _file_path, _env)
        except:
            LogUtil.get_cur_logger().exception('DingTalk email sending exception and failed!')

    def robot_send(self, _token, _job_name, _biz_model_id, _pricing_timestamp, _file_path, _text, _env):
        try:
            if _token is None or _file_path is None or _job_name is None or _file_path is None or _env is None:
                LogUtil.get_cur_logger().warn('DingTalkPy.robot_send, Parameter error')
                return
            self.ddt_jvm_wrapper.get_class(DdtDiskSdkConf.get_class_cspace_app()).robotSend(_token, _job_name, _biz_model_id,
                                                                                    str(_pricing_timestamp), _file_path,
                                                                                    _text, _env)
            LogUtil.get_cur_logger().info('DingTalkPy.robot_send successful')
        except:
            LogUtil.get_cur_logger().exception('DingTalkPy.robot_send>>>token: {0}, job_name: {1}, biz_model_id:{2}, '
                                               'pricing_date: {3}, file_path: {4}, text: {5}, env: {6}'.format(
                _token, _job_name, _biz_model_id, _pricing_timestamp, _file_path, _text, _env))
            LogUtil.get_cur_logger().exception('DingTalk robot_send exception and failed!')


if __name__ == '__main__':
    logger = LogUtil.create_logger(join_path(cur_path, 'log'), 'log.txt')
    LogUtil.set_cur_logger(logger)
    token = "da906d9d5014c7f8a327038a00f5fd52b3c265de66d3b75bd22dd0760875d6a6"
    token = "a6e89792a9b40c383d8fcc2937b3fc35f46c6428ca6a142669710ccf4ea8cd2c"
    biz_model_id = "2"
    job_name = "KC_daily"
    pricing_date = "1557720000"
    # file_path = "/Users/oyo/Downloads/KC_hotel_preset_price_2019_05_13_10_19.xls"
    file_path = "/Users/oyo/Documents/pricing-center系统操作手册_V1.0.docx"
    text = "this is a unit test for KC_hotel_preset_price from franchise_pricing_v1"
    env = "uat"
    # (token, jobName, bizModelId, pricingDate, filePath, text, env)
    DingTalkPy().robot_send(token, job_name, biz_model_id, pricing_date, file_path, text, env)
    job_name = "ChinaRebirth"
    pricing_date = "1557720000"
    # file_path = "/Users/oyo/Downloads/ChinaRebirth_hotel_price_2019_05_13_13_26.xls"
    file_path = "/Users/oyo/Documents/OYO业务pricingcenter项目架构设计.pptx"
    text = "this is a unit test for ChinaRebirth_hotel_price from franchise_pricing_v1"
    DingTalkPy().robot_send(token, job_name, biz_model_id, pricing_date, file_path, text, env)
    exit()
