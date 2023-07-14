#!/usr/bin/env python
# -*- coding:utf-8 -*-
from enum import Enum


class Job(Enum):
    OPERATOR_ID = 16067


class PriceChannel(Enum):
    CHANNEL_IDS_ALL = "1,2,3,4,5,6,7,8,9,10,11,19,20,21,22,24,26,27,28"  # 全量
    CHANNEL_IDS_APP = "8,9,24"  # APP
    CHANNEL_IDS_NON_APP = "1,2,3,4,5,6,7,10,11,19,20,21,22,26,27,28"  # 非APP
    CHANNEL_IDS_DIRECT_OTA = "2,20,22,26,27,28"
    CHANNEL_IDS_DIRECT_OTA_AND_APP = "8,9,24,2,20,22,26,27,28"
    CHANNEL_IDS_WALKIN = "1"
    CHANNEL_IDS_WALKIN_AND_APP = "1,8,9,24"
    CHANNEL_IDS_WALKIN_AND_APP_DIRECT_OTA = "1,8,9,24,2,20,22,26,27,28"


class BizModule(Enum):
    BIZ_MODEL_ID_FRANCHISE = 1
    BIZ_MODEL_ID_OM = 2
    BIZ_MODEL_ID_P2P = 3
    BIZ_MODEL_ID_KC = 4
    BIZ_MODEL_ID_CHINA2 = 5


class EmailReceiver(Enum):
    EMAIL_ID_3001 = 3001
    EMAIL_ID_3002 = 3002
    EMAIL_ID_3003 = 3003
    EMAIL_ID_3004 = 3004
    EMAIL_ID_3005 = 3005
    EMAIL_ID_3006 = 3006
    EMAIL_ID_3007 = 3007
    EMAIL_ID_3008 = 3008
    EMAIL_ID_3009 = 3009
    EMAIL_ID_3010 = 3010
    EMAIL_ID_3011 = 3011
    EMAIL_DES_3001 = 'pricing_log数据上报结果'
    EMAIL_DES_3002 = 'OTA改价CC人工干预结果'
    EMAIL_DES_3003 = '中间计算结果汇总'
    EMAIL_DES_3004 = '门市价结果写入汇总'
    EMAIL_DES_3005 = 'CRS改价结果写入汇总'
    EMAIL_DES_3006 = 'OTA改价插件导入数据'
    EMAIL_DES_3007 = '尾房甩卖人工中间结果'
    EMAIL_DES_3008 = 'ota_plugin_OTA改价CC人工干预结果'
    EMAIL_DES_3009 = 'ota_plugin_中间计算结果汇总'
    EMAIL_DES_3010 = 'ota_plugin_门市价结果写入汇总'
    EMAIL_DES_3011 = '尾房自动化甩卖酒店列表结果'
