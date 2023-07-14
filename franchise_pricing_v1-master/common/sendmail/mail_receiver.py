#!/usr/bin/env python
# -*- coding:utf-8 -*-
import os
import sys
from os.path import join as join_path

cur_path = os.path.dirname(os.path.realpath(__file__))
sys.path.append(join_path(cur_path, '..'))

from common.util.utils import LogUtil

BIZ_MODEL_ID_FRANCHISE = 1
BIZ_MODEL_ID_OM = 2
BIZ_MODEL_ID_P2P = 3
BIZ_MODEL_ID_KC = 4
BIZ_MODEL_ID_CHINA2 = 5

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


class MyHash(object):

    def __init__(self, length=10):
        self.length = length
        self.items = [[] for i in range(self.length)]

    def hash(self, key):
        """计算该key在items哪个list中，针对不同类型的key需重新实现"""
        return key % self.length

    def equals(self, key1, key2):
        """比较两个key是否相等，针对不同类型的key需重新实现"""
        return key1 == key2

    def insert(self, key, value):
        index = self.hash(key)
        if self.items[index]:
            for item in self.items[index]:
                if self.equals(key, item[0]):
                    # 添加时若有已存在的key，则先删除再添加（更新value）
                    self.items[index].remove(item)
                    break
        self.items[index].append([key, value])
        return True

    def get(self, key):
        index = self.hash(key)
        if self.items[index]:
            for item in self.items[index]:
                if self.equals(key, item[0]):
                    return item[1]
        # 找不到key，则抛出KeyError异常
        # raise KeyError

    def __setitem__(self, key, value):
        """支持以 myhash[1] = 30000 方式添加"""
        return self.insert(key, value)

    def __getitem__(self, key):
        """支持以 myhash[1] 方式读取"""
        return self.get(key)


class Receiver:
    def __init__(self, email_id, email_address, business_model_id):
        self.email_id = email_id
        self.email_address = email_address
        self.business_model_id = business_model_id


class MailReceiver:

    def __init__(self, receiver_map, db_query_mgr):
        self.receiver_map = receiver_map
        self.db_query_mgr = db_query_mgr

    def get_receiver_map(self):
        return self.receiver_map

    def get_email_receiver_from_db(self, business_model_id):
        logger = LogUtil.get_cur_logger()
        receiver_query = """SELECT e.email_id, e.email_address, e.business_model_id
                FROM email_receiver e
                LEFT JOIN notify_email n
                ON e.email_id = n.email_id
                WHERE e.business_model_id = {0}
                AND e.enabled = 1
                AND e.deleted = 0
            """.format(business_model_id)

        receiver_df = self.db_query_mgr.read_sql(receiver_query)
        logger.info('email_receiver query done')
        group_by_email_id = receiver_df.groupby("email_id")
        for email_id, group in group_by_email_id:
            self.receiver_map.insert(email_id, group['email_address'].unique().tolist())

    def get_receivers_by_email_id(self, key):
        return self.get_receiver_map().get(key)
