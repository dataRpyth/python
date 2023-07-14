#!/usr/bin/env python
# -*- coding:utf-8 -*-

from __future__ import unicode_literals

import configparser
import datetime
import json
import logging
import math
import multiprocessing
import numbers
import os
import sys
import threading
import time
import traceback
from functools import reduce
from math import ceil
from os.path import join as join_path

import pandas as pd
import requests
from openpyxl import Workbook
from openpyxl.utils.dataframe import dataframe_to_rows
from sqlalchemy import types, create_engine
from tabulate import tabulate

_LOG_FORMAT = "%(threadName)s - %(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s"
_ROBOT_ALERT_HEADERS = {'Content-Type': 'application/json; charset=utf-8'}
POST_PRE_PRICING_RATIO = 1.1
DEFAULT_NULL_VALUE = ''

FLOOR_PRICE_TYPE_NUMBER = 1

FLOOR_PRICE_TYPE_RATIO = 2

CEILING_PRICE_TYPE_NUMBER = 1

CEILING_PRICE_TYPE_RATIO = 2


class DdtUtil:

    @staticmethod
    def _compose_ddt_post_data(msg, at_mobiles, is_at_all):
        at_dict = None
        if at_mobiles is not None and is_at_all is not None:
            at_dict = {"atMobiles": at_mobiles, "isAtAll": is_at_all}
        elif at_mobiles is not None:
            at_dict = {"atMobiles": at_mobiles}
        elif is_at_all is not None:
            at_dict = {"isAtAll": is_at_all}
        else:
            pass
        if at_dict is not None:
            return {"msgtype": "text", "text": {"content": msg},
                    "at": at_dict}
        else:
            return {"msgtype": "text", "text": {"content": msg}}

    @staticmethod
    def robot_send_ddt_msg(msg, access_token, mobiles=None, is_at_all=True):
        url = 'https://oapi.dingtalk.com/robot/send?access_token={0}'.format(access_token)
        d = DdtUtil._compose_ddt_post_data(msg, mobiles, is_at_all)
        try:
            requests.post(url=url, headers=_ROBOT_ALERT_HEADERS, data=json.dumps(d))
        except Exception:
            LogUtil.get_cur_logger().exception('exception when sending ddt msg')


class TimeoutError(AssertionError):
    """Thrown when a timeout occurs in the `timeout` context manager."""

    def __init__(self, value="Timed Out"):
        self.value = value

    def __str__(self):
        return repr(self.value)


def _raise_exception(exception, exception_message):
    if exception_message is None:
        raise exception()
    else:
        raise exception(exception_message)


def _target(queue, function, *args, **kwargs):
    try:
        queue.put((True, function(*args, **kwargs)))
    except:
        queue.put((False, sys.exc_info()[1]))


class _Timeout(object):

    def __init__(self, function, timeout_exception, exception_message, limit):
        self.__limit = limit
        self.__function = function
        self.__timeout_exception = timeout_exception
        self.__exception_message = exception_message
        self.__name__ = function.__name__
        self.__doc__ = function.__doc__
        self.__timeout = time.time()
        self.__process = multiprocessing.Process()
        self.__queue = multiprocessing.Queue()

    def __call__(self, *args, **kwargs):
        self.__limit = kwargs.pop('timeout', self.__limit)
        self.__queue = multiprocessing.Queue(1)
        args = (self.__queue, self.__function) + args
        self.__process = multiprocessing.Process(target=_target,
                                                 args=args,
                                                 kwargs=kwargs)
        self.__process.daemon = True
        self.__process.start()
        self.__timeout = self.__limit + time.time()
        while not self.ready:
            time.sleep(0.01)
        return self.value

    def cancel(self):
        if self.__process.is_alive():
            self.__process.terminate()

        _raise_exception(self.__timeout_exception, self.__exception_message)

    @property
    def ready(self):
        if self.__timeout < time.time():
            self.cancel()
        return self.__queue.full() and not self.__queue.empty()

    @property
    def value(self):
        if self.ready is True:
            flag, load = self.__queue.get()
            if flag:
                return load
            raise load


class CommonUtil:
    class TimeoutError(Exception):
        pass

    @staticmethod
    def call_func_with_timeout_or_error_fallback(func, args=(), kwargs={}, timeout_in_seconds=1, fallback=None,
                                                 timeout_handler=None,
                                                 handler_args=(), handler_kwargs={}):
        is_main_thread = threading.current_thread() is threading.main_thread()
        if is_main_thread:
            return CommonUtil.call_func_with_timeout_or_error_fallback_using_signal(func, args, kwargs,
                                                                                    timeout_in_seconds, fallback,
                                                                                    timeout_handler, handler_args,
                                                                                    handler_kwargs)
        else:
            return CommonUtil.call_func_with_timeout_or_error_fallback_using_multiprocessing(func, args, kwargs,
                                                                                             timeout_in_seconds,
                                                                                             fallback,
                                                                                             timeout_handler,
                                                                                             handler_args,
                                                                                             handler_kwargs)

    @staticmethod
    def call_func_with_timeout_or_error_fallback_using_signal(func, args=(), kwargs={}, timeout_in_seconds=1,
                                                              fallback=None,
                                                              timeout_handler=None,
                                                              handler_args=(), handler_kwargs={}):
        import signal

        def default_timeout_handler(signum, frame):
            raise TimeoutError()

        signal.signal(signal.SIGALRM, default_timeout_handler)
        signal.alarm(timeout_in_seconds)
        try:
            result = func(*args, **kwargs)
        except (TimeoutError, Exception) as e:
            if e.__class__ == TimeoutError:
                msg = 'function {0} called time out for {1} seconds'.format(func.__name__, timeout_in_seconds)
            else:
                msg = 'function {0} called but exception was thrown: {1}'.format(func.__name__, traceback.format_exc())
            LogUtil.get_cur_logger().warn(msg)
            result = fallback
            if timeout_handler is not None:
                timeout_handler(*handler_args, **handler_kwargs)
        finally:
            signal.alarm(0)
        return result

    @staticmethod
    def call_func_with_timeout_or_error_fallback_using_multiprocessing(func, args=(), kwargs={}, timeout_in_seconds=1,
                                                                       fallback=None,
                                                                       timeout_handler=None,
                                                                       handler_args=(), handler_kwargs={}):
        new_func = _Timeout(func, TimeoutError, None, timeout_in_seconds)
        try:
            result = new_func(*args, **kwargs)
        except (TimeoutError, Exception) as e:
            if e.__class__ == TimeoutError:
                msg = 'function {0} called time out for {1} seconds'.format(func.__name__, timeout_in_seconds)
            else:
                msg = 'function {0} called but exception was thrown: {1}'.format(func.__name__, traceback.format_exc())
            LogUtil.get_cur_logger().warn(msg)
            result = fallback
            if timeout_handler is not None:
                timeout_handler(*handler_args, **handler_kwargs)
        return result

    @staticmethod
    def get_count_for_chunk_list_attr(pojo_chunk_list, attr):
        if len(pojo_chunk_list) == 0:
            return 0
        return reduce(lambda x, y: x + y, list(map(lambda x: len(x.__getattribute__(attr)), pojo_chunk_list)))


class LogUtil:
    _CUR_LOGGER = None

    @staticmethod
    def create_logger(log_folder_path, log_file_name):
        log_file_path = join_path(log_folder_path, log_file_name)
        log_formatter = logging.Formatter(_LOG_FORMAT)
        logger = logging.getLogger()
        logger.setLevel(logging.INFO)

        cnosole_logger = logging.StreamHandler()
        cnosole_logger.setLevel(logging.INFO)
        cnosole_logger.setFormatter(log_formatter)
        logger.addHandler(cnosole_logger)

        if not os.path.exists(log_folder_path):
            os.makedirs(log_folder_path, 0o777, True)
        file_logger = logging.FileHandler(log_file_path)
        file_logger.setLevel(logging.DEBUG)
        file_logger.setFormatter(log_formatter)
        logger.addHandler(file_logger)
        return logger

    @staticmethod
    def set_cur_logger(logger):
        LogUtil._CUR_LOGGER = logger

    @staticmethod
    def get_cur_logger():
        return LogUtil._CUR_LOGGER

    @staticmethod
    def init_cur_logger(cur_path, folder, name=None):
        name = DateUtil.stamp_to_date_format(time.time(), "%Y_%m_%d_%H" + ".log") if name==None else name
        logger = LogUtil.create_logger(join_path(cur_path, folder), name)
        LogUtil.set_cur_logger(logger)
        return LogUtil._CUR_LOGGER


class AlertUtil:

    @staticmethod
    def send_stack_trace_msg(msg, access_token, mobiles=None, is_at_all=True):
        try:
            stack_trace = traceback.format_exc()
            LogUtil.get_cur_logger().error(stack_trace)
            DdtUtil.robot_send_ddt_msg(
                'Error when {}, stack trace: {}'.format(msg, stack_trace),
                access_token, mobiles, is_at_all)
        except:
            pass


class DBUtil:

    @staticmethod
    def create_mysql_engine(username, password, host, port, dbname):
        con_string = 'mysql+pymysql://{0}:{1}@{2}:{3}/{4}'.format(username, password, host, port, dbname)
        return create_engine(con_string)

    @staticmethod
    def create_oracle_engine(username, password, host, port, dbname):
        con_string = 'oracle+cx_oracle://{0}:{1}@{2}:{3}/{4}'.format(username, password, host, port, dbname)
        return create_engine(con_string)


class DataFrameWithSheetName:

    def __init__(self, data_frame, sheet_name):
        self.data_fame = data_frame
        self.sheet_name = sheet_name

    def get_data_frame(self):
        return self.data_fame

    def get_sheet_name(self):
        return self.sheet_name


class DFUtil:
    @staticmethod
    def print_data_frame(df, df_name=None, is_debug=False, head_count=100):
        if not is_debug:
            return
        if df_name is not None:
            LogUtil.get_cur_logger().info(
                'data frame: {0}, rows: {1}, columns: {2}'.format(df_name, df.shape[0], df.shape[1]))
        LogUtil.get_cur_logger().info(tabulate(df.head(head_count), headers='keys', tablefmt='psql'))

    @staticmethod
    def is_invalid(value):
        return MiscUtil.is_empty_value(value)

    @staticmethod
    def add_columns_for_empty_data_frame(df, columns_to_add):
        return pd.DataFrame(columns=df.columns.tolist() + columns_to_add)

    @staticmethod
    def apply_func_for_df(df, tgt_column, src_columns, func):
        if df.shape[0] <= 0:
            # data frame 为空，添加一个空列
            if tgt_column in df.columns:
                return df
            else:
                return DFUtil.add_columns_for_empty_data_frame(df, [tgt_column])
        else:
            df[tgt_column] = df[src_columns].apply(func, axis=1)
            return df

    @staticmethod
    def gen_excel_content_by_html(head, attach_file=None):
        mail_content = """
                         <html>
                              <head>
                                  {0}
                              </head>
                              <body>
                                     {1}
                              </body>
                         </html>
                         """.format(head, attach_file.to_html(
            float_format=lambda x: '%.3f' % x) if attach_file is not None else "")
        return mail_content

    @staticmethod
    def save_to_openpyxl(df, file_name):
        wb = Workbook()
        ws = wb.active
        for row in dataframe_to_rows(df, index=False, header=True):
            ws.append(row)
        wb.save(file_name)

    @staticmethod
    def check_duplicate(df, df_name, subset, fail_on_duplicate=False):
        rows = df.shape[0]
        df = df.drop_duplicates(subset)
        new_rows = df.shape[0]
        rows_diff = new_rows - rows
        if rows_diff > 0:
            error_msg = 'DataFrame: {0} has duplicated rows, original row count: {1}, ' \
                        'row count after duplicated rows dropped: {2}, ' \
                        'duplicated rows count: {4}'.format(
                df_name, str(rows), str(new_rows), str(rows_diff))
            if fail_on_duplicate:
                raise Exception(error_msg)
            else:
                LogUtil.get_cur_logger().warning(error_msg)
        return df

    @staticmethod
    def write_multiple_df_to_excel(excel_file_path, data_frame_with_sheet_name_list):
        with pd.ExcelWriter(excel_file_path) as writer:
            for data_frame_with_sheet_name in data_frame_with_sheet_name_list:
                data_frame_with_sheet_name.get_data_frame().to_excel(writer,
                                                                     sheet_name=data_frame_with_sheet_name.get_sheet_name())
        return excel_file_path

    @staticmethod
    def get_item_from_row(row, index, fallback=None):
        if index not in row.index:
            return fallback
        return row[index]

    @staticmethod
    def get_oracle_auxiliary_insert_dtype_for_df(df):
        return {c: types.VARCHAR(df[c].str.len().max())
                for c in df.columns[df.dtypes == 'object'].tolist()}


class ConfigFileUtil:

    @staticmethod
    def create_config_from_file(config_file_path):
        config = configparser.ConfigParser()
        config.read(config_file_path)
        return config


class EnvUtil:

    @staticmethod
    def is_local_env(env):
        return 'local' == env

    @staticmethod
    def is_dev_env(env):
        return 'dev' == env

    @staticmethod
    def is_test_env(env):
        return 'test' == env

    @staticmethod
    def is_uat_env(env):
        return 'uat' == env

    @staticmethod
    def is_prod_env(env):
        return 'prod' == env

    @staticmethod
    def is_debug(env):
        return not EnvUtil.is_prod_env(env)

    @staticmethod
    def setup_env() -> object:
        os.environ['NLS_LANG'] = '.AL32UTF8'


class DateUtil:
    __PRESET_TIME = None

    @staticmethod
    def init_preset_time():
        # 转为时间戳-取模到分钟
        temp = time.time()
        DateUtil.__PRESET_TIME = int(temp - (temp % 60))

    @staticmethod
    def get_preset_time():
        return DateUtil.__PRESET_TIME

    @staticmethod
    def week_of_month(dt):
        """ Returns the week of the month for the specified date.
        """
        first_day = dt.replace(day=1)
        dom = dt.day
        adjusted_dom = dom + first_day.weekday()
        return int(ceil(adjusted_dom / 7.0))

    @staticmethod
    def stamp_to_date_format0(_stamp):
        _start_time = datetime.datetime.fromtimestamp(_stamp)
        return datetime.datetime.strftime(_start_time, "%Y-%m-%d")

    @staticmethod
    def stamp_to_date_format1(_stamp):
        _start_time = datetime.datetime.fromtimestamp(_stamp)
        return datetime.datetime.strftime(_start_time, "%Y-%m-%d-%H")

    @staticmethod
    def stamp_to_date_format2(_stamp):
        _start_time = datetime.datetime.fromtimestamp(_stamp)
        return datetime.datetime.strftime(_start_time, "%Y_%m_%d_%H_%M")

    @staticmethod
    def stamp_to_date_format3(_stamp):
        _start_time = datetime.datetime.fromtimestamp(_stamp)
        return datetime.datetime.strftime(_start_time, "%Y-%m-%d %H:%M:%S")

    @staticmethod
    def stamp_to_date_format4(_stamp):
        _start_time = datetime.datetime.fromtimestamp(_stamp)
        return datetime.datetime.strftime(_start_time, "%Y-%m-%d %H:%M")

    @staticmethod
    def stamp_to_date_format5(_stamp):
        _start_time = datetime.datetime.fromtimestamp(_stamp)
        return datetime.datetime.strftime(_start_time, "%Y-%m-%d %H")

    @staticmethod
    def stamp_to_date_format(_stamp, format_str="%Y/%m/%d %H:%M:%S", offset=0):
        _start_time = datetime.datetime.fromtimestamp(_stamp)
        return datetime.datetime.strftime(_start_time + datetime.timedelta(days=offset), format_str)

    @staticmethod
    def string_to_timestamp(st, format_str="%Y-%m-%d"):
        return time.mktime(time.strptime(st, format_str))

class NumberUtil:
    @staticmethod
    def round_number_to_str(number, digits):
        if not isinstance(number, numbers.Number):
            return number
        return '%.{0}f'.format(digits) % number

    @staticmethod
    def try_cast_to_float(commission, value_on_failure=None):
        try:
            if not isinstance(commission, float) or not math.isnan(commission):
                commission = float(commission)
            return commission
        except Exception as e:
            LogUtil.get_cur_logger().exception('exception on casting')
            return value_on_failure

    @staticmethod
    def decimal_to_percentile(decimal_ratio, digits=0, null_value=DEFAULT_NULL_VALUE):
        if MiscUtil.is_empty_value(decimal_ratio):
            return null_value
        return NumberUtil.round_number_to_str(decimal_ratio * 100, digits)


class PriceUtil:
    __SCALE_001 = 0.01
    __SCALE_100 = 100
    __SCALE_RET_DIG_0 = 0
    __SCALE_RET_DIG_2 = 2

    @staticmethod
    def calc_room_type_difference(price, difference_type, price_delta, price_multiplier):
        if DFUtil.is_invalid(difference_type) or DFUtil.is_invalid(price_delta) or DFUtil.is_invalid(
                price_multiplier):
            return price
        if difference_type == 1:
            return price + price_delta
        elif difference_type == 2:
            return price * price_delta
        else:
            # TODO(yry) log warning here
            return price

    @staticmethod
    def floor_price_check(price, floor_price_type, floor_price):
        try:
            if MiscUtil.is_empty_value(price) or MiscUtil.is_empty_value(floor_price) or MiscUtil.is_empty_value(
                    floor_price_type):
                return price
            price = pd.to_numeric(price)
            if floor_price_type == FLOOR_PRICE_TYPE_NUMBER:
                price_ret = round(max(price, floor_price), 0)
            elif floor_price_type == FLOOR_PRICE_TYPE_RATIO:
                price_ret = round(max(price, floor_price * price), 0)
            else:
                price_ret = price
            return price_ret
        except Exception as e:
            LogUtil.get_cur_logger().exception(
                'floor_price_check, error happened, input params: '
                'price: {0}, '
                'floor_price_type: {1}, '
                'floor_price: {2}'.format(price, floor_price_type, floor_price))
            return price

    @staticmethod
    def override_floor_price_check(pms_price, override_floor_price):
        try:
            if MiscUtil.is_empty_value(pms_price) or MiscUtil.is_empty_value(override_floor_price):
                return pms_price
            price = pd.to_numeric(pms_price)
            override_floor_price = pd.to_numeric(override_floor_price)
            return round(max(price, override_floor_price), 0)
        except Exception as e:
            LogUtil.get_cur_logger().exception(
                'override_floor_price_check, error happened, input params: '
                'price: {0}, '
                'override_floor_price: {1}'.format(pms_price, override_floor_price))
            return pms_price

    @staticmethod
    def override_ceiling_price_check(pms_price, override_ceiling_price):
        try:
            if MiscUtil.is_empty_value(pms_price) or MiscUtil.is_empty_value(override_ceiling_price):
                return pms_price
            price = pd.to_numeric(pms_price)
            override_ceiling_price = pd.to_numeric(override_ceiling_price)
            return round(min(price, override_ceiling_price), 0)
        except Exception as e:
            LogUtil.get_cur_logger().exception(
                'override_ceiling_price_check, error happened, input params: '
                'price: {0}, '
                'override_ceiling_price: {1}'.format(pms_price, override_ceiling_price))
            return pms_price

    @staticmethod
    def scale_commission(commission, ratio, digits):
        if commission == DEFAULT_NULL_VALUE or pd.isna(commission):
            return commission
        commission = NumberUtil.try_cast_to_float(commission)
        return round(commission * ratio, digits)

    @staticmethod
    def scale_commission100(commission):
        if MiscUtil.is_empty_value(commission):
            return commission
        commission = NumberUtil.try_cast_to_float(commission)
        return round(commission * PriceUtil.__SCALE_100, PriceUtil.__SCALE_RET_DIG_0)

    @staticmethod
    def scale_commission001(commission):
        if MiscUtil.is_empty_value(commission):
            return commission
        commission = NumberUtil.try_cast_to_float(commission)
        return commission * PriceUtil.__SCALE_001

    @staticmethod
    def commission_ratio_to_percentile_for_df_columns(df, columns, digits):
        for column in columns:
            df[column] = df[column].apply(
                lambda ratio: NumberUtil.decimal_to_percentile(ratio, digits))

    @staticmethod
    def ceiling_price_check(price, ceiling_price_type, ceiling_price):
        try:
            if MiscUtil.is_empty_value(price) or MiscUtil.is_empty_value(ceiling_price) or MiscUtil.is_empty_value(
                    ceiling_price_type):
                return price
            price = pd.to_numeric(price)
            if ceiling_price_type == FLOOR_PRICE_TYPE_NUMBER:
                price_ret = round(min(price, ceiling_price), 0)
            elif ceiling_price_type == FLOOR_PRICE_TYPE_RATIO:
                price_ret = round(min(price, ceiling_price * price), 0)
            else:
                price_ret = price
            return price_ret
        except Exception as e:
            LogUtil.get_cur_logger().exception(
                'ceiling_price_check, error happened, input params: '
                'price: {0}, '
                'ceiling_price_type: {1}, '
                'ceiling_price: {2}'.format(price, ceiling_price_type, ceiling_price))
            return price

    @staticmethod
    def filter_ota_prices_by_ota_room_type_name(price, ota_room_type_name):
        if ota_room_type_name is None or ota_room_type_name == DEFAULT_NULL_VALUE or pd.isna(ota_room_type_name):
            return DEFAULT_NULL_VALUE
        return price

    @staticmethod
    def filter_ota_price_for_data_frame(df, price_column_name, ota_room_type_column):
        df[price_column_name] = df[[price_column_name, ota_room_type_column]].apply(
            lambda values: PriceUtil.filter_ota_prices_by_ota_room_type_name(*values), axis=1)

    @staticmethod
    def filter_ota_price_list_for_data_frame(df, price_column_name_list, ota_room_type_column):
        for price_column_name in price_column_name_list:
            PriceUtil.filter_ota_price_for_data_frame(df, price_column_name, ota_room_type_column)

    @staticmethod
    def _wrap_to_null_value_by_benchmark_value(benchmark_value, origin_value):
        if MiscUtil.is_empty_value(benchmark_value):
            return DEFAULT_NULL_VALUE
        return origin_value

    @staticmethod
    def wrap_to_null_value_for_columns_by_benchmark_column(df, benchmark_column, target_columns):
        for target_column in target_columns:
            # 有的渠道对应的列不一定存在，比如飞猪的现付卖价，因此做check
            if target_column in df.columns:
                DFUtil.apply_func_for_df(df, target_column, [benchmark_column, target_column],
                                         lambda values: PriceUtil._wrap_to_null_value_by_benchmark_value(*values))

    @staticmethod
    def _wrap_to_null_by_multiple_benchmark_values(value_list):
        origin_value = value_list[0]
        ret = False
        for bench_mark_value in value_list[1:]:
            if MiscUtil.is_not_empty_value(bench_mark_value):
                ret = True
                break
        if not ret:
            return DEFAULT_NULL_VALUE
        else:
            return origin_value

    @staticmethod
    def wrap_to_null_value_for_columns_by_benchmark_columns(df, benchmark_columns, target_columns):
        for target_column in target_columns:
            new_benchmark_columns = list()
            new_benchmark_columns.append(target_column)
            for benchmark_column in benchmark_columns:
                if benchmark_column in df.columns:
                    new_benchmark_columns.append(benchmark_column)
            if df.shape[0] <= 0:
                # data frame 为空，添加一个空列
                DFUtil.add_columns_for_empty_data_frame(df, [target_column])
            else:
                df[target_column] = df[new_benchmark_columns].apply(
                    lambda values: PriceUtil._wrap_to_null_by_multiple_benchmark_values(values), axis=1)

    @staticmethod
    def calc_top_rating_discount_activity_price(price_before_top_rating_discount_activity, discount_type,
                                                discount_delta, discount_multiplier):
        if pd.isna(discount_type) or pd.isna(discount_delta) or pd.isna(discount_multiplier):
            return price_before_top_rating_discount_activity
        if discount_type == 1 and not pd.isna(discount_multiplier):
            return round(price_before_top_rating_discount_activity * discount_multiplier, 0)
        elif discount_type == 2 and not pd.isna(discount_delta):
            return round(price_before_top_rating_discount_activity + discount_delta, 0)
        else:
            return price_before_top_rating_discount_activity

    @staticmethod
    def calc_net_price(original_price, net_commission):
        if pd.isna(net_commission) or net_commission == DEFAULT_NULL_VALUE:
            return DEFAULT_NULL_VALUE
        return original_price * (1 - float(net_commission))

    @staticmethod
    def calc_price_for_promo(price_column_name, price_before_promo, promo_type, promo_delta, promo_multiplier, date, from_date, till_date):
        if MiscUtil.is_empty_value(from_date) or MiscUtil.is_empty_value(till_date):
            return price_before_promo
        if date < from_date or date > till_date:
            return price_before_promo
        if pd.isna(promo_type) or MiscUtil.is_empty_value(price_before_promo):
            return price_before_promo
        if promo_type == 1:
            # 优惠券为1， 全部价格做价差活动
            if not pd.isna(promo_delta):
                return round(price_before_promo + promo_delta)
            else:
                return price_before_promo
        elif promo_type == 2:
            # 优惠券为2，全部价格做系数活动
            if not pd.isna(promo_multiplier):
                return ceil(price_before_promo * promo_multiplier)
            else:
                return price_before_promo
        elif promo_type == 3:
            # 优惠券为3， 只对预付卖价做活动
            if not MiscUtil.is_pre_sell_price(price_column_name) or pd.isna(promo_delta):
                return price_before_promo
            else:
                return price_before_promo + promo_delta
        else:
            # TODO(yry) 添加更多活动
            return price_before_promo

    @staticmethod
    def calc_post_price_from_pre_price(pre_price):
        if MiscUtil.is_empty_value(pre_price):
            return pre_price
        return round(pre_price * POST_PRE_PRICING_RATIO, 0)

    @staticmethod
    def wrap_null_value(value):
        if MiscUtil.is_empty_value(value):
            return DEFAULT_NULL_VALUE
        return value

    @staticmethod
    def round_digits_for_df_columns(df, columns, digits):
        for column in columns:
            df[column] = df[column].apply(lambda price: PriceUtil.round_price_to_digits(price, digits))

    @staticmethod
    def round_price_to_digits(price, digits):
        if MiscUtil.is_empty_value(price):
            return price
        return '%.{0}f'.format(digits) % price


class JsonUtil:

    @staticmethod
    def _filter_dict_entry(obj):
        new_dict = {}
        for key, value in obj.__dict__.items():
            if isinstance(value, list) or MiscUtil.is_not_empty_value(value):
                new_dict[key] = value
        return new_dict

    @staticmethod
    def json_serialize(obj):
        return json.dumps(obj, default=lambda obj: JsonUtil._filter_dict_entry(obj),
                          sort_keys=True, indent=4, ensure_ascii=False)

    @staticmethod
    def print_json_str_file(path, json_str):
        try:
            f_open = open(path, 'w+')
            print(json_str, file=f_open)
        except Exception as e:
            raise e
        else:
            f_open.close()

    @staticmethod
    def print_json_file(path, pojo_chunk):
        JsonUtil.print_json_str_file(path, pojo_chunk.to_json())


class HashUtil:

    @staticmethod
    def get_hash_code(st):

        def convert_n_bytes(n, b):
            bits = b * 8
            return (n + 2 ** (bits - 1)) % 2 ** bits - 2 ** (bits - 1)

        def convert_4_bytes(n):
            return convert_n_bytes(n, 4)

        def hash_code(s):
            h = 0
            n = len(s)
            for i, c in enumerate(s):
                h = h + ord(c) * 31 ** (n - 1 - i)
            return convert_4_bytes(h)

        return hash_code(st)


class MiscUtil:

    @staticmethod
    def get_or_create_from_map(map, key, value_constructor):
        value = map.get(key)
        if value is not None:
            return value
        value = value_constructor()
        map[key] = value
        return value

    @staticmethod
    def compose_key_for_ota_channel_name(ota_channel_name, key):
        return ota_channel_name + '_' + key

    @staticmethod
    def is_pre_sell_price(price_column_name):
        return price_column_name is not None and price_column_name.find('_pre_sell_') != -1

    @staticmethod
    def is_empty_value(value):
        return value is None or value == DEFAULT_NULL_VALUE or pd.isna(value)

    @staticmethod
    def is_not_empty_value(value):
        return not MiscUtil.is_empty_value(value)

    @staticmethod
    def value_add(value, offset):
        return value if MiscUtil.is_empty_value(value) else value + offset

    @staticmethod
    def convert_list_to_tuple_list_str(lst):
        sz = len(lst)
        if sz == 0:
            return "('')"
        elif sz == 1:
            return "('{0}')".format(lst[0])
        else:
            return str(tuple(lst))

    @staticmethod
    def convert_to_oracle_query_oyo_id_list_str(field_name, lst, group_size=1000):
        lst = list(set(lst))
        grouped_lists = MiscUtil.group_by_list(lst, group_size)
        ret = ''
        for idx in range(len(grouped_lists)):
            frac = '({0} in {1})'.format(field_name, MiscUtil.convert_list_to_tuple_list_str(grouped_lists[idx]))
            if idx == 0:
                ret = frac
            else:
                ret = '{0} or {1}'.format(ret, frac)
            idx += 1
        return '({})'.format(ret)

    @staticmethod
    def group_by_list(lst, group_size):
        lst_sz = len(lst)
        if lst_sz <= group_size:
            return [lst]
        return [lst[i:i + group_size] for i in range(0, len(lst), group_size)]

    @staticmethod
    def split_into_groups(total, group_count):
        remain = total % group_count
        return int(total / group_count) if remain == 0 else int(total / group_count) + 1

    @staticmethod
    def convert_set_to_tuple_list_str(sets):
        return MiscUtil.convert_list_to_tuple_list_str(list(sets))

    @staticmethod
    def wrap_to_ota_plugin_percent_values(value):
        if value == DEFAULT_NULL_VALUE:
            return DEFAULT_NULL_VALUE
        return str(value) + '%'

    @staticmethod
    def set_columns_to_value(df, column_name_list, value):
        for column_name in column_name_list:
            df[column_name] = value

    @staticmethod
    def is_weekend(weekday_number):
        return weekday_number == 4 or weekday_number == 5

    @staticmethod
    def create_folder_if_necessary(folder_path):
        if not os.path.exists(folder_path):
            os.makedirs(folder_path, 0o777, True)

    @staticmethod
    def wrap_read_adb_df(read_sql_func, origin_set, *args, **kargs):
        oyo_id_groups = MiscUtil.group_by_list(list(origin_set), 4000)
        df_list = list()
        for oyo_id_group in oyo_id_groups:
            df = read_sql_func(oyo_id_group, *args, **kargs)
            df_list.append(df)
        return pd.concat(df_list, ignore_index=True)


class PricingPipelineUtil:
    _OTA_ROOM_TYPE_COMMISSION_QUERY = """
        select oyo_id,
               room_type_id,
               ota_pre_commission  as {0}_pre_commission,
               ota_post_commission as {0}_post_commission
        from {1}
        where ota_channel_id = {2}{3}
    """

    _OTA_ROOM_TYPE_META_QUERY = """
        SELECT
            t_d.ota_code,
            t_d.oyo_id,
            t_d.room_type_id,
            LISTAGG(t_d.ota_room_type_name, ' | ')
            WITHIN GROUP (ORDER BY t_d.ota_room_type_name) ota_room_type_name
        FROM
            (
            SELECT DISTINCT
                oh.ota_code,
                oh.ota_hotel_name,
                ohm.oyo_id,
                rt.room_type_id,
                rt.room_type_name,
                ort.ota_room_type_name 
            FROM
                oyo_dw_pricing.pricing_dim_ota_room_type ort
                INNER JOIN oyo_dw_pricing.pricing_dim_ota_room_type_mapping ortm ON ort.id = ortm.ota_room_type_id
                INNER JOIN oyo_dw_pricing.pricing_dim_ota_hotel oh ON ort.ota_hotel_id = oh.id
                INNER JOIN oyo_dw_pricing.pricing_dim_room_type rt ON rt.id = ortm.room_type_id
                INNER JOIN oyo_dw_pricing.pricing_dim_ota_hotel_mapping ohm ON ort.ota_hotel_id = oh.id 
                AND ohm.ota_hotel_id = oh.id 
            WHERE
                ort.is_deleted = 0 
                AND ohm.is_deleted = 0 
                AND ortm.is_deleted = 0 
                {0}
            ORDER BY
                ota_code,
                oyo_id,
                room_type_id
        ) t_d 
        GROUP BY ota_code, oyo_id, room_type_id
    """

    _EBK_ROOM_TYPE_QUERY = """
        SELECT CASE
           WHEN ota_code = 'ctrip' THEN 1
           WHEN ota_code = 'meituan' THEN 2
           WHEN ota_code = 'fliggy' THEN 3
           WHEN ota_code = 'elong' THEN 4
           WHEN ota_code = 'qunar' THEN 5 END as ota_channel_id,
           oyo_id,
           cast(room_type_id as SIGNED) as room_type_id,
           min(ota_room_type_name) as ota_room_type_name
        FROM (
                 SELECT DISTINCT oh.ota_code,
                                 oh.ota_hotel_name,
                                 ohm.oyo_id,
                                 rt.room_type_id,
                                 ort.ota_room_type_name
                 FROM ota_room_type ort
                          INNER JOIN ota_room_type_mapping ortm ON ort.id = ortm.ota_room_type_id
                          INNER JOIN ota_hotel oh ON ort.ota_hotel_id = oh.id
                          INNER JOIN ota_sync_room_type rt ON rt.id = ortm.room_type_id
                          INNER JOIN ota_hotel_mapping ohm ON ort.ota_hotel_id = oh.id
                     AND ohm.ota_hotel_id = oh.id
                 WHERE ort.is_deleted = 0
                   AND ohm.is_deleted = 0
                   AND ortm.is_deleted = 0
                   AND substring(rt.room_type_id, 1, 1) <> 'h'
                   {0}
                 ORDER BY ota_code,
                          oyo_id,
                          room_type_id
             ) ota_room_type_mappings
        WHERE ota_room_type_name is not null
        AND ota_room_type_name <> ''
        AND TRIM(ota_room_type_name) <> ''
        GROUP BY ota_code, oyo_id, room_type_id
    """

    _OTA_NET_COMMISSION_META_QUERY = """
        select oyo_id, room_type_id, commission as {0}_net_commission
        from {1}
        where ota_channel_id = {2}{3}
    """

    _OTA_PROMO_META_QUERY = """
        select oyo_id,
           room_type_id,
           promo_type       as {0}_promo_type,
           promo_delta      as {0}_promo_delta,
           promo_multiplier as {0}_promo_multiplier,
           date_format(from_date, '%%Y-%%m-%%d') as {0}_promo_from_date,
           date_format(till_date, '%%Y-%%m-%%d') as {0}_promo_till_date
        from {1}
        where ota_channel_id = {2}
          and date_format(now(), '%%Y-%%m-%%d') between from_date and till_date{3}
    """

    _OTA_TOP_RATING_DISCOUNT_ACTIVITY_META_QUERY = """
            select oyo_id, room_type_id, discount_type as {0}_discount_type, discount_delta as {0}_discount_delta,
                discount_multiplier as {0}_discount_multiplier
            from {1}
            where ota_channel_id = {2}{3}
    """

    @staticmethod
    def calc_merged_ota_commission_df(src_table_name, ota_channel_map, mysql_query_manager,
                                      join_on_columns,
                                      optional_oyo_ids_filter=None):
        ota_commission_df = PricingPipelineUtil.calc_merged_ota_df(src_table_name, mysql_query_manager,
                                                                   PricingPipelineUtil._OTA_ROOM_TYPE_COMMISSION_QUERY,
                                                                   ota_channel_map, join_on_columns,
                                                                   optional_oyo_ids_filter)
        return ota_commission_df

    @staticmethod
    def calc_fold_ota_room_type_df(mysql_engine, optional_oyo_ids_filter):
        ota_room_type_name_df = PricingPipelineUtil.calc_ota_room_type_df(mysql_engine,
                                                                          PricingPipelineUtil._EBK_ROOM_TYPE_QUERY,
                                                                          optional_oyo_ids_filter)

        return ota_room_type_name_df

    @staticmethod
    def calc_merged_ota_net_commission_df(src_table_name, ota_channel_map, db_engine, join_on_columns,
                                          optional_oyo_ids_filter=None):
        return PricingPipelineUtil.calc_merged_ota_df(src_table_name, db_engine,
                                                      PricingPipelineUtil._OTA_NET_COMMISSION_META_QUERY,
                                                      ota_channel_map, join_on_columns, optional_oyo_ids_filter)

    @staticmethod
    def calc_merged_ota_promo_df(src_table_name, ota_channel_map, db_engine, join_on_columns,
                                 optional_oyo_ids_filter=None):
        return PricingPipelineUtil.calc_merged_ota_df(src_table_name, db_engine,
                                                      PricingPipelineUtil._OTA_PROMO_META_QUERY, ota_channel_map,
                                                      join_on_columns, optional_oyo_ids_filter)

    @staticmethod
    def calc_merged_ota_top_rating_discount_activity_df(src_table_name, ota_channel_map, db_engine, join_on_columns,
                                                        optional_oyo_ids_filter=None):
        return PricingPipelineUtil.calc_merged_ota_df(src_table_name, db_engine,
                                                      PricingPipelineUtil._OTA_TOP_RATING_DISCOUNT_ACTIVITY_META_QUERY,
                                                      ota_channel_map, join_on_columns, optional_oyo_ids_filter)

    @staticmethod
    def calc_merged_ota_df(src_table_name, mysql_query_manager, mysql_sql, ota_channel_map, join_on_columns,
                           optional_oyo_ids_filter):
        if optional_oyo_ids_filter is not None:
            oyo_id_filter = ' and oyo_id in {0}'.format(optional_oyo_ids_filter)
        else:
            oyo_id_filter = ''

        df_list = []
        for ota_channel_name, ota_channel_id in ota_channel_map.items():
            query = mysql_sql.format(ota_channel_name, src_table_name, ota_channel_id, oyo_id_filter)
            df = mysql_query_manager.read_sql(query)
            df = DFUtil.check_duplicate(df, src_table_name, join_on_columns)
            df_list.append(df)
        merged_df = reduce(
            lambda left, right: pd.merge(left, right, how='outer', on=join_on_columns),
            df_list)
        return merged_df

    @staticmethod
    def calc_ota_room_type_df(mysql_engine_manager, query_sql, smart_hotel_oyo_id_list):
        # 获取pc中的OTA_ROOM_TYPE_META
        if smart_hotel_oyo_id_list is not None and len(smart_hotel_oyo_id_list) > 0:
            oyo_id_filter = ' AND {0}'.format(
                MiscUtil.convert_to_oracle_query_oyo_id_list_str('ohm.oyo_id', smart_hotel_oyo_id_list))
        else:
            oyo_id_filter = ''
        query = query_sql.format(oyo_id_filter)
        df = mysql_engine_manager.read_sql(query, 60)
        return df

    @staticmethod
    def calc_promo_for_df(df, ota_channel_name, price_column_names):
        for price_column_name in price_column_names:
            DFUtil.apply_func_for_df(df, price_column_name,
                                     [price_column_name,
                                      MiscUtil.compose_key_for_ota_channel_name(
                                          ota_channel_name,
                                          'promo_type'),
                                      MiscUtil.compose_key_for_ota_channel_name(
                                          ota_channel_name,
                                          'promo_delta'),
                                      MiscUtil.compose_key_for_ota_channel_name(
                                          ota_channel_name,
                                          'promo_multiplier'),
                                      'date',
                                      MiscUtil.compose_key_for_ota_channel_name(
                                          ota_channel_name,
                                          'promo_from_date'),
                                      MiscUtil.compose_key_for_ota_channel_name(
                                          ota_channel_name,
                                          'promo_till_date')
                                      ],
                                     lambda values: PriceUtil.calc_price_for_promo(
                                         price_column_name,
                                         *values))


    @staticmethod
    def calc_prices_for_df(df, ota_channel_map):
        for ota_channel_name in ota_channel_map.keys():
            DFUtil.apply_func_for_df(df, MiscUtil.compose_key_for_ota_channel_name(ota_channel_name,
                                                                                   'price_after_ota_top_rating'),
                                     ['pms_price',
                                      MiscUtil.compose_key_for_ota_channel_name(ota_channel_name, 'discount_type'),
                                      MiscUtil.compose_key_for_ota_channel_name(ota_channel_name, 'discount_delta'),
                                      MiscUtil.compose_key_for_ota_channel_name(ota_channel_name,
                                                                                'discount_multiplier')],
                                     lambda values: PriceUtil.calc_top_rating_discount_activity_price(
                                         *values))
            DFUtil.apply_func_for_df(df, MiscUtil.compose_key_for_ota_channel_name(ota_channel_name,
                                                                                   'net_price'),
                                     [MiscUtil.compose_key_for_ota_channel_name(ota_channel_name,
                                                                                'price_after_ota_top_rating'),
                                      MiscUtil.compose_key_for_ota_channel_name(ota_channel_name, 'net_commission')],
                                     lambda values: PriceUtil.calc_net_price(*values))

    @staticmethod
    def wrap_ota_columns_to_null_for_df(df, ota_channel_map):
        for ota_channel_name in ota_channel_map.keys():
            ota_pre_sell_price_column = MiscUtil.compose_key_for_ota_channel_name(ota_channel_name, 'pre_sell_price')
            ota_pre_net_price_column = MiscUtil.compose_key_for_ota_channel_name(ota_channel_name, 'pre_net_price')
            ota_post_sell_price_column = MiscUtil.compose_key_for_ota_channel_name(ota_channel_name, 'post_sell_price')
            ota_pre_commission_column = MiscUtil.compose_key_for_ota_channel_name(ota_channel_name, 'pre_commission')
            ota_post_commission_column = MiscUtil.compose_key_for_ota_channel_name(ota_channel_name, 'post_commission')
            # 如果OTA对应的预付佣金列为空或None，那么预付相关的价格+自身置空
            PriceUtil.wrap_to_null_value_for_columns_by_benchmark_column(df, ota_pre_commission_column,
                                                                         [ota_pre_commission_column,
                                                                          ota_pre_sell_price_column,
                                                                          ota_pre_net_price_column])

            # 如果OTA对应的现付佣金列为空或None，那么现付付相关的价格+自身置空
            PriceUtil.wrap_to_null_value_for_columns_by_benchmark_column(df, ota_post_commission_column,
                                                                         [ota_post_commission_column,
                                                                          ota_post_sell_price_column])
    @staticmethod
    def compose_row_id(oyo_id, room_type_id, ota_channel_id):
        return '{}-{}-{}'.format(oyo_id, room_type_id, ota_channel_id)
