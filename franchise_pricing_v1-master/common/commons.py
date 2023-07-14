import os
import sys
from os.path import join as join_path

cur_path = os.path.dirname(os.path.realpath(__file__))
sys.path.append(join_path(cur_path, '..'))

from common.util.utils import LogUtil, CommonUtil, DdtUtil

import pandas as pd


def send_ddt_fallback_msg(msg, ddt_robot_token):
    LogUtil.get_cur_logger().warn(msg)
    DdtUtil.robot_send_ddt_msg(msg, ddt_robot_token, None, None)


class DbQueryManager:

    def __init__(self, candidate_engine_list, alert_robot_token):
        if candidate_engine_list is None or len(candidate_engine_list) < 2:
            raise Exception('two difference engines at least must be provided')
        self._candidate_engine_list = candidate_engine_list
        self._current_engine_index = 0
        self._alert_robot_token = alert_robot_token

    def _loop_next_engine(self):
        engine_len = len(self._candidate_engine_list)
        next_engine_index = (self._current_engine_index + 1) % engine_len
        self._current_engine_index = next_engine_index

    def get_current_engine(self):
        return self._candidate_engine_list[self._current_engine_index]

    def read_sql(self, query_sql, initial_time_out_seconds=30):
        engine_len = len(self._candidate_engine_list)
        final_result = None
        for x in range(engine_len):
            current_engine = self.get_current_engine()
            # enlarge time out for better chances not time out
            actual_time_out_seconds = int(initial_time_out_seconds * (1 + x * 1))
            query_result = CommonUtil.call_func_with_timeout_or_error_fallback(pd.read_sql, args=(query_sql, current_engine),
                                                                               kwargs={},
                                                                               timeout_in_seconds=actual_time_out_seconds,
                                                                               timeout_handler=send_ddt_fallback_msg,
                                                                               fallback=None,
                                                                               handler_args=(
                                                                 'db query `{0}` time out({1})s or exception happened for engine: {2}'.format(
                                                                     query_sql, actual_time_out_seconds,
                                                                     current_engine), self._alert_robot_token),
                                                                               handler_kwargs={})
            if query_result is not None:
                final_result = query_result
                break
            else:
                self._loop_next_engine()

                DdtUtil.robot_send_ddt_msg('falling back to engine: {0}'.format(self.get_current_engine()),
                                           self._alert_robot_token, None, None)
        return final_result
