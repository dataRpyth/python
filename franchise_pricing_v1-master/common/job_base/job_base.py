import os
import sys
import time
from os.path import join as join_path

cur_path = os.path.dirname(os.path.realpath(__file__))
sys.path.append(join_path(cur_path, '..'))

from common.commons import DbQueryManager
from common.util.utils import EnvUtil, LogUtil

# 测试
_JOB_ALERT_ROBOT_TOKEN_DEBUG = 'a6e89792a9b40c383d8fcc2937b3fc35f46c6428ca6a142669710ccf4ea8cd2c'

# 生产环境数据报警(OP)
_JOB_ALERT_ROBOT_TOKEN_OP = '9af45caa15416ef385fd33c6a963f0aee6dccf8d173c86c8bc972374027c64ae'

# 生产环境报警（内部）
_JOB_ALERT_ROBOT_TOKEN_INTERNAL = '42d9608e03776f74b5ef897b5e882522d8645fc6d19c9ea2796c3dbfb2aff37e'

# 生产oracle数仓数据产出报警
_JOB_ALERT_ROBOT_TOKEN_ORACLE_UPSTREAM = 'a097b2f10aab9324c0c057aabc8e6e195741e078eb8da3aad9bc6e51814db76a'

class JobBase:
    EnvUtil.setup_env()

    def __init__(self, job_config):
        self._job_config = job_config
        self._job_config.set_job(self)
        self._oracle_query_manager = DbQueryManager(
            [job_config.get_oracle_engine(), job_config.get_oracle_backup_engine()],
            self.get_robot_token_internal_alert())
        self._adb_query_manager = DbQueryManager([job_config.get_adb_engine(), job_config.get_adb_backup_engine()],
                                                 self.get_robot_token_internal_alert())
        self._mysql_query_manager = DbQueryManager(
            [job_config.get_mysql_engine(), job_config.get_mysql_backup_engine()],
            self.get_robot_token_internal_alert())
        self._hive_query_manager = DbQueryManager(
            [job_config.get_hive_engine(), job_config.get_hive_engine()], self.get_robot_token_internal_alert())

    def get_job_name(self):
        raise Exception('must be implemented by sub-classes')

    def get_algorithm_model(self):
        return 'DefaultAlgModel'

    def get_mysql_query_manager(self):
        return self._mysql_query_manager

    def get_oracle_query_manager(self):
        return self._oracle_query_manager

    def get_adb_query_manager(self):
        return self._adb_query_manager

    def get_hive_query_manager(self):
        return self._hive_query_manager

    def get_job_config(self):
        return self._job_config

    def is_prod(self):
        return self.get_job_config().is_prod()

    def get_robot_token_internal_alert(self):
        return _JOB_ALERT_ROBOT_TOKEN_INTERNAL if self.is_prod() else _JOB_ALERT_ROBOT_TOKEN_DEBUG

    def get_robot_token_op_alert(self):
        return _JOB_ALERT_ROBOT_TOKEN_OP if self.is_prod() else _JOB_ALERT_ROBOT_TOKEN_DEBUG

    def get_robot_token_oracle_upstream(self):
        return _JOB_ALERT_ROBOT_TOKEN_ORACLE_UPSTREAM if self.is_prod() else _JOB_ALERT_ROBOT_TOKEN_DEBUG

    def run(self):
        logger = LogUtil.get_cur_logger()

        begin_time = time.time()
        logger.info('********************* job begin ******************************')
        self.run_job()
        logger.info('******************* job end, time cost: %.2fs ********************', time.time() - begin_time)

    def run_job(self):
        pass
