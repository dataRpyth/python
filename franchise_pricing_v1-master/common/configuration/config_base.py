import argparse
import os
import sys
from os.path import join as join_path

from pyhive import hive

cur_path = os.path.dirname(os.path.realpath(__file__))
sys.path.append(join_path(cur_path, '..'))

from common.enum.pricing_enum import EmailReceiver
from common.util.utils import *
from common.sendmail.mail_receiver import MailReceiver, MyHash

DEFAULT_ENV_LIST = ["local", "dev", "test", "uat", "prod"]


class ConfigBase:
    DEFAULT_DAY_SHIFT_END = 1
    DEFAULT_PRESET_DAY_SHIFT_START = DEFAULT_DAY_SHIFT_END + 1
    DEFAULT_PRESET_DAY_SHIFT_END = 30
    DEFAULT_OTA_PRICING_SLICES_SIZE = 500
    DEFAULT_PMS_PRICING_INSERT_BATCH_SIZE = 200
    DEFAULT_PRICING_LOG_INSERT_BATCH_SIZE = 2000
    DEFAULT_CHANNEL_HOTEL_INFOS_BATCH_SIZE = 2000
    RESULT_FOLDER = 'log'
    RESULT_PRESET_DAYS = 2

    def _create_db_engine_from_file(self, section_name, engine_create_func, db):
        config = self.get_base_config()
        db_name = config.get(section_name, 'dbname')
        host = config.get(section_name, 'host')
        port = config.get(section_name, 'port')
        user = config.get(section_name, 'user')
        password = config.get(section_name, 'password')
        engine = engine_create_func(user, password, host, port, db_name)
        LogUtil.get_cur_logger().info("{0} db engine from {1} created".format(db, section_name))
        return engine

    def get_local_file_folder(self):
        return join_path(cur_path, self.get_result_folder())

    @staticmethod
    def get_result_folder():
        return ConfigBase.RESULT_FOLDER

    @staticmethod
    def create_config_for_file_path(config_file_path):
        LogUtil.get_cur_logger().info('Initializing config from config file: %s', config_file_path)
        return ConfigFileUtil.create_config_from_file(config_file_path)

    def __init__(self, config_env):
        self._parser = argparse.ArgumentParser(description="Parsing Base")
        self.add_args_for_parser(self._parser)
        self.args = self._parser.parse_args()
        cmd_env = self.args.env
        if cmd_env is not None and cmd_env not in DEFAULT_ENV_LIST:
            raise Exception('invalid env passed in: {}, can only be: {}'.format(cmd_env, DEFAULT_ENV_LIST))
        elif cmd_env is None:
            cmd_env = config_env
        self._config_env = cmd_env
        self._base_config = ConfigBase.create_config_for_file_path(
            join_path(cur_path, 'config', 'config_{0}.ini'.format(self._config_env)))
        self._job = None
        self._job_preset_time = None
        self._mail_send = None
        self._mail_receiver = None
        self.init_job_preset_time()

    def is_prod(self):
        return EnvUtil.is_prod_env(self._config_env)

    def get_base_config(self):
        return self._base_config

    def set_job(self, job):
        self._job = job

    def get_job(self):
        return self._job

    def set_mail_send(self, _mail_send):
        self._mail_send = _mail_send

    def get_mail_send(self):
        return self._mail_send

    def set_mail_receiver(self):
        if self._mail_receiver is None:
            self._mail_receiver = MailReceiver(MyHash(), self._job.get_mysql_query_manager())
            self._mail_receiver.get_email_receiver_from_db(self.get_robot_send_biz_model_id())

    def get_mail_receiver(self):
        self.set_mail_receiver()
        return self._mail_receiver

    def get_config_env(self):
        return self._config_env

    def add_args_for_parser(self, parser):
        parser.add_argument('-env', help='The env in dev/test/uat/prod', default=None, type=str)
        parser.add_argument('-bo', help='The batch order in 1/2/3', default='1', type=str)
        # 是否预埋90天，走特殊逻辑
        parser.add_argument('-preset', help='The job is running preset logic or not', default=False,
                            action='store_true')
        parser.add_argument('-otaPlugin', help='The job is running ota plugin logic or not', default=False,
                            action='store_true')

    def get_batch_order(self):
        return self.args.bo

    def is_preset(self):
        return self.args.preset

    def is_ota_plugin(self):
        return self.args.otaPlugin

    def get_calc_days(self):
        return ConfigBase.DEFAULT_PRESET_DAY_SHIFT_END if self.is_preset() else ConfigBase.DEFAULT_PRESET_DAY_SHIFT_START

    def init_job_preset_time(self):
        # 转为时间戳-取模到分钟
        temp = time.time()
        self._job_preset_time = int(temp - (temp % 60))

    def get_preset_days(self):
        return ConfigBase.RESULT_PRESET_DAYS

    def get_job_preset_time(self):
        return self._job_preset_time

    def get_mysql_engine(self):
        if self.is_prod():
            return ConfigBase._create_db_engine_from_os_env('PROD_MYSQL_PC_USER',
                                                            'PROD_MYSQL_PC_PASSWORD',
                                                            'PROD_MYSQL_PC_HOST',
                                                            'PROD_MYSQL_PC_PORT',
                                                            'PROD_MYSQL_PC_DB_NAME',
                                                            DBUtil.create_mysql_engine)
        else:
            return self._create_db_engine_from_file('mysql', DBUtil.create_mysql_engine, 'mysql')

    @staticmethod
    def _create_db_engine_from_os_env(os_env_user, os_env_pwd, os_env_host, os_env_port, os_env_db_name, engine_create_func):
        user = os.environ[os_env_user]
        pwd = os.environ[os_env_pwd]
        host = os.environ[os_env_host]
        port = os.environ[os_env_port]
        db_name = os.environ[os_env_db_name]
        return engine_create_func(user, pwd, host, port, db_name)

    def get_mysql_backup_engine(self):

        if self.is_prod():
            return ConfigBase._create_db_engine_from_os_env('PROD_BACKUP_MYSQL_PC_USER',
                                                            'PROD_BACKUP_MYSQL_PC_PASSWORD',
                                                            'PROD_BACKUP_MYSQL_PC_HOST',
                                                            'PROD_BACKUP_MYSQL_PC_PORT',
                                                            'PROD_BACKUP_MYSQL_PC_DB_NAME',
                                                            DBUtil.create_mysql_engine)
        else:
            return self._create_db_engine_from_file('mysql-backup', DBUtil.create_mysql_engine, 'mysql')

    def get_oracle_engine(self):
        user = os.environ['PROD_ORACLE_USER']
        pwd = os.environ['PROD_ORACLE_PASSWORD']
        host = os.environ['PROD_ORACLE_HOST']
        port = os.environ['PROD_ORACLE_PORT']
        db_name = os.environ['PROD_ORACLE_DB_NAME']
        return DBUtil.create_oracle_engine(user, pwd, host, port, db_name)

    def get_oracle_backup_engine(self):
        user = os.environ['PROD_BACKUP_ORACLE_USER']
        pwd = os.environ['PROD_BACKUP_ORACLE_PASSWORD']
        host = os.environ['PROD_BACKUP_ORACLE_HOST']
        port = os.environ['PROD_BACKUP_ORACLE_PORT']
        db_name = os.environ['PROD_BACKUP_ORACLE_DB_NAME']
        return DBUtil.create_oracle_engine(user, pwd, host, port, db_name)

    def get_adb_engine(self):
        user = os.environ['PROD_ADB_USER']
        pwd = os.environ['PROD_ADB_PASSWORD']
        host = os.environ['PROD_ADB_HOST']
        port = os.environ['PROD_ADB_PORT']
        db_name = os.environ['PROD_ADB_DB_NAME']
        return DBUtil.create_mysql_engine(user, pwd, host, port, db_name)

    def get_adb_backup_engine(self):
        user = os.environ['PROD_BACKUP_ADB_USER']
        pwd = os.environ['PROD_BACKUP_ADB_PASSWORD']
        host = os.environ['PROD_BACKUP_ADB_HOST']
        port = os.environ['PROD_BACKUP_ADB_PORT']
        db_name = os.environ['PROD_BACKUP_ADB_DB_NAME']
        return DBUtil.create_mysql_engine(user, pwd, host, port, db_name)

    def get_hive_engine(self):
        host = os.environ['PROD_HIVE_HOST']
        port = os.environ['PROD_HIVE_PORT']
        user = os.environ['PROD_HIVE_USER']
        password = os.environ['PROD_HIVE_PASSWORD']
        database = os.environ['PROD_HIVE_DB_NAME']
        conn = hive.Connection(host=host, port=port, username=user, password=password, database=database,
                               auth='CUSTOM')
        return conn

    def get_mail_user(self):
        return self.get_base_config().get('email', 'mail_user')

    def get_mail_pass(self):
        return self.get_base_config().get('email', 'mail_pass')

    def get_env(self):
        return self.get_base_config().get('env', 'env')

    def get_ddt_env(self):
        return self.get_base_config().get('env', 'ddt_env')

    def get_pricing_log_insert_batch_size(self):
        return ConfigBase.DEFAULT_PRICING_LOG_INSERT_BATCH_SIZE

    def get_pms_price_sink_url(self):
        return self.get_base_config().get('env', 'pms_price_sink_url')

    def get_pms_pricing_insert_batch_size(self):
        return ConfigBase.DEFAULT_OTA_PRICING_SLICES_SIZE

    def get_ota_pricing_upload_url(self):
        return self.get_base_config().get('env', 'ota_pricing_upload_url')

    def get_ota_pricing_slices_size(self):
        return ConfigBase.DEFAULT_OTA_PRICING_SLICES_SIZE

    def get_price_to_channel_rate_url(self):
        return self.get_base_config().get('env', 'price_to_channel_rate_url')

    def get_channel_hotel_infos_batch_size(self):
        return ConfigBase.DEFAULT_CHANNEL_HOTEL_INFOS_BATCH_SIZE

    def get_toggle_on_price_to_channel_rate(self):
        return self.get_child_config().get('misc', 'toggle_on_price_to_channel_rate', fallback="False") == "True"

    def get_toggle_on_pms_price(self):
        return self.get_child_config().get('misc', 'toggle_on_pms_price', fallback="False") == "True"

    def get_toggle_on_ota_pricing_upload(self):
        return self.get_child_config().get('misc', 'toggle_on_ota_pricing_upload', fallback="False") == "True"

    def get_toggle_on_pricing_log(self):
        return self.get_child_config().get('misc', 'toggle_on_pricing_log', fallback="False") == "True"

    def get_toggle_on_lms_to_ump(self):
        return self.get_child_config().get('misc', 'toggle_on_lms_to_ump', fallback="False") == "True"

    def get_toggle_on_robot_send(self):
        return self.get_child_config().get('misc', 'toggle_on_robot_send', fallback="False") == "True"

    def get_robot_send_biz_name(self):
        return self.get_child_config().get('misc', 'robot_send_biz_name')

    def get_robot_send_biz_model_id(self):
        return self.get_child_config().get('misc', 'robot_send_biz_model_id', fallback="False")

    def get_robot_send_token(self):
        return self.get_child_config().get('misc', 'robot_send_token')

    def get_child_config(self):
        raise Exception('must implement!!')

    def get_price_log_post_result_receivers(self):
        return ''.join(self.get_child_config().get('email', 'price_log_post_result_receivers').split()).split(';')

    def get_ota_prices_receivers(self):
        self.set_mail_receiver()
        return self.get_mail_receiver().get_receivers_by_email_id(EmailReceiver.EMAIL_ID_3002.value)

    def get_final_result_receivers(self):
        self.set_mail_receiver()
        return self.get_mail_receiver().get_receivers_by_email_id(EmailReceiver.EMAIL_ID_3003.value)

    def get_hotel_price_data_receivers(self):
        self.set_mail_receiver()
        return self.get_mail_receiver().get_receivers_by_email_id(EmailReceiver.EMAIL_ID_3004.value)

    def get_hotel_price_insert_result_mail_receivers(self):
        self.set_mail_receiver()
        return self.get_mail_receiver().get_receivers_by_email_id(EmailReceiver.EMAIL_ID_3005.value)

    def get_ota_plugin_result_receivers(self):
        self.set_mail_receiver()
        return self.get_mail_receiver().get_receivers_by_email_id(EmailReceiver.EMAIL_ID_3006.value)

    def get_manual_lms_mail_receivers(self):
        self.set_mail_receiver()
        return self.get_mail_receiver().get_receivers_by_email_id(EmailReceiver.EMAIL_ID_3007.value)

    def get_ota_plugin_ota_prices_receivers(self):
        self.set_mail_receiver()
        return self.get_mail_receiver().get_receivers_by_email_id(EmailReceiver.EMAIL_ID_3008.value)

    def get_ota_plugin_final_result_receivers(self):
        self.set_mail_receiver()
        return self.get_mail_receiver().get_receivers_by_email_id(EmailReceiver.EMAIL_ID_3009.value)

    def get_ota_plugin_hotel_price_data_receivers(self):
        self.set_mail_receiver()
        return self.get_mail_receiver().get_receivers_by_email_id(EmailReceiver.EMAIL_ID_3010.value)

    def get_liquidation_v1_hotel_list_mail_receivers(self):
        self.set_mail_receiver()
        return self.get_mail_receiver().get_receivers_by_email_id(EmailReceiver.EMAIL_ID_3011.value)
