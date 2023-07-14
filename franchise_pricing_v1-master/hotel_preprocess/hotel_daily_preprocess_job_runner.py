import os
import sys
import warnings
from os.path import join as join_path

cur_path = os.path.dirname(os.path.realpath(__file__))
sys.path.append(join_path(cur_path, '..'))
warnings.filterwarnings("ignore")

from pathos.multiprocessing import ProcessingPool
from common.job_base.job_base import JobBase
from common.job_common.job_source import JobSource
from strategy.china_rebirth import *
from strategy.rule_base import *
from CHINA_Rebirth.run_rule_base_version import ChinaRebirthRuleBaseJob
from hotel_preprocess.configuration import PROCESS_TYPE_LIQUIDATION_PREPARE, PROCESS_TYPE_LIQUIDATION_WALKIN_RAISE


class HotelDailyPreprocessJob(JobBase):

    def __init__(self, job_config):
        JobBase.__init__(self, job_config)

    def get_job_name(self):
        return 'HotelDailyPreprocess'

    @staticmethod
    def get_oracle_insert_engine():
        db = os.environ['OYO_PRICING_ORACLE_SINK_DB_NAME']
        host = os.environ['OYO_PRICING_ORACLE_SINK_DB_HOST']
        port = os.environ['OYO_PRICING_ORACLE_SINK_DB_PORT']
        user = os.environ['OYO_PRICING_ORACLE_SINK_DB_USER']
        password = os.environ['OYO_PRICING_ORACLE_SINK_DB_PASSWORD']
        return DBUtil.create_oracle_engine(user, password, host, port, db)

    def insert_hotel_metrics_to_oracle(self, df_hotel_metrics, sink_table_name):
        engine = self.get_oracle_insert_engine()
        LogUtil.get_cur_logger().info('sinking into table: {}'.format(sink_table_name))
        aux_dtypes = DFUtil.get_oracle_auxiliary_insert_dtype_for_df(df_hotel_metrics)
        LogUtil.get_cur_logger().info('start inserting to oracle')
        df_hotel_metrics.to_sql(sink_table_name, engine.connect(), if_exists='append', index=False, dtype=aux_dtypes)
        LogUtil.get_cur_logger().info('end inserting to oracle')

    def run(self):

        start_stamp = time.time()

        LogUtil.get_cur_logger().info(
            'job: {0} started running, start_time: {1}'.format(self.get_job_name(), start_stamp))

        DateUtil.init_preset_time()

        job_source = JobSource(self.get_adb_query_manager(), self.get_mysql_query_manager(),
                               self.get_oracle_query_manager())

        config = self.get_job_config()

        start_time = dt.datetime.fromtimestamp(start_stamp)

        batch_order = config.get_batch_order()

        process_type = config.get_process_type()

        set_oyo_id = job_source.get_hotel_batch_oyo_set_from_center(batch_order, start_stamp)

        LogUtil.get_cur_logger().info('total hotels to calculate: {}'.format(len(set_oyo_id)))

        lst_oyo_id = list(set_oyo_id)

        calc_start = time.time()

        start_date = DateUtil.stamp_to_date_format(start_stamp, format_str="%Y-%m-%d", offset=0)

        if process_type == PROCESS_TYPE_LIQUIDATION_PREPARE:

            oyo_id_list_groups = MiscUtil.group_by_list(lst_oyo_id, 4000)

            smart_hotel_params_df_list = list()

            pool = ProcessingPool()

            disable_ebase = config.disable_ebase()

            for oyo_id_list in oyo_id_list_groups:
                group_smart_hotel_params_df = ChinaRebirthRuleBaseJob.static_run_core(oyo_id_list, start_time, pool,
                                                                                      self.get_mysql_query_manager(),
                                                                                      self.get_oracle_query_manager(),
                                                                                      self.get_adb_query_manager(),
                                                                                      self.get_robot_token_op_alert(),
                                                                                      self.get_robot_token_internal_alert(),
                                                                                      self.get_robot_token_oracle_upstream(),
                                                                                      disable_ebase)
                smart_hotel_params_df_list.append(group_smart_hotel_params_df)

            pool.close()

            pool.join()

            smart_hotel_params_df = pd.concat(smart_hotel_params_df_list, ignore_index=True)

            two_weeks_before = DateUtil.stamp_to_date_format(start_stamp, format_str="%Y-%m-%d", offset=-7)

            base_price_df = smart_hotel_params_df[smart_hotel_params_df.date == start_date][
                ['oyo_id', 'date', 'corrected_base']]

            base_price_df.rename(columns={'corrected_base': 'base', 'date': 'calc_date'}, inplace=True)

            history_trade_df = MiscUtil.wrap_read_adb_df(job_source.get_trade_price_df, set_oyo_id,
                                                                two_weeks_before,
                                                                start_date)

            two_weeks_arr_df = history_trade_df.groupby(by=['oyo_id']).mean()

            two_weeks_arr_df.rename(columns={'price': 'two_week_arr'}, inplace=True)

            df_to_insert = base_price_df.join(two_weeks_arr_df, how='inner', on=['oyo_id'])[
                ['oyo_id', 'calc_date', 'base', 'two_week_arr']]

            sink_table = self.get_job_config().get_hotel_metrics_sink_table()
        else:
            raise Exception('unknown process type: {}'.format(process_type))

        calc_end = time.time()

        calc_time = int(calc_end - calc_start)

        hotels_cnt = df_to_insert.shape[0]

        insert_start = time.time()

        self.insert_hotel_metrics_to_oracle(df_to_insert, sink_table)

        insert_end = time.time()

        insert_time = int(insert_end - insert_start)

        LogUtil.get_cur_logger().info('job end, successfully inserted {0} records'.format(hotels_cnt))

        LogUtil.get_cur_logger().info(
            'process type: {}, time consumed: {}s, records in the result: {}'.format(process_type, calc_time,
                                                                                     hotels_cnt))

        ddt_msg = 'job: {} successfully calculated for {} records for {} hotels,' \
                  'env: {}, ' \
                  'process type: {}, '\
                  'sink table: {} ' \
                  'calculate time: {}s, ' \
                  'db-insert time: {}s'.format(self.get_job_name(), hotels_cnt, hotels_cnt,
                                               self.get_job_config().get_env(), process_type,
                                               sink_table, calc_time, insert_time)

        DdtUtil.robot_send_ddt_msg(ddt_msg, self.get_robot_token_op_alert(), is_at_all=False)
