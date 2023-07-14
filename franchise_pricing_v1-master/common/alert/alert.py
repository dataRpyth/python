import os
import sys
from os.path import join as join_path

cur_path = os.path.dirname(os.path.realpath(__file__))
sys.path.append(join_path(cur_path, '..'))
import traceback
import time
from common.util.utils import LogUtil, DdtUtil, DateUtil

PRICING_JOB_ALERT_ROBOT_ACCESS_TOKEN_INVALID = 'e37a87f4c7dc137f1a4b0c59d67640325c4588e50558478572dd14ceaa8c07b2'

PRICING_JOB_ALERT_ROBOT_ACCESS_TOKEN_DEBUG = 'a6e89792a9b40c383d8fcc2937b3fc35f46c6428ca6a142669710ccf4ea8cd2c'

INTERNAL_PROD_DDT_ALERT_ROBOT_TOKEN = '42d9608e03776f74b5ef897b5e882522d8645fc6d19c9ea2796c3dbfb2aff37e'


_ROBOT_ALERT_MOBILES = ['17610066978', '17754400587']


class PricingAlert:
    @staticmethod
    def run_job(job):
        try:
            job.run()
        except Exception:
            stack_trace = traceback.format_exc()
            send_ddt_alert = job.is_prod()
            LogUtil.get_cur_logger().error('exception happened, stack trace: {0}'.format(stack_trace))
            if not send_ddt_alert:
                sys.exit(1)
            job_run_time = DateUtil.stamp_to_date_format4(time.time())
            msg = PricingAlert._compose_alert_msg(job.get_job_name(), job_run_time, stack_trace)
            DdtUtil.robot_send_ddt_msg(msg, job.get_robot_token_internal_alert(), None, False)
        sys.exit(0)

    @staticmethod
    def _compose_alert_msg(job_name, job_run_time, stack_trace):
        return 'job name: {} \n' \
               'job start time: {} \n' \
               'stack trace: {}'.format(job_name, job_run_time, stack_trace)
