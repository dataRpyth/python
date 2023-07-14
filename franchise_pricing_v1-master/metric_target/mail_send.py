import os
import sys
import time
from os.path import join as join_path

cur_path = os.path.dirname(os.path.realpath(__file__))
sys.path.append(join_path(cur_path, '..'))

import pandas as pd
from common.sendmail.mail_sender import MailSender, DdtUtil, DataFrameWithSheetName
from common.util.utils import LogUtil, DateUtil, DFUtil, CommonUtil


class MailSend:

    # 发送邮件Type1
    def send_mail_for_metrics_target(self, config, df_for_metrics_target):
        _begin = time.time()
        _job_name = config.get_job().get_job_name()
        date_time = DateUtil.stamp_to_date_format2(_begin)

        df_for_metrics_target = df_for_metrics_target[["oyo_id", "data_date", "hour", "metrics_id", "metrics_value", "metrics_details", "last_update"]]
        sub = '{0} metrics_target_{1}'.format(_job_name, date_time)
        ctx = 'metrics_target【{0}】to_PC, env: {1}'.format(len(df_for_metrics_target), config.get_config_env())
        df_for_metrics_target.reset_index(drop=True, inplace=True)
        head = "Dear all!\n This is the data for metrics_target of {0}".format(_job_name)
        mail_content = DFUtil.gen_excel_content_by_html(head)
        attach_name = '{0}_data_to_PC_{1}.xlsx'.format(_job_name, date_time)
        local_file_folder = config.get_local_file_folder()

        file_path = join_path(local_file_folder, attach_name)
        df_for_metrics_target.to_excel(file_path, index=False)

        MailSender.send_for_df(config, config.get_metrics_target_mail_receivers(), sub, mail_content,
                               df_for_metrics_target, attach_name, ctx, True)
        LogUtil.get_cur_logger().info('send metrics_target for receivers done %d s', time.time() - _begin)

    def send_mail_for_metrics_target_monitor(self, config, failed_pojo_chunk_list, succeeded_pojo_chunk_list):
        time_finished = DateUtil.stamp_to_date_format(time.time(), "%Y_%m_%d_%H_%M_%S")
        biz_name = config.get_job().get_job_name()

        succeeded_report_df = self.get_df_from_pojo_chunk_list(succeeded_pojo_chunk_list)
        failed_report_df = self.get_df_from_pojo_chunk_list(failed_pojo_chunk_list)

        fail_count = CommonUtil.get_count_for_chunk_list_attr(failed_pojo_chunk_list, "items")
        suc_count = CommonUtil.get_count_for_chunk_list_attr(succeeded_pojo_chunk_list, "items")

        robot_token = config.get_robot_send_token()
        if fail_count > 0:
            DdtUtil.robot_send_ddt_msg('业务线: {0} 上传metrics_target数据到PricingCenter出现异常'.format(biz_name),
                                       robot_token)
            multi_sheets = [DataFrameWithSheetName(succeeded_report_df, '上传成功'),
                            DataFrameWithSheetName(failed_report_df, '上传失败')]

            sub = '{0} the report of metrics_target to PC {1}'.format(biz_name, DateUtil.stamp_to_date_format5(
                time.time()))
            mail_content = "Dear all!\n This is the report of metrics_target to PC for {0}, succeeded: {1}, " \
                           "failed: {2} ".format(biz_name, succeeded_report_df.shape[0], failed_report_df.shape[0])
            attach_name = '{0}_data_to_PC_report_{1}.xlsx'.format(biz_name, time_finished)
            head = "业务线: {0}, OCC-target数据上传完成, 共:{1}, 成功:{2}, 失败:{3}, 时间:{4}".format(
                biz_name, str(suc_count + fail_count), str(suc_count), str(fail_count), time_finished)
            MailSender.send_for_multi_sheet(config, config.get_metrics_target_mail_receivers(), sub, mail_content,
                                            multi_sheets, attach_name, head, True)

    @staticmethod
    def get_df_from_pojo_chunk_list(pojo_chunk_list):
        if len(pojo_chunk_list) == 0:
            return pd.DataFrame()
        row_dict_list = list()
        for pojo_chunk in pojo_chunk_list:
            infos = pojo_chunk.items
            for info in infos:
                # "oyo_id", "data_date", "hour", "metrics_id", "metrics_value", "metrics_details", "last_update"
                row_dict = {'oyo_id': info.oyoId, 'data_date': info.dataDate,
                            'hour': info.hour, 'metrics_id': info.metricsId,
                            'metrics_value': info.metricsValue, 'metrics_details': info.metricsDetails
                            , 'last_update': info.lastUpdate
                            }
                row_dict_list.append(row_dict)
        report_df = pd.DataFrame(row_dict_list)
        return report_df
