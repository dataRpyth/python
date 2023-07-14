import os
import sys
import time
from os.path import join as join_path

cur_path = os.path.dirname(os.path.realpath(__file__))
sys.path.append(join_path(cur_path, '..'))

import pandas as pd
from common.sendmail.mail_send_base import MailSendPriceBase
from common.sendmail.mail_sender import MailSender, DdtUtil, DataFrameWithSheetName
from common.util.utils import LogUtil, DateUtil, DFUtil


class MailSend(MailSendPriceBase):

    # 发送邮件Type1
    def send_mail_for_holiday_base_price(self, config, df_for_holiday_base):
        _begin = time.time()
        _job_name = config.get_job().get_job_name()
        batch = config.get_future_batch()
        holiday_desc = config.get_holiday_desc()
        date_time = DateUtil.stamp_to_date_format2(_begin)
        df_for_holiday_base = df_for_holiday_base[
            ["oyo_id", "hotel_id", "hotel_name", "total_count", "date", "srn", "brn", "occ", "base", "rate_diff",
             "base_override", "strategy_type", "price_start_date"]]
        sub = '{0} holiday-base-price_{1}-{2}_{3}'.format(_job_name, holiday_desc, batch, date_time)
        df_for_holiday_base.reset_index(drop=True, inplace=True)
        head = "Dear all!\n This is the hotel list for base price of {0}, holiday:{1}, future_batch:{2}".format(
            _job_name, holiday_desc, batch)
        mail_content = DFUtil.gen_excel_content_by_html(head)
        attach_name = '{0}_holiday_base_price_{1}-{2}_{3}.xlsx'.format(_job_name, holiday_desc, batch, date_time)
        local_file_folder = config.get_local_file_folder()

        file_path = join_path(local_file_folder, attach_name)
        df_for_holiday_base.to_excel(file_path, index=False)

        MailSender.send_for_df(config, config.get_holiday_future_v1_mail_receivers(), sub, mail_content,
                               df_for_holiday_base, attach_name, sub, True)
        LogUtil.get_cur_logger().info('send holiday-base-price for receivers done %d s', time.time() - _begin)

    def send_mail_for_holiday_base_price_monitor(self, config, failed_pojo_chunk_list, succeeded_pojo_chunk_list):
        upload_finish_readable_time = DateUtil.stamp_to_date_format3(time.time())
        upload_finish_file_name_time = DateUtil.stamp_to_date_format(time.time(), "%Y_%m_%d_%H_%M_%S")
        temp_file_folder = config.get_local_file_folder()
        biz_name = config.get_job().get_job_name()
        batch = config.get_future_batch()
        holiday_desc = config.get_holiday_desc()

        succeeded_report_df = self.get_df_from_pojo_chunk_list(biz_name, succeeded_pojo_chunk_list,
                                                               upload_finish_readable_time)
        failed_report_df = self.get_df_from_pojo_chunk_list(biz_name, failed_pojo_chunk_list,
                                                            upload_finish_readable_time)
        robot_token = config.get_robot_send_token()
        if not failed_report_df.empty:
            DdtUtil.robot_send_ddt_msg('业务线: {0} 上传假日滚动base_price数据到PricingCenter出现异常'.format(biz_name),
                                       robot_token)
        df_with_sheet_names = [DataFrameWithSheetName(succeeded_report_df, '上传成功'),
                               DataFrameWithSheetName(failed_report_df, '上传失败')]
        excel_file_path = join_path(temp_file_folder, '{0}_holiday_base_price_report_{1}.xlsx'
                                    .format(biz_name, upload_finish_file_name_time))
        excel_file = DFUtil.write_multiple_df_to_excel(excel_file_path, df_with_sheet_names)

        sub = '{0} the report of holiday_base_price to pricing center_{1}_{2}_{3}'.format(biz_name, holiday_desc, batch,
                                                                                          DateUtil.stamp_to_date_format5(
                                                                                              time.time()))
        mail_content = "Dear all!\n This is the report of holiday_base_price to pricing center for {0}, succeeded: {1}, " \
                       "failed: {2} ".format(biz_name, succeeded_report_df.shape[0], failed_report_df.shape[0])
        attach_name = '{0}_holiday_base_price_report_{1}_{2}_{3}.xlsx'.format(biz_name, holiday_desc, batch,
                                                                              upload_finish_file_name_time)

        MailSender.send_mail(config.get_mail_user(), config.get_mail_pass(),
                             config.get_holiday_future_v1_report_receivers(), sub, mail_content, excel_file,
                             attach_name)

    @staticmethod
    def get_df_from_pojo_chunk_list(biz_name, pojo_chunk_list, upload_time):
        if len(pojo_chunk_list) == 0:
            return pd.DataFrame()
        row_dict_list = list()
        for pojo_chunk in pojo_chunk_list:
            datas = pojo_chunk.basePrices
            for data in datas:
                row_dict = {'bizName': biz_name, 'oyoId': data.oyoId,
                            'roomTypeId': data.roomTypeId, 'pricingDate': data.pricingDate, 'basePrice': data.basePrice,
                            "reasonId": data.reasonId, 'uploadTime': upload_time}
                row_dict_list.append(row_dict)
        report_df = pd.DataFrame(row_dict_list)
        return report_df
