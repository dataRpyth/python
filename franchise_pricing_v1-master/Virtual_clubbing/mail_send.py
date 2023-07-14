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
    def send_mail_for_share_inventory(self, config, df_share_inventor):
        _begin = time.time()
        _job_name = config.get_job().get_job_name()
        date_time = DateUtil.stamp_to_date_format2(_begin)
        df_for_holiday_base = df_share_inventor[
            ["hotel_id", "room_type_id", "share_type", "share_room_type_id", "share_count", "is_valid", "valid_from",
             "valid_to", "channel_list"]]
        sub = '{0} share-inventory_{1}'.format(_job_name, date_time)
        df_for_holiday_base.reset_index(drop=True, inplace=True)
        head = "Dear all!\n This is the hotel list for share-inventory of {0}".format(_job_name)
        mail_content = DFUtil.gen_excel_content_by_html(head)
        attach_name = '{0}_share-inventory_to_trade_{1}.xlsx'.format(_job_name, date_time)
        local_file_folder = config.get_local_file_folder()

        file_path = join_path(local_file_folder, attach_name)
        df_for_holiday_base.to_excel(file_path, index=False)

        MailSender.send_for_df(config, config.get_share_inventory_to_trade_v1_mail_receivers(), sub, mail_content,
                               df_for_holiday_base, attach_name, sub, True)
        LogUtil.get_cur_logger().info('send share-inventory for receivers done %d s', time.time() - _begin)

    def send_mail_for_share_inventory_monitor(self, config, failed_pojo_chunk_list, succeeded_pojo_chunk_list):
        time_finished = DateUtil.stamp_to_date_format(time.time(), "%Y_%m_%d_%H_%M_%S")
        biz_name = config.get_job().get_job_name()

        succeeded_report_df = self.get_df_from_pojo_chunk_list(succeeded_pojo_chunk_list)
        failed_report_df = self.get_df_from_pojo_chunk_list(failed_pojo_chunk_list)

        fail_count = CommonUtil.get_count_for_chunk_list_attr(failed_pojo_chunk_list, "shareInventoryList")
        suc_count = CommonUtil.get_count_for_chunk_list_attr(succeeded_pojo_chunk_list, "shareInventoryList")

        robot_token = config.get_robot_send_token()
        if not failed_report_df.empty:
            DdtUtil.robot_send_ddt_msg('业务线: {0} 上传share_inventory数据到Trade出现异常'.format(biz_name),
                                       robot_token)
        multi_sheets = [DataFrameWithSheetName(succeeded_report_df, '上传成功'),
                        DataFrameWithSheetName(failed_report_df, '上传失败')]

        sub = '{0} the report of share_inventory to Trade {1}'.format(biz_name, DateUtil.stamp_to_date_format5(
            time.time()))
        mail_content = "Dear all!\n This is the report of share_inventory to Trade for {0}, succeeded: {1}, " \
                       "failed: {2} ".format(biz_name, succeeded_report_df.shape[0], failed_report_df.shape[0])
        attach_name = '{0}_share-inventory_to_trade_report_{1}.xlsx'.format(biz_name, time_finished)
        head = "业务线: {0}, 共享库存数据上传完成, 共:{1}, 成功:{2}, 失败:{3}, 时间:{4}".format(
            biz_name, str(suc_count + fail_count), str(suc_count), str(fail_count), time_finished)
        MailSender.send_for_multi_sheet(config, config.get_share_inventory_monitor_receivers(), sub, mail_content,
                                        multi_sheets, attach_name, head, True)

    @staticmethod
    def get_df_from_pojo_chunk_list(pojo_chunk_list):
        if len(pojo_chunk_list) == 0:
            return pd.DataFrame()
        row_dict_list = list()
        for pojo_chunk in pojo_chunk_list:
            share_inventor_lst = pojo_chunk.shareInventoryList
            for share_inventor in share_inventor_lst:
                row_dict = {'hotel_id': share_inventor.hotelId, 'room_type_id': share_inventor.roomTypeId,
                            'share_type': share_inventor.shareType,
                            'share_room_type_id': share_inventor.shareRoomTypeId,
                            'share_count': share_inventor.shareCount, 'is_valid': share_inventor.isValid,
                            "valid_from": share_inventor.validFrom, 'valid_to': share_inventor.validTo,
                            "channel_list": share_inventor.channelList}
                row_dict_list.append(row_dict)
        report_df = pd.DataFrame(row_dict_list)
        return report_df
