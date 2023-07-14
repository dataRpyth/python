import os
import sys
import time
from os.path import join as join_path

cur_path = os.path.dirname(os.path.realpath(__file__))
sys.path.append(join_path(cur_path, '..'))

from common.sendmail.mail_send_base import MailSendSpecialSaleBase
from common.sendmail.mail_sender import MailSender
from common.util.utils import LogUtil, DateUtil, DFUtil


class MailSend(MailSendSpecialSaleBase):
    # 发送邮件Type1
    def send_mail_for_liquidation_to_pc(self, config, df_for_pc_liquidation):
        _begin = time.time()
        _job_name = config.get_job().get_job_name()
        batch = config.get_liq_batch()
        sub = '{0} special-sale hotels to PricingCenter, batch:{1}, {2}'.format(config.get_job().get_job_name(), batch,
                                                                                DateUtil.stamp_to_date_format2(_begin))
        df_for_pc_liquidation.reset_index(drop=True, inplace=True)
        head = "Dear all!\n This is the hotel list for {0}".format(_job_name)
        mail_content = DFUtil.gen_excel_content_by_html(head)
        attach_name = '{0}_special_sale_to_pc_{1}.xlsx'.format(config.get_job().get_job_name(),
                                                               DateUtil.stamp_to_date_format2(_begin))
        local_file_folder = config.get_local_file_folder()

        file_path = join_path(local_file_folder, attach_name)
        df_for_pc_liquidation.to_excel(file_path, index=False)

        MailSender.send_for_df(config, config.get_liquidation_v1_hotel_list_mail_receivers(), sub, mail_content,
                               df_for_pc_liquidation, attach_name, sub, True)
        LogUtil.get_cur_logger().info('send special-sale hotels to PricingCenter for receivers done %d s',
                                      time.time() - _begin)
