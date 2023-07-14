#!/usr/bin/env python
# -*- coding:utf-8 -*-

import os
import sys
import time
from os.path import join as join_path

cur_path = os.path.dirname(os.path.realpath(__file__))
sys.path.append(join_path(cur_path, '..'))

from common.util.utils import LogUtil, DateUtil, DFUtil
from common.sendmail.mail_sender import MailSender
from common.sendmail.mail_send_base import MailSendPriceBase, MailSendReportBase, MailSendSpecialSaleBase


class MailSend(MailSendPriceBase, MailSendReportBase, MailSendSpecialSaleBase):

    def send_mail_for_hotel_special_sale_override(self, config, attach_file):
        __begin = time.time()
        _job_name = config.get_job().get_job_name()
        sub = "{0} the list of hotels covered by special sales {1}".format(_job_name,
                                                                           DateUtil.stamp_to_date_format1(__begin))
        attach_name = '{0}_special_sales_covered_result_{1}.xlsx'.format(_job_name,
                                                                         DateUtil.stamp_to_date_format1(__begin))
        attach_file.reset_index(drop=True, inplace=True)
        head = "Dear all!\n This is the list of hotels ({0}) covered by special sales for {1}".format(
            attach_file.shape[0], _job_name)
        mail_content = DFUtil.gen_excel_content_by_html(head)
        MailSender.send_for_df(config, config.get_liquidation_v1_hotel_list_mail_receivers(), sub, mail_content,
                               attach_file, attach_name, sub, True)
        LogUtil.get_cur_logger().info('send mail for the list of hotels covered by special sales %d s',
                                      time.time() - __begin)
