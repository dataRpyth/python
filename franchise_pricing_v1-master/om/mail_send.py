import datetime as dt
import os
import sys
import time
from os.path import join as join_path

cur_path = os.path.dirname(os.path.realpath(__file__))
sys.path.append(join_path(cur_path, '..'))

from common.sendmail.mail_send_base import MailSendPriceBase, MailSendReportBase
from common.sendmail.mail_sender import MailSender
from common.util.utils import LogUtil, DateUtil, DFUtil

OTA_PLUGIN_PREFIX = '(OM_OTA_PLUGIN)'


def ota_plugin_sub(config, sub_text):
    return OTA_PLUGIN_PREFIX + sub_text if config.is_ota_plugin() else sub_text


def get_date_str(config, _stamp):
    start_time = dt.datetime.fromtimestamp(_stamp)
    start_date = start_time + dt.timedelta(days=config.get_day_shift_start())
    start_date_str = dt.datetime.strftime(start_date, "%Y-%m-%d")
    end_date = start_date + dt.timedelta(days=config.get_day_range_len() - 1)
    end_date_str = dt.datetime.strftime(end_date, "%Y-%m-%d")
    return end_date_str, start_date_str


class MailSend(MailSendPriceBase, MailSendReportBase):

    # 发送邮件Type1
    def send_mail_for_intermediate_result(self, config, attach_file):
        _begin = time.time()
        _job_name = config.get_job().get_job_name()
        attach_file.reset_index(drop=True, inplace=True)
        if not config.is_preset():
            sub = "{0} hotels dynamic pricing intermediate results {1}".format(_job_name,
                                                                               DateUtil.stamp_to_date_format2(_begin))
            head = "Dear all!\n This is the dynamic pricing intermediate result for OM hotels, batch order: {0}" \
                .format(config.get_batch_order())
            mail_content = DFUtil.gen_excel_content_by_html(head, attach_file)
            attach_name = '{0}_intermediate_results_{1}.xlsx'.format(_job_name, config.get_file_date_str())
        else:
            end_date_str, start_date_str = get_date_str(config, _begin)
            sub = "{0} hotels dynamic pricing intermediate results for preset {1} days".format(_job_name,
                                                                                               config.get_day_range_len())

            head = "Dear all!\n This is the dynamic pricing intermediate result for {0} hotels, start date: {1}, end date: {2}".format(
                _job_name, start_date_str, end_date_str)
            mail_content = DFUtil.gen_excel_content_by_html(head, attach_file)
            attach_name = '{0}_preset_intermediate_results_{0}.xlsx'.format(_job_name, config.get_file_date_str())
        sub = ota_plugin_sub(config, sub)
        MailSender.send_for_df(config, config.get_final_result_receivers(), sub, mail_content, attach_file, attach_name,
                               "from " + _job_name, False)
        LogUtil.get_cur_logger().info('send ota_prices_receivers done %d s', time.time() - _begin)

    # 发送邮件Type2
    def send_mail_for_pms_result(self, config, attach_file):
        __begin = time.time()
        _job_name = config.get_job().get_job_name()
        attach_file.reset_index(drop=True, inplace=True)
        _date_with_hour = DateUtil.stamp_to_date_format2(__begin)
        if not config.is_preset():
            sub = "{0} hotels dynamic pricing PMS results {1}".format(_job_name, _date_with_hour)
            head = "Dear all!\n This is the dynamic pricing PMS result for {0} hotels, batch order: {1}".format(
                _job_name, config.get_batch_order())
            mail_content = DFUtil.gen_excel_content_by_html(head, attach_file)
            attach_name = '{0}_pms_prices_{1}.xlsx'.format(_job_name, _date_with_hour)
        else:
            end_date_str, start_date_str = get_date_str(config, __begin)
            head = "Dear all!\n This is the dynamic pricing PMS result for {0} hotels from: {1} to: {2}".format(
                _job_name, start_date_str, end_date_str)
            mail_content = DFUtil.gen_excel_content_by_html(head, attach_file)
            sub = "{0} hotels dynamic pricing PMS results for preset {1} days".format(_job_name,
                                                                                      config.get_day_range_len())
            attach_name = '{0}_preset_pms_prices_{1}.xlsx'.format(_job_name, _date_with_hour)
        sub = ota_plugin_sub(config, sub)
        MailSender.send_for_df(config, config.get_hotel_price_data_receivers(), sub, mail_content, attach_file,
                               attach_name, "from " + _job_name, False)
        LogUtil.get_cur_logger().info('send price_insert_result_mail_receivers done %d s', time.time() - __begin)

    # 发送邮件Type3
    def send_mail_for_cc_result(self, config, attach_file):
        __begin = time.time()
        _job_name = config.get_job().get_job_name()
        attach_file.reset_index(drop=True, inplace=True)
        if not config.is_preset():
            sub = "{0} hotels dynamic pricing OTA results {1}".format(_job_name,
                                                                      DateUtil.stamp_to_date_format2(__begin))
            head = "Dear all!\n This is the dynamic pricing OTA result for {0} hotels, batch order: {1}".format(
                _job_name, config.get_batch_order())
            mail_content = DFUtil.gen_excel_content_by_html(head, attach_file)
            attach_name = '{0}_ota_results_{1}.xlsx'.format(_job_name, DateUtil.stamp_to_date_format2(__begin))
        else:
            end_date_str, start_date_str = get_date_str(config, __begin)
            sub = "(预埋价格) {0} hotels dynamic pricing OTA results for {1} days".format(_job_name,
                                                                                      config.get_day_range_len())
            head = "此邮件为直营价格预埋邮件，本次预埋改价时间段为：{0} ~ {1}".format(start_date_str, end_date_str)
            mail_content = DFUtil.gen_excel_content_by_html(head, attach_file)
            attach_name = '{0}_preset_ota_results_{1}.xlsx'.format(_job_name, DateUtil.stamp_to_date_format2(__begin))
        sub = ota_plugin_sub(config, sub)
        MailSender.send_for_df(config, config.get_ota_prices_receivers(), sub, mail_content, attach_file, attach_name,
                               "from " + _job_name, True)
        LogUtil.get_cur_logger().info('send ota_prices_receivers done %d s', time.time() - __begin)
