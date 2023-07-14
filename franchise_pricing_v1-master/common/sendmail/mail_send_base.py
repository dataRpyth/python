#!/usr/bin/env python
# -*- coding:utf-8 -*-

import os
import sys
import time
from os.path import join as join_path

cur_path = os.path.dirname(os.path.realpath(__file__))
sys.path.append(join_path(cur_path, '..'))
os.environ['NLS_LANG'] = '.AL32UTF8'

from common.pricing_pipeline.pipeline import PricingPipeline, DataFrameWithSheetName
from common.util.utils import LogUtil, DateUtil, DFUtil, DdtUtil
from common.sendmail.mail_sender import MailSender
import pandas as pd

MAIL_FILE_TYPE = ".xlsx"


def get_mail_suffix(config, file_suffix=None):
    _begin = time.time()
    _sub_suffix = DateUtil.stamp_to_date_format1(_begin)
    _job_name = config.get_job().get_job_name()
    if file_suffix is None:
        return _begin, _job_name, _sub_suffix
    _suffix_attach_file = DateUtil.stamp_to_date_format2(_begin) + file_suffix
    return _begin, _job_name, _sub_suffix, _suffix_attach_file


class MailSendPriceBase:

    # 发送邮件Type1
    def send_mail_for_ota_prices(self, config, attach_file):
        _begin, _job_name, _sub_suffix, _suffix_attach_file = get_mail_suffix(config, MAIL_FILE_TYPE)
        if config.is_preset():
            sub = "(Preset result){0} hotel Dynamic Pricing {1}".format(_job_name, _sub_suffix)
            attach_name = '{0}_hotel_preset_price_{1}'.format(_job_name, _suffix_attach_file)
        else:
            sub = "{0} hotel Dynamic Pricing {1}".format(_job_name, _sub_suffix)
            attach_name = '{0}_hotel_price_{1}'.format(_job_name, _suffix_attach_file)
        attach_file = attach_file.sort_values(['oyo_id', 'room_type_id', 'date'], ascending=True)
        attach_file.reset_index(drop=True, inplace=True)
        attach_file = PricingPipeline.ota_prices_attach_file_rename(attach_file)
        head = "Dear all!\n This is the hotel dynamic pricing result（{0}） for the {1} ".format(attach_file.shape[0],
                                                                                               _job_name)
        mail_content = DFUtil.gen_excel_content_by_html(head)
        MailSender.send_for_df(config, config.get_ota_prices_receivers(), sub, mail_content, attach_file, attach_name,
                               "from " + _job_name, True)
        LogUtil.get_cur_logger().info('send mail for ota prices done %d s', time.time() - _begin)

    # 发送邮件Type2
    def send_mail_for_final_result(self, config, attach_file):
        _begin, _job_name, _sub_suffix, _suffix_attach_file = get_mail_suffix(config, MAIL_FILE_TYPE)
        sub = "{0} Result final {1}".format(_job_name, _sub_suffix)
        attach_name = '{0}_result_final_{1}'.format(_job_name, _suffix_attach_file)
        attach_file.reset_index(drop=True, inplace=True)
        head = "Dear all!\n This is the hotel final result({0}) for the {1}.".format(attach_file.shape[0], _job_name)
        mail_content = DFUtil.gen_excel_content_by_html(head)
        MailSender.send_for_df(config, config.get_final_result_receivers(), sub, mail_content, attach_file, attach_name)
        LogUtil.get_cur_logger().info('send mail for final result done %d s', time.time() - _begin)

    # 发送邮件Type3
    def send_mail_for_pms_price(self, config, attach_file):
        _begin, _job_name, _sub_suffix, _suffix_attach_file = get_mail_suffix(config, MAIL_FILE_TYPE)
        if config.is_preset():
            sub = "(Preset){0} hotel Pms Pricing {1}".format(_job_name, _sub_suffix)
            attach_name = '{0}_pms_preset_price_data_{1}'.format(_job_name, _suffix_attach_file)
        else:
            sub = "{0} hotel Pms Pricing {1}".format(_job_name, _sub_suffix)
            attach_name = '{0}_pms_price_data_{1}'.format(_job_name, _suffix_attach_file)
        attach_file.reset_index(drop=True, inplace=True)
        head = "Dear all!\n This is the hotel Pms pricing result({0}) for the {1} .".format(attach_file.shape[0],
                                                                                            _job_name)
        mail_content = DFUtil.gen_excel_content_by_html(head)
        MailSender.send_for_df(config, config.get_hotel_price_data_receivers(), sub, mail_content, attach_file,
                               attach_name)
        LogUtil.get_cur_logger().info('send mail for price data done %d s', time.time() - _begin)

    # 发送邮件Type4
    def send_mail_for_report_hotel_price_to_pms(self, config, mail_content, attach_file, attach_name):
        _begin, _job_name, _sub_suffix = get_mail_suffix(config)
        attach_name = "{0}_HotelPriceToPms_{1}".format(_job_name, attach_name) if attach_name is not None else ""
        sub = "{0} data to PMS insert result for {1} days {2}".format(_job_name, str(config.get_calc_days()),
                                                                      _sub_suffix)
        MailSender.send_for_df(config, config.get_hotel_price_insert_result_mail_receivers(), sub, mail_content,
                               attach_file, attach_name)
        LogUtil.get_cur_logger().info('send mail for insertHotelPriceToPms done %d s', time.time() - _begin)

    # 发送邮件—ota-price-upload-5
    def send_mail_for_ota_price_upload(self, config, mail_content, attach_file, attach_name):
        _begin, _job_name, _sub_suffix = get_mail_suffix(config)
        sub = "{0} data to otaPriceUpload api result {1}".format(_job_name, _sub_suffix)
        MailSender.send_for_df(config, config.get_price_log_post_result_receivers(), sub, mail_content, attach_file,
                               attach_name)
        LogUtil.get_cur_logger().info('send mail for otaPriceUpload done %d s', time.time() - _begin)

    # 发送OTA改价插件结果v1
    def send_mail_for_ota_plugin_v1_result(self, config, ota_plugin_dfs, file_date_str, batch_order):
        PricingPipeline.send_ota_plugin_v1_dfs_by_mail(ota_plugin_dfs[0], ota_plugin_dfs[1],
                                                       config.get_local_file_folder(),
                                                       file_date_str, config.is_preset(), config.get_mail_user(),
                                                       config.get_mail_pass(),
                                                       config.get_ota_plugin_result_receivers(), batch_order,
                                                       config.get_job().get_job_name())

    # 发送OTA改价插件结果v2
    def send_mail_for_ota_plugin_v2_result(self, config, ota_plugin_dfs, file_date_str, batch_order):
        PricingPipeline.send_ota_plugin_v2_dfs_by_mail_and_ddt_robot(ota_plugin_dfs, config.get_local_file_folder(),
                                                                     file_date_str, config.get_mail_user(),
                                                                     config.get_mail_pass(),
                                                                     config.get_ota_plugin_result_receivers(),
                                                                     batch_order, config.get_job().get_job_name(),
                                                                     config.get_robot_send_biz_model_id(),
                                                                     config.get_robot_send_token(),
                                                                     config.get_ddt_env(), config.get_job_preset_time(),
                                                                     config.get_toggle_on_robot_send())


class MailSendReportBase:
    # 发送邮件—price_log_post-1
    def send_mail_for_report_franchise_v1_inter_result(self, config, mail_content, attach_file, attach_name):
        _begin, _job_name, _sub_suffix = get_mail_suffix(config)
        sub = "{0} data to FranchiseV1Inter insert pricing log for {1} days by {2}".format(_job_name,
                                                                                           str(config.get_calc_days()),
                                                                                           _sub_suffix)
        MailSender.send_for_df(config, config.get_price_log_post_result_receivers(), sub, mail_content, attach_file,
                               attach_name)
        LogUtil.get_cur_logger().info('send mail for insertFranchiseV1InterResult done,cost: %0.2fs', time.time() - _begin)

    # 发送邮件—price_log_post-2
    def send_mail_for_report_price_insert(self, config, mail_content, attach_file, attach_name):
        _begin, _job_name, _sub_suffix = get_mail_suffix(config)
        sub = "{0} data to PriceReport insert pricing log for {1} days by {2}".format(_job_name,
                                                                                      str(
                                                                                          config.get_calc_days()),
                                                                                      _sub_suffix)
        MailSender.send_for_df(config, config.get_price_log_post_result_receivers(), sub, mail_content, attach_file,
                               attach_name)
        LogUtil.get_cur_logger().info('send mail for PriceReport done,cost: %0.2fs', time.time() - _begin)

    # 发送邮件—price_log_post-3
    def send_mail_for_report_om_v1_inter_result_insert(self, config, mail_content, attach_file, attach_name):
        _begin, _job_name, _sub_suffix = get_mail_suffix(config)

        sub = "{0} data to OMV1InterResult insert pricing log for {1} days by {2}".format(_job_name,
                                                                                          str(
                                                                                              config.get_calc_days()),
                                                                                          _sub_suffix)
        MailSender.send_for_df(config, config.get_price_log_post_result_receivers(), sub, mail_content, attach_file,
                               attach_name)
        LogUtil.get_cur_logger().info('send mail for OMV1InterResult done,cost: %0.2fs', time.time() - _begin)

    # 发送邮件—price_log_post-4
    def send_mail_for_report_ota_price_insert(self, config, mail_content, attach_file, attach_name):
        _begin, _job_name, _sub_suffix = get_mail_suffix(config)

        sub = "{0} data to OtaPriceReport insert pricing log for {1} days by {2}".format(_job_name,
                                                                                         str(
                                                                                             config.get_calc_days()),
                                                                                         _sub_suffix)
        MailSender.send_for_df(config, config.get_price_log_post_result_receivers(), sub, mail_content, attach_file,
                               attach_name)
        LogUtil.get_cur_logger().info('send mail for OtaPriceReport done,cost: %0.2fs', time.time() - _begin)


class MailSendSpecialSaleBase:

    def send_mail_for_hotel_special_sale_monitor(self, config, failed_pojo_chunk_list, succeeded_pojo_chunk_list):
        upload_finish_readable_time = DateUtil.stamp_to_date_format3(time.time())
        upload_finish_file_name_time = DateUtil.stamp_to_date_format(time.time(), "%Y_%m_%d_%H_%M_%S")
        temp_file_folder = config.get_local_file_folder()
        biz_name = config.get_job().get_job_name()
        succeeded_report_df = self.get_df_from_pojo_chunk_list(biz_name, succeeded_pojo_chunk_list,
                                                               upload_finish_readable_time)
        failed_report_df = self.get_df_from_pojo_chunk_list(biz_name, failed_pojo_chunk_list,
                                                            upload_finish_readable_time)
        robot_token = config.get_robot_send_token()
        if not failed_report_df.empty:
            DdtUtil.robot_send_ddt_msg('业务线: {0} 上传特殊数据到PricingCenter出现异常, 上传时间: {1}'.format(biz_name,
                                                                                             upload_finish_file_name_time),
                                       robot_token)
        df_with_sheet_names = [DataFrameWithSheetName(succeeded_report_df, '上传成功'),
                               DataFrameWithSheetName(failed_report_df, '上传失败')]
        excel_file_path = join_path(temp_file_folder, '{0}_hotel_special_sale_report_{1}.xlsx'
                                    .format(biz_name, upload_finish_file_name_time))
        excel_file = DFUtil.write_multiple_df_to_excel(excel_file_path, df_with_sheet_names)

        sub = '{0} the report of special-sale to pricing center {1}'.format(biz_name,
                                                                            DateUtil.stamp_to_date_format5(time.time()))
        mail_content = "Dear all!\n This is the report of special-sale to pricing center for {0}, succeeded: {1}, " \
                       "failed: {2} ".format(biz_name, succeeded_report_df.shape[0], failed_report_df.shape[0])
        attach_name = '{0}_hotel_special_sale_report_{1}.xlsx'.format(biz_name, upload_finish_file_name_time)

        MailSender.send_mail(config.get_mail_user(), config.get_mail_pass(),
                             config.get_liquidation_to_pc_report_receivers(), sub, mail_content, excel_file,
                             attach_name)

    @staticmethod
    def get_df_from_pojo_chunk_list(biz_name, pojo_chunk_list, upload_time):
        if len(pojo_chunk_list) == 0:
            return pd.DataFrame()
        row_dict_list = list()
        for pojo_chunk in pojo_chunk_list:
            operate_type = pojo_chunk.operateType
            datas = pojo_chunk.items
            for data in datas:
                row_dict = {'bizName': biz_name, 'operateType': operate_type, 'oyoId': data.oyoId,
                            'roomTypeId': data.roomTypeId, 'pricingDate': data.pricingDate, 'enabled': data.enabled,
                            "strategyType": data.strategyType, 'uploadTime': upload_time}
                row_dict_list.append(row_dict)
        report_df = pd.DataFrame(row_dict_list)
        return report_df
