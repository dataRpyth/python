#!/usr/bin/env python
# -*- coding:utf-8 -*-

import os
import sys
from os.path import join as join_path

cur_path = os.path.dirname(os.path.realpath(__file__))
sys.path.append(join_path(cur_path, '..'))
import smtplib
from common.dingtalk_sdk.dingtalk_py_cmd import DingTalkPy
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from common.util.utils import *


class MailAttachment:

    def __init__(self, local_file_path, attach_name):
        self.local_file_path = local_file_path
        self.attach_name = attach_name


class MailSender:
    @staticmethod
    def send_mail_with_attachment_list(user, password, to_list, sub, mail_content, mail_attachments):
        logger = LogUtil.get_cur_logger()

        mail_host = "smtp.mxhichina.com"
        me = "Pricing" + "<" + user + ">"
        msg = MIMEMultipart()
        msg['Subject'] = sub
        msg['From'] = me
        if to_list is None:
            logger.warn("send_mail>>>Sub:%s, >>>To:%s", sub, to_list)
            return
        msg['To'] = ";".join(to_list)
        logger.info("send_mail>>>Sub:%s, >>>To:%s", sub, msg['To'])
        text_plain = MIMEText(mail_content, 'html')
        msg.attach(text_plain)
        for mail_attachment in mail_attachments:
            local_file_path = mail_attachment.local_file_path
            attach_name = mail_attachment.attach_name
            if local_file_path is not None and attach_name is not None:
                attachment = MIMEText(open(local_file_path, 'rb').read(), 'xls', 'gb2312')
                attachment["Content-Type"] = 'application/octet-stream'
                attachment["Content-Disposition"] = 'attachment;filename=' + attach_name[-100:]
                msg.attach(attachment)

        return MailSender.send_email(user, password, mail_host, msg, to_list)

    @staticmethod
    def send_email(user, password, mail_host, msg, to_list):
        logger = LogUtil.get_cur_logger()
        msg['To'] = ";".join(to_list)
        server = None
        try:
            server = smtplib.SMTP_SSL(mail_host, 465)
            server.set_debuglevel(0)
            server.connect(mail_host)
            server.login(user, password)
            server.sendmail(msg['From'], to_list, msg.as_string())
            logger.info("Email sending successful！！ Host：%s , 主题：%s ", mail_host, msg['Subject'])
            return True
        except Exception as e:
            logger.error("Email sending failed: %s %s ", str(e), '')
            return False
        finally:
            if server is not None:
                server.close()

    @staticmethod
    def send_mail(user, password, to_list, sub, mail_content, local_file_path, attach_name):
        attachments = []
        if attach_name is not "" and attach_name is not None:
            attachment = MailAttachment(local_file_path, attach_name)
            attachments.append(attachment)
        MailSender.send_mail_with_attachment_list(user, password, to_list, sub, mail_content, attachments)

    @staticmethod
    def send_for_df(config, to_list, sub, mail_content, attach_file, attach_name, context=None, send_ddt_robot=False):
        local_file_path = join_path(config.get_local_file_folder(), attach_name)
        MailSender.gen_excel(local_file_path, attach_file)
        if send_ddt_robot:
            MailSender.ddt_robot_send(config, local_file_path, context)
        MailSender.send_mail(config.get_mail_user(), config.get_mail_pass(), to_list, sub, mail_content,
                             local_file_path, attach_name)

    @staticmethod
    def send_for_multi_sheet(config, to_list, sub, mail_content, multi_sheets, attach_name, context=None,
                             send_ddt_robot=False):
        local_file_path = join_path(config.get_local_file_folder(), attach_name)
        DFUtil.write_multiple_df_to_excel(local_file_path, multi_sheets)
        if send_ddt_robot:
            MailSender.ddt_robot_send(config, local_file_path, context)
        MailSender.send_mail(config.get_mail_user(), config.get_mail_pass(), to_list, sub, mail_content,
                             local_file_path, attach_name)

    @staticmethod
    def ddt_robot_send(config, local_file_path, context=None):
        if config.get_toggle_on_robot_send():
            DingTalkPy().robot_send(config.get_robot_send_token(), config.get_robot_send_biz_name(),
                                  config.get_robot_send_biz_model_id(), config.get_job_preset_time(), local_file_path,
                                  context, config.get_ddt_env())

    @staticmethod
    def gen_excel(local_file_path, attach_file):
        logger = LogUtil.get_cur_logger()
        try:
            if attach_file is not None:
                attach_file.to_excel(local_file_path, index=True)
            return local_file_path
        except Exception as e:
            logger.error("file gen failed: %s", str(e))
            return

    @staticmethod
    def gen_multiple_df_to_excel(excel_file_path, multi_sheets):
        with pd.ExcelWriter(excel_file_path) as writer:
            for data_frame_with_sheet_name in multi_sheets:
                data_frame_with_sheet_name.get_data_frame().to_excel(writer,
                                                                     sheet_name=data_frame_with_sheet_name.get_sheet_name())
