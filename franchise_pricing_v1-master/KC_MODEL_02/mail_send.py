import os
import sys
from os.path import join as join_path

cur_path = os.path.dirname(os.path.realpath(__file__))
sys.path.append(join_path(cur_path, '..'))

from common.sendmail.mail_send_base import MailSendPriceBase, MailSendReportBase


class MailSend(MailSendPriceBase, MailSendReportBase):
    pass
