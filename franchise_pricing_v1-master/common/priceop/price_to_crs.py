import datetime as dt
import os
import sys
from os.path import join as join_path

from pandas import DataFrame

cur_path = os.path.dirname(os.path.realpath(__file__))
sys.path.append(join_path(cur_path, '..'))

from common.util.utils import *
from common.priceop.api_base import ApiBase
from common.pricing_pipeline.pipeline_pojo import PmsBatchPrice, PmsPrice

COLUMN_DATE = "date"
COLUMN_HOTEL_ID = "hotel_id"
COLUMN_ROOM_TYPE_ID = "room_type_id"
COLUMN_PRICE = "price"
COLUMN_FAIL_CODE = "fail_code"
COLUMN_FAIL_MSG = "fail_message"

MAIL_FILE_TYPE = ".xlsx"
POST_BATCH_SIZE = 500


def is_date(x):
    return isinstance(x, dt.date)


def strf_time(v):
    return v.strftime("%Y-%m-%d") if (is_date(v)) else v


class PriceInsert(ApiBase):

    def __init__(self, config):
        self.__config = config
        self.__df_price_to_crs = DataFrame()
        self.__succeeded_pojo_chunk_list = list()
        self.__failed_pojo_chunk_list = list()

    @staticmethod
    def batch_insert_pms_price_to_crs_and_send_mail(config, df_price_to_crs):
        try:
            if not config.get_toggle_on_pms_price():
                return
            __begin = time.time()
            price_insert = PriceInsert(config)
            price_insert.__config = config
            price_insert.__df_price_to_crs = df_price_to_crs
            price_insert.batch_insert_price_to_crs()
            price_insert.send_mail_for_monitor()
            LogUtil.get_cur_logger().info('batch_insert_hotel_price_to_pms_and_send_mail, cost: %.2fs',
                                          time.time() - __begin)
        except Exception:
            ApiBase.send_stack_trace("batch_insert_hotel_price_to_pms_and_send_mail", config)

    def batch_insert_price_to_crs(self):
        pojo_chunk_list, items_len, slices_size = self.get_pojo_chunk_list(self.__df_price_to_crs, POST_BATCH_SIZE,
                                                                           biz="price_to_crs")
        self.__succeeded_pojo_chunk_list, self.__failed_pojo_chunk_list = self.http_request_post(
            self.__config.get_pms_price_sink_url(), pojo_chunk_list, items_len, slices_size,
            biz="price_to_crs", item1="dataList")

    def df_to_pojo_chunk_list(self, df_price, batch_size):
        pms_price_lst = self.json_data_create(df_price)
        items_len = len(pms_price_lst)
        # prices数组切片
        slices_len = int(items_len / batch_size) if (items_len % batch_size) == 0 else int(
            items_len / batch_size) + 1
        pms_price_lst = [PmsBatchPrice(pms_price_lst[idx * batch_size:min((idx + 1) * batch_size, items_len)])
                         for idx in range(slices_len)]
        return pms_price_lst, items_len, slices_len

    def send_mail_for_monitor(self):
        attach_name, failed_df, mail_content = self.failed_list_build(self.__succeeded_pojo_chunk_list,
                                                                      self.__failed_pojo_chunk_list)
        self.__config.get_mail_send().send_mail_for_report_hotel_price_to_pms(self.__config, mail_content, failed_df,
                                                                              attach_name)

    def failed_list_build(self, succeeded_pojo_chunk_list, failed_pojo_chunk_list):
        suced_pojos = [suced_pojo for a in succeeded_pojo_chunk_list for suced_pojo in a.dataList]
        suc_count = len(suced_pojos)
        failed_pojos = [failed_pojo for a in failed_pojo_chunk_list for failed_pojo in a.dataList]
        fail_count = len(failed_pojos)
        failed_df = None
        attach_name = None
        head = "Price to PMS completed, succeeded: {0}, failed: {1}".format(suc_count, fail_count)
        if fail_count != 0:
            head = "Price to PMS completed, succeeded: {0}, failed: {1}, please refer to attachment for more details".\
                format(suc_count, fail_count)
            columns = [COLUMN_HOTEL_ID, COLUMN_DATE, COLUMN_ROOM_TYPE_ID, COLUMN_PRICE]
            failed_series = [pd.Series([e.hotelId, e.date, e.roomTypeId, e.rate], index=columns) for e in failed_pojos]
            failed_df = pd.DataFrame(failed_series, columns=columns)
            failed_df.reset_index(drop=True, inplace=True)
            attach_name = "price_to_crs_" + DateUtil.stamp_to_date_format1(time.time()) + MAIL_FILE_TYPE
        mail_content = DFUtil.gen_excel_content_by_html(head, failed_df)
        return attach_name, failed_df, mail_content

    def post_json_chunk_with_retry(self, pojo_chunk, url, retry_times):
        post_result = None
        for retry_count in range(retry_times):
            post_result = self.post_pojo_chunk_to_remote(pojo_chunk, url)
            if post_result[0] == '000':
                break
            elif post_result[0] == '2200':
                LogUtil.get_cur_logger().info('post_json_chunk_with_retry, retrying count: {0}'.format(retry_count))
                time.sleep(15)
                continue
            else:
                return '-2', post_result[1]
        return post_result

    def compose_pms_price_pojo_from_row(self, row):
        return PmsPrice(row['id'], strf_time(row['date']), row['room_type'], row['final_price'], 16067)

    def json_data_create(self, df):
        data_list = [self.compose_pms_price_pojo_from_row(row) for index, row in df.iterrows()]
        return data_list
