import concurrent
from concurrent.futures import as_completed
from datetime import time
import numpy as np

from common.util.my_thread_executor import MyThreadExecutor
from common.util.utils import *

JSON_HEADER = {'Content-Type': 'application/json; charset=utf-8'}
INTERNAL_PROD_DDT_ALERT_ROBOT_TOKEN = '42d9608e03776f74b5ef897b5e882522d8645fc6d19c9ea2796c3dbfb2aff37e'
INTERNAL_DEBUG_DDT_ALERT_ROBOT_TOKEN = 'a6e89792a9b40c383d8fcc2937b3fc35f46c6428ca6a142669710ccf4ea8cd2c'


class ApiBase:
    SOURCE_BIG_DATA = 0

    # 1. 多线程组装pojo_chunk_list
    def get_pojo_chunk_list(self, df_pojo_info, batch_size, biz, gp_by='oyo_id'):
        lst_oyo_id = list(set(df_pojo_info[gp_by]))
        len_oyo_id = len(lst_oyo_id)
        slices = 500
        slices_len = int(len_oyo_id / slices) if (len_oyo_id % slices) == 0 else int(len_oyo_id / slices) + 1
        set_oyo_id_lst = [set(lst_oyo_id[idx * slices:min((idx + 1) * slices, len_oyo_id)]) for idx in
                          range(slices_len)]
        pojo_chunk_list = list()
        items_len = 0
        slices_size = 0
        t_executor = MyThreadExecutor(max_workers=10, t_name_prefix=biz)
        for oyo_ids in set_oyo_id_lst:
            df_price = df_pojo_info[df_pojo_info[gp_by].isin(oyo_ids)]
            t_executor.sub_task(self.df_to_pojo_chunk_list, df_price, batch_size)
        for idx, future in enumerate(as_completed(t_executor.get_all_task())):
            pojo_chunk_list.extend(future.result()[0])
            items_len += future.result()[1]
            slices_size = future.result()[2]
            LogUtil.get_cur_logger().info(
                "df_to_pojo_chunk_list-%d, pojo_chunk_list_len: %d, items_len: %d, slices_size: %d, batch_size: %d",
                idx, len(future.result()[0]), future.result()[1], future.result()[2], batch_size)
        LogUtil.get_cur_logger().info(
            "df_to_pojo_chunk_list-all, pojo_chunk_list_len: %d, items_len: %d, slices_size: %d, batch_size: %d",
            len(pojo_chunk_list), items_len, slices_size, batch_size)
        return pojo_chunk_list, items_len, slices_size

    def df_to_pojo_chunk_list(self, df_price, batch_size):
        return list(), 0, 0

    # 2. 多线程提交http_request_post
    def http_request_post(self, url, pojo_list, items_len, batch_size, **op_params):
        logger = LogUtil.get_cur_logger()
        begin = time.time()
        succeeded_pojo_chunk_list = list()
        failed_pojo_chunk_list = list()
        biz_name = op_params.get('biz')
        attr = op_params.get('item1')
        with concurrent.futures.ThreadPoolExecutor(max_workers=8, thread_name_prefix="http_request_post") as executor:
            futures = {executor.submit(self.pojo_chunk_post, pojo, url, idx + 1, **op_params) for idx, pojo in
                       enumerate(pojo_list)}
            for future in concurrent.futures.as_completed(futures):
                results = future.result()
                if results[0] is not None:
                    succeeded_pojo_chunk_list.append(results[0])
                if results[1] is not None:
                    failed_pojo_chunk_list.append(results[1])
        size_suc = sum([len(pojo_chunk.__getattribute__(attr)) for pojo_chunk in succeeded_pojo_chunk_list])
        size_failed = sum([len(pojo_chunk.__getattribute__(attr)) for pojo_chunk in failed_pojo_chunk_list])
        logger.info('%s, totalSize: %s, succeeded: %s, failed: %s, batchSize: %s, cost: %0.2fs',
                    biz_name, items_len, size_suc, size_failed, batch_size, time.time() - begin)
        return succeeded_pojo_chunk_list, failed_pojo_chunk_list

    def pojo_chunk_post(self, pojo_chunk, url, num_post, **op_params):
        logger = LogUtil.get_cur_logger()
        attr = op_params["item1"]
        size_failed = 0
        size_suc = 0
        failed_pojo_chunk = None
        succeeded_pojo_chunk = None
        __begin0 = time.time()
        items = pojo_chunk.__getattribute__(attr)
        post_result = self.post_json_chunk_with_retry(pojo_chunk, url, 3)
        if not post_result[0] == '000':
            failed_pojo_chunk = pojo_chunk
            size_failed += len(items)
            logger.exception("http_request Exception, code: %s, msg: %s", post_result[0], post_result[1])
            JsonUtil.print_json_file("./" + op_params['biz'] + "-errorParamsJson-{0}.json".format(num_post), pojo_chunk)
        else:
            size_suc += len(items)
            succeeded_pojo_chunk = pojo_chunk
        logger.info(op_params['biz'] + "-%s, size: %d, code: %s, cost: %0.2fs", num_post, len(items),
                    post_result[0], (time.time() - __begin0))
        return succeeded_pojo_chunk, failed_pojo_chunk, num_post

    # 单线程组装http_request
    def http_request(self, url, pojo_chunk_list, items_len, slices_size, *op_desc):
        logger = LogUtil.get_cur_logger()
        attr = op_desc[1]
        begin = time.time()
        size_failed = 0
        size_suc = 0
        failed_pojo_chunk_list = list()
        succeeded_pojo_chunk_list = list()
        num_post = 0
        for pojo_chunk in pojo_chunk_list:
            num_post += 1
            __begin0 = time.time()
            items = pojo_chunk.__getattribute__(attr)
            post_result = self.post_json_chunk_with_retry(pojo_chunk, url, 3)
            if not post_result[0] == '000':
                failed_pojo_chunk_list.append(pojo_chunk)
                size_failed += len(items)
                logger.exception("http_request Exception, code: %s, msg: %s", post_result[0], post_result[1])
                JsonUtil.print_json_file("./" + op_desc[0] + "-errorParamsJson-{0}.json".format(num_post), pojo_chunk)
            else:
                size_suc += len(items)
                succeeded_pojo_chunk_list.append(pojo_chunk)
            logger.info(op_desc[0] + "-%s, size: %d, code: %s, cost: %0.2fs", num_post, len(items),
                        post_result[0], (time.time() - __begin0))
        logger.info(op_desc[0] + ", totalSize: %s, batchSize: %s, succeeded: %s, failed: %s, cost: %0.2fs",
                    items_len, slices_size, size_suc, size_failed, (time.time() - begin))
        return succeeded_pojo_chunk_list, failed_pojo_chunk_list

    # post_pojo_chunk_to_remote
    def post_pojo_chunk_to_remote(self, pojo_chunk, url):
        result = None
        try:
            result = requests.post(url=url, headers=JSON_HEADER, data=pojo_chunk.to_json().encode("utf-8"),
                                   timeout=self.get_post_timeout())
            json_result = json.loads(result.text, encoding="utf-8")
            code = json_result['code']
            msg = json_result['msg']
            return code, msg
        except Exception as e:
            LogUtil.get_cur_logger().exception("post Exception, url: %s, result: %s, e: %s, params: %s", url,
                                               result if result is None else str(result.text), str(e),
                                               pojo_chunk.to_json()[0:500])
            return -1, str(e)

    def get_post_timeout(self):
        return 40

    def post_json_chunk_with_retry(self, pojo_chunk, url, retry_times):
        post_result = None
        for retry_count in range(retry_times):
            post_result = self.post_pojo_chunk_to_remote(pojo_chunk, url)
            if post_result[0] == '000':
                break
            elif post_result[0] == '999':
                LogUtil.get_cur_logger().warn('post_json_chunk_with_retry, retrying count: {0}'.format(retry_count))
                time.sleep(10)
                continue
            else:
                return -2, post_result[1]
        return post_result

    # 3. send_mail_for_monitor
    def send_mail_for_monitor(self):
        pass

    @staticmethod
    def value_trim(row, param, default=None):
        if row.get(param) is np.nan:
            return None
        if type(row.get(param)) is np.int64:
            return int(row.get(param))
        return row.get(param, default)

    @staticmethod
    def value_arr_trim(row, param, default=None):
        value = row.get(param, default)
        return value if None else value.split(",")

    @staticmethod
    def send_stack_trace(msg, job_config):
        is_debug = EnvUtil.is_debug(job_config.get_env())
        if is_debug == True:
            token = INTERNAL_DEBUG_DDT_ALERT_ROBOT_TOKEN
        else:
            token = INTERNAL_PROD_DDT_ALERT_ROBOT_TOKEN
        AlertUtil.send_stack_trace_msg(msg, token)
