from concurrent.futures import ALL_COMPLETED, wait, as_completed
from concurrent.futures.thread import ThreadPoolExecutor

from common.util.utils import AlertUtil


class MyThreadExecutor:

    def __init__(self, max_workers=5, t_name_prefix='', token=''):
        self.__executor = ThreadPoolExecutor(max_workers, thread_name_prefix=t_name_prefix)
        self.__all_task = []
        self.__results = []
        self.__token = token

    def sub_task(self, func, *args):
        task = self.__executor.submit(func, *args)
        self.__all_task.append(task)

    def get_all_task(self):
        return self.__all_task

    def get_all_result(self):
        for future in as_completed(self.__all_task):
            self.__results.append(future.result())
        return self.__results

    def completed(self, timeout=1800):
        try:
            wait(self.get_all_task(), timeout=timeout, return_when=ALL_COMPLETED)
        except Exception:
            AlertUtil.send_stack_trace_msg("MyThreadExecutor.completed()", self.__token)
        return True
