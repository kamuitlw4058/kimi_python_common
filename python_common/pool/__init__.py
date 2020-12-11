
import time

from concurrent.futures import ProcessPoolExecutor
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed

from multiprocessing import cpu_count


class ExecutorPool():
    def __init__(self):
        self.cpu_count = cpu_count()

    def start(self,handler,data_list):
        start = time.time()
        with ThreadPoolExecutor(max_workers=self.cpu_count) as executor:
                # 将 10 个任务提交给 executor，并收集 futures
                futures = [executor.submit(handler, item) for item in data_list]

                # as_completed 方法等待 futures 中的 future 完成
                # 一旦某个 future 完成，as_completed 就立即返回该 future
                # 这个方法，使每次返回的 future，总是最先完成的 future
                # 而不是先等待任务 1，再等待任务 2...
                for future in as_completed(futures):
                        print(f'result:{future.result()}')
        print ("Pool execution in " + str(time.time() - start), "seconds")


