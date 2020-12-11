import time

from python_common.pool import ExecutorPool




number_list = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]

def evaluate_item(x):
    for i in range(10):  # count 是无限迭代器，会一直递增。
        print(f"start: {x} - {i}")
        time.sleep(0.01)


pool =  ExecutorPool()
pool.start(evaluate_item,number_list)