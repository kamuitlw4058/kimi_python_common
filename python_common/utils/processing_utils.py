from multiprocessing import Pool


class ProcessingPool():
    def __init__(self):
        self.pool = Pool(8)

    def map(self,run,params_list):
        results =[]
        pool = Pool(8)  # 创建拥有3个进程数量的进程池
        for  i in params_list:
            results.append(pool.apply_async(run, kwds=i))
        pool.close()  
        pool.join() 
        return results