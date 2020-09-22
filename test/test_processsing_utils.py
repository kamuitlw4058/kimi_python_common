from kimi_common.utils.processing_utils import ProcessingPool

def run(*args,**kwargs):
    return kwargs


if __name__ == '__main__':
    pool = ProcessingPool()
    results = pool.map(run,[{'test':'123'}])
    for i in results:
        print(i.get())