import os
import sys
import abc
import numpy as np
import pyarrow.parquet as pq


from python_common.utils.logger import getLogger
logger = getLogger(__name__)


class DataGenerator(metaclass=abc.ABCMeta):
    def __init__(self,dim,filedir, subffix='.parquet'):
        self.dim = dim
        self.file_list = [os.path.join(filedir, i) for i in os.listdir(filedir) if i.endswith(subffix)]
        logger.info(f'data file number = {len(self.file_list)}')


    def log_progress(self,epoch,ratio):
        print(f'reporter progress:{ratio}', file=sys.stderr)
        logger.info('finish epoch %d, %.2f', epoch, ratio)

    @abc.abstractmethod
    def generator(self,x,y):
        pass

    def get_dim(self):
        return self.dim


class LRDataGenerator(DataGenerator):
    def __init__(self, filedir,batch_size, epoch=10,label='is_clk',cate_sparse_features='cate_sparse_features',number_features='number_features', subffix='.parquet'):
        self.batch_size = batch_size
        file_list = [os.path.join(filedir, i) for i in os.listdir(filedir) if i.endswith(subffix)]
        f = file_list[0]
        table = pq.read_table(f)
        for batch in table.to_batches(1):
            df = batch.to_pandas()
            for i, row in df.iterrows():
                self.cate_dim = row[cate_sparse_features]['size']
                self.number_dim =  len(list(row[number_features]['values']))
                break
        
        dim = 0
        if cate_sparse_features is not None:
            dim += self.cate_dim
        else:
            self.cate_dim = 0

        if number_features is not None:
            dim += self.number_dim
        else:
            self.number_dim = 0

        super().__init__(dim,filedir)
        self.epoch = epoch
        self.cate_sparse_features = cate_sparse_features
        self.number_features = number_features
        self.label = label
        logger.info(f'cate dim:{self.cate_dim} number dim: {self.number_dim} epoch: {epoch} batch size:{batch_size}')

    def generator(self):
        total = self.epoch * len(self.file_list)
        progress = 0

        x = np.zeros(shape=[self.batch_size, self.dim], dtype=np.float32)
        y = np.zeros(shape=[self.batch_size, 1], dtype=np.float32)


        for epoch in range(self.epoch):
            for idx, f in enumerate(self.file_list):
                self.log_progress(epoch, progress / total)
                progress += 1
                table = pq.read_table(f)
                for batch in table.to_batches(self.batch_size):
                    df = batch.to_pandas()
                    row_num = df.shape[0]
                    
                    for i, row in df.iterrows():
                        if self.cate_dim != 0:
                            if len(list(row[self.cate_sparse_features]['values'])) > 0:
                                x[i, list(row[self.cate_sparse_features]['indices'])] = 1.0

                        if self.number_dim != 0:
                            number_values = list(row[self.number_features]['values'])
                            for j in  range(self.number_dim):
                                x[i, self.cate_dim + j] = number_values[j]
                        y[i, 0] = row[self.label]

                    yield (x[:row_num], np.reshape(y[:row_num],[-1,1]))

                    x.fill(0)
                    y.fill(0)

        self.log_progress(epoch, 1.0)