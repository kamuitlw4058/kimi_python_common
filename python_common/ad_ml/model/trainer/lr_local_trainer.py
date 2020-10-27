from python_common.ad_ml.datasource.data_generator import LRDataGenerator
from python_common.ad_ml.model.tf.lr import LogisticRegression

import matplotlib.pyplot as plt
import numpy as np
import tensorflow as tf

from python_common.utils.logger import getLogger
logger = getLogger(__name__)


class LRLocalTrainer():
    def __init__(self,
                input_dir,
                ckpt_dir,
                batch_size =32,
                label = 'is_clk',
                learning_rate = 0.01,
                l2 = 0.001,
                epoch=10):
        self._input_dir = input_dir
        self._batch_size = batch_size
        self._epoch = epoch
        self._label = label
        self._ckpt_dir = ckpt_dir
        self._learning_rate = learning_rate
        self._l2 = l2
        self._data_generator =  LRDataGenerator(self._input_dir,self._batch_size,label=self._label,epoch=self._epoch)
        self._dim = self._data_generator.get_dim()
        self._lr =  LogisticRegression(self._dim,self._learning_rate,self._ckpt_dir,l2=self._l2)


    
    def train(self):
        generator = self._data_generator.generator()
        dim = self._data_generator.get_dim()
        self._lr.build_model()
        train_op =  self._lr.train_op()
        loss  = self._lr.loss_op()
        auc_op =  self._lr.auc_op()
        x = self._lr.x()
        y = self._lr.y()
        self._lr.init()
        sess = self._lr.sess()
        while True:
            try:
                train_x, train_y = next(generator)
                logger.info(train_x)
                sess.run(train_op, feed_dict={x: train_x, y: train_y})
                logger.info(f'loss:{sess.run(loss, feed_dict={x: train_x, y: train_y})}')
                logger.info(f'auc:{sess.run(auc_op, feed_dict={x: train_x, y: train_y})}')
            except StopIteration:
                break
        saver = tf.train.Saver()
        saver.save(sess, 'data/model/my_test_model')

        





