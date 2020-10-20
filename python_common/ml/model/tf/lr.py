#!/usr/bin/env python3
# -*- coding: utf-8 -*-

__author__ = 'kimi'
__email__ = 'kimi@199524943@qq.com'



import tensorflow as tf

from python_common.utils.logger import getLogger
logger = getLogger(__name__)

from python_common.ml.model.tf.base import TFModel




class LogisticRegression(TFModel):
    def __init__(self, input_dim,learning_rate, ckpt_dir, l2=1.0,sess=None):
        super.__init__(ckpt_dir,sess=sess)
        self._sess = sess
        self._input_dim = input_dim
        self.learning_rate = learning_rate
        self._l2 = float(l2)
        self._input_name = 'input/x:0'
        self._output_name = 'lr/Sigmoid:0'


    def train_op(self, x, y_):
        regularizer = tf.contrib.layers.l2_regularizer(self._l2)

        lr = tf.layers.Dense(units=1, activation=tf.nn.sigmoid, kernel_regularizer=regularizer, name='lr')
        #logger.info("kernel_regularizer:" + str(lr.activation) + " type:" + str(type(lr.activation)))
        #logger.info("dense:" + str(lr) + " type:"+ str(type(lr)))
        y = lr(x)
        #logger.info("y:" + str(y) + " type:" + str(type(y)))


        loss = tf.losses.log_loss(y_, y) + tf.reduce_sum(lr.losses)
        tf.summary.scalar('loss', loss)

        _, auc_op = tf.metrics.auc(predictions=y, labels=y_)
        tf.summary.scalar('auc', auc_op)

        global_step = tf.train.get_or_create_global_step()
        train_op = tf.train.AdamOptimizer(self.learning_rate).minimize(loss, global_step=global_step)

        return train_op

    def predict(self, input_x):
        x = self._sess.graph.get_tensor_by_name(self._input_name)
        y = self._sess.graph.get_tensor_by_name(self._output_name)
        return self._sess.run(y, feed_dict={x: input_x})


    def get_tensor(self,ckpt_dir):
        from tensorflow.python.tools import inspect_checkpoint as inspect_chkp
        ckpt = tf.train.get_checkpoint_state(ckpt_dir)
        inspect_chkp.print_tensors_in_checkpoint_file(ckpt.model_checkpoint_path, tensor_name=None, all_tensors=True,
                                              all_tensor_names=True)
        reader = tf.train.NewCheckpointReader(ckpt.model_checkpoint_path)
        all_variables = reader.get_variable_to_shape_map()
        w = reader.get_tensor("lr/kernel")
        return w



    def get_weight(self):
        return self._sess.run('lr/kernel:0')

    def get_bias(self):
        return self._sess.run('lr/bias:0')

    def input_dim(self):
        return self._input_dim
