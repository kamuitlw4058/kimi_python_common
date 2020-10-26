#from python_common.ml.io.data_generator import LRDataGenerator
from python_common.ad_ml.model.trainer.lr_local_trainer import LRLocalTrainer

import matplotlib.pyplot as plt
import numpy as np
import tensorflow as tf
from sklearn import datasets
#from tensorflow.python.framework import ops


trainer = LRLocalTrainer('data/test_datasource/train','data/model/test_model',label='clk')
trainer.train()