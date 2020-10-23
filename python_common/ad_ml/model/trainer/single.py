import tensorflow as tf
import numpy as np
import math
from tensorflow import keras
from tensorflow.keras import datasets,layers,optimizers,Sequential
from tensorflow.keras.callbacks import TensorBoard
import datetime
import matplotlib.pyplot as plt
plt.rcParams['axes.unicode_minus']=False


#导入数据
(x_train,y_train),(x_test,y_test)=tf.keras.datasets.mnist.load_data()
print(x_train.shape,x_test.shape)


def normalize(x,y):
    x=tf.cast(x,tf.float32)
    x/=255
    return x,y

#添加一层维度，方便后续扁平化
x_train=tf.expand_dims(x_train,axis=-1)
x_test=tf.expand_dims(x_test,axis=-1)

train_dataset=tf.data.Dataset.from_tensor_slices((x_train,y_train))
test_dataset=tf.data.Dataset.from_tensor_slices((x_test,y_test))
train_dataset=train_dataset.map(normalize)
test_dataset=test_dataset.map(normalize)


#画图
plt.figure(figsize=(10,15))
i=0
for (x_test,y_test) in test_dataset.take(25):
    x_test=x_test.numpy().reshape((28,28))
    plt.subplot(5,5,i+1)
    plt.grid(False)
    plt.xticks([])
    plt.imshow(x_test,cmap=plt.cm.binary)
    plt.xlabel([y_test.numpy()],fontsize=10)
    i+=1
plt.show()

model=tf.keras.Sequential([
    tf.keras.layers.Flatten(input_shape=(28,28,1)),
    tf.keras.layers.Dense(128,activation=tf.nn.relu),
    tf.keras.layers.Dense(64,activation=tf.nn.relu),
    tf.keras.layers.Dense(10,activation=tf.nn.softmax)
])

model.summary()

model.compile(optimizer='adam',loss='sparse_categorical_crossentropy',metrics=['accuracy'])

#开始训练
batch_size=32
train_dataset=train_dataset.repeat().shuffle(60000).batch(batch_size)
test_dataset=test_dataset.batch(batch_size)
#为tensorboard可视化保存数据
tensorboard_callback=tf.keras.callbacks.TensorBoard(histogram_freq=1)
model.fit(train_dataset,epochs=5,steps_per_epoch=math.ceil(60000/batch_size),callbacks=[tensorboard_callback])


test_loss,test_accuracy=model.evaluate(test_dataset,steps=math.ceil(10000/32))
print('Accuracy on test_dataset',test_accuracy)
