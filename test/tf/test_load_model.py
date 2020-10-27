import tensorflow as tf
# graph = tf.get_default_graph()
# sess=tf.Session(graph=graph)    
sess=tf.Session()    
#First let's load meta graph and restore weights

ckpt = tf.train.get_checkpoint_state('data/model/my_test_model/')
saver = tf.train.import_meta_graph(f'{ckpt.model_checkpoint_path}.meta', clear_devices=True)
print('ckpt meta: %s', f'{ckpt.model_checkpoint_path}.meta')

#saver = tf.train.import_meta_graph('data/model/my_test_model/model.ckpt-1000.meta')
saver.restore(sess,tf.train.latest_checkpoint('data/model/my_test_model/'))


# Access saved Variables directly
print(sess.run('bias:0'))
# This will print 2, which is the value of bias that we saved


# Now, let's access and create placeholders variables and
# create feed-dict to feed new data

w1 = sess.graph.get_tensor_by_name("w1:0")
w2 = sess.graph.get_tensor_by_name("w2:0")
feed_dict ={w1:13.0,w2:17.0}

#Now, access the op that you want to run. 
op_to_restore = sess.graph.get_tensor_by_name("op_to_restore:0")

print(sess.run(op_to_restore,feed_dict))