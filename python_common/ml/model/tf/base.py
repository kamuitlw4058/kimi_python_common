import abc
import tensorflow as tf 

from python_common.utils.logger import getLogger
logger = getLogger(__name__)


class TFModel(metaclass=abc.ABCMeta):
    def __init__(self, ckpt_dir,sess):
        if sess is None:
            self._sess =  tf.Session()
        else:
            self._sess = sess
        self._ckpt_dir = ckpt_dir
        self._train_op = None
        self._loss_op = None
        self._summary_op = None
        self._x = None
        self._y = None
        self._y_ = None

    def sess(self):
        return self._sess

    def init(self):
        self._sess.run(tf.global_variables_initializer())
        self._sess.run(tf.local_variables_initializer())

    
    def from_checkpoint(self):
        tf.reset_default_graph()
        ckpt = tf.train.get_checkpoint_state(self._ckpt_dir)

        saver = tf.train.import_meta_graph(f'{ckpt.model_checkpoint_path}.meta', clear_devices=True)
        logger.info('local ckpt dir: %s', ckpt.model_checkpoint_path)

        saver.restore(self._sess, tf.train.latest_checkpoint(self._ckpt_dir))

        return self
    
    @abc.abstractmethod
    def build_model(self):
        pass

    @abc.abstractmethod
    def input_name(self):
        pass


    @abc.abstractmethod
    def output_name(self):
        pass

    def train_op(self):
        return self._train_op

    def loss_op(self):
        return self._loss_op
    
    def summary_op(self):
        return self._summary_op

    def x(self):
        if self._x is None:
            self._x = self._sess.graph.get_tensor_by_name(self.input_name())
        return self._x

    
    def y(self):
        if self._y is None:
            self._y = self._sess.graph.get_tensor_by_name(self.output_name())
        return self._y

    # def predict(self, input_x):
    #     x = self._sess.graph.get_tensor_by_name(self._input_name)
    #     y = self._sess.graph.get_tensor_by_name(self._output_name)
    #     return self._sess.run(y, feed_dict={x: input_x})

    def save(self, filename):
        input_graph_def = self.sess.graph.as_graph_def()
        output_graph_def = tf.graph_util.convert_variables_to_constants(
            sess=self.sess,
            input_graph_def=input_graph_def,
            output_node_names=self._output_name[:-2].split(',')
        )
        with tf.gfile.GFile(filename, 'wb') as f:
            f.write(output_graph_def.SerializeToString())
        logger.info('%d ops in the final graph.', len(output_graph_def.node))

    
    # def get_tensor(self):
    #     from tensorflow.python.tools import inspect_checkpoint as inspect_chkp
    #     ckpt = tf.train.get_checkpoint_state(self.ckpt_dir)
    #     inspect_chkp.print_tensors_in_checkpoint_file(ckpt.model_checkpoint_path, tensor_name=None, all_tensors=True,
    #                                           all_tensor_names=True)
    #     reader = tf.train.NewCheckpointReader(ckpt.model_checkpoint_path)
    #     all_variables = reader.get_variable_to_shape_map()
    #     w = reader.get_tensor("lr/kernel")
    #     return w