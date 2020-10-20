import abc
import tensorflow as tf 

from python_common.utils.logger import getLogger
logger = getLogger(__name__)


class TFModel(metaclass=abc.ABCMeta):
    def __init__(self, ckpt_dir,sess=None):
        self.sess = sess
        self.ckpt_dir = ckpt_dir

    
    def from_checkpoint(self):
        tf.reset_default_graph()
        ckpt = tf.train.get_checkpoint_state(self.ckpt_dir)

        saver = tf.train.import_meta_graph(f'{ckpt.model_checkpoint_path}.meta', clear_devices=True)
        logger.info('local ckpt dir: %s', ckpt.model_checkpoint_path)

        self._sess = tf.Session()
        saver.restore(self._sess, tf.train.latest_checkpoint(self.ckpt_dir))

        return self

    @abc.abstractmethod
    def train_op(self, x, y):
        pass


    # def predict(self, input_x):
    #     x = self._sess.graph.get_tensor_by_name(self._input_name)
    #     y = self._sess.graph.get_tensor_by_name(self._output_name)
    #     return self._sess.run(y, feed_dict={x: input_x})

    def save(self, filename):
        input_graph_def = self._sess.graph.as_graph_def()
        output_graph_def = tf.graph_util.convert_variables_to_constants(
            sess=self._sess,
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