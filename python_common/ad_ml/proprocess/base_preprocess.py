import abc

from python_common.utils.logger import getLogger
logger = getLogger(__name__)


class Preprocessor(metaclass=abc.ABCMeta):
    pass