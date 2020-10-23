

from python_common.utils.logger import getLogger

logger = getLogger(__name__)

class Task():
    pass

    @classmethod
    def run(cls,task):
        logger.info(task)        
        pass

    def __str__(self):
        return 'test task'