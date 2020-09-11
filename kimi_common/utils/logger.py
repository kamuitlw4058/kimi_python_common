import logging
formatter_str = '%(asctime)s %(name)s-%(lineno)d[%(levelname)s]:%(message)s'
logging.basicConfig(level = logging.INFO,format = formatter_str)

def getLogger(name,level =logging.INFO):
    logger = logging.getLogger(name)
    logger.setLevel(level)
    return logger