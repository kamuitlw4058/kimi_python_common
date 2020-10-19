
import time

from elasticsearch import Elasticsearch
from elasticsearch.exceptions import NotFoundError

from ..utils.logger import getLogger

logger = getLogger(__name__)

class ElasticsearchClient():
    def __init__(self,server_list,default_index = 'default_index',default_doc_type='default_doc_type'):
        self.es = Elasticsearch(server_list)
        self.default_index = default_index
        self.default_doc_type = default_doc_type 

    def get(self,key,index=None,doc_type=None):
        start = time.time()
        if index is None:
            index = self.default_index
        if doc_type is None:
            doc_type = self.default_doc_type
        try:
            res = self.es.get(index = index,doc_type=doc_type,id=key)
            ret = res['_source']
        except NotFoundError as e:
            ret = None
        end = time.time()
        logger.info(f'elapsed:{end-start}')
        return ret

    def set(self,key,value,index=None,doc_type=None):
        if index is None:
            index = self.default_index
        if doc_type is None:
            doc_type = self.default_doc_type
        if isinstance(value,str):
            value = {
                'value':value
            }
        self.es.index(index=index,doc_type=doc_type,id=key,body=value)
