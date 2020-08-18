import hmac
import base64
import urllib.parse
import time

from hashlib import sha1


from ..utils.url_utils import dict2url



class Crypto():
    def __init__(self, public_key=None, private_key=None, hash_func = sha1,url_auto_key=None):
        self.hash_func = hash_func
        self.private_key = private_key
        self.public_key = public_key
        if url_auto_key is None:
            self.url_auto_key = default_auto_key=[
                ('ts',False,lambda : int(time.time())),
                ('ttl',False,lambda : 300),
            ]
        else:
            self.url_auto_key = url_auto_key
        

    def hmac(self,content, private_key=None,hash_func=None,output_base64=True):
        if hash_func is None:
            hash_func = self.hash_func
        
        if private_key is None:
            private_key = self.private_key
        
        if private_key is None:
            raise ValueError("private_key is None")

        hmac_code = hmac.new(private_key.encode(), content.encode(), hash_func)
        if output_base64:
            return base64.b64encode(hmac_code.digest()).decode() 
        else:
            return hmac_code.hexdigest()

    
    def url_hmac(self,input_dict,url_auto_key=None,hmac_key='sig',sort_by_key=True):
        if url_auto_key is None:
            url_auto_key = self.url_auto_key

        url=dict2url(input_dict,auto_key=url_auto_key,sort_by_key=sort_by_key)
        sig_value =  self.hmac(url)
        urlencode_sig_value = urllib.parse.quote(sig_value)
        return f'{url}&{hmac_key}={urlencode_sig_value}'

