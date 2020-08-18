import hmac
from hashlib import sha1
import base64



class Crypto():
    def __init__(self, public_key=None, private_key=None, hash_func = sha1):
        self.hash_func = hash_func
        self.private_key = private_key
        self.public_key = public_key

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
