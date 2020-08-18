from zmc_common.crypto.base import Crypto
from zmc_common.utils.url_utils import dict2url


if __name__ == '__main__':
    d = {
        'fields':'air_daily',
        'public_key':'YOUR_PUBLIC_KEY',
        'ts':1562165673,
        'ttl':'300',
        'locations':'39:118'
    }
    #s=sorted(d.items(),key=lambda x:x[0])
    #print(s)
    c = dict2url(d,sort_by_key=True,override_ts=False)
    print(c)
    #c = 'fields=air_daily&locations=39:118&public_key=YOUR_PUBLIC_KEY&ts=1562165673&ttl=300'

    #print(hash_hmac( , 'YOUR_PRIVATE_KEY',sha1))
    crypto = Crypto(private_key='YOUR_PRIVATE_KEY')
    print(crypto.hmac(c))