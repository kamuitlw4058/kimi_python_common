
import time
import requests

from zmc_common.crypto.base import Crypto
from zmc_common.utils.url_utils import dict2url


if __name__ == '__main__':


    public_key = 'PSCzqP5LrvLcTR2wu'
    private_key = 'SI1CUL2GLasinAPHz'
    d = {
        'fields':'weather_hourly_1h',
        'public_key':public_key,
        'locations':'29.5617:120.0962'
    }
    crypto = Crypto(private_key=private_key,public_key=public_key)
    print(crypto.url_hmac(d))
    params =crypto.url_hmac(d)
    response = requests.get('https://api.seniverse.com/v4?' + params)
    print(response.json())

