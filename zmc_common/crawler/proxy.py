from ..proxy.proxy import ProxyList
from ..utils.logger import getLogger
logger = getLogger(__name__)

# Scrapy 内置的 Downloader Middleware 为 Scrapy 供了基础的功能，
# 定义一个类，其中（object）可以不写，效果一样
class ProxyMiddleware(object):
    # 声明一个数组
    #proxyList = ['http://218.75.158.153:3128','http://188.226.141.61:8080']
    proxy_list = ProxyList()
    
    # Downloader Middleware的核心方法，只有实现了其中一个或多个方法才算自定义了一个Downloader Middleware
    def process_request(self, request, spider):
        # 随机从其中选择一个，并去除左右两边空格
        proxy = proxy_list.get_proxy()
        host = proxy['host']
        port = proxy['prot']
        proxy_url = f'http://{host}:{port}'
        # 打印结果出来观察
        logger.debug("this is request ip:" + proxy_url)
        # 设置request的proxy属性的内容为代理ip
        request.meta['proxy'] = proxy_url

    # # Downloader Middleware的核心方法，只有实现了其中一个或多个方法才算自定义了一个Downloader Middleware
    # def process_response(self, request, response, spider):
    #     # 请求失败不等于200
    #     if response.status != 200:
    #         # 重新选择一个代理ip
    #         proxy = random.choice(self.proxyList).strip()
    #         print("this is response ip:" + proxy)
    #         # 设置新的代理ip内容
    #         request.mete['proxy'] = proxy
    #         return request
    #     return response

