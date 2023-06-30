class ProxyMiddleware:
    def __init__(self, proxy):
        self.proxy = proxy

    def process_request(self, request):
        request.proxies = {"http": self.proxy, "https": self.proxy}
        return request