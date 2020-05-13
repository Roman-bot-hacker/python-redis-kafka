import urllib.request as rq


class DataReader:

    def __init__(self, timeout=5):
        self.timeout = timeout

    def read_file_from_url(self, file_url):
        return rq.urlopen(file_url, timeout=self.timeout).read().decode("utf-8").split('\n')
