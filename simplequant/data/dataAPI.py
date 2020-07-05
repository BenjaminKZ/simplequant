from simplequant.environment import Env


class DataAPI(Env):
    def __init__(self):
        self.viewer = None  # 关联一个视图类

    def changePath(self, data_path):
        Env._database.changePath(data_path)
        # 然后调用viewer的相关函数输出成功与否的信息，其他函数同理

    def load(self):
        Env._database.load()

    def isloaded(self):
        return Env._database.isLoaded()

    def auth(self, account, password):
        Env._database.auth(account, password)

    def is_auth(self):
        return Env._database.is_auth()

    def historyBars(self, order_book_id, frequency='1d', fields=None, start=None, end=None,
                    skip_suspended=True, include_now=False, adjust_type='pre', adjust_orig=None):
        return Env._database.historyBars(order_book_id, frequency, fields, start, end, skip_suspended,
                                         include_now, adjust_type, adjust_orig)

    def get_price(self, security, start_date=None, end_date=None, frequency='daily', fields=None,
                  skip_paused=False, fq='pre', count=None, panel=True, fill_paused=True):
        return Env._database.get_price(security, start_date, end_date, frequency, fields, skip_paused,
                                       fq, count, panel, fill_paused)

    def get_fundamentals(self, query_object, date=None, statDate=None):
        return Env._database.get_fundamentals(query_object, date, statDate)

