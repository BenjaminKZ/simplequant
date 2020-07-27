import datetime
import requests
import dateutil
import os
import shutil
import pandas as pd
import jqdatasdk
import numpy as np
from bisect import bisect_right
import six

from simplequant import utils
from simplequant.constant import FIELDS_REQUIRE_ADJUSTMENT, PRICE_FIELDS


class Database:

    CDN_URL = 'http://bundle.assets.ricequant.com/bundles_v4/rqbundle_%04d%02d.tar.bz2'
    data_dir = 'data_files'
    stock_file = 'stocks.h5'
    ex_cum_factor_file = 'ex_cum_factor.h5'
    trading_dates_file = 'trading_dates.npy'
    indexes_file = 'indexes.h5'

    def __init__(self):
        self.data_path = os.path.join(os.path.dirname(__file__), self.data_dir)  # .表示当前工作路径，并不是本文件所在的目录
        self.loaded = False
        self.mergeJQData()

    def mergeJQData(self):
        '''
        让jqdatasdk.fun()都可以以Database的实例database.fun()的形式调用
        :return:
        '''
        for fun_name in jqdatasdk.__dict__['__all__']:
            setattr(self, fun_name, jqdatasdk.__dict__[fun_name])

    def changePath(self, data_path):
        self.data_path = data_path

    def load(self):
        year, month = self._getLatestTimestamp()

        timestamp_path = os.path.join(self.data_path, str(year) + str(month) + '.txt')
        stocks_path = os.path.join(self.data_path, self.stock_file)
        ex_cum_factor_path = os.path.join(self.data_path, self.ex_cum_factor_file)
        trading_dates_path = os.path.join(self.data_path, self.trading_dates_file)

        if os.path.exists(timestamp_path) and os.path.exists(stocks_path)\
                and os.path.exists(ex_cum_factor_path) and os.path.exists(trading_dates_path):
            self.loaded = True
            print('data is already loaded')
        else:
            if os.path.exists(self.data_path):
                shutil.rmtree(self.data_path)
            os.makedirs(self.data_path)
            url = self._getExactURL()
            tar_path = utils.download(url, self.data_path)
            utils.extract(tar_path, self.data_path, 'bz2')
            os.remove(tar_path)
            open(os.path.join(self.data_path, str(year) + str(month) + '.txt'), 'a').close()
            self.loaded = True
            print('successfully load data')

    def isLoaded(self):
        return self.loaded

    def _getLatestTimestamp(self):
        day = datetime.date.today()
        while True:  # get exactly url
            url = self.CDN_URL % (day.year, day.month)
            r = requests.get(url, stream=True)
            if r.status_code == 200:
                return day.year, day.month
            day -= dateutil.relativedelta.relativedelta(months=1)

    def _getExactURL(self):
        day = datetime.date.today()
        while True:  # get exactly url
            url = self.CDN_URL % (day.year, day.month)
            print(u"try {} ...".format(url))
            r = requests.get(url, stream=True)
            if r.status_code == 200:
                print(u"succeed in {} ...".format(url))
                return url
            day -= dateutil.relativedelta.relativedelta(months=1)

    def allHistoryBars(self, frequency='1d', fields=None, start=None, end=None, skip_suspended=True,
                       include_now=False, adjust_type='pre', adjust_orig=None):
        if frequency != '1d':
            raise NotImplementedError('暂不支持调取日频以外的行情数据')
        if adjust_type != 'pre' and adjust_type != 'None':
            raise NotImplementedError('暂不支持前复权以外的复权方式')

        if isinstance(start, str):
            try:
                start = datetime.datetime.strptime(start, '%Y-%m-%d')
            except ValueError:
                raise ValueError('传入了无效的start参数。start应为datetime.datetime类型或形如2000-01-01的字符串')
        if isinstance(end, str):
            try:
                end = datetime.datetime.strptime(end, '%Y-%m-%d')
            except ValueError:
                raise ValueError('传入了无效的end参数。end应为datetime.datetime类型或形如2000-01-01的字符串')

        if end is None:
            end = datetime.datetime.now()

        if adjust_orig is None:
            adjust_orig = datetime.datetime.now()

        stocks_bars = self._allDayBars()
        stocks_bars = self._stockFilter(skip_suspended, stocks_bars)

        adjusted_stocks_bars = {}
        for order_book_id, bars in stocks_bars.items():
            if not self._areFieldsValid(fields, bars.dtype.names):
                raise ValueError("invalid fields: {}".format(fields))

            end_int = np.uint64(utils.convert_date_to_int(end))
            right = bars['datetime'].searchsorted(end_int, side='right')
            if start is not None:
                start_int = np.uint64(utils.convert_date_to_int(start))
                left = bars['datetime'].searchsorted(start_int)
                bars = bars[left:right]
            else:
                bars = bars[:right]

            if adjust_type == 'None':
                return bars if fields is None else bars[fields]

            if isinstance(fields, str) and fields not in FIELDS_REQUIRE_ADJUSTMENT:
                return bars if fields is None else bars[fields]

            out_arr = self._adjustBars(bars, self._getExCumFactor(order_book_id), fields, adjust_type, adjust_orig)
            out_arr['datetime'] = (out_arr['datetime'] / 1000000).astype(int)  # 默认的数据格式除了年月日之外还包含时分秒
            out_df = pd.DataFrame(out_arr, index=out_arr['datetime'])
            # out_df.index = out_df['datetime'].apply(lambda dt: datetime.datetime.strptime(str(dt), '%Y%m%d%H%M%S'))
            # 若采用datetime类型作为索引，耗时很长

            adjusted_stocks_bars[order_book_id] = out_df
        return adjusted_stocks_bars

    def _allDayBars(self):
        stocks_path = os.path.join(self.data_path, self.stock_file)
        stocks = utils.open_h5(stocks_path)
        stocks_bars = {}
        for order_book_id in stocks:
            stocks_bars[order_book_id] = stocks[order_book_id][:]
        return stocks_bars

    def _stockFilter(self, skip_suspended, stocks_bars):
        stock_list = self.getStockList()
        filtered_stocks_bars = {}
        for order_book_id in stock_list:
            filtered_stocks_bars[order_book_id] = stocks_bars[order_book_id]
        if skip_suspended:
            for order_book_id, bars in filtered_stocks_bars.items():
                filtered_stocks_bars[order_book_id] = bars[bars['volume'] > 0]

        return filtered_stocks_bars

    def getStockList(self):
        stocks_path = os.path.join(self.data_path, self.stock_file)
        stocks = utils.open_h5(stocks_path)
        stocks_bars = {}
        for order_book_id in stocks:
            stocks_bars[order_book_id] = stocks[order_book_id][:]

        filtered_volume0_stocks_bars = {}
        for order_book_id, bars in stocks_bars.items():
            bars = bars[bars['volume'] > 0]
            if len(bars) > 0:
                filtered_volume0_stocks_bars[order_book_id] = bars

        ex_cum_factor_path = os.path.join(self.data_path, self.ex_cum_factor_file)
        ex_cum_factor = utils.open_h5(ex_cum_factor_path)
        stock_list = list(set(filtered_volume0_stocks_bars.keys()) & set(ex_cum_factor.keys()))
        # 有一些股票（退市？）在记录了送股和转股信息的ex_cum_factor当中没有出现

        return stock_list

    def _adjustBars(self, bars, ex_factors, fields, adjust_type, adjust_orig):
        if ex_factors is None or len(bars) == 0:
            return bars if fields is None else bars[fields]

        dates = ex_factors['start_date']
        ex_cum_factors = ex_factors['ex_cum_factor']

        if adjust_type == 'pre':
            adjust_orig_dt = np.uint64(utils.convert_date_to_int(adjust_orig))
            base_adjust_rate = self._factor4Date(dates, ex_cum_factors, adjust_orig_dt)
        else:
            base_adjust_rate = 1.0

        start_date = bars['datetime'][0]
        end_date = bars['datetime'][-1]

        if (self._factor4Date(dates, ex_cum_factors, start_date) == base_adjust_rate and
                self._factor4Date(dates, ex_cum_factors, end_date) == base_adjust_rate):
            return bars if fields is None else bars[fields]

        factors = ex_cum_factors.take(dates.searchsorted(bars['datetime'], side='right') - 1)

        # 复权
        factors /= base_adjust_rate
        if isinstance(fields, str):
            if fields in PRICE_FIELDS:
                return bars[fields] * factors
            elif fields == 'volume':
                return bars[fields] * (1 / factors)
            # should not got here
            return bars[fields]

        result = np.copy(bars if fields is None else bars[fields])
        for f in result.dtype.names:
            if f in PRICE_FIELDS:
                result[f] *= factors
            elif f == 'volume':
                result[f] *= (1 / factors)
        return result

    @staticmethod
    def _areFieldsValid(fields, valid_fields):
        if fields is None:
            return True
        if isinstance(fields, six.string_types):
            return fields in valid_fields
        for field in fields:
            if field not in valid_fields:
                return False
        return True

    def _getExCumFactor(self, order_book_id):
        ex_cum_factor_path = os.path.join(self.data_path, self.ex_cum_factor_file)
        ex_cum_factor = utils.open_h5(ex_cum_factor_path)
        return ex_cum_factor[order_book_id][:]

    @staticmethod
    def _factor4Date(dates, factors, d):
        pos = bisect_right(dates, d)
        return factors[pos-1]

    def getStartDate(self):
        trading_dates_path = os.path.join(self.data_path, self.trading_dates_file)
        trading_dates = np.load(trading_dates_path)
        return trading_dates[0]

    def getEndDate(self):
        trading_dates_path = os.path.join(self.data_path, self.trading_dates_file)
        trading_dates = np.load(trading_dates_path)
        now = datetime.datetime.now()
        first_day_curr_month = datetime.date(now.year, now.month, 1)
        last_day_pre_month = first_day_curr_month - datetime.timedelta(days=1)
        last_day_pre_month = datetime.datetime.strftime(last_day_pre_month, '%Y%m%d')
        last_day_pre_month = int(last_day_pre_month)
        ind = trading_dates.searchsorted(last_day_pre_month, side='right')  # 返回符合要求的插入位置的最后一位的索引
        return trading_dates[ind - 1]

    def getTradingDates(self):
        trading_dates_path = os.path.join(self.data_path, self.trading_dates_file)
        trading_dates = np.load(trading_dates_path)
        end = self.getEndDate()
        ind = trading_dates.searchsorted(end, side='right')
        return trading_dates[:ind]

    def allHistoryIndexes(self):
        indexes_path = os.path.join(self.data_path, self.indexes_file)
        indexes = utils.open_h5(indexes_path)
        indexes_data = {}
        for symbol in indexes.keys():
            index_arr = indexes[symbol][:]
            index_arr['datetime'] = (index_arr['datetime'] / 1000000).astype(int)
            indexes_data[symbol] = pd.DataFrame(index_arr, index=index_arr['datetime'])
        return indexes_data

if __name__ == '__main__':
    database = Database()
    database.load()
    database.auth('聚宽账号', '密码')
    database.is_auth()
    # print(database.historyBars('300722.XSHE', start='2020-05-01', end='2020-05-31'))  # 新余国科这只股票2020年5月21日进行过除权
    all_history_bars = database.allHistoryBars()
    print(all_history_bars['300722.XSHE'].loc[20200501:20200531, :])
    indexes = database.allHistoryIndexes()

