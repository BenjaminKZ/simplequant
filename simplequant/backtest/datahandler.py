from abc import ABCMeta, abstractmethod
from queue import PriorityQueue

from simplequant.environment import Env
from simplequant.backtest.event import MarketEvent
from simplequant.backtest.exception import NotTradable
from simplequant.constant import OrderTime


class BaseDataHandler:
    """
    DataHandler is an abstract base class providing an interface for
    all subsequent (inherited) data handlers (both live and historic).
    The goal of a (derived) DataHandler object is to output a generated
    set of bars (OHLCVI) for each symbol requested.
    This will replicate how a live strategy would function as current
    market data would be sent "down the pipe". Thus a historic and live
    system will be treated identically by the rest of the backtesting suite.
    """
    __metaclass__ = ABCMeta

    @abstractmethod
    def updateBars(self, events):
        """
        Pushes the latest bars to the bars_queue for each symbol
        in a tuple OHLCVI format: (datetime, open, high, low,
        close, volume, open interest).
        """
        raise NotImplementedError("Should implement update_bars()")


class RQBundleDataHandler(BaseDataHandler, Env):
    def __init__(self, start, end):
        if Env._database.isLoaded() is False:
            Env._database.load()

        self.symbol_list = Env._database.getStockList()
        self.trading_dates = self._adjustTradingDates(start, end)
        self.trading_dates_generator = self._datesGenerator(self.trading_dates)
        self.symbol_data = self._adjustSymbolData(self.trading_dates)
        self.tradable = self._isTradable(self.trading_dates)
        self.date = None

    @staticmethod
    def _adjustTradingDates(start, end):
        trading_dates = Env._database.getTradingDates()
        left = trading_dates.searchsorted(start)
        right = trading_dates.searchsorted(end, side='right')
        return trading_dates[left:right]

    @staticmethod
    def _datesGenerator(dates):
        for date in dates:
            yield date

    @staticmethod
    def _adjustSymbolData(trading_dates):
        symbol_data = Env._database.allHistoryBars()
        for symbol, bars in symbol_data.items():
            symbol_data[symbol] = bars.reindex(index=trading_dates, method='ffill').fillna(0)
            # 直接把没有数据的价格和交易量置零吧
        return symbol_data

    @staticmethod
    def _isTradable(trading_dates):
        symbol_data = Env._database.allHistoryBars()
        tradable = {}
        for symbol, bars in symbol_data.items():
            bars = bars.reindex(index=trading_dates)
            tradable[symbol] = bars.isna().apply(lambda row: not any(row), axis=1)
        return tradable

    def updateBars(self, events_queue):
        try:
            date = next(self.trading_dates_generator)
        except StopIteration:
            raise StopIteration('回测结束')
        else:
            curr_symbol_data = {}
            for symbol in self.symbol_list:
                curr_symbol_data[symbol] = self.symbol_data[symbol].loc[date, :]  # 每个curr_symbol_data[symbol]都是Series

            market_event = MarketEvent(date, curr_symbol_data)
            events_queue.put((market_event.priority, market_event))

            self.date = date

    def getSimulatedRealTimePrice(self, symbol, datetime, order_time):
        try:
            tradable = self.tradable[symbol][datetime]
        except IndexError:
            raise NotTradable('回测已进入最后一天，不能继续在第二天下单')
        else:
            if tradable:
                if order_time == OrderTime.OPEN:
                    return self.symbol_data[symbol].loc[datetime, 'open']
                elif order_time == OrderTime.CLOSE:
                    return self.symbol_data[symbol].loc[datetime, 'close']
            else:
                raise NotTradable('{d}日{s}停牌不可交易'.format(d=datetime, s=symbol))

    def getSymbolList(self):
        return self.symbol_list

    def getTradingDates(self):
        return self.trading_dates

    def nextTradingDate(self, datetime):
        try:
            ind = self.trading_dates.searchsorted(datetime, side='right')
            return self.trading_dates[ind]
        except IndexError:
            raise NotTradable('回测已进入最后一天，不能继续在第二天下单')


if __name__ == '__main__':
    events_queue = PriorityQueue()
    handler = RQBundleDataHandler(20180101, 20200726)
    # handler.trading_dates
    # handler.symbol_data['000156.XSHE']
    # handler.latest_symbol_data
    # handler.tradable['000001.XSHE']
    handler.updateBars(events_queue)
    handler.updateBars(events_queue)
    handler.updateBars(events_queue)

