import datetime

from simplequant.strategy.basestrategy import BaseStrategy
from simplequant.backtest.event import SignalEvent
from simplequant.constant import Direction, OrderTime


class PEStrategy(BaseStrategy):
    """
    演示策略4：每月买入动态市盈率最低的若干只股票，在通过市值和营收筛选的股票池内
    """
    def __init__(self, portfolio):
        self.portfolio = portfolio

        self.num = 10  # 10只股票
        self.market_value = 1000  # 1000亿市值，单位是亿元
        self.operating_revenue = 20000000000  # 200亿营业总收入，单位是元
        self.quantity = 400  # 每只股票买入400股

        self.api.auth('13802947200', '947200')
        self.query = self.api.query
        self.code = self.api.valuation.code
        self.market_cap = self.api.valuation.market_cap
        self.pe_ratio = self.api.valuation.pe_ratio
        self.total_operating_revenue = self.api.income.total_operating_revenue

    def handleBar(self, events_queue, event):
        now = event.datetime
        trading_dates = self.portfolio.trading_dates
        try:
            post = trading_dates[trading_dates.searchsorted(now, side='right')]
        except IndexError:  # 第二天已在回测区间以外
            return
        now = datetime.datetime.strptime(str(now), '%Y%m%d')
        post = datetime.datetime.strptime(str(post), '%Y%m%d')
        if now.month == post.month:  # 每月调仓一次，只有每月最后一个交易日才会运行后面的代码
            return

        q = self.query(self.code, self.market_cap, self.pe_ratio, self.total_operating_revenue
                       ).filter(self.market_cap > self.market_value, self.pe_ratio > 0,
                                self.total_operating_revenue > self.operating_revenue
                                ).order_by(self.pe_ratio.asc()).limit(self.num)
        date = datetime.datetime.strptime(str(event.datetime), '%Y%m%d')
        df = self.api.get_fundamentals(q, date=date)

        current_positions = list(self.portfolio.current_positions[self.portfolio.current_positions != 0].index)
        current_positions.remove('datetime')
        net_set = set(current_positions) - set(df['code'])
        long_set = set(df['code']) - set(current_positions)

        for symbol in net_set:
            signal_event = SignalEvent(event.datetime, symbol, Direction.NET, self.quantity, OrderTime.OPEN)
            events_queue.put((signal_event.priority, signal_event))
        for symbol in long_set:
            signal_event = SignalEvent(event.datetime, symbol, Direction.LONG, self.quantity, OrderTime.OPEN)
            events_queue.put((signal_event.priority, signal_event))

