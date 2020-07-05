import datetime

from simplequant.strategy.basestrategy import BaseStrategy
from simplequant.backtest.event import SignalEvent
from simplequant.constant import Direction, OrderTime


class DoubleMovingAverageStrategy(BaseStrategy):
    """
    演示策略3：双均线策略。当短均线上穿长均线时买入股票，当短均线下穿长均线时卖出所有股票。
    """

    def __init__(self, portfolio):
        self.symbol = '000651.XSHE'  # 格力电器
        self.short = 5
        self.long = 30
        self.field = 'close'
        self.quantity = 1000
        self.api.auth('13802947200', '947200')

    def handleBar(self, events_queue, event):
        date = datetime.datetime.strptime(str(event.datetime), '%Y%m%d')
        bars = self.api.get_price(self.symbol, end_date=date, count=self.long+1, frequency='daily')
        short_average = bars[self.field].rolling(self.short).mean()
        long_average = bars[self.field].rolling(self.long).mean()

        if short_average.iloc[-1] > long_average.iloc[-1] and short_average.iloc[-2] <= long_average.iloc[-2]:
            signal_event = SignalEvent(event.datetime, self.symbol, Direction.LONG, self.quantity, OrderTime.OPEN)
            events_queue.put((signal_event.priority, signal_event))
        elif short_average.iloc[-1] < long_average.iloc[-1] and short_average.iloc[-2] >= long_average.iloc[-2]:
            signal_event = SignalEvent(event.datetime, self.symbol, Direction.NET, self.quantity, OrderTime.OPEN)
            events_queue.put((signal_event.priority, signal_event))

