from simplequant.strategy.basestrategy import BaseStrategy

from simplequant.backtest.event import SignalEvent
from simplequant.constant import Direction, OrderTime


class BuyMultipleStocksEverydayStrategy(BaseStrategy):
    """
    演示策略2：回测期间每日买入固定数量的所有股票
    """
    def __init__(self, portfolio):
        self.num = 5
        self.symbols = portfolio.symbol_list[:self.num]
        self.quantity = 100

    def handleBar(self, events_queue, event):
        for symbol in self.symbols:
            signal_event = SignalEvent(event.datetime, symbol, Direction.LONG, self.quantity, OrderTime.OPEN)
            events_queue.put((signal_event.priority, signal_event))

