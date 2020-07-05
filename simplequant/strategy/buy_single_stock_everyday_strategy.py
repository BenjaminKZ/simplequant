from simplequant.strategy.basestrategy import BaseStrategy

from simplequant.backtest.event import SignalEvent
from simplequant.constant import Direction, OrderTime


class BuySingleStockEverydayStrategy(BaseStrategy):
    """
    演示策略1：回测期间每日买入固定数量的指定股票
    """
    def __init__(self, portfolio):
        self.symbol = '000001.XSHE'  # 平安银行
        self.quantity = 250  # 实际上只会成交200股

    def handleBar(self, events_queue, event):
        signal_event = SignalEvent(event.datetime, self.symbol, Direction.LONG, self.quantity, OrderTime.OPEN)
        events_queue.put((signal_event.priority, signal_event))

