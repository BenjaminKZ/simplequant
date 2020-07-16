import queue
import sys

from simplequant.environment import Env
from simplequant.backtest.datahandler import RQBundleDataHandler
from simplequant.backtest.portfolio import Portfolio
from simplequant.backtest.execution import SimulatedExecutionHandler
from simplequant.backtest.performance import Performance
from simplequant.constant import EventType


class Backtest(Env):
    """
    Enscapsulates the settings and components for carrying out
    an event-driven backtest.
    """

    def __init__(self, Strategy, interval='1d', start=None, end=None, rate=3/10000,
                 slippage=0.2/100, initial_capital=100000, heartbeat=0, benchmark='000300.XSHG'):
        if interval != '1d':
            raise NotImplementedError('暂不支持分钟级别外的回测')
        else:
            self.interval = interval

        # Backtest类仅仅针对数据范围调整了start和end，根据交易日的调整在datahandler当中进行
        self.start, self.end = self._adjustStartEnd(start, end)
        self.rate = rate
        self.slippage = slippage
        self.initial_capital = initial_capital
        self.heartbeat = heartbeat
        self.benchmark = benchmark

        # 初始化需要哪些参数要重新确定
        self.data_handler = RQBundleDataHandler(self.start, self.end)
        self.portfolio = Portfolio(self.data_handler, self.initial_capital)
        self.execution_handler = SimulatedExecutionHandler(self.data_handler, self.portfolio, self.rate, self.slippage)
        self.strategy = Strategy(self.portfolio)
        self.performance = None

        # 优先级队列，一共使用到两个级别，OrderEvent和FillEvent优先，MarketEvent和SignalEvent次优
        self.events_queue = queue.PriorityQueue()

    def changeParameters(self, **args):
        for key, value in args.items():
            if key not in ['strategy', 'interval', 'start', 'end', 'rate', 'slippage', 'initial_capital', 'heartbeat']:
                raise ValueError('输入了无效的参数')

        for key, value in args.items():
            self.__dict__[key] = value
        self.__init__(Strategy=self.strategy, interval=self.interval, start=self.start, end=self.end, rate=self.rate,
                      slippage=self.slippage, initial_capital=self.initial_capital, heartbeat=self.heartbeat)

    def run(self):
        """
        Executes the backtest.
        当已经是最后一条数据的时候，self.data_handler.continue_backtest仍然是True，updateBars之后变为False，
        并且没有新的MarketEvent被插入队列，所以进入内层循环时队列是空的，直接break，不会出错
        """
        total = len(self.data_handler.getTradingDates())
        fininshed = 0
        while True:
            # Update the market bars
            try:
                self.data_handler.updateBars(self.events_queue)
            except StopIteration:
                break

            # Handle the events
            while True:
                try:
                    event = self.events_queue.get(block=False)[1]  # 返回一个元组，第一个元素是优先级，第二个是事件
                except queue.Empty:
                    fininshed += 1
                    sys.stdout.write('\r回测进度：{p}% ({f}/{t})\n'.format(p=round(fininshed/total*100, 2),
                                                                      f=fininshed, t=total))
                    sys.stdout.flush()
                    break
                else:
                    if event is not None:
                        if event.type == EventType.MARKET:
                            self.portfolio.updateFromMarket(event)
                            self.strategy.handleBar(self.events_queue, event)  # 需要调整
                        elif event.type == EventType.SIGNAL:
                            print(event)
                            self.portfolio.updateSignal(self.events_queue, event)
                        elif event.type == EventType.ORDER:
                            print(event)
                            self.execution_handler.executeOrder(self.events_queue, event)
                        elif event.type == EventType.FILL:
                            print(event)
                            self.portfolio.updateFromFill(event)

        self.performance = Performance(self.initial_capital, self.portfolio.all_positions,
                                       self.portfolio.all_holdings, self.benchmark)
        return self.performance

    def report(self):
        return self.performance.report()

    @staticmethod
    def _adjustStartEnd(start, end):
        '''
        约束start和end在能获取到的回测数据范围内。
        调整后的start和end还不一定是真正开始交易的日期，因为start和end可能不是交易日。
        真正开始交易的日期可能在start之后，真正结束交易的日期可能在end之前。
        :param start:
        :param end:
        :return:
        '''
        if isinstance(start, str):
            start_list = start.split('-')
            start = ''.join(start_list)
            start = int(start)
            if start < Env._database.getStartDate():
                start = Env._database.getStartDate()
            elif start >= Env._database.getEndDate():
                raise ValueError('回测开始时间必须在{date}之前，{date}之后的行情尚未更新'.format(date=Env._database.getEndDate()))
        elif isinstance(start, int):
            if start < Env._database.getStartDate():
                start = Env._database.getStartDate()
            elif start >= Env._database.getEndDate():
                raise ValueError('回测开始时间必须在{date}之前，{date}之后的行情尚未更新'.format(date=Env._database.getEndDate()))
        elif start is None:
            start = Env._database.getStartDate()
        else:
            raise ValueError('start参数应为形如2005-01-04形式的字符串或者形如20050104的整型数值')

        if isinstance(end, str):
            end_list = end.split('-')
            end = ''.join(end_list)
            end = int(end)
            if end > Env._database.getEndDate():
                end = Env._database.getEndDate()
            elif end <= Env._database.getStartDate():
                raise ValueError('回测结束时间必须在{date}之后，数据库未存储{date}之前的行情'.format(date=Env._database.getStartDate()))
        elif isinstance(end, int):
            if end > Env._database.getEndDate():
                end = Env._database.getEndDate()
            elif end <= Env._database.getStartDate():
                raise ValueError('回测结束时间必须在{date}之后，数据库未存储{date}之前的行情'.format(date=Env._database.getStartDate()))
        elif end is None:
            end = Env._database.getEndDate()
        else:
            raise ValueError('end参数应为形如2020-05-29形式的字符串或者形如20200529的整型数值')

        return start, end

