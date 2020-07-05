from simplequant.backtest.backtest import Backtest
from simplequant.strategy.pe_strategy import PEStrategy


backtest = Backtest(PEStrategy, start=20180101, end=20191231)
performance = backtest.run()
performance.report()

# from datahandler import RQBundleDataHandler
# d = RQBundleDataHandler(20190101, 20191231)
# from portfolio import Portfolio
# p = Portfolio(d)
# import queue
# q = queue.PriorityQueue()
# d.updateBars(q)

