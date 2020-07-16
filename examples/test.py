from simplequant.backtest.backtest import Backtest
from simplequant.strategy.pe_strategy import PEStrategy


backtest = Backtest(PEStrategy, start=20190101, end=20191231)
performance = backtest.run()
performance.report()

