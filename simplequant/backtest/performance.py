from simplequant.environment import Env


class Performance(Env):
    def __init__(self, all_positions, all_holdings, benchmark='000300.XSHG'):
        self.all_positions = all_positions
        self.all_holdings = all_holdings

        self.trading_dates = list(self.all_positions['datetime'])

        self.return_ = self.calculateReturn()
        self.annualized_return = self.calcualteAnnualizedReturn()
        self.sharpe_ratio = self.calculateSharpeRatio()
        self.max_drawdowns = self.calculateDrawdowns()
        self.volatility = self.calculateVolatility()
        self.beta = self.calculateBeta()
        self.alpha = self.calcualteAlpha()
        self.info_ratio = self.calculateInformationRatio()
        self.equity_curve = self.calculateEquityCurve()

        self.benchmark = benchmark
        self.benchmark_data = self.getBenchmarkData()
        self.benchmark_return
        self.benchmark_annualized_return

    def calculateReturn(self):
        return self.all_holdings['total'].iloc[-1] / self.all_holdings['total'].iloc[0] - 1

    def calculateAnnualizedReturn(self):
        return_ = self.all_holdings['total'].iloc[-1] / self.all_holdings['total'].iloc[0] - 1
        annualized_return = return_ / len(self.trading_dates) * 250
        return annualized_return

    def calculateSharpeRatio(self):
        pass

    def calculateMaxDrawdowns(self):
        pass

    def calculateVolatility(self):
        pass

    def calculateEquityCurve(self):
        pass

    def setBenchmark(self, benchmark):
        self.benchmark = benchmark
        self.benchmark_data = self.getBenchmarkData()

    def getBenchmarkData(self):
        try:
            benchmark_data = Env._database.allHistoryIndexes()[self.benchmark]
        except IndexError:
            benchmark_data = Env._database.allHistoryBars()[self.benchmark]
        benchmark_data = benchmark_data.reindex(self.trading_dates, method='ffill').fillna(0)
        return benchmark_data

    def report(self):
        '''
        画图
        :return:
        '''
        pass


# def createSharpeRatio(returns, periods=252):
#     """
#     Create the Sharpe ratio for the strategy, based on a
#     benchmark of zero (i.e. no risk-free rate information).
#     :param returns: A pandas Series representing period percentage returns.
#     :param periods:vDaily (252), Hourly (252*6.5), Minutely(252*6.5*60) etc.
#     :return:
#     """
#     return np.sqrt(periods) * (np.mean(returns)) / np.std(returns)
#
#
# def createDrawdowns(pnl):
#     """
#     Calculate the largest peak-to-trough drawdown of the PnL curve
#     as well as the duration of the drawdown. Requires that the
#     pnl_returns is a pandas Series.
#     Parameters:
#     pnl - A pandas Series representing period percentage returns.
#     Returns:
#     drawdown, duration - Highest peak-to-trough drawdown and duration.
#     """
#     # Calculate the cumulative returns curve and set up the High Water Mark
#     hwm = [0]
#
#     idx = pnl.index
#     drawdown = pd.Series(index=idx)
#     duration = pd.Series(index=idx)
#
#     for t in range(1, len(idx)):
#         hwm.append(max(hwm[t-1], pnl[t]))
#         drawdown[t] = (hwm[t] - pnl[t])
#         duration[t] = (0 if drawdown[t] == 0 else duration[t-1] + 1)
#     return drawdown, drawdown.max(), int(duration.max())
#

