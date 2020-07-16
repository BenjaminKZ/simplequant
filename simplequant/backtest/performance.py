import statsmodels.api as sm
import pandas as pd
import numpy as np
import datetime

from simplequant.environment import Env


class Performance(Env):
    def __init__(self, initial_capital, all_positions, all_holdings, benchmark='000300.XSHG', risk_free_rate='SHIBOR',
                 market_portfolio='000985.XSHG'):
        if not Env._database.is_auth():
            Env._database.auth('13802947200', '947200')

        self.initial_capital = initial_capital
        self.all_positions = all_positions
        self.all_holdings = all_holdings

        self.trading_dates = list(self.all_positions['datetime'])

        self.benchmark = benchmark  # 默认以沪深300作为比较基准
        self.benchmark_data = self.getBenchmarkData(benchmark, self.trading_dates)
        self.benchmark_return = self.calculateBenchmarkReturn(self.benchmark_data)
        self.benchmark_annualized_return = self.calculateBenchmarkAnnualizedReturn(self.benchmark_data, self.trading_dates)
        self.benchmark_curve = self.calculateBenchmarkCurve(self.benchmark_data)

        self.risk_free_rate = self.getRiskFreeRate(risk_free_rate, self.trading_dates)

        self.market_portfolio = market_portfolio  # 以中证全指作为市场组合

        self.return_ = self.calculateReturn()
        self.annualized_return = self.calculateAnnualizedReturn()
        self.equity_curve = self.calculateEquityCurve()
        self.max_drawdown = self.calculateMaxDrawdown()
        self.max_duration = self.calculateMaxDuration()
        self.alpha, self.beta = self.calculateAlphaNBeta(window=5)  # 对策略收益和市场收益都进行了5日平滑处理，减少噪声
        self.sharpe_ratio = self.calculateSharpeRatio()
        self.info_ratio = self.calculateInformationRatio()
        self.volatility, self.percentile_volatility = self.calculateVolatility()
        self.annualized_volatility, self.percentile_annualized_volatility = self.calculateAnnualizedVolatility()

    def changeBenchmark(self, benchmark):
        self.benchmark = benchmark
        self.benchmark_data = self.getBenchmarkData(benchmark, self.trading_dates)
        self.benchmark_return = self.calculateBenchmarkReturn(self.benchmark_data)
        self.benchmark_annualized_return = self.calculateBenchmarkAnnualizedReturn(benchmark, self.trading_dates)
        self.benchmark_curve = self.calculateBenchmarkCurve(self.benchmark_data)

    @staticmethod
    def getBenchmarkData(benchmark, trading_dates):
        try:
            benchmark_data = Env._database.allHistoryIndexes()[benchmark]
        except IndexError:
            benchmark_data = Env._database.allHistoryBars()[benchmark]
        benchmark_data = benchmark_data.reindex(trading_dates, method='ffill').fillna(0)
        return benchmark_data

    @staticmethod
    def calculateBenchmarkReturn(benchmark_data):
        return (benchmark_data['close'].iloc[-1] - benchmark_data['close'].iloc[0]) / benchmark_data['close'].iloc[0]

    @staticmethod
    def calculateBenchmarkAnnualizedReturn(benchmark_data, trading_dates):
        benchmark_return = (benchmark_data['close'].iloc[-1] - benchmark_data['close'].iloc[0]) \
                           / benchmark_data['close'].iloc[0]
        annualized_benchmark_return = benchmark_return / len(trading_dates) * 250
        return annualized_benchmark_return

    @staticmethod
    def calculateBenchmarkCurve(benchmark_data):
        return benchmark_data['close'] / benchmark_data['close'].iloc[0]

    @staticmethod
    def getRiskFreeRate(risk_free_rate, trading_dates):
        str2arg = {'HIBOR': 1, 'LIBOR': 2, 'CHIBOR': 3, 'SIBOR': 4, 'SHIBOR': 5}
        macro = Env._database.macro
        query = Env._database.query

        q = query(macro.MAC_LEND_RATE).filter(macro.MAC_LEND_RATE.currency_id == 1,
                                              macro.MAC_LEND_RATE.market_id == str2arg[risk_free_rate],
                                              macro.MAC_LEND_RATE.term_id == 20).order_by(macro.MAC_LEND_RATE.day.asc())
        df = macro.run_query(q)

        df['datetime'] = df['day'].apply(lambda s: int(''.join(s.split('-'))))
        df.index = df['datetime']
        df = df.reindex(trading_dates, method='ffill').fillna(0)

        return df[['datetime', 'interest_rate']]

    def calculateReturn(self):
        return self.all_holdings['total'].iloc[-1] / self.all_holdings['total'].iloc[0] - 1

    def calculateAnnualizedReturn(self):
        r = self.all_holdings['total'].iloc[-1] / self.all_holdings['total'].iloc[0] - 1
        annualized_return = r / len(self.trading_dates) * 250
        return annualized_return

    def calculateEquityCurve(self):
        return self.all_holdings['total'] / self.initial_capital

    def calculateMaxDrawdown(self):
        equity_curve = self.all_holdings['total'] / self.initial_capital
        equity_cummax = equity_curve.cummax()
        drawdowns = (equity_curve - equity_cummax) / equity_cummax
        return np.min(drawdowns)

    def calculateMaxDuration(self):
        equity_curve = self.all_holdings['total'] / self.initial_capital
        equity_cummax = equity_curve.cummax()

        max_duration = datetime.timedelta()
        max_ind = equity_curve.index[0]
        for ind in equity_curve.index:
            if equity_curve[ind] == equity_cummax[ind]:
                max_ind = ind
            date = datetime.datetime.strptime(str(ind), '%Y%m%d')
            max_date = datetime.datetime.strptime(str(max_ind), '%Y%m%d')
            duration = date - max_date
            if duration > max_duration:
                max_duration = duration

        return max_duration.days  # 返回以天为单位的计数

    def calculateAlphaNBeta(self, window=5):
        overnight = self.risk_free_rate['interest_rate'] / 100 / 250  # 转化为非年化的无风险利率

        returns = self.all_holdings['total'].pct_change()
        returns.iloc[0] = self.all_holdings['total'].iloc[0] / self.initial_capital
        # # Series和Series四则运算会按照索引配对，所以下面的方法不行，必须直接使用pct_change()
        # returns = (self.all_holdings['total'].iloc[1:] - self.all_holdings['total'].iloc[:-1]) / \
        #           self.all_holdings['total'].iloc[:-1]
        adjusted_returns = returns.rolling(window=window).mean().iloc[window - 1:] \
                           - overnight.rolling(window=window).mean().iloc[window - 1:]

        market_data = self.getBenchmarkData(self.market_portfolio, self.trading_dates)
        market_returns = market_data['close'].pct_change()
        market_returns.iloc[0] = 1
        adjusted_market_returns = market_returns.rolling(window=window).mean().iloc[window - 1:] \
                                  - overnight.rolling(window=window).mean().iloc[window - 1:]

        x = sm.add_constant(adjusted_market_returns)
        model = sm.OLS(adjusted_returns, x)
        results = model.fit()

        return results.params

    def calculateSharpeRatio(self):
        returns = self.all_holdings['total'].pct_change()
        returns.iloc[0] = self.all_holdings['total'].iloc[0] / self.initial_capital
        risk_free_rate = self.risk_free_rate['interest_rate'] / 100
        return (self.annualized_return - np.mean(risk_free_rate)) / np.sqrt(np.var(returns) * 250)

    def calculateInformationRatio(self):
        returns = self.all_holdings['total'].pct_change()
        returns.iloc[0] = self.all_holdings['total'].iloc[0] / self.initial_capital
        market_data = self.getBenchmarkData(self.market_portfolio, self.trading_dates)
        market_returns = market_data['close'].pct_change()
        market_returns.iloc[0] = 1
        return np.mean(returns - market_returns) / np.std(returns - market_returns)

    def calculateVolatility(self):
        returns = self.all_holdings['total'].pct_change()
        returns.iloc[0] = self.all_holdings['total'].iloc[0] / self.initial_capital
        std = np.std(returns)
        mean = np.mean(returns)
        return std, std / mean

    def calculateAnnualizedVolatility(self):
        returns = self.all_holdings['total'].pct_change()
        returns.iloc[0] = self.all_holdings['total'].iloc[0] / self.initial_capital
        annualized_std = np.sqrt(np.var(returns) * 250)
        annualized_return = self.calculateAnnualizedReturn()
        return annualized_std, annualized_std / annualized_return

    def report(self):
        performance = {'benchmark': self.benchmark,
                       'benchmark_return': self.benchmark_return,
                       'benchmark_annualized_return': self.benchmark_annualized_return,
                       'benchmark_curve': self.benchmark_curve,
                       'return': self.return_,
                       'annualized_return': self.annualized_return,
                       'equity_curve': self.equity_curve,
                       'max_drawdown': self.max_drawdown,
                       'max_duration': self.max_duration,
                       'alpha': self.alpha,
                       'beta': self.beta,
                       'sharpe_ratio': self.sharpe_ratio,
                       'info_ratio': self.info_ratio,
                       'volatility': self.volatility,
                       'percentile_volatility': self.percentile_volatility,
                       'annualized_volatility': self.annualized_volatility,
                       'percentile_annualized_volatility': self.percentile_annualized_volatility}

        return performance


if __name__ == '__main__':
    all_positions = pd.read_csv('./all_positions.csv', index_col=0)
    all_holdings = pd.read_csv('./all_holdings.csv', index_col=0)
    performance = Performance(100000, all_positions, all_holdings, '000300.XSHG')

