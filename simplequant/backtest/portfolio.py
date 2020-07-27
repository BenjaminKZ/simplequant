import pandas as pd

from simplequant.backtest.event import OrderEvent
from simplequant.constant import Direction
from simplequant.backtest.exception import NotTradable


class Portfolio:
    """
    The Portfolio class handles the positions and market
    value of all instruments at a resolution of a "bar",
    i.e. secondly, minutely, 5-min, 30-min, 60 min or EOD.
    The positions DataFrame stores a time-index of the
    quantity of positions held.
    The holdings DataFrame stores the cash and total market
    holdings value of each symbol for a particular
    time-index, as well as the percentage change in
    portfolio total across bars.
    """

    def __init__(self, data_handler, initial_capital=100000):
        """
        Initialises the portfolio with bars and an event queue.
        Also includes a starting datetime index and initial capital
        (USD unless otherwise stated).
        :param bars: The DataHandler object with current market data.
        :param events: The Event Queue object.
        :param start_date: The start date (bar) of the portfolio.
        :param initial_capital: The starting capital in USD.
        """
        self.initial_capital = initial_capital
        self.data_handler = data_handler
        self.symbol_list = data_handler.getSymbolList()
        self.trading_dates = data_handler.getTradingDates()

        self.all_positions = pd.DataFrame(columns=['datetime'] + self.symbol_list)
        self.current_positions = self.initCurrentPosition()  # 注意self.current_positions是Series

        self.all_holdings = pd.DataFrame(columns=['datetime', 'total', 'cash', 'commission'] + self.symbol_list)
        self.current_holdings = self.initCurrentHoldings()  # 注意self.current_holdings是Series

        self.equity_curve = None  # will be calculated in method of
        # create_equity_curve_dataframe

    def initCurrentPosition(self):
        """
        :return:
        """

        current_positions = pd.Series(index=['datetime'] + self.symbol_list)
        current_positions[1:] = 0  # datetime空缺

        return current_positions

    def initCurrentHoldings(self):
        """
        This constructs the dictionary which will hold the instantaneous
        value of the portfolio across all symbols.
        """

        current_holdings = pd.Series(index=['datetime', 'total', 'cash', 'commission'] + self.symbol_list)
        current_holdings['total'] = self.initial_capital
        current_holdings['cash'] = self.initial_capital
        current_holdings['commission'] = 0
        current_holdings[self.symbol_list] = 0

        return current_holdings

    def updateCurrentHoldingsFromMarket(self, market_event):
        self.current_holdings.name = market_event.datetime
        self.current_holdings['datetime'] = market_event.datetime
        self.current_holdings['total'] = self.current_holdings['cash'] - self.current_holdings['commission']
        self.current_holdings['cash'] = self.current_holdings['cash']
        self.current_holdings['commission'] = self.current_holdings['commission']

        for symbol in self.symbol_list:
            market_value = self.current_positions[symbol] * market_event.symbol_data[symbol]['close']
            self.current_holdings[symbol] = market_value
            self.current_holdings['total'] += market_value

    def updateAllHoldingsFromMarket(self, market_event):
        try:
            self.all_holdings = pd.concat([self.all_holdings, pd.DataFrame(self.current_holdings).T], axis=0)
        except KeyError:
            self.all_holdings = pd.DataFrame(self.current_holdings).T

    def updateAllPositions(self, market_event):
        self.current_positions.name = market_event.datetime
        self.current_positions['datetime'] = market_event.datetime
        temp = pd.DataFrame(self.current_positions).T
        try:
            self.all_positions = pd.concat([self.all_positions, temp], axis=0)
        except KeyError:
            self.all_positions = temp

    def updateFromMarket(self, market_event):
        """
        Adds a new record to the positions matrix for the current
        market data bar. This reflects the PREVIOUS bar, i.e. all
        current market data at this stage is known (OHLCV).
        Makes use of a MarketEvent from the events queue.
        """

        self.updateCurrentHoldingsFromMarket(market_event)
        self.updateAllHoldingsFromMarket(market_event)
        self.updateAllPositions(market_event)

    def generateOrder(self, signal_event):
        datetime = signal_event.datetime
        post_datetime = self.data_handler.nextTradingDate(datetime)
        symbol = signal_event.symbol
        direction = signal_event.direction
        if direction == Direction.LONG or direction == Direction.SHORT:
            quantity = signal_event.quantity // 100 * 100  # quantity的作用域直到函数结束，if不会形成局部作用域
        elif direction == Direction.NET:
            quantity = self.current_positions[symbol] // 100 * 100
        else:
            raise ValueError('订单类型只能是Direction.LONG、Direction.SHORT或Direction.NET三种类型之一')
        order_time = signal_event.order_time

        return OrderEvent(post_datetime, symbol, direction, quantity, order_time)

    def updateSignal(self, events_queue, signal_event):
        """
        Acts on a SignalEvent to generate new orders
        based on the portfolio logic.
        """
        try:
            order_event = self.generateOrder(signal_event)
        except NotTradable:
            pass
        else:
            if order_event.quantity > 0:
                events_queue.put((order_event.priority, order_event))

    def updateCurrentPositionFromFill(self, fill_event):
        """
        Takes a Fill object and updates the position matrix to
        reflect the new position.
        Parameters:
        fill - The Fill object to update the positions with.
        """

        # 更新时间其实意义不大，因为一天当中可能有多个fill_event，
        # self.current_positions的时间在接收第一个fill_event时就发生改变，但此时的仓位不一定是当天的最终仓位
        self.current_positions.name = fill_event.datetime
        self.current_positions['datetime'] = fill_event.datetime

        if fill_event.direction == Direction.LONG:
            self.current_positions[fill_event.symbol] += fill_event.quantity
        elif fill_event.direction == Direction.SHORT or fill_event.direction == Direction.NET:
            self.current_positions[fill_event.symbol] -= fill_event.quantity
        else:
            raise ValueError('订单类型只能是Direction.LONG、Direction.SHORT或Direction.NET')

    def updateCurrentHoldingsFromFill(self, fill_event):
        """
        Takes a Fill object and updates the holdings matrix to
        reflect the holdings value.
        Parameters:
        fill - The Fill object to update the holdings with.
        """
        datetime = fill_event.datetime
        symbol = fill_event.symbol
        direction = fill_event.direction
        fill_cost = fill_event.fill_cost
        quantity = fill_event.quantity
        commission = fill_event.commission

        self.current_holdings.name = datetime
        self.current_holdings['datetime'] = datetime
        self.current_holdings['commission'] += commission
        self.current_holdings[symbol] = fill_cost * self.current_positions[symbol]
        if direction == Direction.LONG:
            self.current_holdings['cash'] = self.current_holdings['cash'] - fill_cost * quantity - commission
        elif direction == Direction.SHORT or direction == Direction.NET:
            self.current_holdings['cash'] = self.current_holdings['cash'] + fill_cost * quantity - commission
        else:
            raise ValueError('订单类型只能是Direction.LONG、Direction.SHORT或Direction.NET')
        self.current_holdings['total'] = self.current_holdings['cash'] - self.current_holdings['commission']\
                                         + sum(self.current_holdings[self.symbol_list])

        # 更新self.current_holdings[symbol]和self.current_holdings['total']其实没有意义，
        # 因为symbol以外的股票的价格可能已经发生变动，但并未更新，所以self.current_holdings['total']很不准确，
        # 关注点主要在于self.current_holdings['cash']和self.current_holdings['commission']的更新。

    def updateFromFill(self, fill_event):
        """
        Updates the portfolio current positions and holdings
        from a FillEvent.
        """
        self.updateCurrentPositionFromFill(fill_event)
        self.updateCurrentHoldingsFromFill(fill_event)

    def getCurrentCash(self):
        return self.current_holdings['cash']

    def getCurrentPosition(self, symbol):
        return self.current_positions[symbol]

