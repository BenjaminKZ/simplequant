from abc import ABCMeta
from simplequant.constant import EventType, OrderTime


class Event(object):
    """
    Event作为基类，为其他Event类（子类）提供接口。
    """

    __metaclass__ = ABCMeta

    pass


class MarketEvent(Event):
    """
    接收市场价格信息的更新。
    """
    def __init__(self, datetime, symbol_data):
        """
        初始化MarketEvent.
        """
        self.type = EventType.MARKET
        self.priority = 2
        self.datetime = datetime
        self.symbol_data = symbol_data

    def __repr__(self):
        return '<MarketEvent> Datetime={}'.format(self.datetime)

    def __eq__(self, other):
        return True


class SignalEvent(Event):
    """
    通过Strategy对象发出信号事件。Portfolio对象接收该事件，并基于此做出决策。
    """
    def __init__(self, datetime, symbol, direction, quantity, order_time=OrderTime.OPEN):
        """
        :param strategy_id: 唯一标示发出信号的Strategy对象
        :param symbol: 股票代码标识，如'AAPL'
        :param datetime: 信号产生的时间戳
        :param signal_type: Direction.LONG或Direction.SHORT
        :param strength: 调仓的权重系数，在投资组合中建议买入或卖出的数量.
        """

        self.type = EventType.SIGNAL
        self.priority = 2
        self.datetime = datetime
        self.symbol = symbol
        self.direction = direction
        self.quantity = quantity
        self.order_time = order_time

    def __repr__(self):
        return '<SignalEvent> Datetime={}, Symbol={}, Direction={}, Quantity={}, OrderTime={}'.format(
            self.datetime, self.symbol, self.direction, self.quantity, self.order_time)

    def __eq__(self, other):
        '''
        主要是为了应对优先级队列的使用。queue.PriorityQueue首先比较元组第一个元素的大小，然后比较第二个，
        然后一直比较到最后一个，只有元组内所有元素都相等，才会按照先进先出的规则弹出，否则优先弹出小的，
        所以所有的Event都应该重载比较有运算符，重载==在这里最为直接和简便，这样一来，当事件的优先级相同时，
        会按照先进先出的规则弹出队列元素。
        :param other:
        :return:
        '''
        return True


class OrderEvent(Event):
    """
    向交易系统发送OrderEvent。Order包含标识(e.g. 'AAPL')，类型(market or limit)，
    数量和方向。
    """
    def __init__(self, datetime, symbol, direction, quantity, order_time=OrderTime.OPEN):
        """
        初始化order类型，确定是Market order('MKT')还是Limit order('LMT')，还包含数量和
        买卖的方向('BUY' or 'SELL')。
        :param symbol: 交易的对象
        :param order_type: OrderType.MARKET or OrderType.LIMIT for Market or Limit.
        :param quantity: 非负整数表示的数量
        :param direction: Direction.LONG or Direction.SHORT for long or short.
        """
        self.type = EventType.ORDER
        self.priority = 1
        self.datetime = datetime
        self.symbol = symbol
        self.direction = direction
        self.quantity = quantity
        self.order_time = order_time

    def __repr__(self):
        """
        Outputs the values within the order.
        :return: a string that print on screen.
        """
        return '<OrderEvent> Datetime={}, Symbol={}, Direction={}, Quantity={}, OrderTime={}'.format(
            self.datetime, self.symbol, self.direction, self.quantity, self.order_time)

    def __eq__(self, other):
        return True


class FillEvent(Event):
    """
    Encapsulates the notion of a Filled Order, as returned from a brokerage.
    Stores the quantity of an instrument actually filled and at what price. In
    addition, stores the commission of the trade from the brokerage.
    """
    def __init__(self, datetime, symbol, direction, fill_cost, quantity, commission, order_time):
        """
        Initializes the FillEvent object. Sets the symbol, exchange, quantity,
        direction, cost of fill and an optional commission.
        If commission is not provided, the Fill object will calculate it based
        on the trade size and Interactive Brokers fees.
        :param timeindex: The bar-resolution when the order was filled.
        :param symbol: The instrument which was filled.
        :param exchange: The exchange where the order was filled.
        :param quantity: The filled quantity.
        :param direction: The direction of fill ('BUY' or 'SELL').
        :param fill_cost: The holdings value in dollars.
        :param commission: an optional commission sent from IB.
        """

        self.type = EventType.FILL
        self.priority = 1
        self.datetime = datetime
        self.symbol = symbol
        self.direction = direction
        self.fill_cost = fill_cost
        self.quantity = quantity
        self.commission = commission
        self.order_time = order_time

    def __repr__(self):
        return '<FillEvent> Datetime={}, Symbol={}, Direction={}, FillCost={:.2}, Quantity={}, Commission={:.2}, OrderTime={}'.format(
            self.datetime, self.symbol, self.direction, self.fill_cost, self.quantity, self.commission, self.order_time)

    def __eq__(self, other):
        return True

