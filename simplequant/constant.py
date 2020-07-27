from enum import Enum


PRICE_FIELDS = {
    'open', 'close', 'high', 'low', 'limit_up', 'limit_down', 'acc_net_value', 'unit_net_value'
}

FIELDS_REQUIRE_ADJUSTMENT = set(list(PRICE_FIELDS) + ['volume'])


class EventType(Enum):
    MARKET = 'MarketEvent事件'
    SIGNAL = 'SignalEvent事件'
    ORDER = 'OrderEvent事件'
    FILL = 'FillEvent事件'


class OrderType(Enum):
    MARKET = '市价'
    LIMIT = '限价'


class Direction(Enum):
    LONG = '多'
    SHORT = '空'
    NET = '净'


class OrderTime(Enum):
    OPEN = '开盘价成交'
    CLOSE = '收盘价成交'

