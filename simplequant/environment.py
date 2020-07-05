from abc import ABCMeta
from simplequant.data.database import Database


class Env:

    __metaclass__ = ABCMeta

    _database = Database()

    pass

