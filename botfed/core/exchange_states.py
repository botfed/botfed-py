from abc import ABC, abstractmethod, abstractproperty


class ExchangeState:

    @abstractproperty
    def name(self):
        pass

    @abstractproperty
    def has_orderbook(self):
        pass
