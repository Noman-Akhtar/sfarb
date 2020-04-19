import ccxt
import json
import time
import os
import pandas as pd

from decimal import Decimal
from cryptofeed.exchanges import Binance
from cryptofeed import FeedHandler


class Handler:

    def __init__(self, redis_obj):
        self.redis_obj = redis_obj
        self.exchange_obj_cx = ccxt.binance()
        self.exchange_obj_cf = Binance
        self.feed_handler = FeedHandler()
        self.fee = Decimal('0.1') / Decimal('100')
        self.fee_on_bid = Decimal('1') - self.fee
        self.fee_on_ask = Decimal('1') + self.fee
        self.futures = self.get_futures()
        self.spot = self.get_spot()
        self.symbols = self.futures + self.spot

    @staticmethod
    def get_futures():
        return []

    def get_spot(self):
        spot = pd.DataFrame(self.exchange_obj_cx.fetch_markets())
        spot = spot[(spot['quote'].isin(['USDT', 'USDC'])) & (spot['active'])]
        spot = list(spot['symbol'])
        spot = [symbol.replace('/', '-') for symbol in spot]
        return spot

    def on_update(self, pair, bid, bid_size, ask, ask_size, bid_exchange, ask_exchange):
        print('{} -> Exchange: {}, Pair: {}'.format(os.getpid(), ask_exchange, pair))
        symbol_type = 'future' if pair in self.futures else 'spot'
        self.redis_obj.hset(
            pair.split('-')[0],
            '{}_{}_{}'.format(bid_exchange, pair, symbol_type),
            json.dumps([
                str(bid),
                str(ask),
                str(bid * self.fee_on_bid),
                str(ask * self.fee_on_ask),
                time.time_ns()
            ])
        )

    def prepare_feed(self, symbols):
        self.feed_handler.add_nbbo(
            [self.exchange_obj_cf],
            symbols,
            self.on_update
        )

    def start_feed(self):
        self.feed_handler.run()
