import redis
import time
from multiprocessing import Process

from config import config
from exchanges import ftx_handler, binance_handler

redis_obj = redis.Redis(
    host=config.R_CACHE['host'],
    port=config.R_CACHE['port'],
    password=config.R_CACHE['password'],
    db=config.R_CACHE['db'],
    decode_responses=True
)

DERIVATIVE_EXCHANGES = {
    'ftx': ftx_handler.Handler(redis_obj),
}

SPOT_EXCHANGES = {
    'binance': binance_handler.Handler(redis_obj)
}

if __name__ == '__main__':

    redis_keys = redis_obj.keys()
    for key in redis_keys:
        redis_obj.delete(key)

    derivative_bases = []
    processes = {}

    # Derivatives
    for exchange in DERIVATIVE_EXCHANGES:
        exchange_obj = DERIVATIVE_EXCHANGES[exchange]
        exchange_symbols = exchange_obj.symbols
        derivative_bases.extend([symbol.split('-')[0] for symbol in exchange_symbols])
        exchange_feed = exchange_obj.prepare_feed(exchange_symbols)
        processes[exchange] = Process(target=exchange_obj.start_feed, args=())

    # Spot
    for exchange in SPOT_EXCHANGES:
        exchange_obj = SPOT_EXCHANGES[exchange]
        exchange_symbols = exchange_obj.symbols
        exchange_symbols = [symbol for symbol in exchange_symbols if symbol.split('-')[0] in derivative_bases]
        exchange_feed = exchange_obj.prepare_feed(exchange_symbols)
        processes[exchange] = Process(target=exchange_obj.start_feed, args=())

    time.sleep(4)

    # Start Feed
    for process in processes:
        print('Starting Process: {}'.format(process))
        processes[process].start()

    # Join Processes
    # for process in processes:
    #     print('Ending Process: {}'.format(process))
    #     processes[process].join()
