The project comprises of 2 parts (the backend and the dash app)
1. To run the backend, run `python -m run.run` from `sfarb` directory.
2. To run the dash app, run `python index.py` from `sfarb/dashboard` directory.

Dependencies
1. Python 3.7
2. Cryptofeed
3. CCXT
4. Redis
5. Dash 1.9.0 +

Redis:
1. Change `R_CACHE` authentication and database values in `config/config.py`.