import dash_core_components as dcc
import dash_html_components as html
import dash_table
from dash.dependencies import Input, Output

import json
import pandas as pd
import time
import datetime as dt
import sys

from app import app

sys.path.append('..')
from run.run import redis_obj


# --------------------------------
# Page Environment
# --------------------------------
refresh_rate = 5000
columns = [
    'underlying', 'future-exchange', 'future-pair', 'future-bid', 'future-ask', 'future-lag (s)',
    'spot-exchange', 'spot-pair', 'spot-bid', 'spot-ask', 'spot-lag (s)',
    'spread-buy', 'spread-sell'
]
future_columns = [col for col in columns if col.startswith('future')]


# --------------------------------
# Layout
# --------------------------------
layout = html.Div(
    className='row',
    children=[
        html.Div(
            className='twelve columns',
            children=[
                dash_table.DataTable(
                    id='monitor',
                    merge_duplicate_headers=True,
                    style_table={'width': '100%'},
                    style_header={
                        'backgroundColor': 'rgb(50, 50, 50)'
                    },
                    style_cell={
                        'padding': '2px',
                        'margin': '0px',
                        'height': '13px',
                        'backgroundColor': 'rgb(70, 70, 70)',
                        'color': 'white',
                        'max-width': '100px',
                        'min-width': '80px'

                    },
                    style_data_conditional=[
                        {'if': {'filter_query': '{underlying} != -'},
                         'border-top': '15px solid rgb(240, 240, 240)'},
                        {'if': {'column_id': 'spread-buy', 'filter_query': '{spread-buy} > 0'},
                         'backgroundColor': 'green'},
                        {'if': {'column_id': 'spread-sell', 'filter_query': '{spread-sell} > 0'},
                         'backgroundColor': 'green'},
                    ]
                ),
                dcc.Interval(
                    id='refresh',
                    interval=refresh_rate
                )
            ]
        ),
    ]
)


# --------------------------------
# Callbacks
# --------------------------------
@app.callback(
    [Output('monitor', 'columns'),
     Output('monitor', 'data')],
    [Input('refresh', 'n_intervals')]
)
def fetch_redis(n_intervals):
    t0 = dt.datetime.now()

    pipe = redis_obj.pipeline()
    for key in redis_obj.keys():
        pipe.hgetall(key)

    all_hashes = pipe.execute()

    markets = {}
    for hash_data in all_hashes:
        if len(hash_data) == 1:
            continue
        markets.update(hash_data)
    markets = {key: json.loads(markets[key]) for key in markets}

    markets = pd.DataFrame(markets, index=['bid', 'ask', 'act_bid', 'act_ask', 'timestamp']).T.reset_index().rename(
        columns={'index': 'market'}
    )
    markets[['exchange', 'pair', 'type']] = markets['market'].str.split('_', expand=True)
    markets['underlying'] = markets['pair'].str.split('-').str[0]
    markets['lag (s)'] = pd.to_numeric((time.time_ns() - markets['timestamp']) / 10 ** 9).round(2)

    spreads = pd.DataFrame([])
    for underlying, group in markets.groupby('underlying', as_index=False):
        futures = group[group['type'] == 'future']
        if futures.empty:
            continue
        for row in futures.iterrows():
            future = row[1].to_dict()
            spot = group[group['type'] == 'spot'].copy()
            spot['future-pair'] = future['pair']
            spot['future-bid'] = future['bid']
            spot['future-ask'] = future['ask']
            spot['future-act_bid'] = future['act_bid']
            spot['future-act_ask'] = future['act_ask']
            spot['future-exchange'] = future['exchange']
            spot['future-lag (s)'] = future['lag (s)']
            spreads = pd.concat([spreads, spot], sort=False)

    spreads.rename(
        columns={
            'pair': 'spot-pair', 'bid': 'spot-bid', 'ask': 'spot-ask',
            'act_bid': 'spot-act_bid', 'act_ask': 'spot-act_ask',
            'exchange': 'spot-exchange', 'lag (s)': 'spot-lag (s)'
        },
        inplace=True
    )
    spreads.drop(['market', 'type'], axis=1, inplace=True)
    spreads['future-act_ask'] = pd.to_numeric(spreads['future-act_ask']).round(6)
    spreads['future-act_bid'] = pd.to_numeric(spreads['future-act_bid']).round(6)
    spreads['spot-act_ask'] = pd.to_numeric(spreads['spot-act_ask']).round(6)
    spreads['spot-act_bid'] = pd.to_numeric(spreads['spot-act_bid']).round(6)
    spreads['spread-buy'] = (100 * (spreads['future-act_ask'] - spreads['spot-act_bid']) / spreads['spot-act_bid']).round(2)
    spreads['spread-sell'] = (100 * (spreads['future-act_bid'] - spreads['spot-act_ask']) / spreads['spot-act_ask']).round(2)

    spreads = spreads[columns]
    spreads.loc[spreads['underlying'].duplicated(), 'underlying'] = '-'
    spreads.loc[spreads[future_columns].duplicated(), future_columns] = '-'

    spread_columns = [{'name': i.split('-'), 'id': i} for i in columns]

    print('{:.4f}'.format((dt.datetime.now() - t0).total_seconds()))
    return spread_columns, spreads.to_dict('rows')
