#!/usr/bin/env python3
import pandas as pd
import numpy as np
import calendar


def get_trades(df, stop_quantile=0.8, limit_quantile=0.8):
    day = {'buy': 0, 'sell': 4}
    trade_df = pd.DataFrame(columns=['day', 'direction', 'stop', 'limit'])

    for direction in ['buy', 'sell']:
        day_df = df[df.time.dt.weekday == day[direction]]
        trade = {'day': calendar.day_name[day[direction]],
                 'direction': direction,
                 'stop': day_df[direction + '_' + 'stop']
                 .apply(np.abs).quantile(stop_quantile),
                 'limit': day_df[direction + '_' + 'limit']
                 .apply(np.abs).quantile(1 - limit_quantile)
                 }
        trade_df = trade_df.append(trade, ignore_index=True)

    trade_df['risk_reward_ratio'] = trade_df.limit / trade_df.stop
    trade_df['expected_value'] = (limit_quantile * trade_df.limit -
                                  (1 - stop_quantile) * trade_df.stop)

    return trade_df


def backtest_trade(df_prices, direction='buy', stop=20,
                   limit=40, column_name='trade'):
    day = {'buy': 0, 'sell': 4}

    # define results in each scenario
    def backtest(df):
        if np.abs(df[direction + '_stop']) > stop:
            return -stop
        elif np.abs(df[direction + '_limit']) > limit:
            return limit
        else:
            return df[direction + '_profit']

    df_prices[column_name] = df_prices.apply(backtest, axis=1)
    res = df_prices[df_prices.time.dt.weekday == day[direction]][column_name]
    print(f'Summary of profit for {direction} (stop: {stop}, limit: {limit}):')
    print(f"{res.agg(['count', 'sum', 'mean', 'median', 'std'])}\n")
