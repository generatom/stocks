#!/usr/bin/env python3
import requests
import json
import datetime as dt
import numpy as np
import pandas as pd
import os.path as op
import urllib.parse


class APIHandler():
    def __init__(self, api_url, api_key, user_name, password, verbosity=0):
        self.debug_level = verbosity
        self._api_url = api_url
        self._api_key = api_key
        self._user_name = user_name
        self._default_headers = self._set_default_headers()
        self._login_response = self.login(password)
        del password
        self.date_format = '%Y-%m-%d'
        self.time_format = '%H:%M:%S'
        self.dt_format = self.date_format + 'T' + self.time_format

    def __str__(self):
        msg = 'APIHandler('
        msg += str(self._login_response.status_code) + ', '
        msg += self._access_token + ')'
        return msg

    def login(self, password):
        data = {
                'identifier': self._user_name,
                'password': password
            }
        headers = self._default_headers.copy()
        headers['VERSION'] = '3'
        url = self._set_url('/session')
        response = requests.post(url, headers=headers,
                                 data=json.dumps(data))

        while response.status_code != 200:
            if self.debug_level:
                print(response.status_code, response.reason)
            response = requests.post(url, headers=headers,
                                     data=json.dumps(data))

        self._login_data = response.json()
        self._tokens = self._login_data.get('oauthToken')
        self._access_token = self._tokens.get('access_token')
        self._auth_headers = self._set_auth_header()
        self._headers = self._set_headers()
        return response

    def _set_url(self, endpoint):
        url = self._api_url + urllib.parse.quote(endpoint)
        return url

    def _set_default_headers(self):
        headers = {'X-IG-API-KEY': self._api_key,
                   'Content-Type': 'application/json',
                   'Accept': 'application/json',
                   'VERSION': '1'
                   }
        return headers

    def _set_auth_header(self):
        return {'Authorization': self._tokens.get('token_type') + " " +
                self._access_token,
                'IG-ACCOUNT-ID': self._login_data.get('accountId')}

    def _set_headers(self):
        return {**self._default_headers, **self._auth_headers}

    def _get(self, url, request_headers=None):
        if self.debug_level > 0:
            print('request: ' + url)

        if not request_headers:
            request_headers = self._headers

        i = 1
        responses = []

        # Get data, check it's valid and join it to full response
        while url:
            response_page = requests.get(url, headers=request_headers)
            if self.debug_level > 0:
                print('page: {}'.format(i))
                print('response: ' + str(response_page.status_code))
            if self.debug_level > 1:
                print('response: ' + response_page.text + '\n')

            if response_page.status_code != 200 and self.debug_level == 0:
                msg = {'error': response_page.text,
                       'code': response_page.status_code}
                print(msg)
                return [msg]

            response_data = response_page.json()
            responses.append(response_data)
            i += 1

            if response_data.get('metadata'):
                endpt = response_data.get('metadata').get('paging').get('next')
                url = self._set_url(endpt) if endpt else None
            else:
                url = None

        return responses

    def _add_param(self, url, param, value):
        joiner = '&' if '?' in url else '?'
        return url + joiner + param + '=' + value

    def accounts(self):
        url = self._set_url('/accounts')
        return self._get(url)

    def transactions(self):
        pass

    def prices(self, epic, resolution='HOUR', from_date=None, to_date=None):
        valid_resolutions = ['SECOND', 'MINUTE', 'MINUTE_2', 'MINUTE_3',
                             'MINUTE_5', 'MINUTE_10', 'MINUTE_15',
                             'MINUTE_30', 'HOUR', 'HOUR_2', 'HOUR_3',
                             'HOUR_4', 'DAY', 'WEEK', 'MONTH']

        if resolution not in valid_resolutions:
            if self.debug_level:
                msg = 'Resolution of {} not valid. '.format(resolution)
                msg += 'Changed to default of HOUR'
                print(msg)
            resolution = 'HOUR'
        if not from_date:
            from_date = (dt.datetime.now() - dt.timedelta(days=1)
                         ).strftime(self.dt_format)
        if not to_date:
            to_date = dt.datetime.now().strftime(self.dt_format)

        url = self._set_url('/'.join(['/prices', epic, resolution,
                                      from_date, to_date]))
        print(url)
        headers = self._headers.copy()
        headers['VERSION'] = '2'
        pages = self._get(url, request_headers=headers)

        if self.debug_level:
            print(pages)

        # handle API errors
        if pages[0].get('code'):
            return pages[0]

        # organise all results into a dataframe
        prices = pd.DataFrame()
        for page in pages:
            prices = prices.append(page['prices'])

        # expand price columns
        price_columns = [column for column in prices.columns if
                         'Price' in column]

        for column in price_columns:
            new_columns = prices[column].apply(pd.Series)
            new_columns.columns = [column.replace('Price', '') +
                                   '_' + c for c in new_columns.columns]
            prices = prices.join(new_columns).drop(columns=column, axis=1)

        prices = (prices.rename(columns={'snapshotTime': 'time',
                                'lastTradedVolume': 'volume'})
                  .dropna(axis=1))
        prices.time = pd.to_datetime(prices.time)

        return prices

    def positions(self):
        url = self._set_url('/positions')
        headers = self._headers.copy()
        headers['VERSION'] = '2'
        pages = self._get(url, request_headers=headers)
        market_info = ['bid', 'epic', 'instrumentName', 'offer']
        position_info = ['dealId', 'direction', 'level', 'limitLevel', 'size',
                         'stopLevel']
        df_pos = pd.DataFrame()

        for page in pages:
            for frame in page['positions']:
                df = pd.DataFrame.from_dict(frame).T
                df = df.loc['position'].combine_first(df.loc['market'])
                df = df.to_frame().T.reset_index(drop=True)
                df = df[market_info + position_info]
                df = df.rename(columns={'size': 'contractSize'})
                df_pos = df_pos.append(df)
                df = None

        if not df_pos.empty:
            df_pos.contractSize = np.where(df_pos.direction == 'BUY',
                                           df_pos.contractSize,
                                           df_pos.contractSize * -1)
            df_pos['close_level'] = np.where(df_pos.direction == 'BUY',
                                             df_pos.bid, df_pos.offer)
            df_pos['profit'] = ((df_pos.close_level - df_pos.level) *
                                df_pos.contractSize)
            df_pos['stop_distance'] = ((df_pos.stopLevel - df_pos.close_level)
                                       * np.sign(df_pos.contractSize))
            df_pos['limit_distance'] = ((df_pos.limitLevel -
                                         df_pos.close_level) *
                                        np.sign(df_pos.contractSize))

        return df_pos

    def orders(self):
        url = self._set_url('/workingorders')
        headers = self._headers.copy()
        headers['VERSION'] = '2'
        pages = self._get(url, request_headers=headers)
        df_ord = pd.DataFrame()

        for page in pages:
            for frame in page['workingOrders']:
                df = pd.DataFrame(frame).T
                df = (df.loc['workingOrderData']
                        .combine_first(df.loc['marketData']))

                df_ord = df_ord.append(df)

        df_ord = df_ord.reset_index(drop=True)
        if not df_ord.empty:
            df_ord['order_dist'] = (np.where(df_ord.direction == 'BUY',
                                    df_ord.offer, df_ord.bid) -
                                    df_ord.orderLevel)

        return df_ord

    def activity(self, from_date=None, to_date=None, page_size=50):
        url = self._set_url('/history/activity')

        if not from_date:
            from_date = (dt.datetime.now() - dt.timedelta(days=7)
                         ).strftime(self.date_format)
        if not to_date:
            to_date = dt.datetime.now().strftime(self.date_format)

        url = self._add_param(url, 'from', from_date)
        url = self._add_param(url, 'to', to_date)
        url = self._add_param(url, 'pageSize', str(page_size))
        headers = self._headers.copy()
        headers['VERSION'] = '3'
        pages = self._get(url, request_headers=headers)
        records = []
        for page in pages:
            records.extend(page['activities'])
        df = pd.DataFrame.from_records(records)
        df.date = pd.to_datetime(df.date)
        return(df)

    def markets(self, node=None):
        url = self._set_url('/marketnavigation')
        if node is not None:
            url += '/' + str(node)

        response = self._get(url)
        # print(response)
        df = pd.DataFrame(response[0])
        df = pd.concat([pd.DataFrame(list(df[col].values)) for col in
                        df.columns], axis=1).dropna(axis='columns')
        return df

    def print_positions(self):
        cols = ['instrumentName', 'contractSize', 'level', 'close_level',
                'profit', 'stop_distance', 'limit_distance', 'limitLevel']
        df = self.positions()
        print(df[cols] if not df.empty else 'None')


def get_args():
    from argparse import ArgumentParser

    parser = ArgumentParser()
    parser.add_argument('--positions', action='store_true', help='Show' +
                        ' positions', default=False)
    parser.add_argument('--orders', action='store_true', help='Show orders',
                        default=False)
    parser.add_argument('--activity', action='store_true', help='Show' +
                        ' activity', default=False)
    parser.add_argument('--markets', action='store_true', help='Show markets',
                        default=False)
    parser.add_argument('--demo', dest='account', action='store_const',
                        const='demo', help='Use demo account', default='prod')
    parser.add_argument('-v', action='count', dest='verbosity', default=0)

    return parser.parse_args()


def get_pass(file):
    with open(file, 'r') as f:
        return f.read().strip()


def get_api(api_type='demo', base_dir=None):
    if not base_dir:
        base_dir = '/home/jono/projects/stocks'

    api = {'api_key': get_pass(op.join(base_dir, api_type + '_api_key')),
           'user_name': get_pass(op.join(base_dir, api_type + '_api_usr')),
           'password': get_pass(op.join(base_dir, api_type + '_api_pass'))
           }

    if api_type == 'demo':
        api['url'] = 'https://demo-api.ig.com/gateway/deal'
    else:
        api['url'] = 'https://api.ig.com/gateway/deal'
    return api


if __name__ == '__main__':

    args = get_args()
    api_deets = get_api(args.account)

    api = APIHandler(api_deets['url'], api_deets['api_key'],
                     api_deets['user_name'], api_deets['password'],
                     verbosity=args.verbosity)
    # api.print_market_nodes(api.market_details(361365))
    # pprint(api.market_details('IX.D.SUNFUN.DAILY.IP'))
    if args.positions:
        api.print_positions()
    if args.orders:
        cols = ['instrumentName', 'direction', 'orderSize', 'orderLevel',
                'bid', 'offer', 'order_dist']
        df = api.orders()
        print(df[cols] if not df.empty else 'None')
    if args.activity:
        print(api.activity(from_date='2020-06-01').epic.unique())
    if args.markets:
        print(api.markets(361365))
        # pprint(api.market_details(361365))
