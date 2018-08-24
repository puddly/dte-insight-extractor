import math
import time
import logging

import requests

from datetime import datetime, timezone, timedelta

logging.basicConfig(format='[%(asctime)s] %(levelname)8s [%(funcName)s:%(lineno)d] %(message)s')
logger = logging.getLogger(__name__)

logger.setLevel(logging.INFO)


class DTEInsightExtractor:
    def __init__(self, username, password):
        self.username = username
        self.password = password

        self.session = requests.Session()

        self._authorization_token = None
        self._cached_info = None

    def api(self, method, url, authenticated=False, delay=2, **kwargs):
        logger.debug('Sending a %s request to https://dtei-coreapi.pwly.io/v2%s (auth: %s) with args %r', method, url, authenticated, kwargs)

        if authenticated and self._authorization_token is None:
            raise ValueError('Authorization token is None! You must log in first.')

        headers = kwargs.pop('headers', {})

        if authenticated:
            headers['authorization'] = self._authorization_token

        time.sleep(delay)

        response = self.session.request(method, 'https://dtei-coreapi.pwly.io/v2' + url, headers=headers, **kwargs)

        if response.status_code == 502:
            logger.warning('API returned a 502. Increasing delay...')

            return self.api(method, url, authenticated=authenticated, delay=60 + delay, **kwargs)
        else:
            response.raise_for_status()

        return response

    def login(self):
        logger.info('Logging in...')
        response = self.api('post', '/login/17', json={
            'Username': self.username,
            'Password': self.password
        })

        self._authorization_token = response.headers['Authorization']

        logger.info('Loading customer info...')
        info = self.api('post', '/lookup/17', json={
            'Username': self.username,
            'Password': self.password
        }).json()

        self._cached_info = info

        logger.info('Logged in as %s %s (%d)', info['FirstName'], info['LastName'], info['CustomerID'])

        return info.copy()

    def _find_site_start_date(self, site_id):
        logger.info('Binary searching for first reading date at site %d', site_id)

        left = datetime(2000, 1, 1, tzinfo=timezone.utc)
        right = datetime.now(timezone.utc)

        while abs(left - right) > timedelta(days=1):
            midpoint = left + timedelta(seconds=math.ceil((right - left).total_seconds() / 2))
            midpoint = midpoint.replace(hour=0, minute=0, second=0, microsecond=0)

            data = list(self.download_site_data_at(site_id, midpoint))

            logger.info('Found %d readings starting at %s (left: %s, right: %s)', len(data), midpoint.date(), left.date(), right.date())

            if not data:
                left = midpoint
            else:
                right = midpoint

        logger.info('First reading date at site %d is %s', right)

        return right

    def download_site_data_at(self, site_id, start_date, count=1440):
        try:
            response = self.api('get', f'/usage/{self._cached_info["CustomerID"]}/{site_id}', params={
                'deviceType': '2',
                'reportType': '2',
                'startTime': int(start_date.timestamp()),
                'count': count
            }, authenticated=True)
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                return []

            raise e

        for row in response.json():
            yield datetime.fromtimestamp(int(row['d']), timezone.utc), int(1000 * row['u'])

    def download_site_data(self, site_id):
        start_date = self._find_site_start_date(site_id)

        while True:
            data = list(self.download_site_data_at(site_id, start_date))
            logger.info('Loaded %d readings for site %d at %s', len(data), site_id, start_date)

            yield from data

            if not data:
                break

            start_date = data[-1][0] + timedelta(seconds=1)
                

    def download_all_data(self):
        for site in self._cached_info['CustomerSites']:
            yield site, list(self.download_site_data(site['CustomerSiteID']))



if __name__ == '__main__':
    import json
    import getpass

    dumper = DTEInsightExtractor(input('Enter your username: '), getpass.getpass('Enter your account password: '))
    info = dumper.login()

    print(json.dumps({
        'account_info': info,
        'sites': [{
            'info': site,
            'readings': [(t.isoformat(), r) for t, r in readings]
        } for site, readings in dumper.download_all_data()]
    }))