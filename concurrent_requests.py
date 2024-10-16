import base64
import random
import re
import requests
import string
import time

from faker import Faker
from playwright.sync_api import sync_playwright
from pypinyin import pinyin, Style
from PyQt6.QtCore import (
    QRunnable,
    QThreadPool,
    QMutex,
    QWaitCondition
)
from queue import Empty, Queue
from twocaptcha import TwoCaptcha
from urllib.parse import quote

from globals import Globals
from prefix import prefix_en_US, prefix_zh_TW

class AdminRequests(object):
    class TokenManager(object):
        def __init__(self, session):
            self.is_refreshing = False
            self.refresh_condition = QWaitCondition()
            self.session = session
            self.mutex = QMutex()
            self.user = 'TokenManager'
            self._get_token_from_database()

        def _get_token_from_database(self):
            q = Queue()
            Globals._WS.database_operation_signal.emit('read', {
                'table_name': 'tokens',
                'condition': f'name="america_token" AND expire>{int(time.time() + 300)}'
            }, q)
            res = q.get()
            
            if not res:
                Globals._Log.warning(self.user, f'Token not found in the database for america.')
                self.session.headers.update({'Token': ''})
                return

            self.session.headers.update({'Token': res[0][1]})

        def ensure_token(self):
            self.mutex.lock()
            try:
                while self.is_refreshing:
                    self.refresh_condition.wait(self.mutex)
            finally:
                self.mutex.unlock()
                
        def refresh_token_with_playwright(self):
            self.mutex.lock()
            try:
                if self.is_refreshing:
                    self.refresh_condition.wait(self.mutex)
                    return
                
                self.is_refreshing = True
            finally:
                self.mutex.unlock()
            
            try:
                Globals._Log.info(self.user, 'Starting token acquisition process.')
                with sync_playwright() as p:
                    browser = p.chromium.launch(headless=False)
                    try:
                        page = browser.new_page()
                        page.goto(f'{Globals._BASE_URL_AMERICA}/login')

                        captcha_selector = '.login-captcha img'
                        page.wait_for_selector(captcha_selector)

                        captcha_element = page.query_selector(captcha_selector)
                        captcha_src = captcha_element.get_attribute('src')
                        captcha_src = f'{Globals._BASE_URL_AMERICA}{captcha_src}'

                        uuid_match = re.search(r'uuid=([\w-]+)', captcha_src)
                        uuid = uuid_match.group(1) if uuid_match else None

                        captcha_image_data = captcha_element.screenshot()
                        captcha_image_base64 = base64.b64encode(captcha_image_data).decode('utf-8')

                        solver = TwoCaptcha(Globals._TO_CAPTCHA_KEY)
                        captcha_result = solver.normal(captcha_image_base64, numeric=3, minLen=1, maxLen=1)

                        captcha_code = captcha_result.get('code')
                        print(f'captcha_code: {captcha_code}')

                        login_data = {
                            "username": Globals._ADMIN_USER_AMERICA,
                            "password": Globals._ADMIN_PASSWORD_AMERICA,
                            "uuid": uuid,
                            "captcha": captcha_code
                        }

                        token_response = page.evaluate("""loginData => {
                            return fetch('/sqx_fast/sys/login', {
                                method: 'POST',
                                headers: {'Content-Type': 'application/json'},
                                body: JSON.stringify(loginData)
                            }).then(response => response.json());
                        }""", login_data)

                        token = token_response.get('token', '')
                        print(token_response)

                        if token:
                            Globals._Log.info(self.user, f'Token acquired successfully.')
                            self.session.headers.update({'Token': token})
                            token_name = 'america_token'
                            Globals._WS.database_operation_signal.emit('upsert',{
                                'table_name': 'tokens',
                                'columns': ['name', 'token', 'expire'],
                                'values': [token_name, token, int(time.time()+token_response.get('expire', 0))],
                                'unique_columns': ['name']
                            }, None)
                            Globals._Log.info(self.user, f'successed to acquire token for admin.')
                        else:
                            Globals._Log.error(self.user, f'Failed to acquire token for admin.')
                    finally:
                        browser.close()

            except Exception as e:
                Globals._Log.error(self.user, f'Error in token acquisition: {e}')

            finally:
                self.mutex.lock()
                self.is_refreshing = False
                self.refresh_condition.wakeAll()
                self.mutex.unlock()

    class Worker(QRunnable):
        def __init__(self, queue):
            super().__init__()
            self.is_running = False
            self.task_queue = queue

        def run(self):
            self.is_running = True
            while self.is_running and Globals.is_app_running:
                try:
                    task = self.task_queue.get(timeout=3)
                    func, args, kwargs, result_queue = task
                    result = func(*args, **kwargs)
                    result_queue.put(result)
                except Empty:
                    continue
                except Exception as e:
                    Globals._Log.error('Worker error', str(e))
                    result_queue.put({})

    def __init__(self, queue_max = 20):
        self.base_url = Globals._BASE_URL_AMERICA
        self.session = requests.Session()
        self.task_queue = Queue()
        self.thread_pool = QThreadPool()
        self.thread_pool.setMaxThreadCount(queue_max)
        self.token_manager = self.TokenManager(self.session)

        for _ in range(self.thread_pool.maxThreadCount()):
            self.thread_pool.start(self.Worker(self.task_queue))

        self.user = 'AdminRequests'
        Globals._Log.info(self.user, 'Successfully initialized.')

    def _make_request(self, method, url, **kwargs):
        while True:
            try:
                self.token_manager.ensure_token()
                response = self.session.request(method, url, **kwargs)
                if response.status_code != 200:
                    raise Exception(f'Status code {response.status_code}')
                response_data = response.json()
                if response_data.get('code') == 401:
                    Globals._Log.warning(self.user, 'Authentication failed, invalid token.')
                    self.token_manager.refresh_token_with_playwright()
                    continue
                if response_data.get('code') != 0:
                    raise Exception(f'Request failed: {response_data}')
                return response.json()
            except:
                raise
        
    def _retry(self, func, max_attempts=3, delay=3):
        for attempt in range(max_attempts):
            try:
                return func()
            except Exception as e:
                Globals._Log.error('Retry error', f'Attempt {attempt + 1}: {str(e)}')
                time.sleep(delay * (2 ** attempt))
                if attempt == max_attempts - 1:
                    raise

    def request(self, method, url, **kwargs):
        try:
            func = lambda: self._make_request(method, f'{self.base_url}{url}', **kwargs)
            result_queue = Queue()
            self.task_queue.put((self._retry, (func,), {}, result_queue))
            result = result_queue.get()
            return result
        except Exception as e:
            Globals._Log.error(self.user, f'{e}')

class UserRequests(object):
    def __init__(self, userId):
        self.session = requests.Session()
        self.user = f'UserRequests{userId}'
        self.userId = userId
        self._get_token_from_database()
        Globals._Log.info(self.user, 'Successfully initialized.')

    def _get_token_by_phone(self):
        def _fetch_token():
            Globals._Log.info(self.user, f'Attempting to fetch token by phone for {self.phone}...')
            token_info = self._make_request(
                'POST',
                f'{Globals._BASE_URL_AMERICA}/sqx_fast/app/Login/registerCode',
                params={'password': self.password, 'phone': self.phone}
            )
            if token_info['user']['token']:
                Globals._Log.info(self.user, f'Token fetched successfully by phone for {self.phone}.')
            return token_info['user']['token']

        self.token = self._retry(_fetch_token)
        user = {'userId': self.userId, 'token': self.token}
        Globals._WS.database_operation_signal.emit('upsert', {
            'table_name': 'users_america',
            'data': user,
            'unique_columns': ['userId']
        }, None)
        # Globals._WS.users_america_update_row_signal.emit(user)
        self.session.headers.update({'Token': self.token})

    def _get_token_from_database(self):
        q = Queue()
        Globals._WS.database_operation_signal.emit('read', {
            'table_name': 'users_america',
            'columns': ['phone', 'realpassword', 'token'],
            'condition': f'userId="{self.userId}"'
        }, q)
        res = q.get()

        if not res:
            raise ValueError(f'No data found for userId: {self.userId}')
        
        self.phone, self.password, self.token = res[0]

        if self.token:
            self.session.headers.update({'Token': self.token})
        else:
            self._get_token_by_phone()

    def _make_request(self, method, url, **kwargs):
        while True:
            try:
                response = self.session.request(method, url, **kwargs)
                if response.status_code != 200:
                    raise Exception(f'Status code {response.status_code}')
                response_data = response.json()
                if response_data.get('code', 401) == 401:
                    Globals._Log.warning(self.user, 'Authentication failed, invalid token.')
                    self._get_token_by_phone()
                    continue
                if response_data.get('msg') != 'success':
                    raise Exception(f'Error message: {response_data["msg"]}')
                return response.json()
            except:
                raise
        
    @staticmethod
    def _retry(func, max_attempts=3, delay=3):
        for attempt in range(max_attempts):
            try:
                return func()
            except Exception as e:
                Globals._Log.error('Retry error', f'Attempt {attempt + 1}: {str(e)}')
                time.sleep(delay * (2 ** attempt))
                if attempt == max_attempts - 1:
                    raise

    @staticmethod
    def create_user(invitationCode='', style='phone_Taiwan'):

        def _register_user():
            if '|' in invitationCode:
                code, courseId, detailsId = invitationCode.split('|')
                params = {
                    'password': password,
                    'phone': phone,
                    'msg': '151515',
                    'inviterCode': code,
                    'inviterType': '1',
                    'inviterUrl': quote(f'/me/detail/detail?id={courseId}&courseDetailsId={detailsId}'),
                    'platform': 'h5'
                }
            else:
                params = {
                    'password': password,
                    'phone': phone,
                    'msg': '151515',
                    'inviterCode': invitationCode,
                    'inviterType': '0',
                    'inviterUrl': '',
                    'platform': 'h5'
                }
            res = requests.post(
                f'{Globals._BASE_URL_AMERICA}/sqx_fast/app/Login/registerCode',
                headers={'Content-Type': 'application/x-www-form-urlencoded'},
                params=params
            )
            res.raise_for_status()
            data = res.json()
            if data.get('msg') != 'success':
                raise Exception(f'create_user: error msg {data["msg"]}')
            user = data.get('user')
            user['team'] = 'admin'
            user['realpassword'] = password
            user['token'] = data.get('token')
            return user
        
        def _regoin_maps(regoin):
            return {
                'America': 'en_US',
                'Taiwan': 'zh_TW'
            }[regoin]
        
        if 'phone' in style:
            phone = PhoneGenerator.generate_phone(_regoin_maps(style[6:]))
        elif 'email' in style:
            phone = EmailGenerator.generate_email(_regoin_maps(style[6:]))
        else:
            raise ValueError(f'Unsupported style: {style}')

        password = PasswordGenerator.generate_password()

        Globals._Log.info('create_user', f'尝试创建用户: {phone}')
        user = UserRequests._retry(_register_user)
        
        if not user:
            raise Exception("Failed to create user, no user returned from the registration function")

        Globals._WS.database_operation_signal.emit('insert', {
            'table_name': 'users_america',
            'data': user
        }, None)

        # Globals._WS.users_america_update_row_signal.emit(user)
        Globals._Log.info('create_user', f'成功创建用户: {phone}')

        return user['userId']
    
    def consume_vip(self):
        def _get_orderId():
            return self._make_request(
                'GET',
                f'{Globals._BASE_URL_AMERICA}/sqx_fast/app/order/insertVipOrders',
                params={'vipDetailsId': 1, 'time': int(time.time() * 1000)}
            )

        def _process_payment(orderId):
            self._make_request(
                'POST',
                f'{Globals._BASE_URL_AMERICA}/sqx_fast/app/order/payOrders',
                data={'orderId': orderId},
                headers={'Content-Type': 'application/x-www-form-urlencoded'}
            )

        try:
            orderId = self._retry(_get_orderId)['data']['ordersId']
            Globals._Log.info(self.user, f'{self.phone} 生成订单: {orderId}.')
            time.sleep(1)
            self._retry(lambda: _process_payment(orderId))
            Globals._Log.info(self.user, f'{self.phone} 成功消费.')
            return True
        except Exception as e:
            Globals._Log.error('consume_vip', f'Failed to consume VIP for {self.phone}: {str(e)}')
            return False
    
class EmailGenerator(object):
    digits_weights = [0.1, 0.2, 0.3, 0.3, 0.1]
    weights_zh_TW = {
        '@gmail.com': 0.45,
        '@yahoo.com.tw': 0.2,
        '@seed.net.tw': 0.05,
        '@hotmail.com': 0.05,
        '@outlook.com': 0.05,
        '@msn.com': 0.01,
        '@aol.com': 0.01,
        '@ask.com': 0.01,
        '@live.com': 0.02,
        '@livemail.tw': 0.02,
        '@hotmail.com.tw': 0.01,
        '@ms33.url.com.tw': 0.01,
        '@topmarkeplg.com.tw': 0.01,
        '@pchome.com.tw': 0.1,
        '@hinet.net': 0.15,
        '@so-net.net.tw': 0.02,
        '@tpts5.seed.net.tw': 0.01,
        '@umail.hinet.net': 0.05,
        '@mail2000.com.tw': 0.03,
        '@ebizprise.com': 0.01
    }
    weights_en_US = {
        '@gmail.com': 0.35,
        '@yahoo.com': 0.2,
        '@outlook.com': 0.15,
        '@hotmail.com': 0.1,
        '@aol.com': 0.05,
        '@icloud.com': 0.05,
        '@mail.com': 0.03,
        '@protonmail.com': 0.03,
        '@zoho.com': 0.02,
        '@gmx.com': 0.02,
        '@yandex.com': 0.02,
        '@inbox.com': 0.01,
        '@mailfence.com': 0.01,
        '@usa.com': 0.01
    }

    @staticmethod
    def _get_pinyin(name):
        return ''.join([x[0] for x in pinyin(name, style=Style.NORMAL)])

    @staticmethod
    def _split_name(name, region):
        if region == 'zh_TW':
            return [EmailGenerator._get_pinyin(char) for char in name]
        return list(name)

    @staticmethod
    def _append_random_digits(base):
        digits_options = [
            '',
            ''.join(random.choice(string.digits)),
            ''.join(random.choices(string.digits, k=2)),
            ''.join(random.choices(string.digits, k=3)),
            ''.join(random.choices(string.digits, k=4))
        ]
        return base + ''.join(random.choices(digits_options, EmailGenerator.digits_weights, k=1))

    @staticmethod
    def _generate_username(first_name, last_name, region):
        fn_processed = EmailGenerator._split_name(first_name, region)
        ln_processed = EmailGenerator._split_name(last_name, region)

        strategies = [
            lambda fn, ln: EmailGenerator._append_random_digits(ln[0] + ''.join(fn)),
            lambda fn, ln: EmailGenerator._append_random_digits(''.join(ln) + ''.join(f[0] for f in fn)),
            lambda fn, ln: EmailGenerator._append_random_digits(''.join(fn) + ''.join(ln)),
            lambda fn, ln: EmailGenerator._append_random_digits(random.choice(fn) + ''.join(ln)),
            lambda fn, ln: EmailGenerator._append_random_digits(''.join(ln) + random.choice([f[0] for f in fn])),
            lambda fn, ln: EmailGenerator._append_random_digits(''.join(random.sample(fn, len(fn))) + ''.join(ln)),
            lambda fn, ln: EmailGenerator._append_random_digits(''.join(ln) + ''.join(random.choices(string.ascii_lowercase, k=3))),
            lambda fn, ln: EmailGenerator._append_random_digits(''.join(ln) + ''.join(random.choices(string.ascii_lowercase + string.digits, k=random.randint(1, 3))) + random.choice(fn)),
            lambda fn, ln: EmailGenerator._append_random_digits(''.join(ln) + random.choice(fn) + ''.join(random.choices(string.ascii_lowercase + string.digits, k=random.randint(1, 3))))
        ]

        username = ''
        while len(username) < 6:
            choice = random.choices(strategies, weights=[0.1] * 9, k=1)[0]
            username += choice(fn_processed, ln_processed).lower()

        min_length = 8
        if len(username) < min_length:
            diff = min_length - len(username)
            username += ''.join(random.choices(string.digits, k=int(random.uniform(diff, diff + 2))))

        if not username[0].isalpha():
            username = random.choice(string.ascii_lowercase) + username[1:]

        return username

    @staticmethod
    def generate_email(region='zh_TW'):
        if region == 'en_US':
            fake = Faker('en_US')
            weights = EmailGenerator.weights_en_US
        else:
            fake = Faker('zh_TW')
            weights = EmailGenerator.weights_zh_TW
            
        first_name = fake.first_name()
        last_name = fake.last_name()
        username = EmailGenerator._generate_username(first_name, last_name, region)
        domain = random.choices(list(weights.keys()), weights=list(weights.values()), k=1)[0]
        return f'{username}{domain}'
    
class PasswordGenerator(object):
    characters = string.ascii_letters + string.digits

    @staticmethod
    def generate_password(length=8):
        password = ''.join(random.choice(PasswordGenerator.characters) for _ in range(length))
        return password
    
class PhoneGenerator(object):
    length_zh_TW = 10
    length_en_US = 10

    @staticmethod
    def generate_phone(regoin):
        if regoin == 'zh_TW':
            prefix = random.choice(prefix_zh_TW)
            remaining_length = PhoneGenerator.length_zh_TW - len(prefix)
        else:
            prefix = random.choice(prefix_en_US)
            remaining_length = PhoneGenerator.length_en_US - len(prefix)
        remaining_digits = ''.join(random.choices('0123456789', k=remaining_length))
        return prefix + remaining_digits