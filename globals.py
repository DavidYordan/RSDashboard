import asyncio
import base64
import re
import requests
import time
from playwright.async_api import async_playwright
from PyQt6.QtCore import (
    pyqtSignal,
    pyqtSlot,
    QMutex,
    QObject,
    QRunnable,
    Qt,
    QThreadPool
)
from PyQt6.QtWidgets import (
    QDialog,
    QLabel,
    QTextEdit,
    QVBoxLayout
)
from queue import Queue
from twocaptcha import TwoCaptcha

from dylogging import Logging

class WorkerSignals(QObject):
    autoCreatorTab_add_creator_signal = pyqtSignal(dict)
    autoCreatorTab_update_row_signal = pyqtSignal(dict)
    database_operation_signal = pyqtSignal(str, dict, object)
    progress_hide_signal = pyqtSignal()
    progress_set_title_signal = pyqtSignal(str)
    progress_show_signal = pyqtSignal()
    progress_update_signal = pyqtSignal(str, str)
    telegram_bot_signal = pyqtSignal(str, dict)
    users_america_update_row_signal = pyqtSignal(dict)

class ProgressDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle('Progress')
        self.setModal(True)

        layout = QVBoxLayout(self)
        self.label_message = QLabel('Waiting...')
        layout.addWidget(self.label_message)
        self.label_progress = QLabel('')
        layout.addWidget(self.label_progress)
        self.setWindowFlags(self.windowFlags() & ~Qt.WindowType.WindowCloseButtonHint)

        Globals._WS.progress_hide_signal.connect(self.hide)
        Globals._WS.progress_set_title_signal.connect(self.set_title)
        Globals._WS.progress_show_signal.connect(self.show)
        Globals._WS.progress_update_signal.connect(self.update_progress)

    @pyqtSlot()
    def hide(self):
        super().hide()

    @pyqtSlot(str)
    def set_title(self, title):
        self.setWindowTitle(title)

    @pyqtSlot()
    def show(self):
        if not self.isVisible():
            super().show()

    @pyqtSlot(str, str)
    def update_progress(self, message, progress=''):
        self.label_message.setText(message)
        self.label_progress.setText(progress)

class Worker(QRunnable):
    def __init__(self, func, *args, **kwargs):
        super().__init__()
        self.func = func
        self.args = args
        self.kwargs = kwargs

    def run(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(self.func(*self.args, **self.kwargs))
        loop.close()

class Globals(QObject):
    _ADMIN_USER_AMERICA = ''
    _ADMIN_PASSWORD_AMERICA = ''
    _BASE_URL_AMERICA = ''
    _Bot = None
    _CLIENT_ID = ''
    _CLIENT_UUID = ''
    _log_textedit = QTextEdit()
    _log_label = QLabel()
    _Log = Logging(_log_textedit, _log_label)
    _MUTEX_TOKEN = QMutex()
    _ProgressDialog = None
    _SQL = None
    _TELEGRAM_BOT_TOKEN = ''
    _TELEGRAM_CHATID = ''
    _TO_CAPTCHA_KEY = ''
    _WS = WorkerSignals()
    accounts_dict = {}
    binging_dict = {}
    exclusive_components = []
    is_app_running = True
    requests_thread_pool = QThreadPool()
    session_admin_america = requests.Session()
    thread_pool = QThreadPool.globalInstance()
    token_refreshing = False

    @classmethod
    async def _get_token_with_playwright(cls):
        if not cls._MUTEX_TOKEN.tryLock():
            cls._MUTEX_TOKEN.lock()
            cls._MUTEX_TOKEN.unlock()
            return
        
        if cls.token_refreshing:
            return
        cls.token_refreshing = True

        url_base = cls._BASE_URL_AMERICA
        cls._Log.info('Globals', 'Starting token acquisition process.')

        try:
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=False)
                page = await browser.new_page()
                await page.goto(f'{url_base}/login')

                captcha_selector = '.login-captcha img'
                await page.wait_for_selector(captcha_selector)

                captcha_element = await page.query_selector(captcha_selector)
                captcha_src = await captcha_element.get_attribute('src')
                captcha_src = f'{url_base}{captcha_src}'

                uuid_match = re.search(r'uuid=([\w-]+)', captcha_src)
                uuid = uuid_match.group(1) if uuid_match else None

                captcha_element = await page.query_selector(captcha_selector)
                captcha_image_data = await captcha_element.screenshot()
                captcha_image_base64 = base64.b64encode(captcha_image_data).decode('utf-8')

                solver = TwoCaptcha(cls._TO_CAPTCHA_KEY)
                captcha_result = solver.normal(captcha_image_base64, numeric=3, minLength=1, maxLength=1)

                captcha_code = captcha_result.get('code')

                login_data = {
                    "username": cls._ADMIN_USER_AMERICA,
                    "password": cls._ADMIN_PASSWORD_AMERICA,
                    "uuid": uuid,
                    "captcha": captcha_code
                }

                token_response = await page.evaluate("""loginData => {
                    return fetch('/sqx_fast/sys/login', {
                        method: 'POST',
                        headers: {'Content-Type': 'application/json'},
                        body: JSON.stringify(loginData)
                    }).then(response => response.json());
                }""", login_data)

                token = token_response.get('token', '')

                if token:
                    q = Queue()
                    cls._Log.info('Globals', f'Token acquired successfully.')
                    cls.session_admin_america.headers.update({'Token': token})
                    token_name = 'america_token'
                    cls._WS.database_operation_signal.emit('upsert',{
                        'table_name': 'tokens',
                        'columns': ['name', 'token', 'expire'],
                        'values': [token_name, token, int(time.time()+token_response.get('expire', 0))],
                        'unique_columns': ['name']
                    }, q)
                    q.get()
                    cls._Log.info('Globals', f'successed to acquire token for admin.')
                else:
                    cls._Log.error('Globals', f'Failed to acquire token for admin.')

                await browser.close()

        except Exception as e:
            cls._Log.error('Globals', f'Error in token acquisition: {e}')

        finally:
            cls.token_refreshing = False
            cls._MUTEX_TOKEN.unlock()
    
    @classmethod
    async def _get_token_from_database(cls):
        token_name = 'america_token'

        def __get_token():
            q = Queue()
            cls._WS.database_operation_signal.emit('read', {
                'table_name': 'tokens',
                'condition': f'name="{token_name}" AND expire>{int(time.time() + 60)}'
            }, q)
            return q.get()
        
        res = await asyncio.get_event_loop().run_in_executor(None, __get_token)
        if not res:
            cls._Log.warning('Globals', f'Token not found in the database for america.')
            return
        cls.session_admin_america.headers.update({'Token': res[0][1]})

    @classmethod
    async def get_user_token(cls, phone, password):
        async def fetch_token():
            res = requests.post(f'{cls._BASE_URL_AMERICA}/sqx_fast/app/Login/registerCode?password={password}&phone={phone}')
            res.raise_for_status()
            data = res.json()
            if data.get('msg') != 'success':
                raise Exception(f'get_token: error msg{data["msg"]}')
            return data['user']['token']

        try:
            token = await cls.retry(fetch_token)
            user = {'userId': phone, 'token': token}
            cls._WS.database_operation_signal.emit('upsert', {
                'table_name': 'users_america',
                'data': user,
                'unique_columns': ['userId']
            })
            return token
        except Exception as e:
            cls._Log.error('Globals', f'get_token: {e}')
            return None

    @classmethod
    async def _make_admin_request(cls, method, url, **kwargs):
        headers = {'Token': cls.session_admin_america.headers.get('Token', '')}
        kwargs['headers'] = headers
        while True:
            request_args = (method, f'{cls._BASE_URL_AMERICA}{url}')
            request_kwargs = kwargs

            response = await asyncio.get_event_loop().run_in_executor(
                None, lambda: requests.request(*request_args, **request_kwargs)
            )
            if response.status_code != 200:
                cls._Log.error('Globals', f'Failed to fetch data, status code: {response.status_code}')
                raise Exception(f'Status code {response.status_code}')
            response_data = response.json()
            if response_data.get('code', 401) == 401:
                cls._Log.warning('Globals', 'Authentication failed, invalid token.')
                await cls._get_token_with_playwright()
                headers = {'Token': cls.session_admin_america.headers.get('Token', '')}
                continue
            return response_data

    @classmethod
    async def _make_user_request(cls, method, url, headers, **kwargs):
        kwargs['headers'] = headers
        request_args = (method, f'{cls._BASE_URL_AMERICA}{url}')
        request_kwargs = kwargs

        response = await asyncio.get_event_loop().run_in_executor(
            None, lambda: requests.request(*request_args, **request_kwargs)
        )
        response.raise_for_status()
        response_data = response.json()
        if response_data.get('code', 401) == 401:
            raise Exception('Authentication failed, invalid admin token.')
        return response_data

    @classmethod
    async def request_with_admin(cls, method, url, **kwargs):
        if not cls.session_admin_america.headers.get('Token'):
            await cls._get_token_from_database()

        if not cls.session_admin_america.headers.get('Token'):
            await cls._get_token_with_playwright()

        try:
            return await cls.retry(lambda: cls._make_admin_request(method, url, **kwargs), max_attempts=3, delay=3)
        except Exception as e:
            cls._Log.error('Globals', f'Error in request_with_admin: {e}')
            return {}
        
    @classmethod
    async def request_with_user(cls, method, url, userinfo, **kwargs):
        headers = {}
        if 'headers' in kwargs:
            headers = kwargs['headers']
            del kwargs['headers']
        headers['Token'] = userinfo.get('token', '')
        try:
            response_data = await cls.retry(lambda: cls._make_user_request(method, url, headers, **kwargs))
            return response_data
        except Exception as e:
            if 'Authentication failed, invalid user token.' in str(e):
                new_token = await cls.get_user_token(userinfo['phone'], userinfo['password'])
                if new_token:
                    headers['Token'] = new_token
                    return await cls.retry(lambda: cls._make_user_request(method, url, headers, **kwargs))
                else:
                    cls._Log.error('Globals', f'Failed to refresh user token: {e}')
            else:
                cls._Log.error('Globals', f'Error in request_with_user: {e}')
        return {}
    
    @classmethod
    async def retry(cls, operation, max_attempts=3, delay=3, exceptions=(Exception,)):
        attempts = 0
        while attempts < max_attempts:
            try:
                return await operation()
            except exceptions as e:
                cls._Log.error('Globals', f'Attempt {attempts + 1}: {str(e)}')
                if attempts < max_attempts - 1:
                    await asyncio.sleep(delay)
                    attempts += 1
                else:
                    error_message = f'Maximum retry attempts ({max_attempts}) reached, unable to complete the operation.'
                    cls._Log.error('Globals', error_message)
                    raise Exception(error_message) from e
    
    @classmethod
    def run_async_task(cls, func, *args, **kwargs):
        cls.thread_pool.start(Worker(func, *args, **kwargs))