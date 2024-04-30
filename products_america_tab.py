import base64
import re
import requests
import time

from playwright.async_api import async_playwright
from PyQt6.QtWidgets import (
    QAbstractItemView,
    QHBoxLayout,
    QHeaderView,
    QPushButton,
    QTableWidget,
    QVBoxLayout,
    QWidget
)
from twocaptcha import TwoCaptcha

from globals import Globals

class ProductsAmericaTab(QWidget):
    def __init__(
            self,
            parent
        ):
        super().__init__(parent)

        self.columns = ['小組', '昵稱', 'TK', '狀態', 'RS', '區域', '推薦碼']
        self.headers = {'Token': ''}
        self.url_base = 'https://admin.reelshors.com'
        self.user = 'ProductsAmericaTab'
        self.setup_ui()

        Globals._Log.info(self.user, 'Successfully initialized.')

    async def get_token_with_playwright(self):
        Globals._WS.toggle_components_signal.emit(False)
        Globals._Log.info(self.user, 'Starting token acquisition process.')
        try:
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=False)
                page = await browser.new_page()
                await page.goto(self.url_base + '/login')

                captcha_selector = '.login-captcha img'
                await page.wait_for_selector(captcha_selector)

                captcha_element = await page.query_selector(captcha_selector)
                captcha_src = await captcha_element.get_attribute('src')
                captcha_src = self.url_base + captcha_src

                uuid_match = re.search(r'uuid=([\w-]+)', captcha_src)
                if uuid_match:
                    uuid = uuid_match.group(1)

                captcha_element = await page.query_selector(captcha_selector)
                captcha_image_data = await captcha_element.screenshot()
                captcha_image_base64 = base64.b64encode(captcha_image_data).decode('utf-8')

                solver = TwoCaptcha(Globals._To_CAPTCHA_KEY)
                captcha_result = solver.normal(captcha_image_base64, numeric=3, minLength=1, maxLength=1)

                captcha_code = captcha_result.get('code')

                login_data = {
                    "username": Globals._ADMIN_USER,
                    "password": Globals._ADMIN_PASSWORD,
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
                    Globals._Log.info(self.user, f'Token acquired successfully.')
                    self.headers['Token'] = token
                    Globals._WS.database_operation_signal.emit('upsert',{
                        'table_name': 'tokens',
                        'columns': ['name', 'token', 'expire'],
                        'values': ['america_token', token, int(time.time()+token_response.get('expire', 0))],
                        'unique_columns': ['name']
                    }, None)
                else:
                    Globals._Log.error(self.user, 'Failed to acquire token.')

                await browser.close()

        except Exception as e:
            Globals._Log.error(self.user, f'Error in token acquisition: {e}')
        finally:
            Globals._WS.toggle_components_signal.emit(True)

    async def update_products_worker(self):
        Globals._Log.info(self.user, 'Starting products update process.')
        Globals._WS.toggle_components_signal.emit(False)
        Globals._WS.toggle_progress_bar_signal.emit(True)
        Globals._WS.set_progress_bar_title_signal.emit('Updating Products')
        Globals._WS.update_progress_signal.emit('0/0', 0)
        page = 1
        retry_count = 3
        totalPage = 0
        totalCount = 0
        currentCount = 0
        while True:
            try:
                Globals._Log.info(self.user, f'Fetching data for page {page}/{totalPage}')
                res = requests.get(f'{self.url_base}/sqx_fast/course/selectCourse?page={page}&limit=500&isRecommend=-1&status=0', headers=self.headers)
                if res.status_code != 200:
                    Globals._Log.error(self.user, f'Failed to fetch data, status code: {res.status_code}')
                    continue
                if res.json().get('code', 401) == 401:
                    Globals._Log.error(self.user, f'Authentication failed, invalid token.')
                    await self.get_token_with_playwright()
                    continue
            except Exception as e:
                Globals._Log.error(self.user, f'Error fetching data for page {page}: {e}')
                retry_count -= 1
                if not retry_count:
                    Globals._Log.error(self.user, 'Max retry attempts reached, aborting update process.')
                    break
                time.sleep(3)
                continue
            datas = res.json().get('data', {})
            totalPage = datas.get('totalPage', 0)
            totalCount = datas.get('totalCount', 0)
            products = datas.get('list', [])
            currentCount += len(products)
            Globals._WS.update_progress_signal.emit(f'{currentCount}/{totalCount}', int((currentCount / totalCount) * 100))
            Globals._WS.database_operation_signal.emit('bulk_upsert',{
                'table_name': 'products_america',
                'columns': products[0].keys(),
                'data': products,
                'unique_columns': ['courseId'],
            }, None)
            Globals._Log.info(self.user, f'Page {page} data processed successfully.')

            page += 1
            if page > totalPage:
                Globals._Log.info(self.user, 'Products update process completed successfully.')
                break
        Globals._WS.toggle_progress_bar_signal.emit(False)
        Globals._WS.toggle_components_signal.emit(True)

    def cell_was_clicked(self):
        pass

    def cell_was_double_clicked(self):
        pass

    def setup_ui(self):
        layout = QVBoxLayout(self)

        top_layout = QHBoxLayout()
        layout.addLayout(top_layout)
        button_update = QPushButton('Update Products')
        Globals.components.append(button_update)
        button_update.clicked.connect(self.update_products)
        top_layout.addWidget(button_update)
        button_delete = QPushButton('Delete')
        # button_delete.clicked.connect(self.delete_selected_rows)
        top_layout.addWidget(button_delete)
        button_reload = QPushButton('Reload')
        # button_reload.clicked.connect(self.reload_rows)
        top_layout.addWidget(button_reload)
        top_layout.addStretch()
        button_save = QPushButton('Save')
        # button_save.clicked.connect(self.save_data_to_file)
        top_layout.addWidget(button_save)

        middle_layout = QHBoxLayout()
        layout.addLayout(middle_layout)
        self.table = QTableWidget(0, len(self.columns))
        middle_layout.addWidget(self.table)
        header_labels = self.columns
        self.table.setSortingEnabled(True)
        self.table.setHorizontalHeaderLabels(header_labels)
        self.table.verticalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Fixed)
        self.table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self.table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeMode.Fixed)
        self.table.setColumnWidth(0, 50)
        for column in range(0, self.table.columnCount()):
            self.table.horizontalHeader().setSectionResizeMode(column, QHeaderView.ResizeMode.ResizeToContents)
        self.table.horizontalHeader().setSectionResizeMode(self.table.columnCount(), QHeaderView.ResizeMode.Fixed)
        self.table.cellClicked.connect(self.cell_was_clicked)
        self.table.doubleClicked.connect(self.cell_was_double_clicked)

    def get_token_from_database(self):
        if not self.headers['Token']:
            res = Globals._SQL.read('tokens', condition='name="america_token"')
            if not res:
                Globals._Log.error(self.user, 'Token not found in the database.')
                return False
            if res[0][2] < time.time():
                Globals._Log.error(self.user, 'Token has expired.')
                return False
            self.headers['Token'] = res[0][1]
        return True

    def update_products(self):
        if not self.headers['Token']:
            Globals._Log.info(self.user, 'Get token from database.')
            self.get_token_from_database()
        Globals.run_async_task(self.update_products_worker)