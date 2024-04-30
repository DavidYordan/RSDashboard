import asyncio
import pytz
import random
import requests
import string
import time

from ast import literal_eval
from datetime import datetime, timedelta
from faker import Faker
from pypinyin import pinyin, Style
from PyQt6.QtCore import(
    pyqtSlot,
    QDateTime,
    QRunnable,
    Qt
)
from PyQt6.QtGui import (
    QIntValidator
)
from PyQt6.QtWidgets import (
    QAbstractItemView,
    QApplication,
    QComboBox,
    QDateTimeEdit,
    QDialog,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QLineEdit,
    QMenu,
    QPushButton,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
    QWidget
)
from queue import Queue

from globals import Globals

class AutoCreatorTab(QWidget):
    def __init__(
            self,
            parent
        ):
        super().__init__(parent)

        self.columns = []
        q = Queue()
        Globals._WS.database_operation_signal.emit('get_table_fields', {'table_name': 'auto_creator'}, q)
        self.columns = q.get()

        self.autoCreatorWorker = AutoCreatorWorker()

        Globals._WS.autoCreatorTab_update_row_signal.connect(self.update_row)
        Globals._WS.autoCreatorTab_add_creator_signal.connect(self.add_creator)

        self.user = 'AutoCreatorTab'

        self.setup_ui()
        self.reload()

        Globals._Log.info(self.user, 'Successfully initialized.')

    @pyqtSlot(dict)
    def add_creator(self, data):
        AddCreatorDialog(self, data)

    def add_row(self, data, row=-1):
        if row == -1:
            row = self.table.rowCount()
            self.table.insertRow(row)
        for col_index, col_name in enumerate(self.columns):
            if col_name in data:
                item_value = str(data[col_name])
                if col_name == 'userId':
                    item_value = item_value.zfill(6)
            else:
                item_value = ''
            cell_item = QTableWidgetItem(item_value)
            self.table.setItem(row, col_index, cell_item)

    def cell_was_clicked(self):
        current_index = self.table.currentIndex()
        text = self.table.item(current_index.row(), current_index.column()).text()
        if self.columns[current_index.column()] == 'userId':
            text = text.lstrip('0')
        Globals._log_label.setText(text)

    def cell_was_double_clicked(self):
        pass

    def find_row_by_columnName(self, columnValue, columnName='userId'):
        column_index = self.columns.index(columnName)
        for row in range(self.table.rowCount()):
            item = self.table.item(row, column_index)
            if item and item.text() == columnValue:
                return row
        return -1

    def find_userId_by_phone(self, phone):
        phone_index = self.columns.index('phone')
        user_id_index = self.columns.index('userId')
        
        for row in range(self.table.rowCount()):
            phone_item = self.table.item(row, phone_index)
            if phone_item:
                current_phone = phone_item.text()
                if current_phone == phone:
                    user_id_item = self.table.item(row, user_id_index)
                    if user_id_item:
                        return user_id_item.text()
    
        return None

    def reload(self):
        self.table.setSortingEnabled(False)
        self.table.setRowCount(0)
        q = Queue()
        Globals._WS.database_operation_signal.emit('read', {'table_name': 'auto_creator'}, q)

        datas = q.get()
        for data in datas:
            row_data = {}
            for idx, col in enumerate(self.columns):
                row_data[col] = data[idx]
            self.add_row(row_data)

        self.table.setSortingEnabled(True)

        Globals._Log.info(self.user, 'Reload completed')

    def setup_ui(self):
        layout = QVBoxLayout(self)

        top_layout = QHBoxLayout()
        layout.addLayout(top_layout)
        button_addCreator = QPushButton('Add Creator')
        button_addCreator.clicked.connect(lambda: self.add_creator({}))
        top_layout.addWidget(button_addCreator)
        top_layout.addStretch()
        button_reload = QPushButton('Reload')
        button_reload.clicked.connect(self.reload)
        top_layout.addWidget(button_reload)

        middle_layout = QHBoxLayout()
        layout.addLayout(middle_layout)
        self.table = QTableWidget(0, len(self.columns))
        middle_layout.addWidget(self.table)
        self.table.setHorizontalHeaderLabels(self.columns)

        self.table.sortItems(self.columns.index('id'), Qt.SortOrder.AscendingOrder)
        self.table.verticalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Fixed)
        self.table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        # self.table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeMode.Fixed)
        # self.table.setColumnWidth(0, 5)

        self.table.mousePressEvent = self.table_mouse_press_event
        self.table.cellClicked.connect(self.cell_was_clicked)
        self.table.doubleClicked.connect(self.cell_was_double_clicked)

    def show_context_menu(self, pos, index):
        menu = QMenu()

        row = index.row()
        phone = self.table.item(row, self.columns.index('phone')).text()
        invitation_code = self.table.item(row, self.columns.index('invitationCode')).text()
        invitation_link = f'{Globals._BASE_URL_AMERICA}/pages/login/login?inviterType=0&invitation={invitation_code}'

        action_copy_tk_link = menu.addAction('Copy invitation Link')
        action_copy_tk_link.triggered.connect(lambda: QApplication.clipboard().setText(invitation_link))

        menu.exec(self.table.viewport().mapToGlobal(pos))

    def table_mouse_press_event(self, event):
        if event.button() == Qt.MouseButton.RightButton:
            index = self.table.indexAt(event.pos())
            if not index.isValid():
                return
            self.show_context_menu(event.pos(), index)
        else:
            QTableWidget.mousePressEvent(self.table, event)

    @pyqtSlot(dict)
    def update_row(self, data):
        row = self.find_row_by_columnName(str(data['userId']).zfill(6))
        self.table.setSortingEnabled(False)
        if row == -1:
            self.add_row(data)
        else:
            for key, value in data.items():
                if key == 'userId':
                    continue
                self.table.item(row, self.columns.index(key)).setText(str(value))
        self.table.setSortingEnabled(True)

class AddCreatorDialog(QDialog):
    def __init__(self, parent, data={}):
        super().__init__(parent)

        self.columns_creator = []
        self.columns_user = []
        self.emailGenerator = EmailGenerator()
        self.phoneGenerator = PhoneGenerator()
        self.queue = Queue()
        self.tz = pytz.timezone('Asia/Shanghai')
        self.user = 'AddCreatorDialog'

        self.setWindowTitle(f'Add Creator')
        self.setModal(True)
        self.resize(240, 180)
        self.setup_ui()
        self.lineedit_expected.textChanged.connect(self.expected_changed)
        self.fill_data(data)
        self.lineedit_team.textChanged.connect(self.validate_changed)
        self.lineedit_phone.textChanged.connect(self.validate_changed)
        self.combo_isAgent.currentTextChanged.connect(self.validate_changed)
        self.timeedit_startTime.dateTimeChanged.connect(self.validate_changed)
        self.timeedit_endTime.dateTimeChanged.connect(self.validate_changed)

        Globals._WS.database_operation_signal.emit('get_table_fields', {'table_name': 'auto_creator'}, self.queue)
        self.columns_creator = self.queue.get()
        
        Globals._WS.database_operation_signal.emit('get_table_fields', {'table_name': 'users_america'}, self.queue)
        self.columns_user = self.queue.get()

        self.exec()

    def expected_changed(self):
        try:
            expected = int(self.lineedit_expected.text())
            if expected % 2 != 0:
                return
            self.timeedit_startTime.setDateTime(QDateTime.currentDateTime())
            self.timeedit_endTime.setDateTime(QDateTime.currentDateTime().addSecs(60 + 60 * expected))
            phone_count = expected / 2
            remain_count = random.uniform(phone_count * 0.5, phone_count * 2.5)
            email_count = random.uniform(0.8, 0.9) * remain_count - random.choices([0, 1], [0.5, 0.5], k=1)[0]
            phone_count += remain_count - email_count
            self.lineedit_phone_count.setText(str(round(phone_count)))
            self.lineedit_email_count.setText(str(round(email_count)))
        except:
            return

    def fill_data(self, data):
        Globals._WS.database_operation_signal.emit('read', {
            'table_name': 'auto_creator',
            'columns': ['MAX(id)']
        }, self.queue)
        res = self.queue.get()
        
        if res[0][0] is None:
            self.label_id.setText(str(1))
        else:
            self.label_id.setText(str(res[0][0]+1))
        self.lineedit_team.setText(data.get('team', ''))
        self.label_userId.setText(str(data.get('userId', '')))
        self.lineedit_phone.setText(data.get('phone', ''))
        self.label_invitationCode.setText(data.get('invitationCode', ''))
        self.combo_isAgent.setCurrentText(str(data.get('isAgent', False)))
        self.lineedit_expected.setText(str(data.get('expected', 2)))

    def make_tasks(self):
        startTime = self.timeedit_startTime.dateTime().toSecsSinceEpoch()
        endTime = self.timeedit_endTime.dateTime().toSecsSinceEpoch()
        expected = int(self.lineedit_expected.text())
        phone_count = int(self.lineedit_phone_count.text())
        email_count = int(self.lineedit_email_count.text())
        tasks_count = expected // 2
        random_seconds = random.sample(range(startTime, endTime), phone_count + email_count)

        phones = [self.phoneGenerator.get_number() for _ in range(phone_count)]
        emails = [self.emailGenerator.get_email() for _ in range(email_count)]

        task_phones = phones[:tasks_count]
        remain_phones = phones[tasks_count:]

        random.shuffle(random_seconds)
        contacts = phones + emails
        tasks = []

        for sec, cont in zip(random_seconds, contacts):
            task_type = 'super_create' if cont in task_phones else 'create'
            formatted_time = datetime.fromtimestamp(sec, self.tz).strftime('%Y-%m-%d %H:%M:%S')
            tasks.append([task_type, formatted_time, cont])

        remainTasks = sorted(tasks, key=lambda x: x[1])

        return {
            'startTime': datetime.fromtimestamp(startTime, self.tz).strftime('%Y-%m-%d %H:%M:%S'),
            'endTime': datetime.fromtimestamp(endTime, self.tz).strftime('%Y-%m-%d %H:%M:%S'),
            'expected': expected,
            'remainTasks': str(remainTasks)
        }

    def random_call(self):
        selected_method = random.choices([self.emailGenerator.generate_email, self.get_number], [0.6, 0.4], k=1)[0]
        return selected_method()

    def setup_ui(self):
        layout = QVBoxLayout(self)

        layout_id = QHBoxLayout()
        layout_id.addWidget(QLabel('ID:'))
        self.label_id = QLabel(self)
        layout_id.addWidget(self.label_id)
        layout.addLayout(layout_id)

        layout_team = QHBoxLayout()
        layout_team.addWidget(QLabel('Team:'))
        self.lineedit_team = QLineEdit(self)
        layout_team.addWidget(self.lineedit_team)
        layout.addLayout(layout_team)

        layout_userId = QHBoxLayout()
        layout_userId.addWidget(QLabel('userId:'))
        self.label_userId = QLabel(self)
        layout_userId.addWidget(self.label_userId)
        layout.addLayout(layout_userId)

        layout_phone = QHBoxLayout()
        layout_phone.addWidget(QLabel('Phone:'))
        self.lineedit_phone = QLineEdit(self)
        layout_phone.addWidget(self.lineedit_phone)
        layout.addLayout(layout_phone)

        layout_invitationCode = QHBoxLayout()
        layout_invitationCode.addWidget(QLabel('InvitationCode:'))
        self.label_invitationCode = QLabel(self)
        layout_invitationCode.addWidget(self.label_invitationCode)
        layout.addLayout(layout_invitationCode)

        layout_isAgent = QHBoxLayout()
        layout_isAgent.addWidget(QLabel('Is Agent:'))
        self.combo_isAgent = QComboBox(self)
        self.combo_isAgent.addItems(['False', 'True'])
        layout_isAgent.addWidget(self.combo_isAgent)
        layout.addLayout(layout_isAgent)

        layout_startTime = QHBoxLayout()
        layout_startTime.addWidget(QLabel('Start Time:'))
        self.timeedit_startTime = QDateTimeEdit(self)
        self.timeedit_startTime.setDisplayFormat('yyyy-MM-dd HH:mm:ss')
        layout_startTime.addWidget(self.timeedit_startTime)
        layout.addLayout(layout_startTime)

        layout_endTime = QHBoxLayout()
        layout_endTime.addWidget(QLabel('End Time:'))
        self.timeedit_endTime = QDateTimeEdit(self)
        self.timeedit_endTime.setDisplayFormat('yyyy-MM-dd HH:mm:ss')
        layout_endTime.addWidget(self.timeedit_endTime)
        layout.addLayout(layout_endTime)

        layout_expected = QHBoxLayout()
        layout_expected.addWidget(QLabel('expected:'))
        self.lineedit_expected = QLineEdit(self)
        self.lineedit_expected.setValidator(QIntValidator(2, 500))
        layout_expected.addWidget(self.lineedit_expected)
        layout.addLayout(layout_expected)

        layout_count = QHBoxLayout()
        layout_count.addWidget(QLabel('phone:'))
        self.lineedit_phone_count = QLineEdit(self)
        self.lineedit_expected.setValidator(QIntValidator(1, 150))
        layout_count.addWidget(self.lineedit_phone_count)
        layout_count.addWidget(QLabel('email:'))
        self.lineedit_email_count = QLineEdit(self)
        self.lineedit_email_count.setValidator(QIntValidator(1, 150))
        layout_count.addWidget(self.lineedit_email_count)
        layout.addLayout(layout_count)

        layout_gear_5 = QHBoxLayout()
        layout_gear_5.setSpacing(5)
        for i in range(1, 6):
            btn = QPushButton(str(i), self)
            btn.setFixedSize(30, 20)
            btn.clicked.connect(self.set_random_value)
            layout_gear_5.addWidget(btn)
        layout.addLayout(layout_gear_5)

        layout_gear_10 = QHBoxLayout()
        layout_gear_10.setSpacing(5)
        for i in range(6, 11):
            btn = QPushButton(str(i), self)
            btn.setFixedSize(30, 20)
            btn.clicked.connect(self.set_random_value)
            layout_gear_10.addWidget(btn)
        layout.addLayout(layout_gear_10)

        layout_button = QHBoxLayout()
        self.submit_validate = QPushButton('Validate')
        layout_button.addWidget(self.submit_validate)
        self.submit_button = QPushButton('Create')
        self.submit_button.setEnabled(False)
        layout_button.addWidget(self.submit_button)
        layout.addLayout(layout_button)

        self.submit_validate.clicked.connect(self.validate)
        self.submit_button.clicked.connect(self.submit_data)

    def set_random_value(self):

        def range_for_bin(bin_id):
            base_start = 2
            base_end = 10
            increment = 10

            if bin_id == 1:
                start = base_start
                end = base_end
            else:
                start = range_for_bin(bin_id - 1)[1] + 2
                end = start + (bin_id - 1) * increment - 2

            return (start, end)

        gear = int(self.sender().text())
        start, end = range_for_bin(gear)
        start = start if start % 2 == 0 else start + 1
        end = end if end % 2 == 0 else end - 1
        self.lineedit_expected.setText(str(random.randint(start // 2, end // 2) * 2))

    def submit_data(self):
        data = self.make_tasks()
        isAgent = self.combo_isAgent.currentText()
        isAgent = {'True': True, 'False': False}.get(isAgent)

        data.update({
            'id': int(self.label_id.text()),
            'team': self.lineedit_team.text(),
            'userId': self.label_userId.text(),
            'phone': self.lineedit_phone.text(),
            'invitationCode': self.label_invitationCode.text(),
            'isAgent': isAgent
        })

        Globals._WS.database_operation_signal.emit('insert', {
            'table_name': 'auto_creator',
            'data': data
        }, None)

        self.parent().add_row(data)

        Globals._Log.info(self.user, f'set up auto powder successfully.')

        self.accept()

    def validate(self):
        self.submit_validate.setEnabled(False)
        try:
            phone = self.lineedit_phone.text()
            data = {}

            start_time = self.timeedit_startTime.dateTime().toPyDateTime()
            end_time = self.timeedit_endTime.dateTime().toPyDateTime()
            if start_time >= end_time:
                Globals._Log.error(self.user, f'Validation failed: Start time must be less than end time.')
                return
            
            expected = int(self.lineedit_expected.text())
            if expected < 2 or expected > 500 or expected % 2 != 0:
                Globals._Log.error(self.user, f'validate: Only allowed even expected in [2, 500]')
                return
            total_tasks = expected * 4
            time_period = (end_time - start_time).total_seconds()
            average_seconds_per_task = time_period / total_tasks
            if average_seconds_per_task < 10:
                Globals._Log.error(self.user, f'Validation failed: Each task must have at least 10 seconds, current average is {average_seconds_per_task} seconds.')
                return
            Globals._Log.warning(self.user, f'speed: {average_seconds_per_task}')
            
            phone = self.lineedit_phone.text()
            if not phone:
                Globals._Log.error(self.user, f'Validation failed: Phone is a required field.')
                return
            
            Globals._WS.database_operation_signal.emit('read', {
                'table_name': 'users_america',
                'condition': f'phone="{phone}"'
            }, self.queue)
            res = self.queue.get()

            if len(res) != 1:
                Globals._Log.error(self.user, f'Validation failed: The phone in the database error: {res}')
                return
            
            data = res[0]
            team = self.lineedit_team.text()
            data_team = data[self.columns_user.index('team')]
            team = self.lineedit_team.text()
            if team and data_team and team != data_team:
                Globals._Log.error(self.user, f'Validation failed: The team in the database is {data_team}.')
                return
            elif not team and data_team:
                self.lineedit_team.setText(data_team)

            Globals._WS.database_operation_signal.emit('read', {
                'table_name': 'auto_creator',
                'condition': f'phone="{phone}" AND isCompleted IS NOT TRUE'
            }, self.queue)
            res = self.queue.get()
            if res:
                Globals._Log.error(self.user, f'Validation failed: Uncompleted tasks detected: {res}')
                return
            
            self.submit_button.setEnabled(True)
            
        except Exception as e:
            Globals._Log.error(self.user, f'validate: {e}')

        finally:
            self.submit_validate.setEnabled(True)

    def validate_changed(self):
        self.submit_button.setEnabled(False)
        
class AutoCreatorWorker(QRunnable):
    def __init__(self):
        super().__init__()

        self.columns = []
        self.format_timestamp_str = '%Y-%m-%d %H:%M:%S'
        self.is_running = False
        self.setAutoDelete(False)
        self.tz = pytz.timezone('Asia/Shanghai')

        self.user = 'AutoCreatorWorker'

    async def add_task(self, data):
        tasks_str = data.get('remainTasks', '')
        if not tasks_str:
            Globals._Log.error(self.user, f'add_task: No task found: {data}')
            return
        
        try:
            tasks = literal_eval(tasks_str)
            if not isinstance(tasks, list):
                Globals._Log.error(self.user, f'add_task: Task format is incorrect: {data}')
                return
            now = time.time()
            weight = datetime.strptime(tasks[0][1], self.format_timestamp_str).timestamp()
            interval = now - weight
            if interval < -10:
                return
            if interval > 30:
                Globals._Log.warning(self.user, f'add_task: Delay task: {tasks[0]}')
                for task in tasks:
                    task_time = datetime.strptime(task[1], '%Y-%m-%d %H:%M:%S').replace(tzinfo=pytz.timezone('Asia/Shanghai'))
                    new_time = task_time + timedelta(seconds=interval)
                    task[1] = new_time.strftime('%Y-%m-%d %H:%M:%S')
            data['remainTasks'] = tasks
            await self.safe_put(weight, data)
        
        except Exception as e:
            Globals._Log.error(self.user, f'add_task: {e}')
            return
        
    async def async_run(self):
        self.is_running = True
        if not self.columns:
            print('initialize_columns')
            await self.initialize_columns()
        while self.is_running and Globals.is_app_running:
            if self.queue_tasks.empty():
                print('task empty, get tasks')
                await self.get_tasks()
            if self.queue_tasks.empty():
                print('task empty, wait 5')
                await asyncio.sleep(5)
                continue
            now = time.time()
            weight, task = await self.queue_tasks.get()
            interval = weight - now
            if interval > 0:
                print(f'wait {interval} to start: {task["remainTasks"][0]}')
                await asyncio.sleep(interval + 1)
            await self.processing(task)
        self.is_running = False

    async def consume_vip(self, phone, password, token):
        url = f'/sqx_fast/app/order/insertVipOrders'
        params = {'vipDetailsId': 1, 'time': int(time.time() * 1000)}
        userinfo = {'phone': phone, 'password': password, 'token': token}

        res = await Globals.request_with_user('get', url, userinfo, params=params)
        orderId = res.get('data', {}).get('ordersId')
        if orderId is None:
            Globals._Log.error(self.user, 'consume_vip: Failed to create order. No orderId returned.')
            return False

        if not await Globals.request_with_user(
            'post',
            '/sqx_fast/app/order/payOrders',
            userinfo,
            data=f'orderId={orderId}',
            headers={'Content-Type': 'application/x-www-form-urlencoded'}
        ):
            return False
        return True

    async def create_user_worker(self, phone, invitationCode):

        def _generate_password(length=8):
            characters = string.ascii_letters + string.digits
            password = ''.join(random.choice(characters) for _ in range(length))
            return password

        password = _generate_password()

        if isinstance(invitationCode, int):
            return
        else:
            url = f'/sqx_fast/app/Login/registerCode?password={password}&phone={phone}&msg=9999&inviterCode={invitationCode}&inviterType=0&inviterUrl=&platform=h5'
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}

        Globals._Log.info(self.user, f'Attempting to create user with phone: {phone}')

        async def register_user():
            res = requests.post(f'{Globals._BASE_URL_AMERICA}{url}', headers=headers)
            res.raise_for_status()
            data = res.json()
            if data.get('msg') != 'success':
                raise Exception(f'create_user_worker: error msg {data["msg"]}')
            user = data.get('user')
            user['team'] = 'admin'
            user['iscreator'] = True
            user['realpassword'] = password
            user['token'] = data.get('token')
            return user
    
        try:
            user = await Globals.retry(register_user)
        except Exception as e:
            Globals._Log.error(self.user, f'create_user_worker: {e}')
            return '', '', ''
        
        if not user:
            return '', '', ''

        Globals._WS.database_operation_signal.emit('insert', {
            'table_name': 'users_america',
            'data': user
        }, None)

        Globals._WS.users_america_update_row_signal.emit(user)

        Globals._Log.info(self.user, f'User {phone} was created successfully')

        return user['userId'], password, user['token']

    async def reset_tasks(self, data):
        Globals._WS.database_operation_signal.emit('upsert',{
            'table_name': 'auto_creator',
            'data': data,
            'unique_columns': ['id']
        }, None)
        Globals._WS.users_america_update_row_signal.emit(data)

    async def get_tasks(self):
        Globals._WS.database_operation_signal.emit('read', {
            'table_name': 'auto_creator',
            'condition': 'isCompleted IS NOT TRUE'
        }, self.queue_database)
        data_list = self.queue_database.get()

        for data in data_list:
            data_dict = {}
            for idx, col in enumerate(self.columns):
                data_dict[col] = data[idx]
            await self.add_task(data_dict)

    async def initialize_columns(self):
        Globals._WS.database_operation_signal.emit('get_table_fields', {'table_name': 'auto_creator'}, self.queue_database)
        self.columns = self.queue_database.get()
    
    async def make_consume_task(self, phone, password, token):
        delay = random.choices([random.randint(60, 300), random.randint(300, 900), random.randint(900, 1800)], [0.6, 0.3, 0.1], k=1)[0] + int(time.time())
        return ['consume', datetime.fromtimestamp(delay, self.tz).strftime('%Y-%m-%d %H:%M:%S'), phone, password, token]
    
    async def make_recharge_task(self, phone, userId, password, token):
        delay = int(time.time()) + random.randint(180, 900)
        return ['recharge', datetime.fromtimestamp(delay, self.tz).strftime('%Y-%m-%d %H:%M:%S'), phone, userId, password, token]
    
    async def recharge(self, userId):
        if not await Globals.request_with_admin('get', f'/sqx_fast/user/{userId}'):
            return False
        if not await Globals.request_with_admin('get', f'/sqx_fast/moneyDetails/selectUserMoney?userId={userId}'):
            return False
        if not await Globals.request_with_admin('post', f'/sqx_fast/user/addCannotMoney/{userId}/4'):
            return False
        if not await Globals.request_with_admin('get', f'/sqx_fast/user/{userId}'):
            return False
        if not await Globals.request_with_admin('get', f'/sqx_fast/moneyDetails/selectUserMoney?userId={userId}'):
            return False
        return True

    async def processing(self, data):
        tasks = data['remainTasks']
        method, _, phone = tasks[0][:3]
        if method == 'create':
            await self.create_user_worker(phone, data['invitationCode'])
        elif method == 'super_create':
            userId, password, token = await self.create_user_worker(phone, data['invitationCode'])
            if not userId:
                return
            tasks.append(await self.make_recharge_task(phone, userId, password, token))
        elif method == 'recharge':
            userId, password, token = tasks[0][3:6]
            if not await self.recharge(userId):
                Globals._Log.info(self.user, f'processing: {phone} recharge successfully')
                return
            tasks.append(await self.make_consume_task(phone, password, token))
        elif method == 'consume':
            password, token = tasks[0][3:5]
            if not await self.consume_vip(phone, password, token):
                Globals._Log.error(self.user, f'processing: {phone} consume failed')
                return
            Globals._Log.info(self.user, f'processing: {phone} consume successfully')
        else:
            Globals._Log.error(self.user, f'Invalid method: {method}')
            return
        
        del tasks[0]
        if not tasks:
            data.update({
                'remainTasks': '',
                'isCompleted': True,
                'updateTime': datetime.fromtimestamp(time.time(), self.tz).strftime('%Y-%m-%d %H:%M:%S')
            })
        else:
            remainTasks = sorted(tasks, key=lambda x: x[1])
            data.update({
                'remainTasks': str(remainTasks),
                'updateTime': datetime.fromtimestamp(time.time(), self.tz).strftime('%Y-%m-%d %H:%M:%S')
            })
        await self.reset_tasks(data)

    async def safe_put(self, weight, item):
        attempt = 100
        while attempt:
            try:
                await self.queue_tasks.put((weight, item))
                break
            except:
                weight += 0.001
                attempt -= 1

    def run(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        self.queue_database = Queue()
        self.queue_tasks = asyncio.PriorityQueue()

        loop.run_until_complete(self.async_run())
        loop.close()

    def start_task(self):
        if not self.is_running:
            Globals.thread_pool.start(self)

    def stop_task(self):
        self.is_running = False

class EmailGenerator(object):
    def __init__(self):
        self.fake = Faker('zh_TW')
        self.digits_options = [
            '',
            random.choice(string.digits),
            ''.join(random.choices(string.digits, k=2)),
            ''.join(random.choices(string.digits, k=3)),
            ''.join(random.choices(string.digits, k=4))
        ]
        self.digits_weights = [0.1, 0.2, 0.3, 0.3, 0.1]
        self.weights = {
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

    def get_pinyin(self, name):
        return ''.join([x[0] for x in pinyin(name, style=Style.NORMAL)])

    def split_name(self, name):
        return [self.get_pinyin(char) for char in name]

    def append_random_digits(self, base):
        return base + ''.join(random.choices(self.digits_options, self.digits_weights, k=1))

    def generate_username(self):
        first_name = self.fake.first_name()
        last_name = self.fake.last_name()
        fn_pinyin = self.split_name(first_name)
        ln_pinyin = self.split_name(last_name)

        strategies = [
            lambda fn, ln: self.append_random_digits(ln[0] + ''.join(fn)),
            lambda fn, ln: self.append_random_digits(''.join(ln) + ''.join(f[0] for f in fn)),
            lambda fn, ln: self.append_random_digits(''.join(fn) + ''.join(ln)),
            lambda fn, ln: self.append_random_digits(random.choice(fn) + ''.join(ln)),
            lambda fn, ln: self.append_random_digits(''.join(ln) + random.choice([f[0] for f in fn])),
            lambda fn, ln: self.append_random_digits(''.join(random.sample(fn, len(fn))) + ''.join(ln)),
            lambda fn, ln: self.append_random_digits(''.join(ln) + ''.join(random.choices(string.ascii_lowercase, k=3))),
            lambda fn, ln: self.append_random_digits(''.join(ln) + ''.join(random.choices(string.ascii_lowercase + string.digits, k=random.randint(1, 3))) + random.choice(fn)),
            lambda fn, ln: self.append_random_digits(''.join(ln) + random.choice(fn) + ''.join(random.choices(string.ascii_lowercase + string.digits, k=random.randint(1, 3))))
        ]

        username = ''
        while len(username) < 6:
            choice = random.choices(strategies, weights=[0.1] * 9, k=1)[0]
            username += choice(fn_pinyin, ln_pinyin).lower()

        min_length = 8
        if len(username) < min_length:
            username += ''.join(random.choices(string.digits, k=min_length - len(username)))

        if not username[0].isalpha():
            username = random.choice(string.ascii_lowercase) + username[1:]

        return username

    def get_email(self):
        username = self.generate_username()
        domain = random.choices(list(self.weights.keys()), weights=list(self.weights.values()), k=1)[0]
        return f"{username}{domain}"
    
class PhoneGenerator(object):
    def __init__(self):
        self.length = 10
        self.prefix = [
            "0939", "0958", "0980", "0916", "0930", "0988", "0987", "0975", "0926", "0920", 
            "0972", "0911", "0917", "0936", "0989", "0931", "0937", "0981", "0983", "0905", 
            "0903", "0909", "0910", "0912", "0913", "0914", "0915", "0918", "0919", "0921", 
            "0922", "0923", "0925", "0927", "0928", "0929", "0932", "0933", "0934", "0935", 
            "0938", "0940", "0941", "0943", "0945", "0948", "0952", "0953", "0955", "0956", 
            "0957", "0960", "0961", "0963", "0965", "0966", "0968", "0970", "0971", "0973", 
            "0974", "0976", "0977", "0978", "0979", "0982", "0984", "0985", "0986", "0990", 
            "0991", "0992", "0993", "0995", "0996", "0998"
        ]

    def get_number(self):
        prefix = random.choice(self.prefix)
        remaining_length = self.length - len(prefix)
        remaining_digits = ''.join(random.choices('0123456789', k=remaining_length))
        return prefix + remaining_digits