import itertools
import pytz
import random
import time

from ast import literal_eval
from datetime import datetime, timedelta
from PyQt6.QtCore import(
    pyqtSlot,
    QDateTime,
    QRunnable,
    Qt,
    QTime
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
    QTableWidgetItem,
    QVBoxLayout,
    QWidget
)
from queue import Queue, PriorityQueue

from concurrent_requests import UserRequests
from globals import Globals, TableWidget

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

        Globals._WS.autoCreatorTab_add_creator_signal.connect(self.add_creator)
        Globals._WS.autoCreatorTab_update_row_signal.connect(self.update_row)

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
            else:
                item_value = ''
            cell_item = QTableWidgetItem(item_value)
            self.table.setItem(row, col_index, cell_item)

    def cell_was_clicked(self):
        current_index = self.table.currentIndex()
        text = self.table.item(current_index.row(), current_index.column()).text()
        Globals._log_label.setText(text)

    def cell_was_double_clicked(self):
        pass

    def find_row_by_columnName(self, columnValue, columnName='id'):
        column_index = self.columns.index(columnName)
        for row in range(self.table.rowCount()):
            item = self.table.item(row, column_index)
            if item and item.text() == str(columnValue):
                return row
        return -1

    def reload(self):
        self.table.setRowCount(0)
        q = Queue()
        Globals._WS.database_operation_signal.emit('read', {'table_name': 'auto_creator'}, q)

        datas = q.get()
        for data in datas:
            row_data = {}
            for idx, col in enumerate(self.columns):
                row_data[col] = data[idx]
            self.add_row(row_data)

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
        self.table = TableWidget(0, len(self.columns))
        self.table.setNumericColumns([self.columns.index(column) for column in ['id', 'userId', 'isAgent', 'expected', 'isCompleted']])
        middle_layout.addWidget(self.table)
        self.table.setHorizontalHeaderLabels(self.columns)

        self.table.verticalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Fixed)
        self.table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)

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
            TableWidget.mousePressEvent(self.table, event)

    @pyqtSlot(dict)
    def update_row(self, data):
        row = self.find_row_by_columnName(data['id'])
        if row == -1:
            self.add_row(data)
        else:
            for key, value in data.items():
                if key == 'id':
                    continue
                self.table.item(row, self.columns.index(key)).setText(str(value))

class AddCreatorDialog(QDialog):
    def __init__(self, parent, data={}):
        super().__init__(parent)

        self.columns_creator = []
        self.columns_user = []
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
            current_datetime = QDateTime.currentDateTime()
            self.timeedit_startTime.setDateTime(current_datetime)
            self.timeedit_endTime.setDateTime(QDateTime(current_datetime.date(), QTime(23, 10)))
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
        self.combo_isAgent.setCurrentText(str(data.get('isAgent', 0)))
        self.lineedit_expected.setText(str(data.get('expected', 2)))

    def is_agent_changed(self):
        if self.combo_isAgent.currentText() == '1':
            self.lineedit_courseID.setEnabled(True)
            self.lineedit_detailsID.setEnabled(True)
        else:
            self.lineedit_courseID.setEnabled(False)
            self.lineedit_detailsID.setEnabled(False)

    def make_tasks(self):
        startTime = self.timeedit_startTime.dateTime().toSecsSinceEpoch()
        endTime = self.timeedit_endTime.dateTime().toSecsSinceEpoch()
        expected = int(self.lineedit_expected.text())
        phone_count = int(self.lineedit_phone_count.text())
        email_count = int(self.lineedit_email_count.text())
        tasks_count = expected // 2
        random_seconds = random.sample(range(startTime, endTime), phone_count + email_count)

        tasks = []

        for idx, sec in enumerate(random_seconds):
            formatted_time = datetime.fromtimestamp(sec, self.tz).strftime('%Y-%m-%d %H:%M:%S')
            if idx < tasks_count:
                tasks.append([formatted_time, 'super_create', 'phone'])
            elif idx < phone_count:
                tasks.append([formatted_time, 'create', 'phone'])
            else:
                tasks.append([formatted_time, 'create', 'email'])

        remainTasks = sorted(tasks, key=lambda x: x[0])

        return {
            'startTime': datetime.fromtimestamp(startTime, self.tz).strftime('%Y-%m-%d %H:%M:%S'),
            'endTime': datetime.fromtimestamp(endTime, self.tz).strftime('%Y-%m-%d %H:%M:%S'),
            'expected': expected,
            'remainTasks': str(remainTasks)
        }

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
        self.lineedit_team.textChanged.connect(self.strip_team)
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
        self.lineedit_phone.textChanged.connect(self.strip_phone)
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
        self.combo_isAgent.addItems(['0', '1'])
        self.combo_isAgent.currentTextChanged.connect(self.is_agent_changed)
        layout_isAgent.addWidget(self.combo_isAgent)
        layout.addLayout(layout_isAgent)

        layout_courseID = QHBoxLayout()
        layout_courseID.addWidget(QLabel('Course ID:'))
        self.lineedit_courseID = QLineEdit(self)
        self.lineedit_courseID.setEnabled(False)
        layout_courseID.addWidget(self.lineedit_courseID)
        layout.addLayout(layout_courseID)

        layout_detailsID = QHBoxLayout()
        layout_detailsID.addWidget(QLabel('Details ID:'))
        self.lineedit_detailsID = QLineEdit(self)
        self.lineedit_detailsID.setEnabled(False)
        layout_detailsID.addWidget(self.lineedit_detailsID)
        layout.addLayout(layout_detailsID)

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

    def strip_phone(self):
        self.lineedit_team.blockSignals(True)
        self.lineedit_phone.setText(self.lineedit_phone.text().strip())
        self.lineedit_team.blockSignals(False)

    def strip_team(self):
        self.lineedit_phone.blockSignals(True)
        self.lineedit_team.setText(self.lineedit_team.text().strip())
        self.lineedit_phone.blockSignals(False)

    def submit_data(self):
        data = self.make_tasks()
        isAgent = int(self.combo_isAgent.currentText())
        invitationCode = self.label_invitationCode.text()
        if isAgent:
            invitationCode = f'{invitationCode}|{self.lineedit_courseID.text()}|{self.lineedit_detailsID.text()}'
        data.update({
            'id': int(self.label_id.text()),
            'team': self.lineedit_team.text(),
            'userId': int(self.label_userId.text()),
            'phone': self.lineedit_phone.text(),
            'invitationCode': invitationCode,
            'isAgent': isAgent
        })

        Globals._WS.database_operation_signal.emit('insert', {
            'table_name': 'auto_creator',
            'data': data
        }, None)

        team_data = {key: data[key] for key in ['userId', 'team']}
        if isAgent:
            Globals._WS.database_operation_signal.emit('upsert', {
                'table_name': 'agents_america',
                'data': team_data,
                'unique_columns': ['userId'],
            }, None)
            Globals._WS.agents_america_update_row_signal.emit(team_data)
        else:
            Globals._WS.database_operation_signal.emit('upsert', {
                'table_name': 'users_america',
                'data': team_data,
                'unique_columns': ['userId'],
            }, None)
            Globals._WS.users_america_update_row_signal.emit(team_data)

        self.parent().add_row(data)

        Globals._Log.info(self.user, f'set up auto powder successfully.')

        self.accept()

    def validate(self):
        self.submit_validate.setEnabled(False)
        try:
            phone = self.lineedit_phone.text()

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

            isAgent = int(self.combo_isAgent.currentText())
            if isAgent:
                courseID = self.lineedit_courseID.text()
                detailsID = self.lineedit_detailsID.text()
                if not courseID or not detailsID:
                    Globals._Log.error(self.user, f'Validation failed: Course ID and Details ID are required.')
                    return
            
            phone = self.lineedit_phone.text()
            if not phone:
                Globals._Log.error(self.user, f'Validation failed: Phone is a required field.')
                return
            
            if isAgent:
                Globals._WS.database_operation_signal.emit('read', {
                    'table_name': 'agents_america',
                    'condition': f'userName="{phone}"'
                }, self.queue)
            else:
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
        self.counter = itertools.count()
        self.in_processing = set()
        self.is_running = False
        self.queue_database = Queue()
        self.queue_tasks = PriorityQueue()
        self.setAutoDelete(False)
        self.tz = pytz.timezone('Asia/Shanghai')

        self.user = 'AutoCreatorWorker'

    def add_task(self, data):
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
            weight = datetime.strptime(tasks[0][0], self.format_timestamp_str).timestamp()
            interval = now - weight
            if interval < -10:
                return
            if interval > 30:
                Globals._Log.warning(self.user, f'add_task: Delay task: {tasks[0]}')
                for task in tasks:
                    task_time = datetime.strptime(task[0], '%Y-%m-%d %H:%M:%S').replace(tzinfo=self.tz)
                    new_time = task_time + timedelta(seconds=interval)
                    task[0] = new_time.strftime('%Y-%m-%d %H:%M:%S')
            data['remainTasks'] = tasks
            self.queue_tasks.put(((weight, next(self.counter)), data))
        
        except Exception as e:
            Globals._Log.error(self.user, f'add_task: {e}')
            return

    def reset_tasks(self, data):
        Globals._WS.database_operation_signal.emit('upsert',{
            'table_name': 'auto_creator',
            'data': data,
            'unique_columns': ['id']
        }, None)
        Globals._WS.autoCreatorTab_update_row_signal.emit(data)

    def get_tasks(self):
        Globals._WS.database_operation_signal.emit('read', {
            'table_name': 'auto_creator',
            'condition': 'isCompleted IS NOT TRUE'
        }, self.queue_database)
        data_list = self.queue_database.get()

        for data in data_list:
            data_dict = {}
            for idx, col in enumerate(self.columns):
                data_dict[col] = data[idx]
            if data_dict['userId'] not in self.in_processing:
                self.add_task(data_dict)

    def initialize_columns(self):
        Globals._WS.database_operation_signal.emit('get_table_fields', {'table_name': 'auto_creator'}, self.queue_database)
        self.columns = self.queue_database.get()
    
    def make_consume_task(self, userId):
        delay = random.choices([random.randint(60, 300), random.randint(300, 900), random.randint(900, 1800)], [0.6, 0.3, 0.1], k=1)[0] + int(time.time())
        return [datetime.fromtimestamp(delay, self.tz).strftime('%Y-%m-%d %H:%M:%S'), 'consume', userId]
    
    def make_recharge_task(self, userId):
        delay = int(time.time()) + random.randint(180, 900)
        return [datetime.fromtimestamp(delay, self.tz).strftime('%Y-%m-%d %H:%M:%S'), 'recharge', userId]
    
    def recharge(self, userId):
        if not Globals._requests_admin.request('get', f'/sqx_fast/user/{userId}'):
            return False
        if not Globals._requests_admin.request('get', f'/sqx_fast/moneyDetails/selectUserMoney?userId={userId}'):
            return False
        if not Globals._requests_admin.request('post', f'/sqx_fast/user/addCannotMoney/{userId}/4'):
            return False
        if not Globals._requests_admin.request('get', f'/sqx_fast/user/{userId}'):
            return False
        if not Globals._requests_admin.request('get', f'/sqx_fast/moneyDetails/selectUserMoney?userId={userId}'):
            return False
        return True

    def processing(self, data):
        try:
            tasks = data['remainTasks']
            print(f'processing tasks: {tasks[0]}')
            _, method, param = tasks[0][:3]
            if method == 'create':
                UserRequests.create_user(data['invitationCode'], param)
            elif method == 'super_create':
                userId = UserRequests.create_user(data['invitationCode'], param)
                tasks.append(self.make_recharge_task(userId))
            elif method == 'recharge':
                if not self.recharge(param):
                    return
                Globals._Log.info(self.user, f'processing: {param} recharge successfully')
                tasks.append(self.make_consume_task(param))
            elif method == 'consume':
                UserRequests(param).consume_vip()
            else:
                Globals._Log.error(self.user, f'Invalid method: {method}')
                return
            
            del tasks[0]
            if not tasks:
                data.update({
                    'remainTasks': '',
                    'isCompleted': 1,
                    'updateTime': datetime.fromtimestamp(time.time(), self.tz).strftime('%Y-%m-%d %H:%M:%S')
                })
            else:
                remainTasks = sorted(tasks, key=lambda x: x[0])
                data.update({
                    'remainTasks': str(remainTasks),
                    'updateTime': datetime.fromtimestamp(time.time(), self.tz).strftime('%Y-%m-%d %H:%M:%S')
                })
            self.reset_tasks(data)

        except Exception as e:
            Globals._Log.error(self.user, f'processing: {e}')
        
        finally:
            self.in_processing.discard(data['userId'])

    def run(self):
        self.is_running = True
        if not self.columns:
            self.initialize_columns()
        while self.is_running and Globals.is_app_running:
            if self.queue_tasks.empty():
                self.get_tasks()
            if self.queue_tasks.empty():
                print('task empty, wait 5')
                time.sleep(5)
                continue
            now = time.time()
            ((weight, _), data) = self.queue_tasks.get()
            interval = weight - now
            if interval > 0:
                print(f'wait {interval} to start: {data["remainTasks"][0]}')
                time.sleep(interval + 1)
            print(f'will inner: {data}')
            self.in_processing.add(data['userId'])
            Globals.run_task(self.processing, data=data)
        self.is_running = False

    def start_task(self):
        if not self.is_running:
            Globals.thread_pool_global.start(self)

    def stop_task(self):
        self.is_running = False
