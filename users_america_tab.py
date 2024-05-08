from PyQt6.QtCore import(
    pyqtSlot,
    Qt
)
from PyQt6.QtWidgets import (
    QAbstractItemView,
    QApplication,
    QHBoxLayout,
    QHeaderView,
    QInputDialog,
    QMenu,
    QPushButton,
    QTableWidgetItem,
    QVBoxLayout,
    QWidget
)
from queue import Queue

from concurrent_requests import UserRequests
from globals import Globals, TableWidget

class UsersAmericaTab(QWidget):
    def __init__(
            self,
            parent
        ):
        super().__init__(parent)

        q = Queue()
        Globals._WS.database_operation_signal.emit('get_table_fields', {'table_name': 'users_america'}, q)
        self.columns = q.get()

        self.columns_display = [
            'team', 'userId', 'phone', 'iscreator', 'income', 'withdraw', 'recharge', 'jifen', 'money', 'invitations', 'platform', 'inviterCode', 'invitationCode'
        ]

        Globals._WS.users_america_update_row_signal.connect(self.update_row)

        self.user = 'UsersAmericaTab'

        self.setup_ui()
        self.reload()

        Globals._Log.info(self.user, 'Successfully initialized.')

    def add_creator(self):
        current_index = self.table.currentIndex()
        row = current_index.row()
        data = {
            'team': self.table.item(row, self.columns_display.index('team')).text(),
            'userId': self.table.item(row, self.columns_display.index('userId')).text(),
            'phone': self.table.item(row, self.columns_display.index('phone')).text(),
            'invitationCode': self.table.item(row, self.columns_display.index('invitationCode')).text()
        }
        Globals._WS.autoCreatorTab_add_creator_signal.emit(data)

    def add_row(self, data, row=-1):
        if row == -1:
            row = self.table.rowCount()
            self.table.insertRow(row)
        for col_index, col_name in enumerate(self.columns_display):
            if col_name in data:
                value = data[col_name]
                item_value = str('' if value is None else value)
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

    def create_user(self):
        current_index = self.table.currentIndex()
        if current_index.isValid():
            invitationCode_item = self.table.item(current_index.row(), self.columns_display.index('invitationCode'))
            invitationCode = invitationCode_item.text() if invitationCode_item is not None else ''
        else:
            invitationCode = ''

        new_invitationCode, ok = QInputDialog.getText(self, 'invitationCode', 'Please input invitationCode:', text=invitationCode)
        if ok:
            if isinstance(invitationCode, int):
                Globals._Log.error(self.user, f'Unsupported agent: {new_invitationCode}')
                return
            Globals.run_task(UserRequests.create_user, invitationCode=new_invitationCode)

    def find_row_by_columnName(self, columnValue, columnName='userId'):
        column_index = self.columns_display.index(columnName)
        for row in range(self.table.rowCount()):
            item = self.table.item(row, column_index)
            if item and item.text() == str(columnValue):
                return row
        return -1

    def find_userId_by_phone(self, phone):
        phone_index = self.columns_display.index('phone')
        user_id_index = self.columns_display.index('userId')
        
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
        self.table.setRowCount(0)
        q = Queue()
        Globals._WS.database_operation_signal.emit('read', {
            'table_name': 'users_america',
            'condition': 'isdeleted IS NOT TRUE'
        }, q)

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
        button_update_all = QPushButton('Update All Users')
        button_update_all.clicked.connect(self.update_users)
        top_layout.addWidget(button_update_all)
        button_update_one = QPushButton('Update User')
        button_update_one.clicked.connect(self.update_user)
        top_layout.addWidget(button_update_one)
        self.button_create_user = QPushButton('Create User')
        self.button_create_user.clicked.connect(self.create_user)
        top_layout.addWidget(self.button_create_user)
        top_layout.addStretch()
        button_reload = QPushButton('Reload')
        button_reload.clicked.connect(self.reload)
        top_layout.addWidget(button_reload)

        middle_layout = QHBoxLayout()
        layout.addLayout(middle_layout)
        self.table = TableWidget(0, len(self.columns_display))
        self.table.setNumericColumns([self.columns_display.index(column) for column in ['userId', 'income', 'withdraw', 'recharge', 'jifen', 'money']])
        middle_layout.addWidget(self.table)
        self.table.setHorizontalHeaderLabels(self.columns_display)

        self.table.verticalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Fixed)
        self.table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)

        self.table.mousePressEvent = self.table_mouse_press_event
        self.table.cellClicked.connect(self.cell_was_clicked)
        self.table.doubleClicked.connect(self.cell_was_double_clicked)

    def show_context_menu(self, pos, index):
        menu = QMenu()

        row = index.row()
        phone = self.table.item(row, self.columns_display.index('phone')).text()
        invitation_code = self.table.item(row, self.columns_display.index('invitationCode')).text()
        invitation_link = f'{Globals._BASE_URL_AMERICA}/pages/login/login?inviterType=0&invitation={invitation_code}'

        action_add_creator = menu.addAction('Add Creator')
        action_add_creator.triggered.connect(self.add_creator)

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
        row = self.find_row_by_columnName(data['userId'])
        if row == -1:
            self.add_row(data)
            return
        for col_index, col_name in enumerate(self.columns_display):
            if col_name in data:
                value = data[col_name]
                item_value = str('' if value is None else value)
            else:
                continue
            cell_item = QTableWidgetItem(item_value)
            self.table.setItem(row, col_index, cell_item)

    def update_user(self):
        current_index = self.table.currentIndex()
        if current_index.isValid():
            phone_item = self.table.item(current_index.row(), self.columns_display.index('phone'))
            phone = phone_item.text() if phone_item is not None else ''
        else:
            phone = ''

        new_phone, ok = QInputDialog.getText(self, 'Phone', 'Please input phone:', text=phone)
        if ok and new_phone:
            Globals.run_task(self.update_user_worker, phone=new_phone)

    def update_user_worker(self, phone):
        Globals._Log.info(self.user, f'Starting {phone} update process.')
        try:
            userId = self.find_userId_by_phone(phone)
            if not userId:
                response = Globals._requests_admin.request('get', f'/sqx_fast/user/selectUserList?page=1&limit=1&phone={phone}')
                userId = response['data']['list'][0]['userId']

            response = Globals._requests_admin.request('get', f'/sqx_fast/user/{userId}')
            user_info = response['data']['userEntity']
            user_info['recharge'] = response['data']['income']
            user_info['invitations'] = response['data']['count']
            user_info['withdraw'] = response['data']['consume']
            user_info['income'] = response['data']['money']

            response = Globals._requests_admin.request('get', f'/sqx_fast/moneyDetails/selectUserMoney?userId={userId}')
            user_info['money'] = response['data']['money']

            response = Globals._requests_admin.request('get', f'/sqx_fast/integral/selectByUserId?userId={userId}')
            user_info['jifen'] = response['data']['integralNum']

            Globals._WS.database_operation_signal.emit('upsert',{
                'table_name': 'users_america',
                'data': user_info,
                'unique_columns': ['userId'],
            }, None)
            
            self.update_row(user_info)
            Globals._WS.progress_hide_signal.emit(1)
            Globals._Log.info(self.user, f'Updated user data of {phone} successfully.')

        except Exception as e:
            Globals._Log.error(self.user, f'Failed to update user data of {phone}: {e}')

    def update_users(self):
        Globals.run_task(self.update_users_worker)

    def update_users_worker(self):
        Globals._Log.info(self.user, 'Starting users update process.')
        Globals._WS.progress_reset_signal.emit('Updating users')
        Globals._WS.progress_show_signal.emit(0)
        Globals._WS.progress_update_signal.emit('waiting...')
        page = 1
        totalPage = 0
        while True:
            Globals._Log.info(self.user, f'Fetching data for page {page}/{totalPage}')
            res = Globals._requests_admin.request('get', f'/sqx_fast/user/selectUserList?page={page}&limit={500}')
            datas = res.get('data', {})
            totalPage = datas.get('totalPage', 0)
            totalCount = datas.get('totalCount', 0)
            Globals._WS.progress_update_signal.emit(f'waiting({totalCount})...')
            
            Globals._WS.progress_show_signal.emit(len(datas.get('list', [])))

            for data in datas.get('list', []):
                Globals.run_task(self.update_user_worker, phone=data['phone'])

            page += 1
            if page > totalPage:
                Globals._Log.info(self.user, 'Users update process completed successfully.')
                break
        self.reload()
        Globals._WS.progress_hide_signal.emit(0)