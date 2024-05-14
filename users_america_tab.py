import datetime
import pandas as pd

from collections import defaultdict
from PyQt6.QtCore import(
    pyqtSlot,
    Qt
)
from PyQt6.QtWidgets import (
    QAbstractItemView,
    QApplication,
    QHBoxLayout,
    QHeaderView,
    QLineEdit,
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

        Globals._WS.database_operation_signal.emit('get_table_fields', {'table_name': 'withdraw'}, q)
        self.columns_withdraw = q.get()

        Globals._WS.database_operation_signal.emit('get_table_fields', {'table_name': 'recharge'}, q)
        self.columns_recharge = q.get()

        self.columns_display = [
            'team', 'userId', 'phone', 'iscreator', 'income', 'withdraw', 'recharge', 'jifen', 'money', 'invitations', 'invitationType', 'platform', 'inviterCode', 'invitationCode'
        ]

        Globals._WS.users_america_update_row_signal.connect(self.update_row)

        self.user = 'UsersAmericaTab'

        self.setup_ui()
        self.reload()

        Globals._Log.info(self.user, 'Successfully initialized.')

    def add_creator(self):
        current_index = self.table.currentIndex()
        row = current_index.row()
        invitationType = self.table.item(row, self.columns_display.index('invitationType')).text()
        if not invitationType:
            Globals._Log.error(self.user, 'Distribution has not been activated.')
            return
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

        # Globals._WS.database_operation_signal.emit('read', {
        #     'table_name': 'withdraw',
        #     'condition': 'state=1'
        # }, q)
        # withdraw_dict = defaultdict(float)
        # col_userId = self.columns_withdraw.index('userId')
        # col_money = self.columns_withdraw.index('money')
        # for withdraw in q.get():
        #     withdraw_dict[withdraw[col_userId]] += float(withdraw[col_money])

        # Globals._WS.database_operation_signal.emit('read', {
        #     'table_name': 'recharge',
        #     'condition': 'state=1'
        # }, q)
        # recharge_dict = defaultdict(float)
        # for recharge in q.get():
        #     recharge_dict[recharge[self.columns_recharge.index('userId')]] += float(recharge[self.columns_recharge.index('money')])

        for data in datas:
            row_data = {col: data[idx] for idx, col in enumerate(self.columns)}
            # userId = row_data['userId']
            # if userId in withdraw_dict:
            #     row_data['withdraw'] = withdraw_dict[userId]
            # if userId in recharge_dict:
            #     row_data['recharge'] = recharge_dict[userId]
            self.add_row(row_data)

        Globals._Log.info(self.user, 'Reload completed')

    def set_team(self):
        index = self.table.currentIndex()
        if not index.isValid():
            return
    
        row = index.row()
        phone = self.table.item(row, self.columns_display.index('phone')).text()
        current_team = self.table.item(row, self.columns_display.index('team')).text()

        new_team, ok = QInputDialog.getText(self, f'Set Team For {phone}', 'Enter new team:', QLineEdit.EchoMode.Normal, current_team)
        
        if ok:
            self.table.item(row, self.columns_display.index('team')).setText(new_team)
            Globals._WS.database_operation_signal.emit('upsert', {
                'table_name': 'users_america',
                'data': {
                    'userId': self.table.item(row, self.columns_display.index('userId')).text(),
                    'team': new_team
                },
                'unique_columns': ['userId']
            }, None)
            Globals._Log.info(self.user, f'Team updated for user at row {row} to {new_team}')

    def setup_ui(self):
        layout = QVBoxLayout(self)

        top_layout = QHBoxLayout()
        layout.addLayout(top_layout)
        self.button_update_all = QPushButton('Update All Users')
        self.button_update_all.clicked.connect(self.update_users)
        top_layout.addWidget(self.button_update_all)
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

        action_set_team = menu.addAction('Set Team')
        action_set_team.triggered.connect(self.set_team)

        action_show_familyTree = menu.addAction('Show FamilyTree')
        action_show_familyTree.triggered.connect(lambda: Globals._FamilyTree.show(invitation_code))

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
            # user_info['recharge'] = response['data']['income']
            user_info['invitations'] = response['data']['count']
            # user_info['withdraw'] = response['data']['consume']
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
            
            Globals._WS.users_america_update_row_signal.emit(user_info)
            Globals._WS.progress_hide_signal.emit(1)
            Globals._Log.info(self.user, f'Updated user data of {phone} successfully.')

        except Exception as e:
            Globals._Log.error(self.user, f'Failed to update user data of {phone}: {e}')

    def update_users(self):
        self.button_update_all.setEnabled(False)
        Globals.run_task(self.update_users_worker)
        Globals.run_task(self.update_withdraw_and_recharge_worker)

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
        
        Globals._WS.progress_hide_signal.emit(0)

    def update_withdraw_and_recharge_worker(self):
        Globals._Log.info(self.user, 'Starting withdraw and recharge update process.')
        q = Queue()
        today = datetime.date.today().strftime('%Y-%m-%d')
        page = 1
        totalPage = 0
        while True:
            Globals._Log.info(self.user, f'Fetching withdraw for page {page}/{totalPage}')
            params = {
                'page': page,
                'limit': 500,
                'recipient': '',
                'bankNumber': '',
                'state': '',
                'type': '',
                'startTime': '2024-01-01',
                'endTime': today
            }
            res = Globals._requests_admin.request('get', f'/sqx_fast/cash/selectPayDetails', params=params)
            datas = res.get('data', {})
            totalPage = datas.get('pages', 0)
            
            data_list = datas.get('records', [])
            Globals._WS.database_operation_signal.emit('bulk_upsert', {
                'table_name': 'withdraw',
                'datas': data_list,
                'unique_columns': ['id']
            }, q)
            q.get()

            page += 1
            if page > totalPage:
                Globals._Log.info(self.user, 'Withdraw update process completed successfully.')
                break

        page = 1
        totalPage = 0
        while True:
            Globals._Log.info(self.user, f'Fetching recharge for page {page}/{totalPage}')
            params = {
                'page': page,
                'limit': 500,
                'state': '',
                'startTime': '2024-01-01',
                'endTime': today
            }
            res = Globals._requests_admin.request('get', f'/sqx_fast/cash/selectUserRecharge', params=params)
            datas = res.get('data', {})
            totalPage = datas.get('pages', 0)
            
            data_list = datas.get('list', [])
            Globals._WS.database_operation_signal.emit('bulk_upsert', {
                'table_name': 'recharge',
                'datas': data_list,
                'unique_columns': ['id']
            }, q)
            q.get()

            page += 1
            if page > totalPage:
                Globals._Log.info(self.user, 'Recharge update process completed successfully.')
                break

        self.update_withdraw_and_recharge_to_users()

    def update_withdraw_and_recharge_to_users(self):
        q = Queue()
        Globals._WS.database_operation_signal.emit('read', {
            'table_name': 'withdraw',
            'condition': 'state=1'
        }, q)
        data = q.get()
        df_withdraw = pd.DataFrame(data, columns=self.columns_withdraw)
        df_withdraw['withdraw'] = df_withdraw['money'].astype(float).fillna(0)
        summary_withdraw = df_withdraw.groupby('userId')['withdraw'].sum().reset_index()

        Globals._WS.database_operation_signal.emit('read', {
            'table_name': 'recharge',
            'condition': 'state=1'
        }, q)
        data = q.get()
        df_recharge = pd.DataFrame(data, columns=self.columns_recharge)
        df_recharge['recharge'] = df_recharge['money'].astype(float).fillna(0)
        summary_recharge = df_recharge.groupby('userId')['recharge'].sum().reset_index()

        df = pd.merge(summary_withdraw, summary_recharge, on='userId', how='outer').fillna(0)
        df_dict = df.to_dict(orient = 'records')

        Globals._WS.database_operation_signal.emit('bulk_upsert', {
            'table_name': 'users_america',
            'datas': df_dict,
            'unique_columns': ['userId']
        }, q)
        q.get()

        Globals._Log.info(self.user, 'Update withdraw and recharge to users_america successfully.')
        self.reload()
        self.button_update_all.setEnabled(True)