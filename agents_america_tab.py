from PyQt6.QtCore import(
    pyqtSlot,
    Qt
)
from PyQt6.QtWidgets import (
    QAbstractItemView,
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

from globals import Globals, TableWidget

class AgentsAmericaTab(QWidget):
    def __init__(
            self,
            parent
        ):
        super().__init__(parent)

        q = Queue()
        Globals._WS.database_operation_signal.emit('get_table_fields', {'table_name': 'agents_america'}, q)
        self.columns = q.get()

        self.columns_display = [
            'team', 'userId', 'userName', 'mobile', 'inviterCode', 'agentCash', 'agentWithdrawCash', 'agentRate', 'agentType', 'createUserId', 'createTime'
        ]

        Globals._WS.agents_america_update_row_signal.connect(self.update_row)

        self.user = 'AgentsAmericaTab'

        self.setup_ui()
        self.reload()

        Globals._Log.info(self.user, 'Successfully initialized.')

    # def add_creator(self):
    #     current_index = self.table.currentIndex()
    #     row = current_index.row()
    #     data = {
    #         'team': self.table.item(row, self.columns_display.index('team')).text(),
    #         'userId': self.table.item(row, self.columns_display.index('userId')).text(),
    #         'phone': self.table.item(row, self.columns_display.index('mobile')).text()
    #     }
    #     Globals._WS.autoCreatorTab_add_creator_signal.emit(data)

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
            'table_name': 'agents_america',
            'condition': 'isdeleted IS NOT TRUE'
        }, q)

        datas = q.get()
        for data in datas:
            row_data = {}
            for idx, col in enumerate(self.columns):
                row_data[col] = data[idx]
            self.add_row(row_data)

        Globals._Log.info(self.user, 'Reload completed')

    def set_team(self):
        index = self.table.currentIndex()
        if not index.isValid():
            return
    
        row = index.row()
        userName = self.table.item(row, self.columns_display.index('userName')).text()
        current_team = self.table.item(row, self.columns_display.index('team')).text()

        new_team, ok = QInputDialog.getText(self, f'Set Team For {userName}', 'Enter new team:', QLineEdit.EchoMode.Normal, current_team)
        
        if ok:
            self.table.item(row, self.columns_display.index('team')).setText(new_team)
            Globals._WS.database_operation_signal.emit('upsert', {
                'table_name': 'agents_america',
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
        button_update_all = QPushButton('Update All Users')
        button_update_all.clicked.connect(self.update_users)
        top_layout.addWidget(button_update_all)
        top_layout.addStretch()
        button_reload = QPushButton('Reload')
        button_reload.clicked.connect(self.reload)
        top_layout.addWidget(button_reload)

        middle_layout = QHBoxLayout()
        layout.addLayout(middle_layout)
        self.table = TableWidget(0, len(self.columns_display))
        self.table.setNumericColumns([self.columns_display.index(column) for column in ['userId', 'agentCash', 'agentWithdrawCash', 'agentRate', 'agentType', 'createUserId']])
        middle_layout.addWidget(self.table)
        self.table.setHorizontalHeaderLabels(self.columns_display)

        self.table.verticalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Fixed)
        self.table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)

        self.table.mousePressEvent = self.table_mouse_press_event
        self.table.cellClicked.connect(self.cell_was_clicked)
        self.table.doubleClicked.connect(self.cell_was_double_clicked)

    def show_context_menu(self, pos, index):
        menu = QMenu()

        # row = index.row()
        # phone = self.table.item(row, self.columns_display.index('mobile')).text()
        # invitation_code = self.table.item(row, self.columns_display.index('invitationCode')).text()
        # invitation_link = f'{Globals._BASE_URL_AMERICA}/pages/login/login?inviterType=0&invitation={invitation_code}'

        # action_add_creator = menu.addAction('Add Creator')
        # action_add_creator.triggered.connect(self.add_creator)

        action_set_team = menu.addAction('Set Team')
        action_set_team.triggered.connect(self.set_team)

        # action_copy_tk_link = menu.addAction('Copy invitation Link')
        # action_copy_tk_link.triggered.connect(lambda: QApplication.clipboard().setText(invitation_link))

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

    def update_users(self):
        Globals.run_task(self.update_users_worker)

    def update_users_worker(self):
        Globals._Log.info(self.user, 'Starting agents update process.')
        Globals._WS.progress_reset_signal.emit('Updating users')
        Globals._WS.progress_show_signal.emit(0)
        Globals._WS.progress_update_signal.emit('waiting...')
        page = 1
        totalPage = 0
        while True:
            Globals._Log.info(self.user, f'Fetching agents for page {page}/{totalPage}')
            res = Globals._requests_admin.request('get', f'/sqx_fast/agent/agent/list?page={page}&limit={500}')
            datas = res.get('page', {})
            totalPage = datas.get('totalPage', 0)
            totalCount = datas.get('totalCount', 0)
            Globals._WS.progress_update_signal.emit(f'waiting)...')
            Globals._WS.progress_show_signal.emit(totalCount)

            data_list = datas.get('list', [])
            Globals._WS.database_operation_signal.emit('bulk_upsert', {
                'table_name': 'agents_america',
                'datas': data_list,
                'unique_columns': ['userId']
            }, None)

            for data in data_list:
                Globals._WS.agents_america_update_row_signal.emit(data)
                Globals._WS.progress_hide_signal.emit(1)

            page += 1
            if page > totalPage:
                Globals._Log.info(self.user, 'Agents update process completed successfully.')
                break
        self.reload()
        Globals._WS.progress_hide_signal.emit(0)