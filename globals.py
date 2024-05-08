import asyncio
import requests
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
    QHeaderView,
    QTableWidget,
    QTextEdit,
    QVBoxLayout
)

from dylogging import Logging

class WorkerSignals(QObject):
    autoCreatorTab_add_creator_signal = pyqtSignal(dict)
    autoCreatorTab_update_row_signal = pyqtSignal(dict)
    autoCreatorTab_update_task_signal = pyqtSignal(str, str)
    database_operation_signal = pyqtSignal(str, dict, object)
    progress_hide_signal = pyqtSignal(int)
    progress_reset_signal = pyqtSignal(str)
    progress_show_signal = pyqtSignal(int)
    progress_update_signal = pyqtSignal(str)
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
        self.label_progress = QLabel('0/0')
        layout.addWidget(self.label_progress)
        self.setWindowFlags(self.windowFlags() & ~Qt.WindowType.WindowCloseButtonHint)

        Globals._WS.progress_hide_signal.connect(self.hide_progress)
        Globals._WS.progress_reset_signal.connect(self.reset)
        Globals._WS.progress_show_signal.connect(self.show_progress)
        Globals._WS.progress_update_signal.connect(self.update_message)

        self.current_count = 0
        self.total_count = 0

    @pyqtSlot(int)
    def hide_progress(self, count=0):
        if count:
            self.current_count += count
            self.update_progres()
        if self.current_count >= self.total_count and self.isVisible():
            super().hide()

    @pyqtSlot(str)
    def reset(self, title):
        self.setWindowTitle(title)
        self.current_count = 0
        self.total_count = 0

    @pyqtSlot(int)
    def show_progress(self, count=0):
        if count:
            self.total_count += count
            self.update_progres()
        if not self.isVisible():
            super().show()

    @pyqtSlot(str)
    def update_message(self, message):
        self.label_message.setText(f'{message}')

    def update_progres(self):
        self.label_progress.setText(f'{self.current_count}/{self.total_count}')

class Worker(QRunnable):
    def __init__(self, func, *args, **kwargs):
        super().__init__()
        self.func = func
        self.args = args
        self.kwargs = kwargs

    def run(self):
        self.func(*self.args, **self.kwargs)

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
    _requests_admin = None
    _requests_user = None
    _SQL = None
    _TELEGRAM_BOT_TOKEN = ''
    _TELEGRAM_CHATID = ''
    _TO_CAPTCHA_KEY = ''
    _WS = WorkerSignals()
    accounts_dict = {}
    binging_dict = {}
    exclusive_components = []
    is_app_running = True
    thread_pool_global = QThreadPool.globalInstance()
    token_refreshing = False

    @staticmethod
    def run_task(func, *args, **kwargs):
        Globals.thread_pool_global.start(Worker(func, *args, **kwargs))

class TableWidget(QTableWidget):
    class _HeaderView(QHeaderView):
        def __init__(self, orientation, parent=None):
            super().__init__(orientation, parent)
            self.setSectionsClickable(True)

        def mousePressEvent(self, event):
            super().mousePressEvent(event)
            if event.button() == Qt.MouseButton.LeftButton:
                column = self.logicalIndexAt(event.position().toPoint())
                table = self.parentWidget()
                if isinstance(table, TableWidget):
                    table.sortItems(column)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        custom_header = self._HeaderView(Qt.Orientation.Horizontal, self)
        self.setHorizontalHeader(custom_header)
        self.numeric_columns = []
        self.current_sort_order = Qt.SortOrder.AscendingOrder
        self.horizontalHeader().setSortIndicatorShown(True)

    def _numeric_key(self, column):
        def key_func(row):
            text = row[column].text()
            if text.strip() and text.replace('.', '', 1).isdigit():
                return float(text)
            else:
                return float('-inf') if self.current_sort_order == Qt.SortOrder.AscendingOrder else float('inf')
        return key_func

    def _takeRow(self, row_idx):
        row = []
        for col in range(self.columnCount()):
            row.append(self.takeItem(row_idx, col))
        return row

    def setNumericColumns(self, columns):
        self.numeric_columns = columns

    def sortItems(self, column):
        if self.horizontalHeader().sortIndicatorSection() == column:
            if self.current_sort_order == Qt.SortOrder.AscendingOrder:
                self.current_sort_order = Qt.SortOrder.DescendingOrder
            else:
                self.current_sort_order = Qt.SortOrder.AscendingOrder
        else:
            self.current_sort_order = Qt.SortOrder.AscendingOrder

        rows = [self._takeRow(i) for i in range(self.rowCount())]
        
        if column in self.numeric_columns:
            rows.sort(key=self._numeric_key(column), reverse=self.current_sort_order != Qt.SortOrder.DescendingOrder)
        else:
            rows.sort(key=lambda row: row[column].text(), reverse=self.current_sort_order != Qt.SortOrder.DescendingOrder)

        self.setRowCount(0)
        for row in rows:
            self.insertRow(self.rowCount())
            for col, item in enumerate(row):
                self.setItem(self.rowCount() - 1, col, item)

        self.horizontalHeader().setSortIndicator(column, self.current_sort_order)