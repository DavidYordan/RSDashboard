
import os
import sys
from PyQt6.QtCore import (
    Qt
)
from PyQt6.QtGui import (
    QAction,
    QIcon,
    QKeySequence,
    QShortcut
)
from PyQt6.QtWidgets import (
    QApplication,
    QDialog,
    QHBoxLayout,
    QLineEdit,
    QMainWindow,
    QPushButton,
    QSplitter,
    QStyleFactory,
    QTableWidget,
    QTabWidget,
    QVBoxLayout,
    QWidget
)

if __name__ == '__main__':
    app = QApplication(sys.argv)

from agents_america_tab import AgentsAmericaTab
from auto_creator_tab import AutoCreatorTab
from concurrent_requests import AdminRequests
from globals import Globals
from users_america_tab import UsersAmericaTab

class SearchDialog(QDialog):
    def __init__(self, parent):
        super().__init__(parent)
        self.table_widget = parent
        self.current_index = 0
        self.last_search_text = ''
        self.search_results = []
        self.setModal(True)
        self.init_ui()

    def init_ui(self):
        self.setWindowTitle("Search")
        self.main_layout = QVBoxLayout(self)

        self.top_layout = QHBoxLayout()
        self.search_field = QLineEdit(self)
        self.search_field.textChanged.connect(self.strip_input)
        self.search_button = QPushButton("Search", self)
        self.top_layout.addWidget(self.search_field)
        self.top_layout.addWidget(self.search_button)

        self.bottom_layout = QHBoxLayout()
        self.prev_button = QPushButton("Previous(F1)", self)
        self.next_button = QPushButton("Next(F3)", self)
        self.bottom_layout.addWidget(self.prev_button)
        self.bottom_layout.addWidget(self.next_button)

        self.main_layout.addLayout(self.top_layout)
        self.main_layout.addLayout(self.bottom_layout)

        self.search_button.clicked.connect(self.on_search_clicked)
        self.prev_button.clicked.connect(self.prev_result)
        self.next_button.clicked.connect(self.next_result)
        self.prev_shortcut = QShortcut(QKeySequence('F1'), self)
        self.prev_shortcut.activated.connect(self.prev_result)
        self.next_shortcut = QShortcut(QKeySequence('F3'), self)
        self.next_shortcut.activated.connect(self.next_result)
        self.show()

    def highlight_result(self):
        row, col = self.search_results[self.current_index]
        self.table_widget.clearSelection()
        self.table_widget.setCurrentCell(row, col)
        self.table_widget.scrollToItem(self.table_widget.item(row, col), QTableWidget.ScrollHint.PositionAtCenter)

    def next_result(self):
        if self.search_results:
            self.current_index = (self.current_index + 1) % len(self.search_results)
            self.highlight_result()

    def on_search_clicked(self):
        search_text = self.search_field.text().lower()
        if search_text == self.last_search_text and self.search_results:
            self.next_result()
        else:
            self.last_search_text = search_text
            self.search_in_table()

    def prev_result(self):
        if self.search_results:
            self.current_index = (self.current_index - 1) % len(self.search_results)
            self.highlight_result()

    def search_in_table(self):
        self.search_results.clear()
        search_text = self.search_field.text().lower()
        for i in range(self.table_widget.rowCount()):
            for j in range(self.table_widget.columnCount()):
                item = self.table_widget.item(i, j)
                if item and search_text in item.text().lower():
                    self.search_results.append((i, j))
        
        if not self.search_results:
            Globals._Log.warning('Search', 'No matches found.')
            return

        self.current_index = 0
        self.highlight_result()

    def strip_input(self):
        current_text = self.search_field.text().strip()
        self.search_field.blockSignals(True)
        self.search_field.setText(current_text)
        self.search_field.blockSignals(False)

class RSDashboard(QMainWindow):
    def __init__(self):
        super().__init__()

        self.setWindowTitle('RSDashboard')
        if hasattr(sys, '_MEIPASS'):
            icon_path = os.path.join(sys._MEIPASS, 'img/RSDashboard.ico')
        else:
            icon_path = ('img/RSDashboard.ico')
        self.setWindowIcon(QIcon(icon_path))
        self.resize(1080, 640)
        self.showMaximized()

        import config
        config.config_init(self)

        self.create_menu()
        self.create_main_panel()

        Globals.thread_pool_global.setMaxThreadCount(50)

        Globals._requests_admin = AdminRequests()

        self.user = 'RSDashboard'

        Globals._Log.info(self.user, 'Successfully initialized.')

    def activate_search(self):
        current_table = self.tab_left.currentWidget()
        if not hasattr(current_table, 'table'):
            return
        self.search_dialog = SearchDialog(current_table.table)

    def closeEvent(self, event):
        Globals.is_app_running = False
        Globals.thread_pool_global.waitForDone()
        super().closeEvent(event)

    def create_menu(self):
        menubar = self.menuBar()

        menu_autoCreatorWorker = menubar.addMenu('AutoCreatorWorker')
        self.action_autoCreatorWorker_run = QAction('Run', self)
        self.action_autoCreatorWorker_run.triggered.connect(self.autoCreatorWorker_run)
        self.action_autoCreatorWorker_stop = QAction('Stop', self)
        self.action_autoCreatorWorker_stop.triggered.connect(self.autoCreatorWorker_stop)
        menu_autoCreatorWorker.addActions([self.action_autoCreatorWorker_run, self.action_autoCreatorWorker_stop])

        menu_graph = menubar.addMenu('Graph')
        action_family_tree = QAction('FamilyTree', self)
        action_family_tree.triggered.connect(lambda: Globals._FamilyTree.show())
        menu_graph.addActions([action_family_tree])

    def create_main_panel(self):
        central_widget = QWidget(self)
        self.setCentralWidget(central_widget)
        layout_main = QVBoxLayout(central_widget)
        layout_tabs = QHBoxLayout()
        layout_main.addLayout(layout_tabs, 100)

        splitter = QSplitter(Qt.Orientation.Horizontal)
        layout_tabs.addWidget(splitter)

        splitter.setStyleSheet('''
            QSplitter::handle {
                background-color: #FFA500;
                border: 1px solid #E69500;
            }
            QSplitter::handle:hover {
                background-color: #FFB733;
            }
            QSplitter::handle:pressed {
                background-color: #CC8400;
            }
        ''')

        self.search_shortcut = QShortcut(QKeySequence('Ctrl+F'), self)
        self.search_shortcut.activated.connect(self.activate_search)

        self.tab_left = QTabWidget()
        splitter.addWidget(self.tab_left)

        self.users_america_tab = UsersAmericaTab(
            self.tab_left
        )
        self.tab_left.addTab(self.users_america_tab, 'Users')

        self.agents_america_tab = AgentsAmericaTab(
            self.tab_left
        )
        self.tab_left.addTab(self.agents_america_tab, 'Agents')

        self.auto_creator_tab = AutoCreatorTab(
            self.tab_left
        )
        self.tab_left.addTab(self.auto_creator_tab, 'Creator')

        log_widget = QWidget()
        self.tab_left.addTab(log_widget, 'Log')
        log_vlayout = QVBoxLayout(log_widget)
        Globals._log_textedit.setReadOnly(True)
        Globals._log_textedit.document().setMaximumBlockCount(200)
        log_vlayout.addWidget(Globals._log_textedit, 11)

        self.tab_right = QTabWidget()
        splitter.addWidget(self.tab_right)

        splitter.setSizes([1, 0])

        layout_main.addWidget(Globals._log_label, 1)
        Globals._log_label.setWordWrap(True)
        Globals._log_label.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
        Globals._log_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        Globals._log_label.setMaximumHeight(300)
        # Globals._log_label.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)

    def autoCreatorWorker_run(self):
        if not self.auto_creator_tab.autoCreatorWorker.is_running:
            self.auto_creator_tab.autoCreatorWorker.start_task()
        self.action_autoCreatorWorker_run.setEnabled(False)
        self.action_autoCreatorWorker_stop.setEnabled(True)

    def autoCreatorWorker_stop(self):
        self.auto_creator_tab.autoCreatorWorker.stop_task()
        self.action_autoCreatorWorker_run.setEnabled(True)
        self.action_autoCreatorWorker_stop.setEnabled(False)

if __name__ == '__main__':
    os.environ['PLAYWRIGHT_BROWSERS_PATH'] = '0'
    os.environ['XDG_CACHE_HOME'] = './temp'
    os.environ['LOCALAPPDATA'] = './temp'
    os.environ['TEMP'] = './temp'
    os.environ['TMP'] = './temp'
    # app = QApplication(sys.argv)
    app.setStyle(QStyleFactory.create('Fusion'))
    app.setStyleSheet("""
        QTableWidget::item:selected {
            background-color: #3498db;
            color: #ffffff;
        }
    """)
    main_win = RSDashboard()
    main_win.show()
    sys.exit(app.exec())
