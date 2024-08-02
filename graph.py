import pandas as pd
from PyQt6.QtCore import Qt, QEvent
from PyQt6.QtGui import QAction, QCursor, QColor, QGuiApplication
from PyQt6.QtWidgets import (
    QDialog, QFrame, QHBoxLayout, QLineEdit, QPushButton,
    QScrollArea, QToolTip, QVBoxLayout, QWidget, QMenu
)
from queue import Queue
from globals import Globals

class FamilyTree(QDialog):
    def __init__(self, parent):
        super().__init__(parent)

        self.queue = Queue()
        Globals._WS.database_operation_signal.emit('get_table_fields', {'table_name': 'users_america'}, self.queue)
        self.columns_users = self.queue.get()

        Globals._WS.database_operation_signal.emit('get_table_fields', {'table_name': 'agents_america'}, self.queue)
        self.columns_agents = self.queue.get()

        self.family_tree = {}

        self.user = 'FamilyTree'

        self.setWindowTitle(f'Family Tree')
        self.setModal(True)
        self.resize(1080, 720)
        self.setup_ui()
        self.setWindowState(Qt.WindowState.WindowMaximized)
        Globals._Log.info(self.user, 'Successfully initialized.')

    def _build_full_family_tree(self, df):
        users = {row['invitationCode']: dict(row) for _, row in df.iterrows()}
        
        for user in users.values():
            user['children'] = []
            user['parent'] = None

        for code, user in users.copy().items():
            inviter_code = user['inviterCode']
            if inviter_code is None:
                continue
            if inviter_code not in users:
                users[inviter_code] = {
                    'invitationCode': inviter_code,
                    'children': [],
                    'parent': None,
                    'team': None
                }
            users[inviter_code]['children'].append(code)
            user['parent'] = inviter_code
        
        return users
    
    def _display_node_and_relations(self, invitationCode):
        self.clear_layout(self.scene_layout)

        if invitationCode not in self.family_tree:
            self._process_descendants(self.scene_layout)
            return
        
        current_code = invitationCode
        while True:
            parent_code = self.family_tree[current_code].get('parent')
            if not parent_code:
                break
            self.family_tree[parent_code]['children'] = [current_code]
            current_code = parent_code

        self._process_descendants(self.scene_layout, [current_code])

    def _display_top_level_nodes(self):
        self.clear_layout(self.scene_layout)
        top_node = [code for code, node in self.family_tree.items() if not node['parent']]
        self._process_descendants(self.scene_layout, top_node)
    
    def _process_descendants(self, parent_layout: QHBoxLayout, codes, level=0):
        leaf_nodes = {'reals': [], 'robots': []}
        for child_code in codes:
            child_node = self.family_tree[child_code]
            children_codes = child_node['children']
            if not children_codes:
                if child_node['fake']:
                    leaf_nodes['robots'].append(child_code)
                else:
                    leaf_nodes['reals'].append(child_code)
                continue
            frame = Frame(level + 1)
            child_vlayout = frame.layout()
            if child_node.get('phone'):
                button_text = (
                    f'{child_node["phone"]}\n'
                    f'{child_node["invitationCode"]}({child_node["team"]})\n'
                    f'withdraw: {child_node["withdraw"]} recharge: {child_node["recharge"]}\n'
                    f'income: {child_node["income"]} money: {child_node["money"]}'
                )
                button = QPushButton(button_text)
            else:
                button = QPushButton(f'{child_code}({child_node["team"]})')
            button.clicked.connect(lambda _, c=child_code: self._refresh(c))
            button.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
            button.customContextMenuRequested.connect(lambda pos, b=button, c=child_node: self._show_context_menu(b, c))
            child_vlayout.addWidget(button)
            child_hlayout = QHBoxLayout()
            self._process_descendants(child_hlayout, children_codes, level + 1)
            child_vlayout.addLayout(child_hlayout)
            parent_layout.addWidget(frame)

        if leaf_nodes['reals'] or leaf_nodes['robots']:
            frame = Frame(level + 1)
            child_vlayout = frame.layout()
            reals_text = '\n'.join([f'{code}: {self.family_tree[code]["phone"]}' for code in leaf_nodes['reals']])
            button = QPushButton(f'\nreals: {len(leaf_nodes["reals"])}\nrobots: {len(leaf_nodes["robots"])}\n')
            button.setProperty('isLeaf', True)
            button.setToolTip(reals_text)
            button.installEventFilter(self)
            child_vlayout.addWidget(button)
            parent_layout.addWidget(frame)

    def _copy_to_clipboard(self, text):
        clipboard = QGuiApplication.clipboard()
        clipboard.setText(str(text))

    def _show_context_menu(self, button, node):
        menu = QMenu()
        
        copy_user_id_action = QAction("Copy userId", self)
        copy_user_id_action.triggered.connect(lambda: self._copy_to_clipboard(node['userId']))
        menu.addAction(copy_user_id_action)
        
        copy_phone_action = QAction("Copy Phone", self)
        copy_phone_action.triggered.connect(lambda: self._copy_to_clipboard(node['phone']))
        menu.addAction(copy_phone_action)
        
        copy_invitation_code_action = QAction("Copy invitationCode", self)
        copy_invitation_code_action.triggered.connect(lambda: self._copy_to_clipboard(node['invitationCode']))
        menu.addAction(copy_invitation_code_action)
        
        menu.exec(QCursor.pos())

    def eventFilter(self, obj, event):
        if event.type() == QEvent.Type.Enter and obj.property('isLeaf'):
            QToolTip.showText(QCursor.pos(), obj.toolTip(), obj)
        return super().eventFilter(obj, event)

    def _plot_family_tree(self, invitationCode=''):
        self.clear_layout(self.scene_layout)

        Globals._WS.database_operation_signal.emit('read', {'table_name': 'users_america'}, self.queue)
        data = self.queue.get()
        df_users = pd.DataFrame(data, columns=self.columns_users)
        df_users = df_users[['userId', 'phone', 'invitationCode', 'inviterCode', 'withdraw', 'income', 'recharge', 'money', 'team', 'iscreator']]
        df_users[['withdraw', 'income', 'recharge', 'money']] = df_users[['withdraw', 'income', 'recharge', 'money']].astype(float).fillna(0)
        df_users['fake'] = df_users['fake'].fillna(0).astype(int)
        df_users['isAgent'] = 0

        Globals._WS.database_operation_signal.emit('read', {'table_name': 'agents_america'}, self.queue)
        data = self.queue.get()
        df_agents = pd.DataFrame(data, columns=self.columns_agents)
        df_agents = df_agents[['userId', 'userName', 'agentCash', 'agentWithdrawCash', 'team', 'iscreator', 'inviterCode']]
        df_agents.rename(columns={'agentCash': 'income', 'agentWithdrawCash': 'withdraw', 'userName': 'phone'}, inplace=True)
        df_agents[['income', 'withdraw']] = df_agents[['income', 'withdraw']].astype(float).fillna(0)
        df_agents['recharge'] = 0.0
        df_agents['money'] = 0.0
        df_agents['invitationCode'] = df_agents['userId'].astype(str)
        df_agents['isAgent'] = 1

        df = pd.concat([df_users, df_agents], ignore_index=True)

        self.family_tree = self._build_full_family_tree(df)

        if invitationCode:
            self._display_node_and_relations(invitationCode)
        else:
            self._display_top_level_nodes()

        for i in range(self.scene_layout.count()):
            item = self.scene_layout.itemAt(i)
            widget = item.widget()
            if not isinstance(widget, QFrame):
                print(f'error item: {widget}')
                continue
            self._rearrange_layout(widget)

    def _rearrange_layout(self, frame: QFrame, max_width=9):
        vlayout = frame.layout()
        if vlayout.count() < 2:
            return 1, 1

        hlayout = vlayout.itemAt(1).layout()
        sub_vframes = []

        while hlayout.count():
            item = hlayout.takeAt(0)
            if isinstance(item.widget(), QFrame):
                sub_vframes.append(item.widget())
            else:
                print('error')

        layout_rows = [[]]

        def insert_layout(layout_rows, sub_frame, sub_width, sub_height, max_width):
            for row in layout_rows:
                row_width = sum([w for _, w, _ in row])
                if row_width + sub_width <= max_width:
                    row.append((sub_frame, sub_width, sub_height))
                    return layout_rows.index(row)
            
            layout_rows.append([(sub_frame, sub_width, sub_height)])
            return len(layout_rows) - 1

        max_width_used = 0
        total_height_used = 0

        shapes = []
        for sub_frame in sub_vframes:
            sub_width, sub_height = self._rearrange_layout(sub_frame, max_width)
            sub_vlayout = sub_frame.layout()
            is_leaf = sub_vlayout.itemAt(0).widget().property('isLeaf') if sub_vlayout.itemAt(0).widget() else False
            shapes.append((sub_frame, sub_width, sub_height, is_leaf))

        shapes.sort(key=lambda x: (x[1] * x[2], not x[3]), reverse=True)

        for sub_frame, sub_width, sub_height, _ in shapes:
            row_index = insert_layout(layout_rows, sub_frame, sub_width, sub_height, max_width)
            
            max_width_used = max(max_width_used, sum([w for _, w, _ in layout_rows[row_index]]))
            total_height_used = sum([max(h for _, _, h in row) for row in layout_rows])

        for row_index, row in enumerate(layout_rows):
            row_layout = self._get_or_create_row_layout(vlayout, row_index)
            for sub_frame, _, _ in row:
                row_layout.addWidget(sub_frame)

        return max_width_used, total_height_used

    def _get_or_create_row_layout(self, parent_layout: QVBoxLayout, row: int):
        if row < parent_layout.count() - 1:
            return parent_layout.itemAt(row + 1).layout()
        else:
            new_hlayout = QHBoxLayout()
            new_hlayout.setAlignment(Qt.AlignmentFlag.AlignLeft)
            parent_layout.addLayout(new_hlayout)
            return new_hlayout

    def _refresh(self, invitationCode=''):
        self.lineedit_invitationCode.setText(invitationCode)
        self.refresh()

    def _update(self):
        pass

    def clear_layout(self, layout):
        while layout.count():
            item = layout.takeAt(0)
            if not item:
                continue
            widget = item.widget()
            if widget:
                widget.deleteLater()
                continue
            child_layout = item.layout()
            if child_layout:
                self.clear_layout(child_layout)
                child_layout.deleteLater()

    def closeEvent(self, event):
        self.hide()

    def refresh(self):
        invitationCode = self.lineedit_invitationCode.text().strip()
        if not invitationCode:
            self._plot_family_tree()
            return

        Globals._Log.info(self.user, f'Refreshing data for code: {invitationCode}')
        self._plot_family_tree(invitationCode)

    def setup_ui(self):
        main_layout = QVBoxLayout()
        top_bar = QHBoxLayout()

        btn_update = QPushButton("Update")
        btn_update.clicked.connect(self._update)
        top_bar.addWidget(btn_update)

        top_bar.addStretch()

        self.lineedit_invitationCode = QLineEdit()
        top_bar.addWidget(self.lineedit_invitationCode)

        btn_refresh = QPushButton('Refresh')
        btn_refresh.clicked.connect(self.refresh)
        top_bar.addWidget(btn_refresh)

        main_layout.addLayout(top_bar)

        self.scroll_widget = QWidget()
        self.scene_layout = QHBoxLayout(self.scroll_widget)
        self.scene_layout.setAlignment(Qt.AlignmentFlag.AlignTop)
        
        scroll_area = QScrollArea()
        scroll_area.setWidgetResizable(True)
        scroll_area.setWidget(self.scroll_widget)
        
        main_layout.addWidget(scroll_area)

        self.setLayout(main_layout)

    def show(self, invitationCode=''):
        self._refresh(invitationCode)
        super().show()

class Frame(QFrame):
    def __init__(self, level=-1, parent=None):
        super().__init__(parent)
        
        border_colors = [
            QColor(139, 0, 0),     # Dark Red
            QColor(205, 92, 92),   # Indian Red
            QColor(255, 99, 71),   # Tomato
            QColor(255, 69, 0),    # Orange Red
            QColor(255, 165, 0),   # Orange
            QColor(153, 50, 204),  # Dark Orchid
            QColor(75, 0, 130),    # Indigo
            QColor(123, 104, 238), # Medium Slate Blue
            QColor(112, 128, 144)  # Slate Gray
        ]

        level = min(level, len(border_colors) - 1)
        
        border_color = border_colors[level]
        self.setStyleSheet(f"QFrame {{ border: 1px solid {border_color.name()}; }}")
        
        self.setFrameShape(QFrame.Shape.Box)
        self.setFrameShadow(QFrame.Shadow.Plain)

        layout = QVBoxLayout(self)
        layout.setAlignment(Qt.AlignmentFlag.AlignTop)
        layout.setContentsMargins(2, 2, 2, 2)
        layout.setSpacing(0)
        self.setLayout(layout)