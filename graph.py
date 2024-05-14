import pandas as pd

from PyQt6.QtCore import (
    Qt
)
from PyQt6.QtWidgets import (
    QDialog,
    QHBoxLayout,
    QLineEdit,
    QPushButton,
    QScrollArea,
    QSizePolicy,
    QSpacerItem,
    QVBoxLayout,
    QWidget
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

    def _add_apacer(self, layout):
        spacer = QSpacerItem(0, 0, QSizePolicy.Policy.Minimum, QSizePolicy.Policy.Expanding)
        layout.addSpacerItem(spacer)

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

        current_code = invitationCode
        while True:
            parent_code = self.family_tree.get(current_code, {}).get('parent')
            if parent_code:
                current_code = parent_code
            else:
                break
        self._process_descendants(self.scene_layout, [current_code])

        # node_vlayout = QVBoxLayout()
        # for code in reversed(ancestor_codes):
        #     node = self.family_tree[code]
        #     if node.get('phone', None):
        #         button_text = (
        #             f'{node["phone"]}\n{node["invitationCode"]}({node["team"]})\n'
        #             f'withdraw: {node["withdraw"]} recharge: {node["recharge"]}\n'
        #             f'income: {node["income"]} money: {node["money"]}'
        #         )
        #         button = QPushButton(button_text)
        #     else:
        #         button = QPushButton(f'{code}')
        #     button.clicked.connect(lambda _, c=code: self._refresh(c))
        #     node_vlayout.addWidget(button)

        # children_codes = self.family_tree[invitationCode]['children']
        # if children_codes:
        #     node_hlayout = QHBoxLayout()
        #     self._process_descendants(node_hlayout, children_codes)
        #     node_vlayout.addLayout(node_hlayout)

        # self.scene_layout.addLayout(node_vlayout)

    def _calculate_width(self, layout: QVBoxLayout):
        if layout.count() < 2:
            return 1
        hlayout = layout.itemAt(1)
        width = 0
        for i in range(hlayout.count()):
            sub_vlayout = hlayout.itemAt(i)
            width += self._calculate_width(sub_vlayout)
        return width

    def _display_top_level_nodes(self):
        self.clear_layout(self.scene_layout)
        top_node = [code for code, node in self.family_tree.items() if not node['parent']]
        self._process_descendants(self.scene_layout, top_node)

    def _rearrange_layout(self, vlayout: QVBoxLayout, max_width=9):
        if vlayout.count() < 2:
            self._add_apacer(vlayout)
            return

        hlayout = vlayout.itemAt(1)
        sub_vlayouts = []
        
        while hlayout.count():
            item = hlayout.takeAt(0)
            if isinstance(item, QVBoxLayout):
                sub_vlayouts.append(item)
            else:
                print('error')

        new_hlayout = QHBoxLayout()
        current_width = 0
        for sub_vlayout in sub_vlayouts:
            sub_vlayout_width = self._calculate_width(sub_vlayout)
            if current_width + sub_vlayout_width > max_width:
                vlayout.insertLayout(vlayout.count() - 1, new_hlayout)
                self._add_apacer(new_hlayout)
                new_hlayout = QHBoxLayout()
                current_width = 0
            new_hlayout.addLayout(sub_vlayout)
            current_width += sub_vlayout_width
            self._rearrange_layout(sub_vlayout, max_width)

        if new_hlayout.count() > 0:
            self._add_apacer(new_hlayout)
            vlayout.insertLayout(vlayout.count() - 1, new_hlayout)

        self._add_apacer(vlayout)
    
    def _process_descendants(self, parent_layout: QHBoxLayout, codes):
        leaf_nodes = {'reals': [], 'robots': []}
        for child_code in codes:
            child_node = self.family_tree[child_code]
            children_codes = child_node['children']
            if not children_codes:
                if child_node['iscreator']:
                    leaf_nodes['robots'].append(child_code)
                else:
                    leaf_nodes['reals'].append(child_code)
                continue
            child_vlayout = QVBoxLayout()
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
            child_vlayout.addWidget(button)
            child_hlayout = QHBoxLayout()
            self._process_descendants(child_hlayout, children_codes)
            child_vlayout.addLayout(child_hlayout)
            parent_layout.addLayout(child_vlayout)

        if leaf_nodes:
            child_vlayout = QVBoxLayout()
            button = QPushButton(f'reals: {len(leaf_nodes["reals"])}\nrobots: {len(leaf_nodes["robots"])}')
            child_vlayout.addWidget(button)
            parent_layout.addLayout(child_vlayout)

    def _plot_family_tree(self, invitationCode=''):
        self.clear_layout(self.scene_layout)

        Globals._WS.database_operation_signal.emit('read', {'table_name': 'users_america'}, self.queue)
        data = self.queue.get()
        df_users = pd.DataFrame(data, columns=self.columns_users)
        df_users = df_users[['userId', 'phone', 'invitationCode', 'inviterCode', 'withdraw', 'income', 'recharge', 'money', 'team', 'iscreator']]
        df_users[['withdraw', 'income', 'recharge', 'money']] = df_users[['withdraw', 'income', 'recharge', 'money']].astype(float).fillna(0)
        df_users['iscreator'] = df_users['iscreator'].fillna(0).astype(int)
        df_users['isAgent'] = 0

        Globals._WS.database_operation_signal.emit('read', {'table_name': 'agents_america'}, self.queue)
        data = self.queue.get()
        df_agents = pd.DataFrame(data, columns=self.columns_agents)
        df_agents = df_agents[['userId', 'userName', 'agentCash', 'agentWithdrawCash', 'team', 'iscreator']]
        df_agents.rename(columns={'agentCash': 'income', 'agentWithdrawCash': 'withdraw', 'userName': 'phone'}, inplace=True)
        df_agents[['income', 'withdraw']] = df_agents[['income', 'withdraw']].astype(float).fillna(0)
        df_agents['recharge'] = 0.0
        df_agents['money'] = 0.0
        df_agents['invitationCode'] = df_agents['userId'].astype(str)
        df_agents['inviterCode'] = df_agents['team']
        df_agents['isAgent'] = 1

        df = pd.concat([df_users, df_agents], ignore_index=True)

        self.family_tree = self._build_full_family_tree(df)

        if invitationCode:
            self._display_node_and_relations(invitationCode)
        else:
            self._display_top_level_nodes()

        for i in range(self.scene_layout.count()):
            item = self.scene_layout.itemAt(i)
            if not isinstance(item, QVBoxLayout):
                print('error layout')
                continue
            self._rearrange_layout(item)

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