import sqlite3
from PyQt6.QtCore import (
    QMutex,
    pyqtSlot,
    QObject
)

from globals import Globals

class OperateSqlite(QObject):
    _mutex = QMutex()

    def __init__(self, db_path):
        super().__init__()
        self.db_path = db_path
        self.connection = None
        self.cursor = None
        Globals._WS.database_operation_signal.connect(self._perform_database_operation)
        self.user = 'OperateSqlite'

        self._setup_database()

        Globals._Log.info(self.user, 'Successfully initialized.')

    def _connect(self):
        if not self.connection or not self.cursor:
            try:
                self.connection = sqlite3.connect(self.db_path)
                self.cursor = self.connection.cursor()
            except sqlite3.Error as error:
                self.connection = None
                self.cursor = None
                Globals._Log.error(self.user, f'Error occurred during database connection: {error}')

    @pyqtSlot(str, dict, object)
    def _perform_database_operation(self, operation_type, kwargs, queue=None):
        self._mutex.lock()
        self._connect()
        try:
            method_name = f'_{operation_type}'
            if hasattr(self, method_name):
                method = getattr(self, method_name)
                result = method(kwargs)
            else:
                Globals._Log.error(self.user, f'Invalid operation type: {operation_type}')
                result = None

            if queue:
                queue.put(result)

        except Exception as error:
            Globals._Log.error(self.user, f'Error occurred during database operation {operation_type}: {error}')
            result = None

        finally:
            self._mutex.unlock()

    def _setup_database(self):
        self._connect()

        for table_name, table_info in DBSchema.tables.items():
            columns = table_info['columns']
            primary_key = table_info['primary']

            columns_definitions = ', '.join(f"{column} {data_type}" for column, data_type in columns.items())

            self.cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}';")
            if not self.cursor.fetchone():
                self.cursor.execute(f"CREATE TABLE {table_name} ({columns_definitions}, PRIMARY KEY {primary_key});")
                self.connection.commit()
                Globals._Log.info(self.user, f'Table {table_name} created successfully.')
            else:
                self.cursor.execute(f"PRAGMA table_info({table_name});")
                existing_columns = set(column[1] for column in self.cursor.fetchall())

                for column_name, column_type in columns.items():
                    if column_name not in existing_columns:
                        try:
                            self.cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type};")
                            self.connection.commit()
                            Globals._Log.info(self.user, f'Added column {column_name} to table {table_name}.')
                        except sqlite3.Error as error:
                            Globals._Log.error(self.user, f'Error occurred while adding column {column_name} to {table_name}: {error}')
                            self.connection.rollback()
        
    def _clear_table(self, kwargs):
        table_name = kwargs.get('table_name')
        self.cursor.execute(f"DELETE FROM {table_name};")
        self.connection.commit()
        return self.cursor.rowcount

    def _delete(self, kwargs):
        table_name = kwargs.get('table_name')
        condition = kwargs.get('condition')
        params = kwargs.get('params', [])

        query = f"DELETE FROM {table_name} WHERE {condition}"
        return self._execute_query(query, params)
    
    def _get_table_fields(self, kwargs):
        table_name = kwargs.get('table_name')

        query = f"PRAGMA table_info({table_name});"
        self.cursor.execute(query)
        fields = [row[1] for row in self.cursor.fetchall()]
        return fields

    def _execute_query(self, query, params=None, many=False):
        try:
            if many:
                self.cursor.executemany(query, params)
            else:
                self.cursor.execute(query, params or [])
            if query.strip().upper().startswith('SELECT'):
                return self.cursor.fetchall()
            self.connection.commit()
            return self.cursor.rowcount
        except sqlite3.Error as error:
            self.connection.rollback()
            Globals._Log.error(self.user, f'Error occurred during query execution: {error}: {query} - {params}')
            return None

    def _insert(self, kwargs):
        table_name = kwargs.get('table_name')
        columns = kwargs.get('columns')
        values = kwargs.get('values')
        if not columns:
            data = kwargs.get('data')
            columns = list(data.keys())
            values = tuple(data[col] for col in columns)

        columns_str = ", ".join(columns)
        placeholders = ", ".join("?" for _ in columns)
        query = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"
        return self._execute_query(query, values)

    def _bulk_insert(self, kwargs):
        table_name = kwargs.get('table_name')
        columns = kwargs.get('columns')
        values = kwargs.get('values')
        if not columns:
            datas = kwargs.get('datas')
            columns = list(datas[0].keys())
            values = [tuple(data[col] for col in columns) for data in datas]

        columns_str = ", ".join(columns)
        placeholders = ", ".join("?" for _ in columns)
        query = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"
        return self._execute_query(query, values, many=True)

    def _read(self, kwargs):
        table_name = kwargs.get('table_name')
        columns = kwargs.get('columns')
        condition = kwargs.get('condition')
        params = kwargs.get('params', [])

        columns_str = ", ".join(columns) if columns else '*'
        query = f"SELECT {columns_str} FROM {table_name}"
        if condition:
            query += f" WHERE {condition}"
        return self._execute_query(query, params)

    def _update(self, kwargs):
        table_name = kwargs.get('table_name')
        updates = kwargs.get('updates')
        condition = kwargs.get('condition')
        params = kwargs.get('params', [])

        set_clause = ", ".join([f"{key} = ?" for key in updates.keys()])
        update_params = list(updates.values())

        query = f"UPDATE {table_name} SET {set_clause}"
        if condition:
            query += f" WHERE {condition}"
            params = update_params + params
        else:
            params = update_params

        return self._execute_query(query, params)

    def _upsert(self, kwargs):
        table_name = kwargs.get('table_name')
        columns = kwargs.get('columns')
        values = kwargs.get('values')
        unique_columns = kwargs.get('unique_columns')
        if not columns:
            data = kwargs.get('data')
            columns = list(data.keys())
            values = tuple(data[col] for col in columns)

        columns_str = ", ".join(columns)
        placeholders = ", ".join("?" for _ in values)
        on_conflict_set = ", ".join([f"{col}=excluded.{col}" for col in columns if col not in unique_columns])

        query = f"""
        INSERT INTO {table_name} ({columns_str})
        VALUES ({placeholders})
        ON CONFLICT({", ".join(unique_columns)})
        DO UPDATE SET {on_conflict_set}
        """
        return self._execute_query(query, values)
    
    def _bulk_upsert(self, kwargs):
        table_name = kwargs.get('table_name')
        columns = kwargs.get('columns')
        values = kwargs.get('values')
        unique_columns = kwargs.get('unique_columns')
        if not columns:
            datas = kwargs.get('datas')
            columns = list(datas[0].keys())
            values = [tuple(data[col] for col in columns) for data in datas]

        columns_str = ', '.join(columns)
        placeholders = ', '.join(['?' for _ in columns])
        updates = ', '.join([f"{col}=excluded.{col}" for col in columns if col not in unique_columns])

        query = f'''
        INSERT INTO {table_name} ({columns_str})
        VALUES ({placeholders})
        ON CONFLICT({', '.join(unique_columns)})
        DO UPDATE SET {updates}
        '''

        return self._execute_query(query, values, many=True)

class DBSchema(object):
    tables = {
        "agents_america": {
            "columns": {
                "team": "TEXT",
                "userId": "INTEGER",
                "userName": "TEXT",
                "phone": "TEXT",
                "status": "TEXT",
                "salt": "TEXT",
                "roleIdList": "TEXT",
                "password": "TEXT",
                "mobile": "TEXT",
                "isAgent": "INTEGER",
                "email": "TEXT",
                "createUserId": "INTEGER",
                "createTime": "TEXT",
                "ausername": "TEXT",
                "appUserId": "INTEGER",
                "amobile": "TEXT",
                "agent0Money": "TEXT",
                "agentBankAccount": "TEXT",
                "agentBankAddress": "TEXT",
                "agentBankCode": "TEXT",
                "agentBankName": "TEXT",
                "agentBankUser": "TEXT",
                "agentCash": "TEXT",
                "agentId": "INTEGER",
                "agentRate": "TEXT",
                "agentType": "INTEGER",
                "agentWithdrawCash": "TEXT"
            },
            "primary": "(userId)"
        },
        "auto_creator": {
            "columns": {
                "id": "INTEGER",
                "team": "TEXT",
                "userId": "INTEGER",
                "phone": "TEXT",
                "invitationCode": "TEXT",
                "isAgent": "BOOLEAN",
                "startTime": "TEXT",
                "endTime": "TEXT",
                "expected": "TEXT",
                "remainTasks": "TEXT",
                "isCompleted": "TEXT",
                "updateTime": "TEXT"
            },
            "primary": "(id)"
        },
        # "define_config": {
        #     "columns": {
        #         "name": "TEXT",
        #         "category": "TEXT",
        #         "department": "TEXT",
        #         "value": "TEXT",
        #         "updatetime": "INTEGER"
        #     },
        #     "primary": "(name)"
        # },
        "products_america": {
            "columns": {
                "courseId": "INTEGER",
                "weekGoodNum": "INTEGER",
                "isRecommend": "INTEGER",
                "titleImg": "TEXT",
                "courseType": "INTEGER",
                "img": "TEXT",
                "msgType": "TEXT",
                "bannerId": "TEXT",
                "classifyId": "TEXT",
                "title": "TEXT",
                "payMoney": "TEXT",
                "payNum": "INTEGER",
                "price": "REAL",
                "classificationName": "TEXT",
                "details": "TEXT",
                "bannerImg": "TEXT",
                "goodNum": "INTEGER",
                "over": "TEXT",
                "courseLabel": "TEXT",
                "languageType": "TEXT",
                "isDelete": "INTEGER",
                "viewCounts": "INTEGER",
                "videoType": "INTEGER",
                "bannerName": "TEXT",
                "msgUrl": "TEXT",
                "updateTime": "TEXT",
                "courseDetailsId": "INTEGER",
                "courseCount": "INTEGER",
                "createTime": "TEXT",
                "isPrice": "INTEGER",
                "courseDetailsName": "TEXT",
                "status": "INTEGER"
            },
            "primary": "(courseId)"
        },
        "tokens": {
            "columns": {
                "name": "TEXT",
                "token": "TEXT",
                "expire": "INTEGER"
            },
            "primary": "(name)"
        },
        "users_america": {
            "columns": {
                "team": "TEXT",
                "userId": "INTEGER",
                "userName": "TEXT",
                "phone": "TEXT",
                "avatar": "TEXT",
                "sex": "TEXT",
                "openId": "TEXT",
                "googleId": "TEXT",
                "wxId": "TEXT",
                "wxOpenId": "TEXT",
                "password": "TEXT",
                "createTime": "TEXT",
                "updateTime": "TEXT",
                "appleId": "TEXT",
                "sysPhone": "TEXT",
                "status": "INTEGER",
                "platform": "TEXT",
                "jifen": "TEXT",
                "invitationCode": "TEXT",
                "inviterCode": "TEXT",
                "bankCode": "TEXT",
                "clientid": "TEXT",
                "zhiFuBao": "TEXT",
                "recipient": "TEXT",
                "bankNumber": "TEXT",
                "bankName": "TEXT",
                "bankAddress": "TEXT",
                "zhiFuBaoName": "TEXT",
                "rate": "INTEGER",
                "twoRate": "INTEGER",
                "onLineTime": "TEXT",
                "invitationType": "INTEGER",
                "inviterType": "INTEGER",
                "inviterUrl": "TEXT",
                "inviterCustomId": "INTEGER",
                "agent0Money": "INTEGER",
                "agent1Money": "INTEGER",
                "agent0MoneyDelete": "INTEGER",
                "agent1MoneyDelete": "INTEGER",
                "member": "TEXT",
                "email": "TEXT",
                "firstName": "TEXT",
                "lastName": "TEXT",
                "counts": "TEXT",
                "money": "TEXT",
                "endTime": "TEXT",
                "ausername": "TEXT",
                "amobile": "TEXT",
                "asusername": "TEXT",
                "asmobile": "TEXT",
                "cusername": "TEXT",
                "cmobile": "TEXT",
                "recharge": "TEXT",
                "invitations": "TEXT",
                "withdraw": "TEXT",
                "income": "TEXT",
                "isdeleted": "BOOLEAN",
                "iscreator": "BOOLEAN",
                "realpassword": "TEXT",
                "token": "TEXT"
            },
            "primary": "(userId)"
        }
    }