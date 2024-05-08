import logging
import os
import re
import traceback

from datetime import datetime, timedelta
from PyQt6 import (
    QtCore,
    QtWidgets,
    QtGui
)

class Logging(logging.Handler):
    def __init__(self, textedit:QtWidgets.QTextEdit, textlabel:QtGui.QAction):
        super().__init__()
        
        self.date_pattern = re.compile(r'^\d{4}-\d{2}-\d{2}')
        self.textedit = textedit
        self.textlabel = textlabel
        self.setFormatter(logging.Formatter('<font color=\'#%(color)s\'>%(user)s - %(asctime)s - %(levelname)s - %(message)s</font>'))
        self.log_directory = 'logs'
        os.makedirs(self.log_directory, exist_ok=True)
        self.logger = logging.getLogger('RSDashboard')
        self.logger.addHandler(self)
        self.logger.setLevel(logging.INFO)
        self.update_file_handler()

        self.timer = QtCore.QTimer()
        self.setup_daily_log_rotation()

        self.info('Logging', 'Logging successfully initialized.')

    def delete_old_logs(self):
        retention_period = timedelta(days=7)
        now = datetime.now()
        for file in os.listdir(self.log_directory):
            match = self.date_pattern.match(file)
            if not match:
                continue
            file_path = os.path.join(self.log_directory, file)
            file_date = datetime.strptime(match.group(), '%Y-%m-%d')
            if now - file_date >= retention_period:
                os.remove(file_path)
                self.debug('System', f'Deleted old log file: {file}')

    def setup_daily_log_rotation(self):
        now = datetime.now()
        next_run = datetime(now.year, now.month, now.day) + timedelta(days=1)
        self.timer.start(int((next_run - now).total_seconds()) * 1000)
        self.timer.timeout.connect(self.update_file_handler)

    def update_file_handler(self):
        if hasattr(self, 'file_handler'):
            self.logger.removeHandler(self.file_handler)
            self.file_handler.close()

        today = datetime.now().strftime('%Y-%m-%d')
        file_name = f'{self.log_directory}/{today}.log'
        self.file_handler = logging.FileHandler(file_name, encoding='utf-8')
        self.file_handler.setFormatter(logging.Formatter('%(user)s - %(asctime)s - %(levelname)s - %(message)s - %(exc_text)s'))
        self.logger.addHandler(self.file_handler)

        self.delete_old_logs()

    def emit(self, record):
        if not hasattr(record, 'color'):
            record.color = '000000'
        record.exc_text = ''
        if record.exc_info:
            exc_type, exc_value, exc_traceback = record.exc_info
            record.exc_text = ''.join(traceback.format_exception(exc_type, exc_value, exc_traceback))
        
        msg_gui = self.format(record)
        QtCore.QMetaObject.invokeMethod(self.textedit, 'append', QtCore.Qt.ConnectionType.QueuedConnection, QtCore.Q_ARG(str, msg_gui))

    def info(self, user, message, color='3CB371'):
        self.logger.info(f"{user} - {message}", extra={'user': user, 'color': color})

    def error(self, user, message, color='FF0000', exc_info=True):
        self.logger.error(f"{user} - {message}", exc_info=exc_info, extra={'user': user, 'color': color})

    def warning(self, user, message, color='FF8C00'):
        self.logger.warning(f"{user} - {message}", extra={'user': user, 'color': color})

    def debug(self, user, message, color='000000'):
        self.logger.debug(f"{user} - {message}", extra={'user': user, 'color': color})