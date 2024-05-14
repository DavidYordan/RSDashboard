import json

from globals import Globals, ProgressDialog
from graph import FamilyTree
from operate_sqlite import OperateSqlite

def config_init(parent):
    with open('config/config.json', 'r') as file:
        config = json.load(file)

    Globals._ADMIN_USER_AMERICA = config['ADMIN_USER_AMERICA']
    Globals._ADMIN_PASSWORD_AMERICA = config['ADMIN_PASSWORD_AMERICA']
    Globals._BASE_URL_AMERICA = config['BASE_URL_AMERICA']
    Globals._CLIENT_ID = config['CLIENT_ID']
    Globals._CLIENT_UUID = config['CLIENT_UUID']
    Globals._ProgressDialog = ProgressDialog(parent)
    Globals._SQL = OperateSqlite(config['DB_PATH'])
    Globals._FamilyTree = FamilyTree(parent)
    Globals._TELEGRAM_BOT_TOKEN = config['TELEGRAM_BOT_TOKEN']
    Globals._TELEGRAM_CHATID = config['TELEGRAM_CHATID']
    Globals._TO_CAPTCHA_KEY = config['TO_CAPTCHA_KEY']