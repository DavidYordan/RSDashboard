import asyncio
from PyQt6.QtCore import (
    QRunnable
)
from queue import (
    Empty,
    Queue
)
from telegram import (
    Bot,
    Update
)
from telegram.ext import (
    Application,
    CallbackContext,
    CommandHandler
)

from globals import Globals

class TelegramBot(QRunnable):
    def __init__(self):
        super().__init__()

        self.app = Application.builder().token(Globals._TELEGRAM_BOT_TOKEN).build()
        self.bot = Bot(Globals._TELEGRAM_BOT_TOKEN)
        self.is_running = False
        self.queue = Queue()
        self.setAutoDelete(False)
        self.user = 'TelegramBot'

        Globals._Log.info(self.user, 'Successfully initialized.')

    async def setup_handlers(self):
        self.app.add_handler(CommandHandler("start", self.start))
        self.app.add_handler(CommandHandler("help", self.help))

    async def start(self, update: Update, context: CallbackContext) -> None:
        await update.message.reply_text('Hello! This is the start command. How can I help you?')

    async def help(self, update: Update, context: CallbackContext) -> None:
        await update.message.reply_text('Send /start to test this bot.')

    async def send_text(self, params):
        chat_id = params.get('chat_id', Globals._TELEGRAM_CHATID)
        text = params.get('text', 'Test')
        await self.bot.send_message(chat_id=chat_id, text=text)

    async def send_document(self, chat_id, document_path):
        with open(document_path, 'rb') as document:
            await self.bot.send_document(chat_id=chat_id, document=document)

    async def send_photo(self, params):
        chat_id = params.get('chat_id', Globals._TELEGRAM_CHATID)
        photo_path = params.get('photo_path', 'img/test.jpg')
        with open(photo_path, 'rb') as photo:
            await self.bot.send_photo(chat_id=chat_id, photo=photo)

    def run(self):
        self.is_running = True
        while self.is_running and Globals.is_app_running:
            try:
                func, params = self.queue.get(timeout=3)
            except Empty:
                continue
            asyncio.run(getattr(self, func)(params))
        self.is_running = False

    def start_task(self):
        if not self.is_running:
            Globals.thread_pool.start(self)

    def stop_task(self):
        self.is_running = False