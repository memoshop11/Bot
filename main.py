import os
import logging
import asyncio
import aiosqlite
import traceback
from datetime import datetime, timedelta
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command, Text
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import FSInputFile, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.utils.keyboard import ReplyKeyboardBuilder
from aiogram.exceptions import TelegramAPIError, RetryAfter
from dotenv import load_dotenv
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# Настройка логирования
logging.basicConfig(
    filename="memo_bot.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger()

# Загрузка конфигурации
load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_IDS = [int(x) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip()]
DB_PATH = "database.db"

if not BOT_TOKEN or not ADMIN_IDS:
    logger.error("Ошибка: BOT_TOKEN или ADMIN_IDS не заданы в .env")
    raise ValueError("BOT_TOKEN или ADMIN_IDS не заданы в .env")

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()
scheduler = AsyncIOScheduler()

# Определение состояний FSM
class Form(StatesGroup):
    squad_name = State()
    delete_squad = State()
    escort_info = State()
    remove_escort = State()
    balance_amount = State()
    add_order = State()
    ban_permanent = State()
    ban_duration = State()
    restrict_duration = State()
    unban_user = State()
    unrestrict_user = State()
    zero_balance = State()
    profit_user = State()
    support_message = State()
    reply_to_user = State()

# Словарь сообщений
MESSAGES = {
    "error": "⚠️ Произошла ошибка. Пожалуйста, попробуйте снова.",
    "no_access": "🚫 У вас нет доступа к этой команде.",
    "cancel_action": "✅ Действие отменено.",
    "invalid_format": "⚠️ Неверный формат ввода. Пожалуйста, следуйте примеру.",
    "no_squads": "⚠️ Нет созданных сквадов.",
    "no_escorts": "⚠️ Нет зарегистрированных сопровождающих.",
    "squad_deleted": "🏠 Сквад '{squad_name}' успешно расформирован!",
    "balance_added": "💸 Начислено {amount:.2f} руб. пользователю ID {user_id}.",
    "order_added": "📝 Заказ #{order_id} добавлен!\nКлиент: {customer}\nСумма: {amount:.2f} руб.\nОписание: {description}",
    "user_banned": "🚫 Вы заблокированы навсегда.",
    "user_restricted": "⛔ Ваши действия ограничены до {date}.",
    "user_unbanned": "🔒 Бан снят с пользователя @{username}.",
    "user_unrestricted": "🔓 Ограничение снято с пользователя @{username}.",
    "balance_zeroed": "💰 Баланс пользователя ID {user_id} обнулен.",
    "no_data_to_export": "⚠️ Нет данных для экспорта.",
    "export_success": "📤 Данные экспортированы в {filename}.",
    "support_request": "📩 Введите ваш запрос в поддержку:",
    "support_sent": "✅ Запрос отправлен в поддержку!",
    "no_orders": "⚠️ Нет доступных заказов.",
    "no_active_orders": "⚠️ У вас нет активных заказов."
}

# Инициализация базы данных
async def init_db():
    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS squads (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT UNIQUE NOT NULL,
                    total_orders INTEGER DEFAULT 0,
                    total_balance REAL DEFAULT 0,
                    rating REAL DEFAULT 0,
                    rating_count INTEGER DEFAULT 0
                )
            ''')
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS escorts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    telegram_id INTEGER UNIQUE NOT NULL,
                    username TEXT,
                    pubg_id TEXT,
                    squad_id INTEGER,
                    balance REAL DEFAULT 0,
                    reputation INTEGER DEFAULT 0,
                    completed_orders INTEGER DEFAULT 0,
                    rating REAL DEFAULT 0,
                    rating_count INTEGER DEFAULT 0,
                    is_banned INTEGER DEFAULT 0,
                    ban_until TEXT,
                    restrict_until TEXT,
                    rules_accepted INTEGER DEFAULT 0,
                    FOREIGN KEY (squad_id) REFERENCES squads(id)
                )
            ''')
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS orders (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    memo_order_id TEXT UNIQUE NOT NULL,
                    customer_info TEXT,
                    amount REAL NOT NULL,
                    status TEXT DEFAULT 'pending',
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    commission_amount REAL DEFAULT 0,
                    escort_id INTEGER,
                    FOREIGN KEY (escort_id) REFERENCES escorts(id)
                )
            ''')
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS action_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    action_type TEXT NOT NULL,
                    user_id INTEGER,
                    order_id TEXT,
                    description TEXT,
                    action_date TEXT DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS payouts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    escort_id INTEGER,
                    amount REAL NOT NULL,
                    payout_date TEXT DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (escort_id) REFERENCES escorts(id)
                )
            ''')
            await conn.commit()
        logger.info("База данных успешно инициализирована")
    except aiosqlite.Error as e:
        logger.error(f"Ошибка инициализации базы данных: {e}\n{traceback.format_exc()}")
        raise

# Проверка доступа
async def check_access(message: types.Message) -> bool:
    user_id = message.from_user.id
    async with aiosqlite.connect(DB_PATH) as conn:
        cursor = await conn.execute(
            "SELECT is_banned, ban_until, restrict_until FROM escorts WHERE telegram_id = ?",
            (user_id,)
        )
        user = await cursor.fetchone()
    if not user:
        await message.answer("⚠️ Вы не зарегистрированы. Обратитесь к администратору.")
        return False
    is_banned, ban_until, restrict_until = user
    if is_banned:
        if ban_until and datetime.fromisoformat(ban_until) > datetime.now():
            formatted_date = datetime.fromisoformat(ban_until).strftime("%d.%m.%Y %H:%M")
            await message.answer(f"🚫 Вы заблокированы до {formatted_date}.")
            return False
        elif not ban_until:
            await message.answer(MESSAGES["user_banned"])
            return False
    if restrict_until and datetime.fromisoformat(restrict_until) > datetime.now():
        formatted_date = datetime.fromisoformat(restrict_until).strftime("%d.%m.%Y %H:%M")
        await message.answer(f"⛔ Ваши действия ограничены до {formatted_date}.")
        return False
    return True

# Проверка, является ли пользователь администратором
def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS

# Главная админ-клавиатура (группы, размер 1)
def get_admin_keyboard():
    builder = ReplyKeyboardBuilder()
    buttons = [
        "📝 Заказы",
        "🏠 Сквады",
        "👤 Сопровождающие",
        "🚫 Бан/ограничение",
        "💰 Балансы",
        "📈 Отчеты/справка",
        "🔙 Назад"
    ]
    for button in buttons:
        builder.add(types.KeyboardButton(text=button))
    builder.adjust(1)  # Одна кнопка в строке
    return builder.as_markup(resize_keyboard=True)

# Клавиатура для группы "Заказы" (размер 0.5)
def get_orders_keyboard():
    builder = ReplyKeyboardBuilder()
    buttons = ["📝 Добавить заказ", "🔙 Назад"]
    for button in buttons:
        builder.add(types.KeyboardButton(text=button))
    builder.adjust(2)  # Две кнопки в строке
    return builder.as_markup(resize_keyboard=True)

# Клавиатура для группы "Сквады" (размер 0.5)
def get_squads_keyboard():
    builder = ReplyKeyboardBuilder()
    buttons = [
        "🏠 Добавить сквад",
        "📋 Список сквадов",
        "🗑️ Расформировать сквад",
        "🔙 Назад"
    ]
    for button in buttons:
        builder.add(types.KeyboardButton(text=button))
    builder.adjust(2)  # Две кнопки в строке
    return builder.as_markup(resize_keyboard=True)

# Клавиатура для группы "Сопровождающие" (размер 0.5)
def get_escorts_keyboard():
    builder = ReplyKeyboardBuilder()
    buttons = [
        "👤 Добавить сопровождающего",
        "🗑️ Удалить сопровождающего",
        "👥 Пользователи",
        "🔙 Назад"
    ]
    for button in buttons:
        builder.add(types.KeyboardButton(text=button))
    builder.adjust(2)  # Две кнопки в строке
    return builder.as_markup(resize_keyboard=True)

# Клавиатура для группы "Бан/ограничение" (размер 0.5)
def get_ban_restrict_keyboard():
    builder = ReplyKeyboardBuilder()
    buttons = [
        "🚫 Бан навсегда",
        "⏰ Бан на время",
        "⛔ Ограничить",
        "🔒 Снять бан",
        "🔓 Снять ограничение",
        "🔙 Назад"
    ]
    for button in buttons:
        builder.add(types.KeyboardButton(text=button))
    builder.adjust(2)  # Две кнопки в строке
    return builder.as_markup(resize_keyboard=True)

# Клавиатура для группы "Балансы" (размер 0.5)
def get_balances_keyboard():
    builder = ReplyKeyboardBuilder()
    buttons = [
        "💰 Балансы сопровождающих",
        "💸 Начислить",
        "💰 Обнулить баланс",
        "🔙 Назад"
    ]
    for button in buttons:
        builder.add(types.KeyboardButton(text=button))
    builder.adjust(2)  # Две кнопки в строке
    return builder.as_markup(resize_keyboard=True)

# Клавиатура для группы "Отчеты/справка" (размер 0.5)
def get_reports_keyboard():
    builder = ReplyKeyboardBuilder()
    buttons = [
        "📈 Отчет за месяц",
        "📤 Экспорт данных",
        "📜 Журнал действий",
        "📈 Доход пользователя",
        "🔙 Назад"
    ]
    for button in buttons:
        builder.add(types.KeyboardButton(text=button))
    builder.adjust(2)  # Две кнопки в строке
    return builder.as_markup(resize_keyboard=True)

# Клавиатура отмены
def get_cancel_keyboard(admin=False):
    builder = ReplyKeyboardBuilder()
    builder.add(types.KeyboardButton(text="🚫 Отмена"))
    return builder.as_markup(resize_keyboard=True)

# Клавиатура главного меню
def get_menu_keyboard(user_id: int):
    builder = ReplyKeyboardBuilder()
    buttons = ["📩 Поддержка"]
    if is_admin(user_id):
        buttons.append("📖 Админ-панель")
    for button in buttons:
        builder.add(types.KeyboardButton(text=button))
    builder.adjust(1)
    return builder.as_markup(resize_keyboard=True)

# Безопасная отправка сообщений
async def safe_send_message(chat_id, text, **kwargs):
    try:
        await bot.send_message(chat_id, text, **kwargs)
    except RetryAfter as e:
        logger.warning(f"Rate limit: ждем {e.timeout} секунд")
        await asyncio.sleep(e.timeout)
        await bot.send_message(chat_id, text, **kwargs)
    except TelegramAPIError as e:
        logger.error(f"Ошибка отправки сообщения для chat_id {chat_id}: {e}\n{traceback.format_exc()}")
        return False
    return True

# Логирование действий
async def log_action(action_type: str, user_id: int, order_id: str | None, description: str):
    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            await conn.execute(
                "INSERT INTO action_log (action_type, user_id, order_id, description) VALUES (?, ?, ?, ?)",
                (action_type, user_id, order_id, description)
            )
            await conn.commit()
        logger.info(f"Действие '{action_type}' для user_id {user_id}: {description}")
    except aiosqlite.Error as e:
        logger.error(f"Ошибка логирования действия '{action_type}' для user_id {user_id}: {e}\n{traceback.format_exc()}")

# Уведомление админов
async def notify_admins(message: str, reply_to_user_id: int | None = None):
    for admin_id in ADMIN_IDS:
        if reply_to_user_id:
            markup = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="Ответить", callback_data=f"reply_{reply_to_user_id}")]
            ])
            await safe_send_message(admin_id, message, reply_markup=markup)
        else:
            await safe_send_message(admin_id, message)

# Уведомление сквада
async def notify_squad(squad_id: int | None, message: str):
    if squad_id is None:
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute("SELECT telegram_id FROM escorts")
            escorts = await cursor.fetchall()
    else:
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute("SELECT telegram_id FROM escorts WHERE squad_id = ?", (squad_id,))
            escorts = await cursor.fetchall()
    for (telegram_id,) in escorts:
        await safe_send_message(telegram_id, message)

# Информация о скваде
async def get_squad_info(squad_id: int):
    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute(
                "SELECT name, total_orders, total_balance, rating, rating_count FROM squads WHERE id = ?",
                (squad_id,)
            )
            squad = await cursor.fetchone()
            if not squad:
                return None
            cursor = await conn.execute("SELECT COUNT(*) FROM escorts WHERE squad_id = ?", (squad_id,))
            member_count = (await cursor.fetchone())[0]
        return (*squad, member_count)
    except aiosqlite.Error as e:
        logger.error(f"Ошибка получения информации о скваде {squad_id}: {e}\n{traceback.format_exc()}")
        return None

# Экспорт заказов в CSV
async def export_orders_to_csv():
    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute("SELECT * FROM orders")
            orders = await cursor.fetchall()
            if not orders:
                return None
        filename = f"orders_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        with open(filename, 'w', encoding='utf-8') as f:
            f.write("ID,Memo Order ID,Customer Info,Amount,Status,Created At,Commission Amount\n")
            for order in orders:
                f.write(','.join(str(x) for x in order) + '\n')
        return filename
    except (aiosqlite.Error, OSError) as e:
        logger.error(f"Ошибка экспорта заказов: {e}\n{traceback.format_exc()}")
        return None

# Проверка незавершенных заказов
async def check_pending_orders():
    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute(
                "SELECT memo_order_id, customer_info, amount FROM orders WHERE status = 'pending'"
            )
            orders = await cursor.fetchall()
        if orders:
            message = "⏰ Напоминание о незавершенных заказах:\n"
            for order_id, customer, amount in orders:
                message += f"📝 Заказ #{order_id}, клиент: {customer}, сумма: {amount:.2f} руб.\n"
            await notify_admins(message)
        logger.info("Проверка незавершенных заказов выполнена")
    except aiosqlite.Error as e:
        logger.error(f"Ошибка проверки незавершенных заказов: {e}\n{traceback.format_exc()}")

# Обработчик инлайн-кнопки "Ответить"
@dp.callback_query(lambda c: c.data.startswith("reply_"))
async def process_reply_callback(callback: types.CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    if not is_admin(user_id):
        await callback.answer("🚫 У вас нет доступа.")
        return
    try:
        reply_to_user_id = int(callback.data.split("_")[1])
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute("SELECT username FROM escorts WHERE telegram_id = ?", (reply_to_user_id,))
            user = await cursor.fetchone()
            username = user[0] if user else "Unknown"
        await callback.message.answer(
            f"📨 Введите ответ для пользователя @{username} (ID: {reply_to_user_id}):",
            reply_markup=get_cancel_keyboard(True)
        )
        await state.set_state(Form.reply_to_user)
        await state.update_data(reply_to_user_id=reply_to_user_id)
        await callback.answer()
    except (ValueError, aiosqlite.Error) as e:
        logger.error(f"Ошибка в process_reply_callback для {user_id}: {e}\n{traceback.format_exc()}")
        await callback.message.answer("⚠️ Ошибка при обработке ответа.", reply_markup=get_admin_keyboard())
        await state.clear()
        await callback.answer()

@dp.message(Form.reply_to_user)
async def process_reply_to_user(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if message.text == "🚫 Отмена":
        await message.answer(MESSAGES["cancel_action"], reply_markup=get_admin_keyboard())
        await state.clear()
        return
    try:
        data = await state.get_data()
        reply_to_user_id = data.get("reply_to_user_id")
        if not reply_to_user_id:
            await message.answer("⚠️ Ошибка: ID пользователя не найден.", reply_markup=get_admin_keyboard())
            await state.clear()
            return
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute("SELECT username FROM escorts WHERE telegram_id = ?", (reply_to_user_id,))
            user = await cursor.fetchone()
            username = user[0] if user else "Unknown"
        reply_text = message.text.strip()
        if not reply_text:
            await message.answer("⚠️ Ответ не может быть пустым.", reply_markup=get_cancel_keyboard(True))
            return
        await safe_send_message(reply_to_user_id, f"📨 Ответ от поддержки: {reply_text}")
        await message.answer(
            f"✅ Ответ отправлен пользователю @{username} (ID: {reply_to_user_id}).",
            reply_markup=get_admin_keyboard()
        )
        await log_action(
            "reply_to_support",
            user_id,
            None,
            f"Отправлен ответ пользователю @{username} (ID: {reply_to_user_id}): {reply_text}"
        )
        await state.clear()
    except aiosqlite.Error as e:
        logger.error(f"Ошибка базы данных в process_reply_to_user для {user_id}: {e}\n{traceback.format_exc()}")
        await message.answer("⚠️ Ошибка базы данных при отправке ответа.", reply_markup=get_admin_keyboard())
        await state.clear()
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в process_reply_to_user для {user_id}: {e}\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_admin_keyboard())
        await state.clear()

# Команда /start
@dp.message(Command("start"))
async def cmd_start(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    try:
        await message.answer("👋 Добро пожаловать!", reply_markup=get_menu_keyboard(user_id))
        await state.clear()
        logger.info(f"Пользователь {user_id} запустил бота")
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в cmd_start для {user_id}: {e}\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_menu_keyboard(user_id))

# Обработчик админ-панели
@dp.message(Text("📖 Админ-панель"))
async def admin_panel(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer(MESSAGES["no_access"], reply_markup=get_menu_keyboard(user_id))
        return
    try:
        await message.answer("📖 Админ-панель:", reply_markup=get_admin_keyboard())
        await state.clear()
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в admin_panel для {user_id}: {e}\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_menu_keyboard(user_id))

# Обработчик группы "Заказы"
@dp.message(Text("📝 Заказы"))
async def orders_menu(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer(MESSAGES["no_access"], reply_markup=get_menu_keyboard(user_id))
        return
    try:
        await message.answer("📝 Меню заказов:", reply_markup=get_orders_keyboard())
        await state.clear()
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в orders_menu для {user_id}: {e}\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_admin_keyboard())

# Обработчик группы "Сквады"
@dp.message(Text("🏠 Сквады"))
async def squads_menu(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer(MESSAGES["no_access"], reply_markup=get_menu_keyboard(user_id))
        return
    try:
        await message.answer("🏠 Меню сквадов:", reply_markup=get_squads_keyboard())
        await state.clear()
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в squads_menu для {user_id}: {e}\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_admin_keyboard())

# Обработчик группы "Сопровождающие"
@dp.message(Text("👤 Сопровождающие"))
async def escorts_menu(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer(MESSAGES["no_access"], reply_markup=get_menu_keyboard(user_id))
        return
    try:
        await message.answer("👤 Меню сопровождающих:", reply_markup=get_escorts_keyboard())
        await state.clear()
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в escorts_menu для {user_id}: {e}\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_admin_keyboard())

# Обработчик группы "Бан/ограничение"
@dp.message(Text("🚫 Бан/ограничение"))
async def ban_restrict_menu(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer(MESSAGES["no_access"], reply_markup=get_menu_keyboard(user_id))
        return
    try:
        await message.answer("🚫 Меню бана/ограничений:", reply_markup=get_ban_restrict_keyboard())
        await state.clear()
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в ban_restrict_menu для {user_id}: {e}\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_admin_keyboard())

# Обработчик группы "Балансы"
@dp.message(Text("💰 Балансы"))
async def balances_menu(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer(MESSAGES["no_access"], reply_markup=get_menu_keyboard(user_id))
        return
    try:
        await message.answer("💰 Меню балансов:", reply_markup=get_balances_keyboard())
        await state.clear()
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в balances_menu для {user_id}: {e}\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_admin_keyboard())

# Обработчик группы "Отчеты/справка"
@dp.message(Text("📈 Отчеты/справка"))
async def reports_menu(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer(MESSAGES["no_access"], reply_markup=get_menu_keyboard(user_id))
        return
    try:
        await message.answer("📈 Меню отчетов:", reply_markup=get_reports_keyboard())
        await state.clear()
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в reports_menu для {user_id}: {e}\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_admin_keyboard())

# Обработчик добавления сквада
@dp.message(Text("🏠 Добавить сквад"))
async def add_squad(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer(MESSAGES["no_access"], reply_markup=get_menu_keyboard(user_id))
        return
    try:
        await message.answer(
            "🏠 Введите название нового сквада:",
            reply_markup=get_cancel_keyboard(True)
        )
        await state.set_state(Form.squad_name)
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в add_squad для {user_id}: {e}\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_squads_keyboard())
        await state.clear()

@dp.message(Form.squad_name)
async def process_squad_name(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if message.text == "🚫 Отмена":
        await message.answer(MESSAGES["cancel_action"], reply_markup=get_squads_keyboard())
        await state.clear()
        return
    squad_name = message.text.strip()
    if not squad_name:
        await message.answer("⚠️ Название сквада не может быть пустым.", reply_markup=get_cancel_keyboard(True))
        return
    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute("SELECT id FROM squads WHERE name = ?", (squad_name,))
            if await cursor.fetchone():
                await message.answer(f"⚠️ Сквад '{squad_name}' уже существует.", reply_markup=get_cancel_keyboard(True))
                return
            await conn.execute("INSERT INTO squads (name) VALUES (?)", (squad_name,))
            await conn.commit()
        await message.answer(f"🏠 Сквад '{squad_name}' успешно создан!", reply_markup=get_squads_keyboard())
        await log_action("add_squad", user_id, None, f"Создан сквад '{squad_name}'")
        await state.clear()
    except aiosqlite.Error as e:
        logger.error(f"Ошибка базы данных в process_squad_name для {user_id}: {e}\n{traceback.format_exc()}")
        await message.answer("⚠️ Ошибка базы данных при создании сквада.", reply_markup=get_squads_keyboard())
        await state.clear()
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в process_squad_name для {user_id}: {e}\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_squads_keyboard())
        await state.clear()

@dp.message(Text("📋 Список сквадов"))
async def list_squads(message: types.Message):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer(MESSAGES["no_access"], reply_markup=get_menu_keyboard(user_id))
        return
    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute("SELECT id, name FROM squads")
            squads = await cursor.fetchall()
        if not squads:
            await message.answer(MESSAGES["no_squads"], reply_markup=get_squads_keyboard())
            return
        response = "🏠 Список сквадов:\n"
        for squad_id, squad_name in squads:
            squad_info = await get_squad_info(squad_id)
            if squad_info:
                name, total_orders, total_balance, rating, rating_count, member_count = squad_info
                avg_rating = rating / rating_count if rating_count > 0 else 0
                response += (
                    f"📌 {name}\n"
                    f"👥 Участников: {member_count}\n"
                    f"📋 Заказов: {total_orders}\n"
                    f"💰 Баланс: {total_balance:.2f} руб.\n"
                    f"🌟 Рейтинг: {avg_rating:.2f} ⭐ ({rating_count} оценок)\n\n"
                )
        await message.answer(response, reply_markup=get_squads_keyboard())
    except aiosqlite.Error as e:
        logger.error(f"Ошибка базы данных в list_squads для {user_id}: {e}\n{traceback.format_exc()}")
        await message.answer("⚠️ Ошибка базы данных при получении списка сквадов.", reply_markup=get_squads_keyboard())
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в list_squads для {user_id}: {e}\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_squads_keyboard())

@dp.message(Text("🗑️ Расформировать сквад"))
async def delete_squad(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer(MESSAGES["no_access"], reply_markup=get_menu_keyboard(user_id))
        return
    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute("SELECT id, name FROM squads")
            squads = await cursor.fetchall()
        if not squads:
            await message.answer(MESSAGES["no_squads"], reply_markup=get_squads_keyboard())
            await state.clear()
            return
        response = "🏠 Введите название сквада для расформирования:\n"
        for _, name in squads:
            response += f"- {name}\n"
        await message.answer(response, reply_markup=get_cancel_keyboard(True))
        await state.set_state(Form.delete_squad)
    except aiosqlite.Error as e:
        logger.error(f"Ошибка базы данных в delete_squad для {user_id}: {e}\n{traceback.format_exc()}")
        await message.answer("⚠️ Ошибка базы данных при получении списка сквадов.", reply_markup=get_squads_keyboard())
        await state.clear()
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в delete_squad для {user_id}: {e}\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_squads_keyboard())
        await state.clear()

@dp.message(Form.delete_squad)
async def process_delete_squad(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if message.text == "🚫 Отмена":
        await message.answer(MESSAGES["cancel_action"], reply_markup=get_squads_keyboard())
        await state.clear()
        return
    squad_name = message.text.strip()
    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute("SELECT id FROM squads WHERE name = ?", (squad_name,))
            squad = await cursor.fetchone()
            if not squad:
                await message.answer(f"⚠️ Сквад '{squad_name}' не найден.", reply_markup=get_squads_keyboard())
                await state.clear()
                return
            squad_id = squad[0]
            await conn.execute("DELETE FROM squads WHERE id = ?", (squad_id,))
            await conn.execute("UPDATE escorts SET squad_id = NULL WHERE squad_id = ?", (squad_id,))
            await conn.commit()
        await message.answer(MESSAGES["squad_deleted"].format(squad_name=squad_name), reply_markup=get_squads_keyboard())
        await log_action("delete_squad", user_id, None, f"Сквад '{squad_name}' расформирован")
        await state.clear()
    except aiosqlite.Error as e:
        logger.error(f"Ошибка базы данных в process_delete_squad для {user_id}: {e}\n{traceback.format_exc()}")
        await message.answer("⚠️ Ошибка базы данных при удалении сквада.", reply_markup=get_squads_keyboard())
        await state.clear()
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в process_delete_squad для {user_id}: {e}\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_squads_keyboard())
        await state.clear()

@dp.message(Text("👤 Добавить сопровождающего"))
async def add_escort_admin(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer(MESSAGES["no_access"], reply_markup=get_menu_keyboard(user_id))
        return
    try:
        await message.answer(
            "👤 Введите Telegram ID, username (через @), PUBG ID и название сквада через запятую\n"
            "Пример: 123456789, @username, 987654321, SquadName",
            reply_markup=get_cancel_keyboard(True)
        )
        await state.set_state(Form.escort_info)
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в add_escort_admin для {user_id}: {e}\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_escorts_keyboard())
        await state.clear()

@dp.message(Form.escort_info)
async def process_escort_info(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if message.text == "🚫 Отмена":
        await message.answer(MESSAGES["cancel_action"], reply_markup=get_escorts_keyboard())
        await state.clear()
        return
    try:
        data = [x.strip() for x in message.text.split(",", 3)]
        if len(data) != 4:
            await message.answer(
                "⚠️ Неверный формат. Ожидается: Telegram ID, @username, PUBG ID, SquadName",
                reply_markup=get_cancel_keyboard(True)
            )
            return
        telegram_id, username, pubg_id, squad_name = data
        telegram_id = int(telegram_id)
        username = username.lstrip("@")
        if not squad_name:
            await message.answer("⚠️ Название сквада не может быть пустым.", reply_markup=get_cancel_keyboard(True))
            return
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute("SELECT id FROM squads WHERE name = ?", (squad_name,))
            squad = await cursor.fetchone()
            if not squad:
                await message.answer(f"⚠️ Сквад '{squad_name}' не найден.", reply_markup=get_cancel_keyboard(True))
                await state.clear()
                return
            squad_id = squad[0]
            cursor = await conn.execute("SELECT id FROM escorts WHERE telegram_id = ?", (telegram_id,))
            existing_escort = await cursor.fetchone()
            if existing_escort:
                await conn.execute(
                    "UPDATE escorts SET username = ?, pubg_id = ?, squad_id = ?, rules_accepted = 1 WHERE telegram_id = ?",
                    (username, pubg_id, squad_id, telegram_id)
                )
            else:
                await conn.execute(
                    "INSERT INTO escorts (telegram_id, username, pubg_id, squad_id, rules_accepted) VALUES (?, ?, ?, ?, 1)",
                    (telegram_id, username, pubg_id, squad_id)
                )
            await conn.commit()
        await message.answer(
            f"👤 Сопровождающий @{username} добавлен в сквад '{squad_name}'!", reply_markup=get_escorts_keyboard()
        )
        await log_action(
            "add_escort", user_id, None, f"Добавлен сопровождающий @{username} в сквад '{squad_name}'"
        )
        await state.clear()
    except ValueError as e:
        logger.error(f"Ошибка преобразования данных в process_escort_info для {user_id}: {e}\n{traceback.format_exc()}")
        await message.answer(
            "⚠️ Неверный формат Telegram ID или данных.", reply_markup=get_cancel_keyboard(True)
        )
        await state.clear()
    except aiosqlite.Error as e:
        logger.error(f"Ошибка базы данных в process_escort_info для {user_id}: {e}\n{traceback.format_exc()}")
        await message.answer("⚠️ Ошибка базы данных при добавлении сопровождающего.", reply_markup=get_escorts_keyboard())
        await state.clear()
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в process_escort_info для {user_id}: {e}\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_escorts_keyboard())
        await state.clear()

@dp.message(Text("🗑️ Удалить сопровождающего"))
async def remove_escort(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer(MESSAGES["no_access"], reply_markup=get_menu_keyboard(user_id))
        return
    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute("SELECT telegram_id, username FROM escorts")
            escorts = await cursor.fetchall()
        if not escorts:
            await message.answer(MESSAGES["no_escorts"], reply_markup=get_escorts_keyboard())
            await state.clear()
            return
        response = "👤 Введите Telegram ID сопровождающего для удаления:\n"
        for telegram_id, username in escorts:
            response += f"@{username or 'Unknown'} - {telegram_id}\n"
        await message.answer(response, reply_markup=get_cancel_keyboard(True))
        await state.set_state(Form.remove_escort)
    except aiosqlite.Error as e:
        logger.error(f"Ошибка базы данных в remove_escort для {user_id}: {e}\n{traceback.format_exc()}")
        await message.answer("⚠️ Ошибка базы данных при получении списка сопровождающих.", reply_markup=get_escorts_keyboard())
        await state.clear()
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в remove_escort для {user_id}: {e}\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_escorts_keyboard())
        await state.clear()

@dp.message(Form.remove_escort)
async def process_remove_escort(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if message.text == "🚫 Отмена":
        await message.answer(MESSAGES["cancel_action"], reply_markup=get_escorts_keyboard())
        await state.clear()
        return
    try:
        escort_telegram_id = int(message.text.strip())
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute("SELECT username FROM escorts WHERE telegram_id = ?", (escort_telegram_id,))
            escort = await cursor.fetchone()
            if not escort:
                await message.answer(f"⚠️ Сопровождающий с ID {escort_telegram_id} не найден.", reply_markup=get_escorts_keyboard())
                await state.clear()
                return
            username = escort[0]
            await conn.execute("DELETE FROM escorts WHERE telegram_id = ?", (escort_telegram_id,))
            await conn.commit()
        await message.answer(f"👤 Сопровождающий @{username or 'Unknown'} удален!", reply_markup=get_escorts_keyboard())
        await log_action("remove_escort", user_id, None, f"Удален сопровождающий @{username or 'Unknown'}")
        await state.clear()
    except ValueError:
        logger.error(f"Ошибка преобразования ID в process_remove_escort для {user_id}: Неверный формат ID\n{traceback.format_exc()}")
        await message.answer("⚠️ Неверный формат Telegram ID.", reply_markup=get_cancel_keyboard(True))
        await state.clear()
    except aiosqlite.Error as e:
        logger.error(f"Ошибка базы данных в process_remove_escort для {user_id}: {e}\n{traceback.format_exc()}")
        await message.answer("⚠️ Ошибка базы данных при удалении сопровождающего.", reply_markup=get_escorts_keyboard())
        await state.clear()
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в process_remove_escort для {user_id}: {e}\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_escorts_keyboard())
        await state.clear()

@dp.message(Text("📝 Добавить заказ"))
async def add_order(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer(MESSAGES["no_access"], reply_markup=get_menu_keyboard(user_id))
        return
    try:
        await message.answer(
            "📝 Введите ID заказа, описание клиента и сумму через запятую\nПример: ORDER123, Клиент Иванов, 1000.00",
            reply_markup=get_cancel_keyboard(True)
        )
        await state.set_state(Form.add_order)
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в add_order для {user_id}: {e}\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_orders_keyboard())
        await state.clear()

@dp.message(Form.add_order)
async def process_add_order(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if message.text == "🚫 Отмена":
        await message.answer(MESSAGES["cancel_action"], reply_markup=get_orders_keyboard())
        await state.clear()
        return
    try:
        parts = [x.strip() for x in message.text.split(",", 2)]
        if len(parts) != 3:
            await message.answer(
                "⚠️ Неверный формат. Ожидается: ID заказа, описание клиента, сумма",
                reply_markup=get_cancel_keyboard(True)
            )
            return
        order_id, customer, amount = parts
        amount = float(amount)
        if amount <= 0 or not order_id or not customer:
            await message.answer(
                "⚠️ ID заказа и описание не могут быть пустыми, сумма должна быть положительной.",
                reply_markup=get_cancel_keyboard(True)
            )
            return
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute("SELECT id FROM orders WHERE memo_order_id = ?", (order_id,))
            if await cursor.fetchone():
                await message.answer(f"⚠️ Заказ с ID {order_id} уже существует.", reply_markup=get_cancel_keyboard(True))
                return
            await conn.execute(
                '''
                INSERT INTO orders (memo_order_id, customer_info, amount)
                VALUES (?, ?, ?)
                ''', (order_id, customer, amount)
            )
            await conn.commit()
        await message.answer(
            MESSAGES["order_added"].format(order_id=order_id, amount=amount, description=customer, customer=customer),
            reply_markup=get_orders_keyboard()
        )
        await log_action(
            "add_order",
            user_id,
            None,
            f"Добавлен заказ #{order_id}, клиент: {customer}, сумма: {amount:.2f} руб."
        )
        await notify_squad(None, f"📝 Новый заказ #{order_id} добавлен!\nКлиент: {customer}\nСумма: {amount:.2f} руб.")
        await state.clear()
    except ValueError:
        logger.error(f"Ошибка преобразования суммы в process_add_order для {user_id}: Неверный формат\n{traceback.format_exc()}")
        await message.answer("⚠️ Неверный формат суммы.", reply_markup=get_cancel_keyboard(True))
        await state.clear()
    except aiosqlite.Error as e:
        logger.error(f"Ошибка базы данных в process_add_order для {user_id}: {e}\n{traceback.format_exc()}")
        await message.answer("⚠️ Ошибка базы данных при добавлении заказа.", reply_markup=get_orders_keyboard())
        await state.clear()
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в process_add_order для {user_id}: {e}\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_orders_keyboard())
        await state.clear()

@dp.message(Text("💰 Балансы сопровождающих"))
async def list_escort_balances(message: types.Message):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer(MESSAGES["no_access"], reply_markup=get_menu_keyboard(user_id))
        return
    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute(
                "SELECT telegram_id, username, balance FROM escorts WHERE balance > 0 ORDER BY balance DESC"
            )
            escorts = await cursor.fetchall()
        if not escorts:
            await message.answer("⚠️ Нет сопровождающих с положительным балансом.", reply_markup=get_balances_keyboard())
            return
        response = "💰 Балансы сопровождающих:\n"
        for telegram_id, username, balance in escorts:
            response += f"@{username or 'Unknown'} (ID: {telegram_id}): {balance:.2f} руб.\n"
        await message.answer(response, reply_markup=get_balances_keyboard())
    except aiosqlite.Error as e:
        logger.error(f"Ошибка базы данных в list_escort_balances для {user_id}: {e}\n{traceback.format_exc()}")
        await message.answer("⚠️ Ошибка базы данных при получении балансов.", reply_markup=get_balances_keyboard())
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в list_escort_balances для {user_id}: {e}\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_balances_keyboard())

@dp.message(Text("💸 Начислить"))
async def add_balance(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer(MESSAGES["no_access"], reply_markup=get_menu_keyboard(user_id))
        return
    try:
        await message.answer(
            "💸 Введите Telegram ID и сумму для начисления (например, 123456789, 500.00):",
            reply_markup=get_cancel_keyboard(True)
        )
        await state.set_state(Form.balance_amount)
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в add_balance для {user_id}: {e}\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_balances_keyboard())
        await state.clear()

@dp.message(Form.balance_amount)
async def process_balance_amount(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if message.text == "🚫 Отмена":
        await message.answer(MESSAGES["cancel_action"], reply_markup=get_balances_keyboard())
        await state.clear()
        return
    try:
        telegram_id, amount = [x.strip() for x in message.text.split(",", 1)]
        telegram_id = int(telegram_id)
        amount = float(amount)
        if amount <= 0:
            await message.answer("⚠️ Сумма должна быть положительной.", reply_markup=get_cancel_keyboard(True))
            return
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute("SELECT username FROM escorts WHERE telegram_id = ?", (telegram_id,))
            escort = await cursor.fetchone()
            if not escort:
                await message.answer(f"⚠️ Пользователь с ID {telegram_id} не найден.", reply_markup=get_balances_keyboard())
                await state.clear()
                return
            username = escort[0]
            await conn.execute(
                "UPDATE escorts SET balance = balance + ? WHERE telegram_id = ?",
                (amount, telegram_id)
            )
            await conn.commit()
        await message.answer(
            MESSAGES["balance_added"].format(amount=amount, user_id=telegram_id),
            reply_markup=get_balances_keyboard()
        )
        await log_action(
            "add_balance",
            user_id,
            None,
            f"Начислено {amount:.2f} руб. пользователю @{username or 'Unknown'}"
        )
        await state.clear()
    except ValueError:
        logger.error(f"Ошибка преобразования данных в process_balance_amount для {user_id}: Неверный формат\n{traceback.format_exc()}")
        await message.answer("⚠️ Неверный формат Telegram ID или суммы.", reply_markup=get_cancel_keyboard(True))
        await state.clear()
    except aiosqlite.Error as e:
        logger.error(f"Ошибка базы данных в process_balance_amount для {user_id}: {e}\n{traceback.format_exc()}")
        await message.answer("⚠️ Ошибка базы данных при начислении баланса.", reply_markup=get_balances_keyboard())
        await state.clear()
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в process_balance_amount для {user_id}: {e}\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_balances_keyboard())
        await state.clear()

@dp.message(Text("💰 Обнулить баланс"))
async def zero_balance(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer(MESSAGES["no_access"], reply_markup=get_menu_keyboard(user_id))
        return
    try:
        await message.answer(
            "💰 Введите Telegram ID пользователя для обнуления баланса:",
            reply_markup=get_cancel_keyboard(True)
        )
        await state.set_state(Form.zero_balance)
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в zero_balance для {user_id}: {e}\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_balances_keyboard())
        await state.clear()

@dp.message(Form.zero_balance)
async def process_zero_balance(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if message.text == "🚫 Отмена":
        await message.answer(MESSAGES["cancel_action"], reply_markup=get_balances_keyboard())
        await state.clear()
        return
    try:
        telegram_id = int(message.text.strip())
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute("SELECT username FROM escorts WHERE telegram_id = ?", (telegram_id,))
            user = await cursor.fetchone()
            if not user:
                await message.answer(f"⚠️ Пользователь с ID {telegram_id} не найден.", reply_markup=get_balances_keyboard())
                await state.clear()
                return
            username = user[0]
            await conn.execute(
                "UPDATE escorts SET balance = 0 WHERE telegram_id = ?",
                (telegram_id,)
            )
            await conn.commit()
        await message.answer(
            MESSAGES["balance_zeroed"].format(user_id=telegram_id),
            reply_markup=get_balances_keyboard()
        )
        await log_action(
            "zero_balance",
            user_id,
            None,
            f"Баланс пользователя @{username or 'Unknown'} обнулен"
        )
        await state.clear()
    except ValueError:
        logger.error(f"Ошибка преобразования ID в process_zero_balance для {user_id}: Неверный формат\n{traceback.format_exc()}")
        await message.answer("⚠️ Неверный формат Telegram ID.", reply_markup=get_cancel_keyboard(True))
        await state.clear()
    except aiosqlite.Error as e:
        logger.error(f"Ошибка базы данных в process_zero_balance для {user_id}: {e}\n{traceback.format_exc()}")
        await message.answer("⚠️ Ошибка базы данных при обнулении баланса.", reply_markup=get_balances_keyboard())
        await state.clear()
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в process_zero_balance для {user_id}: {e}\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_balances_keyboard())
        await state.clear()

@dp.message(Text("🚫 Бан навсегда"))
async def ban_permanent(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer(MESSAGES["no_access"], reply_markup=get_menu_keyboard(user_id))
        return
    try:
        await message.answer(
            "🚫 Введите Telegram ID пользователя для перманентного бана:",
            reply_markup=get_cancel_keyboard(True)
        )
        await state.set_state(Form.ban_permanent)
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в ban_permanent для {user_id}: {e}\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_ban_restrict_keyboard())
        await state.clear()

@dp.message(Form.ban_permanent)
async def process_ban_permanent(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if message.text == "🚫 Отмена":
        await message.answer(MESSAGES["cancel_action"], reply_markup=get_ban_restrict_keyboard())
        await state.clear()
        return
    try:
        ban_user_id = int(message.text.strip())
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute("SELECT username FROM escorts WHERE telegram_id = ?", (ban_user_id,))
            user = await cursor.fetchone()
            if not user:
                await message.answer(f"⚠️ Пользователь с ID {ban_user_id} не найден.", reply_markup=get_ban_restrict_keyboard())
                await state.clear()
                return
            username = user[0]
            await conn.execute(
                "UPDATE escorts SET is_banned = 1, ban_until = NULL WHERE telegram_id = ?",
                (ban_user_id,)
            )
            await conn.commit()
        await message.answer(f"🚫 Пользователь @{username or 'Unknown'} заблокирован навсегда!", reply_markup=get_ban_restrict_keyboard())
        await log_action("ban_permanent", user_id, None, f"Пользователь @{username or 'Unknown'} заблокирован навсегда")
        await safe_send_message(ban_user_id, MESSAGES["user_banned"])
        await state.clear()
    except ValueError:
        logger.error(f"Ошибка преобразования ID в process_ban_permanent для {user_id}: Неверный формат\n{traceback.format_exc()}")
        await message.answer("⚠️ Неверный формат Telegram ID.", reply_markup=get_cancel_keyboard(True))
        await state.clear()
    except aiosqlite.Error as e:
        logger.error(f"Ошибка базы данных в process_ban_permanent для {user_id}: {e}\n{traceback.format_exc()}")
        await message.answer("⚠️ Ошибка базы данных при бане пользователя.", reply_markup=get_ban_restrict_keyboard())
        await state.clear()
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в process_ban_permanent для {user_id}: {e}\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_ban_restrict_keyboard())
        await state.clear()

@dp.message(Text("⏰ Бан на время"))
async def ban_temporary(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer(MESSAGES["no_access"], reply_markup=get_menu_keyboard(user_id))
        return
    try:
        await message.answer(
            "⏰ Введите Telegram ID и количество дней бана (например, 123456789, 7):",
            reply_markup=get_cancel_keyboard(True)
        )
        await state.set_state(Form.ban_duration)
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в ban_temporary для {user_id}: {e}\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_ban_restrict_keyboard())
        await state.clear()

@dp.message(Form.ban_duration)
async def process_ban_duration(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if message.text == "🚫 Отмена":
        await message.answer(MESSAGES["cancel_action"], reply_markup=get_ban_restrict_keyboard())
        await state.clear()
        return
    try:
        telegram_id, days = [x.strip() for x in message.text.split(",", 1)]
        telegram_id = int(telegram_id)
        days = int(days)
        if days <= 0:
            await message.answer("⚠️ Количество дней должно быть положительным.", reply_markup=get_cancel_keyboard(True))
            await state.clear()
            return
        ban_until = datetime.now() + timedelta(days=days)
        formatted_date = ban_until.strftime("%d.%m.%Y %H:%M")
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute("SELECT username FROM escorts WHERE telegram_id = ?", (telegram_id,))
            user = await cursor.fetchone()
            if not user:
                await message.answer(f"⚠️ Пользователь с ID {telegram_id} не найден.", reply_markup=get_ban_restrict_keyboard())
                await state.clear()
                return
            username = user[0]
            await conn.execute(
                "UPDATE escorts SET is_banned = 1, ban_until = ? WHERE telegram_id = ?",
                (ban_until.isoformat(), telegram_id)
            )
            await conn.commit()
        await message.answer(
            f"⏰ Пользователь @{username or 'Unknown'} заблокирован до {formatted_date}!",
            reply_markup=get_ban_restrict_keyboard()
        )
        await log_action(
            "ban_temporary",
            user_id,
            None,
            f"Пользователь @{username or 'Unknown'} заблокирован до {formatted_date}"
        )
        await safe_send_message(telegram_id, f"🚫 Вы заблокированы до {formatted_date}.")
        await state.clear()
    except ValueError:
        logger.error(f"Ошибка преобразования данных в process_ban_duration для {user_id}: Неверный формат\n{traceback.format_exc()}")
        await message.answer("⚠️ Неверный формат Telegram ID или количества дней.", reply_markup=get_cancel_keyboard(True))
        await state.clear()
    except aiosqlite.Error as e:
        logger.error(f"Ошибка базы данных в process_ban_duration для {user_id}: {e}\n{traceback.format_exc()}")
        await message.answer("⚠️ Ошибка базы данных при бане пользователя.", reply_markup=get_ban_restrict_keyboard())
        await state.clear()
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в process_ban_duration для {user_id}: {e}\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_ban_restrict_keyboard())
        await state.clear()

@dp.message(Text("⛔ Ограничить"))
async def restrict_user(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer(MESSAGES["no_access"], reply_markup=get_menu_keyboard(user_id))
        return
    try:
        await message.answer(
            "⛔ Введите Telegram ID и количество дней ограничения (например, 123456789, 7):",
            reply_markup=get_cancel_keyboard(True)
        )
        await state.set_state(Form.restrict_duration)
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в restrict_user для {user_id}: {e}\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_ban_restrict_keyboard())
        await state.clear()

@dp.message(Form.restrict_duration)
async def process_restrict_duration(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if message.text == "🚫 Отмена":
        await message.answer(MESSAGES["cancel_action"], reply_markup=get_ban_restrict_keyboard())
        await state.clear()
        return
    try:
        telegram_id, days = [x.strip() for x in message.text.split(",", 1)]
        telegram_id = int(telegram_id)
        days = int(days)
        if days <= 0:
            await message.answer("⚠️ Количество дней должно быть положительным.", reply_markup=get_cancel_keyboard(True))
            await state.clear()
            return
        restrict_until = datetime.now() + timedelta(days=days)
        formatted_date = restrict_until.strftime("%d.%m.%Y %H:%M")
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute("SELECT username FROM escorts WHERE telegram_id = ?", (telegram_id,))
            user = await cursor.fetchone()
            if not user:
                await message.answer(f"⚠️ Пользователь с ID {telegram_id} не найден.", reply_markup=get_ban_restrict_keyboard())
                await state.clear()
                return
            username = user[0]
            await conn.execute(
                "UPDATE escorts SET restrict_until = ? WHERE telegram_id = ?",
                (restrict_until.isoformat(), telegram_id)
            )
            await conn.commit()
        await message.answer(
            f"⛔ Пользователь @{username or 'Unknown'} ограничен до {formatted_date}!",
            reply_markup=get_ban_restrict_keyboard()
        )
        await log_action(
            "restrict_user",
            user_id,
            None,
            f"Пользователь @{username or 'Unknown'} ограничен до {formatted_date}"
        )
        await safe_send_message(telegram_id, MESSAGES["user_restricted"].format(date=formatted_date))
        await state.clear()
    except ValueError:
        logger.error(f"Ошибка преобразования данных в process_restrict_duration для {user_id}: Неверный формат\n{traceback.format_exc()}")
        await message.answer("⚠️ Неверный формат Telegram ID или количества дней.", reply_markup=get_cancel_keyboard(True))
        await state.clear()
    except aiosqlite.Error as e:
        logger.error(f"Ошибка базы данных в process_restrict_duration для {user_id}: {e}\n{traceback.format_exc()}")
        await message.answer("⚠️ Ошибка базы данных при ограничении пользователя.", reply_markup=get_ban_restrict_keyboard())
        await state.clear()
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в process_restrict_duration для {user_id}: {e}\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_ban_restrict_keyboard())
        await state.clear()

@dp.message(Text("🔒 Снять бан"))
async def unban_user(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer(MESSAGES["no_access"], reply_markup=get_menu_keyboard(user_id))
        return
    try:
        await message.answer(
            "🔒 Введите Telegram ID пользователя для снятия бана:",
            reply_markup=get_cancel_keyboard(True)
        )
        await state.set_state(Form.unban_user)
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в unban_user для {user_id}: {e}\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_ban_restrict_keyboard())
        await state.clear()

@dp.message(Form.unban_user)
async def process_unban_user(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if message.text == "🚫 Отмена":
        await message.answer(MESSAGES["cancel_action"], reply_markup=get_ban_restrict_keyboard())
        await state.clear()
        return
    try:
        telegram_id = int(message.text.strip())
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute("SELECT username FROM escorts WHERE telegram_id = ?", (telegram_id,))
            user = await cursor.fetchone()
            if not user:
                await message.answer(f"⚠️ Пользователь с ID {telegram_id} не найден.", reply_markup=get_ban_restrict_keyboard())
                await state.clear()
                return
            username = user[0]
            await conn.execute(
                "UPDATE escorts SET is_banned = 0, ban_until = NULL WHERE telegram_id = ?",
                (telegram_id,)
            )
            await conn.commit()
        await message.answer(
            MESSAGES["user_unbanned"].format(username=username or 'Unknown'),
            reply_markup=get_ban_restrict_keyboard()
        )
        await log_action(
            "unban_user",
            user_id,
            None,
            f"Снят бан с пользователя @{username or 'Unknown'}"
        )
        await safe_send_message(telegram_id, "🔒 Ваш бан снят!")
        await state.clear()
    except ValueError:
        logger.error(f"Ошибка преобразования ID в process_unban_user для {user_id}: Неверный формат\n{traceback.format_exc()}")
        await message.answer("⚠️ Неверный формат Telegram ID.", reply_markup=get_cancel_keyboard(True))
        await state.clear()
    except aiosqlite.Error as e:
        logger.error(f"Ошибка базы данных в process_unban_user для {user_id}: {e}\n{traceback.format_exc()}")
        await message.answer("⚠️ Ошибка базы данных при снятии бана.", reply_markup=get_ban_restrict_keyboard())
        await state.clear()
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в process_unban_user для {user_id}: {e}\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_ban_restrict_keyboard())
        await state.clear()

@dp.message(Text("🔓 Снять ограничение"))
async def unrestrict_user(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer(MESSAGES["no_access"], reply_markup=get_menu_keyboard(user_id))
        return
    try:
        await message.answer(
            "🔓 Введите Telegram ID пользователя для снятия ограничения:",
            reply_markup=get_cancel_keyboard(True)
        )
        await state.set_state(Form.unrestrict_user)
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в unrestrict_user для {user_id}: {e}\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_ban_restrict_keyboard())
        await state.clear()

@dp.message(Form.unrestrict_user)
async def process_unrestrict_user(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if message.text == "🚫 Отмена":
        await message.answer(MESSAGES["cancel_action"], reply_markup=get_ban_restrict_keyboard())
        await state.clear()
        return
    try:
        telegram_id = int(message.text.strip())
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute("SELECT username FROM escorts WHERE telegram_id = ?", (telegram_id,))
            user = await cursor.fetchone()
            if not user:
                await message.answer(f"⚠️ Пользователь с ID {telegram_id} не найден.", reply_markup=get_ban_restrict_keyboard())
                await state.clear()
                return
            username = user[0]
            await conn.execute(
                "UPDATE escorts SET restrict_until = NULL WHERE telegram_id = ?",
                (telegram_id,)
            )
            await conn.commit()
        await message.answer(
            MESSAGES["user_unrestricted"].format(username=username or 'Unknown'),
            reply_markup=get_ban_restrict_keyboard()
        )
        await log_action(
            "unrestrict_user",
            user_id,
            None,
            f"Снято ограничение с пользователя @{username or 'Unknown'}"
        )
        await safe_send_message(telegram_id, "🔓 Ваше ограничение снято!")
        await state.clear()
    except ValueError:
        logger.error(f"Ошибка преобразования ID в process_unrestrict_user для {user_id}: Неверный формат\n{traceback.format_exc()}")
        await message.answer("⚠️ Неверный формат Telegram ID.", reply_markup=get_cancel_keyboard(True))
        await state.clear()
    except aiosqlite.Error as e:
        logger.error(f"Ошибка базы данных в process_unrestrict_user для {user_id}: {e}\n{traceback.format_exc()}")
        await message.answer("⚠️ Ошибка базы данных при снятии ограничения.", reply_markup=get_ban_restrict_keyboard())
        await state.clear()
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в process_unrestrict_user для {user_id}: {e}\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_ban_restrict_keyboard())
        await state.clear()

@dp.message(Text("👥 Пользователи"))
async def list_users(message: types.Message):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer(MESSAGES["no_access"], reply_markup=get_menu_keyboard(user_id))
        return
    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute(
                '''
                SELECT telegram_id, username, pubg_id, squad_id, balance, reputation, completed_orders,
                       rating, rating_count, is_banned, ban_until, restrict_until
                FROM escorts
                '''
            )
            users = await cursor.fetchall()
        if not users:
            await message.answer("⚠️ Нет зарегистрированных пользователей.", reply_markup=get_escorts_keyboard())
            return
        response = "👥 Список пользователей:\n"
        for user in users:
            telegram_id, username, pubg_id, squad_id, balance, reputation, completed_orders, rating, rating_count, is_banned, ban_until, restrict_until = user
            avg_rating = rating / rating_count if rating_count > 0 else 0
            squad_name = "Не назначен"
            if squad_id:
                async with aiosqlite.connect(DB_PATH) as conn:
                    cursor = await conn.execute("SELECT name FROM squads WHERE id = ?", (squad_id,))
                    squad = await cursor.fetchone()
                    squad_name = squad[0] if squad else "Неизвестно"
            status = "Активен"
            if is_banned:
                if ban_until:
                    formatted_date = datetime.fromisoformat(ban_until).strftime("%d.%m.%Y %H:%M")
                    status = f"Забанен до {formatted_date}"
                else:
                    status = "Забанен навсегда"
            elif restrict_until and datetime.fromisoformat(restrict_until) > datetime.now():
                formatted_date = datetime.fromisoformat(restrict_until).strftime("%d.%m.%Y %H:%M")
                status = f"Ограничен до {formatted_date}"
            response += (
                f"🔹 @{username or 'Unknown'} (ID: {telegram_id})\n"
                f"🔢 PUBG ID: {pubg_id or 'не указан'}\n"
                f"🏠 Сквад: {squad_name}\n"
                f"💰 Баланс: {balance:.2f} руб.\n"
                f"⭐ Репутация: {reputation}\n"
                f"📋 Заказов: {completed_orders}\n"
                f"🌟 Рейтинг: {avg_rating:.2f} ⭐ ({rating_count} оценок)\n"
                f"🚫 Статус: {status}\n\n"
            )
        await message.answer(response, reply_markup=get_escorts_keyboard())
    except aiosqlite.Error as e:
        logger.error(f"Ошибка базы данных в list_users для {user_id}: {e}\n{traceback.format_exc()}")
        await message.answer("⚠️ Ошибка базы данных при получении списка пользователей.", reply_markup=get_escorts_keyboard())
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в list_users для {user_id}: {e}\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_escorts_keyboard())

@dp.message(Text("📈 Отчет за месяц"))
async def monthly_report(message: types.Message):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer(MESSAGES["no_access"], reply_markup=get_menu_keyboard(user_id))
        return
    try:
        thirty_days_ago = (datetime.now() - timedelta(days=30)).isoformat()
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute(
                '''
                SELECT COUNT(*) as order_count, SUM(amount) as total_amount
                FROM orders
                WHERE created_at >= ?
                ''', (thirty_days_ago,)
            )
            orders_data = await cursor.fetchone()
            cursor = await conn.execute(
                '''
                SELECT COUNT(*) as payout_count, SUM(amount) as total_payout
                FROM payouts
                WHERE payout_date >= ?
                ''', (thirty_days_ago,)
            )
            payouts_data = await cursor.fetchone()
        order_count, total_amount = orders_data
        payout_count, total_payout = payouts_data
        total_amount = total_amount or 0
        total_payout = total_payout or 0
        response = (
            "📈 Отчет за последние 30 дней:\n"
            f"📝 Заказов: {order_count}\n"
            f"💰 Общая сумма заказов: {total_amount:.2f} руб.\n"
            f"💸 Выплат: {payout_count}\n"
            f"💵 Общая сумма выплат: {total_payout:.2f} руб.\n"
        )
        await message.answer(response, reply_markup=get_reports_keyboard())
    except aiosqlite.Error as e:
        logger.error(f"Ошибка базы данных в monthly_report для {user_id}: {e}\n{traceback.format_exc()}")
        await message.answer("⚠️ Ошибка базы данных при формировании отчета.", reply_markup=get_reports_keyboard())
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в monthly_report для {user_id}: {e}\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_reports_keyboard())

@dp.message(Text("📤 Экспорт данных"))
async def export_data(message: types.Message):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer(MESSAGES["no_access"], reply_markup=get_menu_keyboard(user_id))
        return
    try:
        filename = await export_orders_to_csv()
        if not filename:
            await message.answer(MESSAGES["no_data_to_export"], reply_markup=get_reports_keyboard())
            return
        file = FSInputFile(filename)
        await message.answer_document(file, caption=MESSAGES["export_success"].format(filename=filename), reply_markup=get_reports_keyboard())
        await log_action("export_data", user_id, None, f"Экспортированы данные в {filename}")
        os.remove(filename)  # Удаляем файл после отправки
    except (aiosqlite.Error, OSError) as e:
        logger.error(f"Ошибка экспорта данных в export_data для {user_id}: {e}\n{traceback.format_exc()}")
        await message.answer("⚠️ Ошибка при экспорте данных.", reply_markup=get_reports_keyboard())
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в export_data для {user_id}: {e}\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_reports_keyboard())

@dp.message(Text("📜 Журнал действий"))
async def action_log(message: types.Message):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer(MESSAGES["no_access"], reply_markup=get_menu_keyboard(user_id))
        return
    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute(
                '''
                SELECT action_type, user_id, order_id, description, action_date
                FROM action_log
                ORDER BY action_date DESC
                LIMIT 50
                '''
            )
            actions = await cursor.fetchall()
        if not actions:
            await message.answer("⚠️ Журнал действий пуст.", reply_markup=get_reports_keyboard())
            return
        response = "📜 Журнал действий (последние 50):\n"
        for action_type, action_user_id, order_id, description, action_date in actions:
            formatted_date = datetime.fromisoformat(action_date).strftime("%d.%m.%Y %H:%M")
            response += (
                f"🕒 {formatted_date}\n"
                f"👤 Admin ID: {action_user_id}\n"
                f"📋 Тип: {action_type}\n"
                f"ℹ️ Описание: {description}\n"
                f"🔢 Заказ: {order_id or 'Нет'}\n\n"
            )
        await message.answer(response, reply_markup=get_reports_keyboard())
    except aiosqlite.Error as e:
        logger.error(f"Ошибка базы данных в action_log для {user_id}: {e}\n{traceback.format_exc()}")
        await message.answer("⚠️ Ошибка базы данных при получении журнала действий.", reply_markup=get_reports_keyboard())
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в action_log для {user_id}: {e}\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_reports_keyboard())

@dp.message(Text("📈 Доход пользователя"))
async def user_profit(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer(MESSAGES["no_access"], reply_markup=get_menu_keyboard(user_id))
        return
    try:
        await message.answer(
            "📈 Введите Telegram ID пользователя для просмотра дохода:",
            reply_markup=get_cancel_keyboard(True)
        )
        await state.set_state(Form.profit_user)
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в user_profit для {user_id}: {e}\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_reports_keyboard())
        await state.clear()

@dp.message(Form.profit_user)
async def process_user_profit(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if message.text == "🚫 Отмена":
        await message.answer(MESSAGES["cancel_action"], reply_markup=get_reports_keyboard())
        await state.clear()
        return
    try:
        telegram_id = int(message.text.strip())
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute(
                "SELECT username, balance, completed_orders FROM escorts WHERE telegram_id = ?",
                (telegram_id,)
            )
            user = await cursor.fetchone()
            if not user:
                await message.answer(f"⚠️ Пользователь с ID {telegram_id} не найден.", reply_markup=get_reports_keyboard())
                await state.clear()
                return
            username, balance, completed_orders = user
            cursor = await conn.execute(
                '''
                SELECT SUM(amount) as total_payout
                FROM payouts
                WHERE escort_id = (SELECT id FROM escorts WHERE telegram_id = ?)
                ''', (telegram_id,)
            )
            total_payout = (await cursor.fetchone())[0] or 0
        response = (
            f"📈 Доход пользователя @{username or 'Unknown'} (ID: {telegram_id}):\n"
            f"💰 Текущий баланс: {balance:.2f} руб.\n"
            f"📋 Завершенных заказов: {completed_orders}\n"
            f"💸 Всего выплачено: {total_payout:.2f} руб.\n"
        )
        await message.answer(response, reply_markup=get_reports_keyboard())
        await log_action(
            "view_user_profit",
            user_id,
            None,
            f"Просмотрен доход пользователя @{username or 'Unknown'} (ID: {telegram_id})"
        )
        await state.clear()
    except ValueError:
        logger.error(f"Ошибка преобразования ID в process_user_profit для {user_id}: Неверный формат\n{traceback.format_exc()}")
        await message.answer("⚠️ Неверный формат Telegram ID.", reply_markup=get_cancel_keyboard(True))
        await state.clear()
    except aiosqlite.Error as e:
        logger.error(f"Ошибка базы данных в process_user_profit для {user_id}: {e}\n{traceback.format_exc()}")
        await message.answer("⚠️ Ошибка базы данных при получении дохода.", reply_markup=get_reports_keyboard())
        await state.clear()
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в process_user_profit для {user_id}: {e}\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_reports_keyboard())
        await state.clear()

@dp.message(Text("📩 Поддержка"))
async def support_request(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if not await check_access(message):
        return
    try:
        await message.answer(MESSAGES["support_request"], reply_markup=get_cancel_keyboard())
        await state.set_state(Form.support_message)
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в support_request для {user_id}: {e}\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_menu_keyboard(user_id))
        await state.clear()

@dp.message(Form.support_message)
async def process_support_message(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if message.text == "🚫 Отмена":
        await message.answer(MESSAGES["cancel_action"], reply_markup=get_menu_keyboard(user_id))
        await state.clear()
        return
    try:
        support_text = message.text.strip()
        if not support_text:
            await message.answer("⚠️ Запрос не может быть пустым.", reply_markup=get_cancel_keyboard())
            return
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute("SELECT username FROM escorts WHERE telegram_id = ?", (user_id,))
            user = await cursor.fetchone()
            username = user[0] if user else "Unknown"
        await notify_admins(
            f"📩 Новый запрос в поддержку от @{username} (ID: {user_id}):\n{support_text}",
            reply_to_user_id=user_id
        )
        await message.answer(MESSAGES["support_sent"], reply_markup=get_menu_keyboard(user_id))
        await log_action(
            "support_request",
            user_id,
            None,
            f"Отправлен запрос в поддержку: {support_text}"
        )
        await state.clear()
    except aiosqlite.Error as e:
        logger.error(f"Ошибка базы данных в process_support_message для {user_id}: {e}\n{traceback.format_exc()}")
        await message.answer("⚠️ Ошибка базы данных при отправке запроса.", reply_markup=get_menu_keyboard(user_id))
        await state.clear()
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в process_support_message для {user_id}: {e}\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_menu_keyboard(user_id))
        await state.clear()

@dp.message(Text("🔙 Назад"))
async def go_back(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    try:
        await message.answer("🔙 Возврат в главное меню.", reply_markup=get_menu_keyboard(user_id))
        await state.clear()
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в go_back для {user_id}: {e}\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_menu_keyboard(user_id))
        await state.clear()

# Обработчик неизвестных команд
@dp.message()
async def unknown_command(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if not await check_access(message):
        return
    try:
        await message.answer("⚠️ Неизвестная команда. Выберите действие из меню.", reply_markup=get_menu_keyboard(user_id))
        await state.clear()
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в unknown_command для {user_id}: {e}\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_menu_keyboard(user_id))
        await state.clear()

async def on_startup():
    await init_db()
    scheduler.add_job(check_pending_orders, 'interval', hours=24)
    scheduler.start()
    logger.info("Бот запущен")

async def on_shutdown():
    scheduler.shutdown()
    await bot.session.close()
    logger.info("Бот остановлен")

if __name__ == "__main__":
    dp.startup.register(on_startup)
    dp.shutdown.register(on_shutdown)
    asyncio.run(dp.start_polling(bot))
