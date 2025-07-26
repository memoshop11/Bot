import logging
import os
import asyncio
import csv
from datetime import datetime, timedelta
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, types, F
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.filters import CommandStart, Command
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardRemove
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
from aiogram.client.default import DefaultBotProperties
import aiosqlite

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler('memo_bot.log'), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# Загрузка переменных окружения
load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise ValueError("❌ Не указан BOT_TOKEN в .env файле")
ADMIN_IDS = [int(id.strip()) for id in os.getenv("ADMIN_IDS", "").split(",") if id.strip()]
if not ADMIN_IDS:
    raise ValueError("❌ Не указаны ADMIN_IDS в .env файле")
DB_PATH = os.getenv("DB_PATH", "/data/memo_bot.db")  # По умолчанию для Fly.io

# Ссылки на документы
OFFER_URL = "https://telegra.ph/Publichnaya-oferta-07-25-7"
PRIVACY_URL = "https://telegra.ph/Politika-konfidencialnosti-07-19-25"
RULES_URL = "https://telegra.ph/Pravila-07-19-160"

# Инициализация бота
bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
dp = Dispatcher(storage=MemoryStorage())
scheduler = AsyncIOScheduler()

# Константы сообщений
MESSAGES = {
    "welcome": (
        "Добро пожаловать в бота сопровождения PUBG Mobile - Metro Royale! 🎮\n"
        "💼 Комиссия сервиса: 20% от суммы заказа."
    ),
    "no_access": "❌ У вас нет доступа к этой команде.",
    "no_squads": "🏠 Нет доступных сквадов.",
    "no_escorts": "👤 Нет зарегистрированных сопровождающих.",
    "no_orders": "📋 Сейчас нет доступных заказов.",
    "no_active_orders": "📋 У вас нет активных заказов.",
    "error": "⚠️ Произошла ошибка. Попробуйте снова позже.",
    "invalid_format": "❌ Неверный формат ввода. Попробуйте снова.",
    "order_completed": "✅ Заказ #{order_id} завершен пользователем @{username} (Telegram ID: {telegram_id}, PUBG ID: {pubg_id})!",
    "order_already_completed": "⚠️ Заказ #{order_id} уже завершен.",
    "balance_added": "💸 Баланс {amount} руб. начислен пользователю {user_id}",
    "squad_full": "⚠️ Сквад '{squad_name}' уже имеет максимум 6 участников!",
    "squad_too_small": "⚠️ В скваде '{squad_name}' должно быть минимум 2 участника для принятия заказа!",
    "order_added": "📝 Заказ #{order_id} добавлен! Сумма: {amount} руб., Описание: {description}, Клиент: {customer}",
    "rules_not_accepted": "📜 Пожалуйста, примите правила, оферту и политику конфиденциальности.",
    "user_banned": "🚫 Вы заблокированы.",
    "user_restricted": "⛔ Ваш доступ к сопровождениям ограничен до {date}.",
    "balance_zeroed": "💰 Баланс пользователя {user_id} обнулен.",
    "pubg_id_updated": "🔢 PUBG ID успешно обновлен!",
    "ping": "🏓 Бот активен!",
    "order_taken": "📝 Заказ #{order_id} принят сквадом {squad_name}!\nУчастники:\n{participants}",
    "order_not_enough_members": "⚠️ В скваде '{squad_name}' недостаточно участников (минимум 2)!",
    "order_already_in_progress": "⚠️ Заказ #{order_id} уже в наборе или принят!",
    "order_joined": "✅ Вы присоединились к набору для заказа #{order_id}!\nТекущий состав:\n{participants}",
    "order_confirmed": "✅ Заказ #{order_id} подтвержден и принят!\nУчастники:\n{participants}",
    "not_in_squad": "⚠️ Вы не состоите в скваде!",
    "max_participants": "⚠️ Максимум 4 участника для заказа!",
    "rating_submitted": "🌟 Оценка {rating} для заказа #{order_id} сохранена! Репутация обновлена.",
    "rate_order": "🌟 Поставьте оценку за заказ #{order_id} (1-5):",
    "payout_log": "💸 Выплата: @{username} получил {amount} руб. за заказ #{order_id}. Дата: {date}",
    "payout_request": "📥 Запрос выплаты от @{username} на сумму {amount} руб. за заказ #{order_id}",
    "payout_receipt": "🧾 Я, @{username}, получил оплату {amount} руб. за заказ #{order_id}.",
    "export_success": "📤 Данные успешно экспортированы в {filename}!",
    "no_data_to_export": "⚠️ Нет данных для экспорта.",
    "reminder": "⏰ Напоминание: Заказ #{order_id} не завершен более 12 часов! Пожалуйста, завершите его."
}

# Состояния FSM
class Form(StatesGroup):
    squad_name = State()
    escort_info = State()
    remove_escort = State()
    zero_balance = State()
    pubg_id = State()
    balance_amount = State()
    complete_order = State()
    add_order = State()
    ban_duration = State()
    restrict_duration = State()
    rate_order = State()
    ban_permanent = State()
    profit_user = State()
    payout_request = State()

# --- Функции базы данных ---
async def init_db():
    async with aiosqlite.connect(DB_PATH) as conn:
        await conn.executescript('''
            CREATE TABLE IF NOT EXISTS squads (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                rating REAL DEFAULT 0,
                rating_count INTEGER DEFAULT 0
            );
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
                ban_until TIMESTAMP,
                restrict_until TIMESTAMP,
                rules_accepted INTEGER DEFAULT 0,
                FOREIGN KEY (squad_id) REFERENCES squads (id) ON DELETE SET NULL
            );
            CREATE TABLE IF NOT EXISTS orders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                memo_order_id TEXT UNIQUE NOT NULL,
                customer_info TEXT NOT NULL,
                amount REAL NOT NULL,
                commission_amount REAL DEFAULT 0.0,
                status TEXT DEFAULT 'pending',
                squad_id INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                completed_at TIMESTAMP,
                rating INTEGER DEFAULT 0,
                FOREIGN KEY (squad_id) REFERENCES squads (id) ON DELETE SET NULL
            );
            CREATE TABLE IF NOT EXISTS order_escorts (
                order_id INTEGER,
                escort_id INTEGER,
                pubg_id TEXT,
                PRIMARY KEY (order_id, escort_id),
                FOREIGN KEY (order_id) REFERENCES orders (id) ON DELETE CASCADE,
                FOREIGN KEY (escort_id) REFERENCES escorts (id) ON DELETE CASCADE
            );
            CREATE TABLE IF NOT EXISTS order_applications (
                order_id INTEGER,
                escort_id INTEGER,
                squad_id INTEGER,
                pubg_id TEXT,
                PRIMARY KEY (order_id, escort_id),
                FOREIGN KEY (order_id) REFERENCES orders (id) ON DELETE CASCADE,
                FOREIGN KEY (escort_id) REFERENCES escorts (id) ON DELETE CASCADE,
                FOREIGN KEY (squad_id) REFERENCES squads (id) ON DELETE SET NULL
            );
            CREATE TABLE IF NOT EXISTS payouts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                order_id INTEGER,
                escort_id INTEGER,
                amount REAL,
                payout_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (order_id) REFERENCES orders (id) ON DELETE SET NULL,
                FOREIGN KEY (escort_id) REFERENCES escorts (id) ON DELETE SET NULL
            );
            CREATE TABLE IF NOT EXISTS action_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                action_type TEXT,
                user_id INTEGER,
                order_id INTEGER,
                description TEXT,
                action_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE INDEX IF NOT EXISTS idx_escorts_telegram_id ON escorts (telegram_id);
            CREATE INDEX IF NOT EXISTS idx_orders_memo_order_id ON orders (memo_order_id);
            CREATE INDEX IF NOT EXISTS idx_order_escorts_order_id ON order_escorts (order_id);
            CREATE INDEX IF NOT EXISTS idx_order_applications_order_id ON order_applications (order_id);
            CREATE INDEX IF NOT EXISTS idx_payouts_order_id ON payouts (order_id);
            CREATE INDEX IF NOT EXISTS idx_action_log_action_date ON action_log (action_date);
        ''')
        await conn.commit()
    logger.info("База данных успешно инициализирована")

async def log_action(action_type: str, user_id: int, order_id: int = None, description: str = None):
    async with aiosqlite.connect(DB_PATH) as conn:
        await conn.execute(
            "INSERT INTO action_log (action_type, user_id, order_id, description) VALUES (?, ?, ?, ?)",
            (action_type, user_id, order_id, description)
        )
        await conn.commit()
    logger.info(f"Лог действия: {action_type}, user_id: {user_id}, order_id: {order_id}, description: {description}")

async def get_escort(telegram_id: int):
    async with aiosqlite.connect(DB_PATH) as conn:
        cursor = await conn.execute(
            "SELECT id, squad_id, pubg_id, balance, reputation, completed_orders, username, "
            "rating, rating_count, is_banned, ban_until, restrict_until, rules_accepted "
            "FROM escorts WHERE telegram_id = ?", (telegram_id,)
        )
        return await cursor.fetchone()

async def add_escort(telegram_id: int, username: str):
    async with aiosqlite.connect(DB_PATH) as conn:
        await conn.execute(
            "INSERT OR IGNORE INTO escorts (telegram_id, username, rules_accepted) VALUES (?, ?, 0)",
            (telegram_id, username)
        )
        await conn.commit()
    logger.info(f"Добавлен пользователь {telegram_id}")

async def get_squad_escorts(squad_id: int):
    async with aiosqlite.connect(DB_PATH) as conn:
        cursor = await conn.execute(
            "SELECT telegram_id, username, pubg_id, rating FROM escorts WHERE squad_id = ?", (squad_id,)
        )
        return await cursor.fetchall()

async def get_squad_info(squad_id: int):
    async with aiosqlite.connect(DB_PATH) as conn:
        cursor = await conn.execute(
            '''
            SELECT s.name, COUNT(e.id) as member_count,
                   COALESCE(SUM(e.completed_orders), 0) as total_orders,
                   COALESCE(SUM(e.balance), 0) as total_balance,
                   s.rating, s.rating_count
            FROM squads s
            LEFT JOIN escorts e ON e.squad_id = s.id
            WHERE s.id = ?
            GROUP BY s.id
            ''', (squad_id,)
        )
        return await cursor.fetchone()

async def notify_squad(squad_id: int, message: str):
    escorts = await get_squad_escorts(squad_id)
    for telegram_id, _, _, _ in escorts:
        try:
            await bot.send_message(telegram_id, message)
        except Exception as e:
            logger.warning(f"Не удалось уведомить {telegram_id}: {e}")

async def notify_admins(message: str, reply_markup=None):
    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(admin_id, message, reply_markup=reply_markup)
        except Exception as e:
            logger.warning(f"Не удалось уведомить админа {admin_id}: {e}")

async def get_order_applications(order_id: int):
    async with aiosqlite.connect(DB_PATH) as conn:
        cursor = await conn.execute(
            '''
            SELECT e.telegram_id, e.username, e.pubg_id, e.squad_id, s.name
            FROM order_applications oa
            JOIN escorts e ON oa.escort_id = e.id
            LEFT JOIN squads s ON e.squad_id = s.id
            WHERE oa.order_id = ?
            ''', (order_id,)
        )
        return await cursor.fetchall()

async def get_order_info(memo_order_id: str):
    async with aiosqlite.connect(DB_PATH) as conn:
        cursor = await conn.execute(
            "SELECT id, customer_info, amount, status, squad_id, commission_amount FROM orders WHERE memo_order_id = ?",
            (memo_order_id,)
        )
        return await cursor.fetchone()

async def update_escort_reputation(escort_id: int, rating: int):
    async with aiosqlite.connect(DB_PATH) as conn:
        await conn.execute(
            '''
            UPDATE escorts SET reputation = reputation + ?, rating = rating + ?, rating_count = rating_count + 1
            WHERE id = ?
            ''', (rating, rating, escort_id)
        )
        await conn.commit()

async def update_squad_reputation(squad_id: int, rating: int):
    async with aiosqlite.connect(DB_PATH) as conn:
        await conn.execute(
            '''
            UPDATE squads SET rating = rating + ?, rating_count = rating_count + 1
            WHERE id = ?
            ''', (rating, squad_id)
        )
        await conn.commit()

async def get_order_escorts(order_id: int):
    async with aiosqlite.connect(DB_PATH) as conn:
        cursor = await conn.execute(
            '''
            SELECT e.telegram_id, e.username, oe.pubg_id, e.squad_id, s.name
            FROM order_escorts oe
            JOIN escorts e ON oe.escort_id = e.id
            LEFT JOIN squads s ON e.squad_id = s.id
            WHERE oe.order_id = ?
            ''', (order_id,)
        )
        return await cursor.fetchall()

async def export_orders_to_csv():
    async with aiosqlite.connect(DB_PATH) as conn:
        cursor = await conn.execute(
            '''
            SELECT o.memo_order_id, o.customer_info, o.amount, o.commission_amount, o.status, o.created_at, o.completed_at,
                   s.name as squad_name, p.amount as payout_amount, p.payout_date
            FROM orders o
            LEFT JOIN squads s ON o.squad_id = s.id
            LEFT JOIN payouts p ON o.id = p.order_id
            '''
        )
        orders = await cursor.fetchall()

    if not orders:
        return None

    filename = f"orders_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['Order ID', 'Customer', 'Amount', 'Commission', 'Status', 'Created At', 'Completed At', 'Squad', 'Payout Amount', 'Payout Date'])
        for order in orders:
            writer.writerow(order)

    return filename

async def check_pending_orders():
    async with aiosqlite.connect(DB_PATH) as conn:
        cursor = await conn.execute(
            '''
            SELECT id, memo_order_id, squad_id
            FROM orders
            WHERE status = 'in_progress' AND created_at < ?
            ''', ((datetime.now() - timedelta(hours=12)).isoformat(),)
        )
        orders = await cursor.fetchall()

    for order_id, memo_order_id, squad_id in orders:
        await notify_squad(squad_id, MESSAGES["reminder"].format(order_id=memo_order_id))
        await log_action("reminder_sent", None, order_id, f"Напоминание о заказе #{memo_order_id}")

# --- Проверка админских прав ---
def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS

# --- Клавиатуры ---
def get_menu_keyboard(user_id: int):
    keyboard = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📋 Доступные заказы"), KeyboardButton(text="📋 Мои заказы")],
            [KeyboardButton(text="✅ Завершить заказ"), KeyboardButton(text="🌟 Оценить заказ")],
            [KeyboardButton(text="👤 Мой профиль"), KeyboardButton(text="🔢 Ввести PUBG ID")],
            [KeyboardButton(text="ℹ️ Информация")],
            [KeyboardButton(text="📥 Получить выплату")],
            [KeyboardButton(text="🔐 Админ-панель")] if is_admin(user_id) else [],
            [KeyboardButton(text="🔙 На главную")]
        ],
        resize_keyboard=True,
        one_time_keyboard=False
    )
    return keyboard

def get_admin_keyboard():
    keyboard = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🏠 Добавить сквад"), KeyboardButton(text="📋 Список сквадов")],
            [KeyboardButton(text="👤 Добавить сопровождающего"), KeyboardButton(text="🗑️ Удалить сопровождающего")],
            [KeyboardButton(text="💰 Балансы сопровождающих"), KeyboardButton(text="💸 Начислить")],
            [KeyboardButton(text="📊 Статистика"), KeyboardButton(text="📝 Добавить заказ")],
            [KeyboardButton(text="🚫 Бан навсегда"), KeyboardButton(text="⏰ Бан на время")],
            [KeyboardButton(text="⛔ Ограничить"), KeyboardButton(text="👥 Пользователи")],
            [KeyboardButton(text="💰 Обнулить баланс"), KeyboardButton(text="📊 Все балансы")],
            [KeyboardButton(text="📜 Журнал действий"), KeyboardButton(text="📤 Экспорт данных")],
            [KeyboardButton(text="📊 Отчет за месяц"), KeyboardButton(text="📈 Доход пользователя")],
            [KeyboardButton(text="📖 Справочник админ-команд"), KeyboardButton(text="🔙 На главную")]
        ],
        resize_keyboard=True,
        one_time_keyboard=False
    )
    return keyboard

def get_rules_keyboard():
    keyboard = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="✅ Принять условия")],
            [KeyboardButton(text="📜 Политика конфиденциальности")],
            [KeyboardButton(text="📖 Правила")],
            [KeyboardButton(text="📜 Публичная оферта")]
        ],
        resize_keyboard=True,
        one_time_keyboard=True
    )
    return keyboard

def get_order_keyboard(order_id: str):
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Готово", callback_data=f"join_order_{order_id}")]
    ])
    return keyboard

def get_confirmed_order_keyboard(order_id: str):
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Завершить заказ", callback_data=f"complete_order_{order_id}")]
    ])
    return keyboard

def get_rating_keyboard(order_id: str):
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="1 ⭐", callback_data=f"rate_{order_id}_1"),
            InlineKeyboardButton(text="2 ⭐", callback_data=f"rate_{order_id}_2"),
            InlineKeyboardButton(text="3 ⭐", callback_data=f"rate_{order_id}_3"),
            InlineKeyboardButton(text="4 ⭐", callback_data=f"rate_{order_id}_4"),
            InlineKeyboardButton(text="5 ⭐", callback_data=f"rate_{order_id}_5")
        ]
    ])
    return keyboard

# --- Проверка доступа ---
async def check_access(message: types.Message, initial_start: bool = False):
    user_id = message.from_user.id
    try:
        escort = await get_escort(user_id)
        if not escort:
            await add_escort(user_id, message.from_user.username or "Unknown")
            escort = await get_escort(user_id)
        if escort[9]:  # is_banned
            await message.answer(MESSAGES["user_banned"], reply_markup=ReplyKeyboardRemove())
            return False
        if escort[10] and datetime.fromisoformat(escort[10]) > datetime.now():  # ban_until
            await message.answer(MESSAGES["user_banned"], reply_markup=ReplyKeyboardRemove())
            return False
        if escort[11] and datetime.fromisoformat(escort[11]) > datetime.now():  # restrict_until
            await message.answer(MESSAGES["user_restricted"].format(date=escort[11]), reply_markup=ReplyKeyboardRemove())
            return False
        if not escort[12] and initial_start:  # rules_accepted
            await message.answer(MESSAGES["rules_not_accepted"], reply_markup=get_rules_keyboard())
            return False
        return True
    except Exception as e:
        logger.error(f"Ошибка в check_access для {user_id}: {e}")
        await message.answer(MESSAGES["error"], reply_markup=ReplyKeyboardRemove())
        return False

# --- Обработчики ---
@dp.message(CommandStart())
async def cmd_start(message: types.Message):
    user_id = message.from_user.id
    username = message.from_user.username or "Unknown"
    try:
        if not await check_access(message, initial_start=True):
            return
        await message.answer(f"{MESSAGES['welcome']}\n📌 Выберите действие:", reply_markup=get_menu_keyboard(user_id))
        logger.info(f"Пользователь {user_id} (@{username}) запустил бота")
    except Exception as e:
        logger.error(f"Ошибка в cmd_start для {user_id}: {e}")
        await message.answer(MESSAGES["error"], reply_markup=ReplyKeyboardRemove())

@dp.message(Command("ping"))
async def cmd_ping(message: types.Message):
    try:
        await message.answer(MESSAGES["ping"], reply_markup=get_menu_keyboard(message.from_user.id))
    except Exception as e:
        logger.error(f"Ошибка в cmd_ping для {message.from_user.id}: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_menu_keyboard(message.from_user.id))

@dp.message(F.text == "✅ Принять условия")
async def accept_rules(message: types.Message):
    user_id = message.from_user.id
    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            await conn.execute("UPDATE escorts SET rules_accepted = 1 WHERE telegram_id = ?", (user_id,))
            await conn.commit()
        await message.answer(f"✅ Условия приняты! Добро пожаловать!\n📌 Выберите действие:", reply_markup=get_menu_keyboard(user_id))
        logger.info(f"Пользователь {user_id} принял условия")
        await log_action("accept_rules", user_id, None, "Пользователь принял условия")
    except Exception as e:
        logger.error(f"Ошибка в accept_rules для {user_id}: {e}")
        await message.answer(MESSAGES["error"], reply_markup=ReplyKeyboardRemove())

@dp.message(F.text == "🔢 Ввести PUBG ID")
async def enter_pubg_id(message: types.Message, state: FSMContext):
    if not await check_access(message):
        return
    try:
        await message.answer("🔢 Введите ваш PUBG ID:", reply_markup=ReplyKeyboardRemove())
        await state.set_state(Form.pubg_id)
    except Exception as e:
        logger.error(f"Ошибка в enter_pubg_id для {message.from_user.id}: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_menu_keyboard(message.from_user.id))

@dp.message(Form.pubg_id)
async def process_pubg_id(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    pubg_id = message.text.strip()
    if not pubg_id:
        await message.answer(MESSAGES["invalid_format"], reply_markup=get_menu_keyboard(user_id))
        await state.clear()
        return
    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            await conn.execute(
                "UPDATE escorts SET pubg_id = ? WHERE telegram_id = ?",
                (pubg_id, user_id)
            )
            await conn.commit()
        await message.answer(MESSAGES["pubg_id_updated"], reply_markup=get_menu_keyboard(user_id))
        logger.info(f"Пользователь {user_id} обновил PUBG ID: {pubg_id}")
        await log_action("update_pubg_id", user_id, None, f"Обновлен PUBG ID: {pubg_id}")
    except Exception as e:
        logger.error(f"Ошибка в process_pubg_id для {user_id}: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_menu_keyboard(user_id))
    finally:
        await state.clear()

@dp.message(F.text == "ℹ️ Информация")
async def info_handler(message: types.Message):
    if not await check_access(message):
        return
    try:
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📜 Политика конфиденциальности", url=PRIVACY_URL)],
            [InlineKeyboardButton(text="📖 Правила", url=RULES_URL)],
            [InlineKeyboardButton(text="📜 Публичная оферта", url=OFFER_URL)],
            [InlineKeyboardButton(text="ℹ️ О проекте", callback_data="about_project")]
        ])
        response = (
            "ℹ️ Информация о боте:\n"
            "💼 Комиссия сервиса: 20% от суммы заказа."
        )
        await message.answer(response, reply_markup=keyboard)
    except Exception as e:
        logger.error(f"Ошибка в info_handler: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_menu_keyboard(message.from_user.id))

@dp.callback_query(F.data == "about_project")
async def about_project(callback: types.CallbackQuery):
    try:
        response = (
            "ℹ️ О проекте:\n"
            "Этот бот предназначен для распределения заказов по сопровождению в Metro Royale. "
            "Все действия фиксируются, выплаты прозрачны."
        )
        await callback.message.answer(response, reply_markup=get_menu_keyboard(callback.from_user.id))
        await callback.answer()
    except Exception as e:
        logger.error(f"Ошибка в about_project для {callback.from_user.id}: {e}")
        await callback.message.answer(MESSAGES["error"], reply_markup=get_menu_keyboard(callback.from_user.id))
        await callback.answer()

@dp.message(F.text.in_(["📜 Политика конфиденциальности", "📖 Правила", "📜 Публичная оферта"]))
async def rules_links(message: types.Message):
    if not await check_access(message):
        return
    try:
        if message.text == "📜 Политика конфиденциальности":
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="📜 Политика конфиденциальности", url=PRIVACY_URL)]
            ])
            await message.answer("📜 Политика конфиденциальности:", reply_markup=keyboard)
        elif message.text == "📖 Правила":
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="📖 Правила", url=RULES_URL)]
            ])
            await message.answer("📖 Правила:", reply_markup=keyboard)
        else:
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="📜 Публичная оферта", url=OFFER_URL)]
            ])
            await message.answer("📜 Публичная оферта:", reply_markup=keyboard)
    except Exception as e:
        logger.error(f"Ошибка в rules_links: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_menu_keyboard(message.from_user.id))

@dp.message(F.text == "👤 Мой профиль")
async def my_profile(message: types.Message):
    if not await check_access(message):
        return
    user_id = message.from_user.id
    try:
        escort = await get_escort(user_id)
        if not escort:
            await add_escort(user_id, message.from_user.username or "Unknown")
            escort = await get_escort(user_id)
        escort_id, squad_id, pubg_id, balance, reputation, completed_orders, username, rating, rating_count, _, ban_until, restrict_until, _ = escort
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute("SELECT name FROM squads WHERE id = ?", (squad_id,))
            squad = await cursor.fetchone()
        avg_rating = rating / rating_count if rating_count > 0 else 0
        response = (
            f"👤 Ваш профиль:\n"
            f"🔹 Username: @{username or 'Unknown'}\n"
            f"🔹 PUBG ID: {pubg_id or 'не указан'}\n"
            f"🏠 Сквад: {squad[0] if squad else 'не назначен'}\n"
            f"💰 Баланс: {balance:.2f} руб.\n"
            f"⭐ Репутация: {reputation}\n"
            f"📊 Выполнено заказов: {completed_orders}\n"
            f"🌟 Рейтинг: {avg_rating:.2f} ⭐ ({rating_count} оценок)\n"
        )
        await message.answer(response, reply_markup=get_menu_keyboard(user_id))
    except Exception as e:
        logger.error(f"Ошибка в my_profile для {user_id}: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_menu_keyboard(user_id))

@dp.message(F.text == "📋 Доступные заказы")
async def available_orders(message: types.Message):
    if not await check_access(message):
        return
    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute(
                "SELECT id, memo_order_id, customer_info, amount FROM orders WHERE status = 'pending'"
            )
            orders = await cursor.fetchall()
        if not orders:
            await message.answer(MESSAGES["no_orders"], reply_markup=get_menu_keyboard(message.from_user.id))
            return
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=f"#{order_id} - {customer}, {amount:.2f} руб.", callback_data=f"select_order_{db_id}")]
            for db_id, order_id, customer, amount in orders
        ])
        await message.answer("📋 Доступные заказы:", reply_markup=keyboard)
    except Exception as e:
        logger.error(f"Ошибка в available_orders для {message.from_user.id}: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_menu_keyboard(message.from_user.id))

@dp.message(F.text == "📋 Мои заказы")
async def my_orders(message: types.Message):
    if not await check_access(message):
        return
    user_id = message.from_user.id
    try:
        escort = await get_escort(user_id)
        if not escort:
            await message.answer("⚠️ Ваш профиль не найден.", reply_markup=get_menu_keyboard(user_id))
            return
        escort_id = escort[0]
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute(
                '''
                SELECT o.memo_order_id, o.customer_info, o.amount, o.status
                FROM orders o
                JOIN order_escorts oe ON o.id = oe.order_id
                JOIN escorts e ON oe.escort_id = e.id
                WHERE e.telegram_id = ?
                ''', (user_id,)
            )
            orders = await cursor.fetchall()
        if not orders:
            await message.answer(MESSAGES["no_active_orders"], reply_markup=get_menu_keyboard(user_id))
            return
        response = "📋 Ваши заказы:\n"
        for order_id, customer, amount, status in orders:
            status_text = "Ожидает" if status == "pending" else "В процессе" if status == "in_progress" else "Завершен"
            response += f"#{order_id} - {customer}, {amount:.2f} руб., Статус: {status_text}\n"
        await message.answer(response, reply_markup=get_menu_keyboard(user_id))
    except Exception as e:
        logger.error(f"Ошибка в my_orders для {user_id}: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_menu_keyboard(user_id))

@dp.message(F.text == "✅ Завершить заказ")
async def complete_order(message: types.Message, state: FSMContext):
    if not await check_access(message):
        return
    user_id = message.from_user.id
    try:
        escort = await get_escort(user_id)
        if not escort:
            await message.answer("⚠️ Ваш профиль не найден.", reply_markup=get_menu_keyboard(user_id))
            return
        escort_id = escort[0]
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute(
                '''
                SELECT o.memo_order_id, o.id, o.squad_id, o.amount
                FROM orders o
                JOIN order_escorts oe ON o.id = oe.order_id
                JOIN escorts e ON oe.escort_id = e.id
                WHERE e.telegram_id = ? AND o.status = 'in_progress'
                ''', (user_id,)
            )
            orders = await cursor.fetchall()
        if not orders:
            await message.answer(MESSAGES["no_active_orders"], reply_markup=get_menu_keyboard(user_id))
            return
        response = "✅ Введите ID заказа для завершения:\n"
        for order_id, _, _, amount in orders:
            response += f"#{order_id} - {amount:.2f} руб.\n"
        await message.answer(response, reply_markup=ReplyKeyboardRemove())
        await state.set_state(Form.complete_order)
    except Exception as e:
        logger.error(f"Ошибка в complete_order для {user_id}: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_menu_keyboard(user_id))

@dp.message(Form.complete_order)
async def process_complete_order(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    order_id = message.text.strip()
    try:
        escort = await get_escort(user_id)
        if not escort:
            await message.answer("⚠️ Ваш профиль не найден.", reply_markup=get_menu_keyboard(user_id))
            await state.clear()
            return
        escort_id, _, pubg_id, _, _, _, username, _, _, _, _, _, _ = escort
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute(
                "SELECT id, status FROM orders WHERE memo_order_id = ?",
                (order_id,)
            )
            order = await cursor.fetchone()
            if not order or order[1] != "in_progress":
                await message.answer(MESSAGES["order_already_completed"].format(order_id=order_id), reply_markup=get_menu_keyboard(user_id))
                await state.clear()
                return
            order_db_id = order[0]
            await conn.execute(
                "UPDATE orders SET status = 'completed', completed_at = ? WHERE id = ?",
                (datetime.now().isoformat(), order_db_id)
            )
            await conn.commit()
        await message.answer(
            MESSAGES["order_completed"].format(
                order_id=order_id,
                username=username or "Unknown",
                telegram_id=user_id,
                pubg_id=pubg_id or "не указан"
            ),
            reply_markup=get_menu_keyboard(user_id)
        )
        await notify_admins(
            MESSAGES["order_completed"].format(
                order_id=order_id,
                username=username or "Unknown",
                telegram_id=user_id,
                pubg_id=pubg_id or "не указан"
            )
        )
        await log_action("complete_order", user_id, order_db_id, f"Заказ #{order_id} завершен пользователем @{username}")
        await state.clear()
    except Exception as e:
        logger.error(f"Ошибка в process_complete_order для {user_id}: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_menu_keyboard(user_id))
        await state.clear()

@dp.message(F.text == "🌟 Оценить заказ")
async def rate_order_start(message: types.Message, state: FSMContext):
    if not await check_access(message):
        return
    user_id = message.from_user.id
    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute(
                '''
                SELECT o.memo_order_id, o.id, o.squad_id, o.amount
                FROM orders o
                JOIN order_escorts oe ON o.id = oe.order_id
                JOIN escorts e ON oe.escort_id = e.id
                WHERE o.status = 'completed' AND o.rating = 0 AND e.telegram_id = ?
                ''', (user_id,)
            )
            orders = await cursor.fetchall()
        if not orders:
            await message.answer("⚠️ Нет заказов для оценки.", reply_markup=get_menu_keyboard(user_id))
            return
        response = "🌟 Введите ID заказа для оценки:\n"
        for order_id, _, _, amount in orders:
            response += f"#{order_id} - {amount:.2f} руб.\n"
        await message.answer(response, reply_markup=ReplyKeyboardRemove())
        await state.set_state(Form.rate_order)
    except Exception as e:
        logger.error(f"Ошибка в rate_order_start для {user_id}: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_menu_keyboard(user_id))

@dp.message(Form.rate_order)
async def process_rate_order(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    order_id = message.text.strip()
    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute(
                '''
                SELECT o.id, o.squad_id
                FROM orders o
                JOIN order_escorts oe ON o.id = oe.order_id
                JOIN escorts e ON oe.escort_id = e.id
                WHERE o.memo_order_id = ? AND o.status = 'completed' AND o.rating = 0 AND e.telegram_id = ?
                ''', (order_id, user_id)
            )
            order = await cursor.fetchone()
            if not order:
                await message.answer("⚠️ Заказ не найден, не завершен или уже оценен.", reply_markup=get_menu_keyboard(user_id))
                await state.clear()
                return
            order_db_id, squad_id = order
        rating_keyboard = get_rating_keyboard(order_id)
        await message.answer(MESSAGES["rate_order"].format(order_id=order_id), reply_markup=rating_keyboard)
        await state.clear()
    except Exception as e:
        logger.error(f"Ошибка в process_rate_order для {user_id}: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_menu_keyboard(user_id))
        await state.clear()

@dp.callback_query(F.data.startswith("rate_"))
async def rate_order(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    try:
        _, order_id, rating = callback.data.split("_")
        rating = int(rating)
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute(
                '''
                SELECT o.id, o.squad_id
                FROM orders o
                JOIN order_escorts oe ON o.id = oe.order_id
                JOIN escorts e ON oe.escort_id = e.id
                WHERE o.memo_order_id = ? AND o.status = 'completed' AND e.telegram_id = ?
                ''', (order_id, user_id)
            )
            order = await cursor.fetchone()
            if not order:
                await callback.message.answer("⚠️ Заказ не найден или не завершен.", reply_markup=get_menu_keyboard(user_id))
                await callback.answer()
                return
            order_db_id, squad_id = order
            cursor = await conn.execute(
                "SELECT escort_id FROM order_escorts WHERE order_id = ?", (order_db_id,)
            )
            escorts = await cursor.fetchall()
            for (escort_id,) in escorts:
                await update_escort_reputation(escort_id, rating)
            await update_squad_reputation(squad_id, rating)
            await conn.execute(
                "UPDATE orders SET rating = ? WHERE id = ?", (rating, order_db_id)
            )
            await conn.commit()
        await callback.message.edit_text(MESSAGES["rating_submitted"].format(rating=rating, order_id=order_id), reply_markup=None)
        await notify_squad(squad_id, f"🌟 Заказ #{order_id} получил оценку {rating}!")
        await log_action("rate_order", user_id, order_db_id, f"Оценка {rating} для заказа #{order_id}")
        await callback.answer()
    except Exception as e:
        logger.error(f"Ошибка в rate_order для {user_id}: {e}")
        await callback.message.answer(MESSAGES["error"], reply_markup=get_menu_keyboard(user_id))
        await callback.answer()

@dp.message(F.text == "📥 Получить выплату")
async def request_payout(message: types.Message, state: FSMContext):
    if not await check_access(message):
        return
    user_id = message.from_user.id
    try:
        escort = await get_escort(user_id)
        if not escort:
            await message.answer("⚠️ Ваш профиль не найден.", reply_markup=get_menu_keyboard(user_id))
            return
        escort_id = escort[0]
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute(
                '''
                SELECT o.memo_order_id, o.id, o.amount
                FROM orders o
                JOIN order_escorts oe ON o.id = oe.order_id
                JOIN escorts e ON oe.escort_id = e.id
                WHERE e.telegram_id = ? AND o.status = 'completed'
                AND NOT EXISTS (
                    SELECT 1 FROM payouts p WHERE p.order_id = o.id AND p.escort_id = e.id
                )
                ''', (user_id,)
            )
            orders = await cursor.fetchall()
        if not orders:
            await message.answer("⚠️ Нет завершенных заказов для выплаты.", reply_markup=get_menu_keyboard(user_id))
            return
        response = "📥 Введите ID заказа для запроса выплаты:\n"
        for order_id, _, amount in orders:
            response += f"#{order_id} - {amount:.2f} руб.\n"
        await message.answer(response, reply_markup=ReplyKeyboardRemove())
        await state.set_state(Form.payout_request)
    except Exception as e:
        logger.error(f"Ошибка в request_payout для {user_id}: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_menu_keyboard(user_id))

@dp.message(Form.payout_request)
async def process_payout_request(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    order_id = message.text.strip()
    try:
        escort = await get_escort(user_id)
        if not escort:
            await message.answer("⚠️ Ваш профиль не найден.", reply_markup=get_menu_keyboard(user_id))
            await state.clear()
            return
        escort_id = escort[0]
        username = escort[6] or "Unknown"
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute(
                '''
                SELECT o.id, o.amount
                FROM orders o
                JOIN order_escorts oe ON o.id = oe.order_id
                JOIN escorts e ON oe.escort_id = e.id
                WHERE o.memo_order_id = ? AND o.status = 'completed' AND e.telegram_id = ?
                ''',
                (order_id, user_id)
            )
            order = await cursor.fetchone()
            if not order:
                await message.answer("⚠️ Заказ не найден или не завершен.", reply_markup=get_menu_keyboard(user_id))
                await state.clear()
                return
            order_db_id, amount = order
            cursor = await conn.execute(
                "SELECT COUNT(*) FROM payouts WHERE order_id = ? AND escort_id = ?",
                (order_db_id, escort_id)
            )
            if (await cursor.fetchone())[0] > 0:
                await message.answer("⚠️ Выплата по этому заказу уже произведена.", reply_markup=get_menu_keyboard(user_id))
                await state.clear()
                return
            commission = amount * 0.2
            payout_amount = amount - commission
            await conn.execute(
                "INSERT INTO payouts (order_id, escort_id, amount) VALUES (?, ?, ?)",
                (order_db_id, escort_id, payout_amount)
            )
            await conn.execute(
                "UPDATE escorts SET balance = balance + ? WHERE id = ?",
                (payout_amount, escort_id)
            )
            await conn.execute(
                "UPDATE orders SET commission_amount = ? WHERE id = ?",
                (commission, order_db_id)
            )
            await conn.commit()
        await message.answer(
            MESSAGES["payout_receipt"].format(username=username, amount=payout_amount, order_id=order_id),
            reply_markup=get_menu_keyboard(user_id)
        )
        await notify_admins(
            MESSAGES["payout_request"].format(username=username, amount=payout_amount, order_id=order_id)
        )
        await log_action("payout_request", user_id, order_db_id, f"Запрос выплаты {payout_amount:.2f} руб. за заказ #{order_id}")
        await state.clear()
    except Exception as e:
        logger.error(f"Ошибка в process_payout_request для {user_id}: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_menu_keyboard(user_id))
        await state.clear()

@dp.callback_query(F.data.startswith("select_order_"))
async def select_order(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    try:
        order_db_id = int(callback.data.split("_")[-1])
        await callback.message.edit_text("📝 Вы выбрали заказ. Нажмите 'Готово', чтобы присоединиться.", reply_markup=get_order_keyboard(order_db_id))
        await callback.answer()
    except Exception as e:
        logger.error(f"Ошибка в select_order для {user_id}: {e}")
        await callback.message.answer(MESSAGES["error"], reply_markup=get_menu_keyboard(user_id))
        await callback.answer()

@dp.callback_query(F.data.startswith("join_order_"))
async def join_order(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    try:
        escort = await get_escort(user_id)
        if not escort:
            await callback.message.answer("⚠️ Ваш профиль не найден. Обратитесь к администратору.", reply_markup=get_menu_keyboard(user_id))
            await callback.answer()
            return
        if not escort[2]:
            await callback.message.answer("⚠️ Укажите ваш PUBG ID!", reply_markup=get_menu_keyboard(user_id))
            await callback.answer()
            return
        if not escort[1]:
            await callback.message.answer(MESSAGES["not_in_squad"], reply_markup=get_menu_keyboard(user_id))
            await callback.answer()
            return
        escort_id = escort[0]
        pubg_id = escort[2]
        squad_id = escort[1]
        order_db_id = int(callback.data.split("_")[-1])
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute("SELECT status FROM orders WHERE id = ?", (order_db_id,))
            order = await cursor.fetchone()
            if not order or order[0] != "pending":
                await callback.message.answer(MESSAGES["order_already_in_progress"], reply_markup=get_menu_keyboard(user_id))
                await callback.answer()
                return
            cursor = await conn.execute(
                "SELECT COUNT(*) FROM order_applications WHERE order_id = ? AND escort_id = ?",
                (order_db_id, escort_id)
            )
            if (await cursor.fetchone())[0] > 0:
                await callback.message.answer("✅ Вы уже присоединились к этому заказу!", reply_markup=get_menu_keyboard(user_id))
                await callback.answer()
                return
            cursor = await conn.execute(
                "SELECT COUNT(*) FROM order_applications WHERE order_id = ?", (order_db_id,)
            )
            participant_count = (await cursor.fetchone())[0]
            if participant_count >= 4:
                await callback.message.answer(MESSAGES["max_participants"], reply_markup=get_menu_keyboard(user_id))
                await callback.answer()
                return
            await conn.execute(
                '''
                INSERT INTO order_applications (order_id, escort_id, squad_id, pubg_id)
                VALUES (?, ?, ?, ?)
                ''',
                (order_db_id, escort_id, squad_id, pubg_id)
            )
            await conn.commit()
        applications = await get_order_applications(order_db_id)
        participants = "\n".join(f"👤 @{u or 'Unknown'} (PUBG ID: {p}, Сквад: {s or 'Не назначен'})" for _, u, p, _, s in applications)
        response = f"📋 Заказ #{order_db_id} в наборе:\n"
        response += f"Участники:\n{participants if participants else 'Пока никто не присоединился'}\n"
        response += f"Участников: {len(applications)}/4"
        if len(applications) >= 2:
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="Начать выполнение", callback_data=f"start_order_{order_db_id}")],
                [InlineKeyboardButton(text="Отмена", callback_data=f"cancel_order_{order_db_id}")]
            ])
        else:
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="Отмена", callback_data=f"cancel_order_{order_db_id}")]
            ])
        await callback.message.edit_text(response, reply_markup=keyboard)
        await callback.message.answer(
            MESSAGES["order_joined"].format(order_id=order_db_id, participants=participants),
            reply_markup=get_menu_keyboard(user_id)
        )
        await log_action("join_order", user_id, order_db_id, f"Пользователь присоединился к заказу #{order_db_id}")
        await callback.answer()
    except aiosqlite.IntegrityError:
        await callback.message.answer("⚠️ Вы уже подали заявку на этот заказ.", reply_markup=get_menu_keyboard(user_id))
        await callback.answer()
    except Exception as e:
        logger.error(f"Ошибка в join_order для {user_id}: {e}")
        await callback.message.answer(MESSAGES["error"], reply_markup=get_menu_keyboard(user_id))
        await callback.answer()

@dp.callback_query(F.data.startswith("start_order_"))
async def start_order(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    try:
        order_db_id = int(callback.data.split("_")[-1])
        escort = await get_escort(user_id)
        if not escort or not escort[1]:
            await callback.message.answer(MESSAGES["not_in_squad"], reply_markup=get_menu_keyboard(user_id))
            await callback.answer()
            return
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute(
                "SELECT memo_order_id, status, amount FROM orders WHERE id = ?", (order_db_id,)
            )
            order = await cursor.fetchone()
            if not order or order[1] != "pending":
                await callback.message.answer(MESSAGES["order_already_in_progress"], reply_markup=get_menu_keyboard(user_id))
                await callback.answer()
                return
            cursor = await conn.execute(
                "SELECT escort_id, squad_id, pubg_id FROM order_applications WHERE order_id = ?", (order_db_id,)
            )
            applications = await cursor.fetchall()
            if len(applications) < 2 or len(applications) > 4:
                await callback.message.answer(
                    MESSAGES["order_not_enough_members"].format(squad_name=""),
                    reply_markup=get_menu_keyboard(user_id)
                )
                await callback.answer()
                return
            winning_squad_id = applications[0][1]
            valid_applications = [app for app in applications if app[1] == winning_squad_id]
            if len(valid_applications) < 2:
                await callback.message.answer(
                    MESSAGES["order_not_enough_members"].format(squad_name=""),
                    reply_markup=get_menu_keyboard(user_id)
                )
                await callback.answer()
                return
            for escort_id, _, pubg_id in valid_applications:
                await conn.execute(
                    '''
                    INSERT INTO order_escorts (order_id, escort_id, pubg_id)
                    VALUES (?, ?, ?)
                    ''',
                    (order_db_id, escort_id, pubg_id)
                )
                await conn.execute(
                    "UPDATE escorts SET completed_orders = completed_orders + 1 WHERE id = ?",
                    (escort_id,)
                )
            commission = order[2] * 0.2
            await conn.execute(
                '''
                UPDATE orders
                SET status = 'in_progress', squad_id = ?, commission_amount = ?
                WHERE id = ?
                ''',
                (winning_squad_id, commission, order_db_id)
            )
            await conn.execute("DELETE FROM order_applications WHERE order_id = ?", (order_db_id,))
            await conn.commit()
        order_id = order[0]
        participants = "\n".join(
            f"👤 @{u or 'Unknown'} (PUBG ID: {p}, Сквад: {s or 'Не назначен'})"
            for _, u, p, _, s in await get_order_escorts(order_db_id)
        )
        response = MESSAGES["order_confirmed"].format(order_id=order_id, participants=participants)
        keyboard = get_confirmed_order_keyboard(order_id)
        await callback.message.edit_text(response, reply_markup=keyboard)
        for telegram_id, _, _, _, _ in await get_order_escorts(order_db_id):
            try:
                await bot.send_message(
                    telegram_id,
                    f"📝 Заказ #{order_id} начат! Готовьтесь к сопровождению.",
                    reply_markup=get_menu_keyboard(telegram_id)
                )
            except Exception as e:
                logger.warning(f"Не удалось уведомить {telegram_id} о начале заказа: {e}")
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute("SELECT name FROM squads WHERE id = ?", (winning_squad_id,))
            squad_name = (await cursor.fetchone())[0] or "Не назначен"
        await notify_admins(
            MESSAGES["order_taken"].format(
                order_id=order_id,
                squad_name=squad_name,
                participants=participants
            )
        )
        await log_action("start_order", user_id, order_db_id, f"Заказ #{order_id} начат сквадом {squad_name}")
        await callback.answer()
    except Exception as e:
        logger.error(f"Ошибка в start_order для {user_id}: {e}")
        await callback.message.answer(MESSAGES["error"], reply_markup=get_menu_keyboard(user_id))
        await callback.answer()

@dp.callback_query(F.data.startswith("complete_order_"))
async def complete_order_callback(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    order_id = callback.data.split("_")[2]
    try:
        escort = await get_escort(user_id)
        if not escort:
            await callback.message.answer("⚠️ Ваш профиль не найден.", reply_markup=get_menu_keyboard(user_id))
            await callback.answer()
            return
        escort_id = escort[0]
        username = escort[6] or "Unknown"
        pubg_id = escort[2] or "Не указан"
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute(
                "SELECT id, status FROM orders WHERE memo_order_id = ?",
                (order_id,)
            )
            order = await cursor.fetchone()
            if not order or order[1] != "in_progress":
                await callback.message.answer(
                    MESSAGES["order_already_completed"].format(order_id=order_id),
                    reply_markup=get_menu_keyboard(user_id)
                )
                await callback.answer()
                return
            order_db_id = order[0]
            await conn.execute(
                '''
                UPDATE orders
                SET status = 'completed', completed_at = ?
                WHERE id = ?
                ''',
                (datetime.now().isoformat(), order_db_id)
            )
            await conn.commit()
        await callback.message.edit_text(
            MESSAGES["order_completed"].format(
                order_id=order_id,
                username=username,
                telegram_id=user_id,
                pubg_id=pubg_id
            ),
            reply_markup=None
        )
        await notify_admins(
            MESSAGES["order_completed"].format(
                order_id=order_id,
                username=username,
                telegram_id=user_id,
                pubg_id=pubg_id
            )
        )
        await log_action(
            "complete_order",
            user_id,
            order_db_id,
            f"Заказ #{order_id} завершен пользователем @{username}"
        )
        await callback.answer()
    except Exception as e:
        logger.error(f"Ошибка в complete_order_callback для {user_id}: {e}")
        await callback.message.answer(MESSAGES["error"], reply_markup=get_menu_keyboard(user_id))
        await callback.answer()

@dp.callback_query(F.data.startswith("cancel_order_"))
async def cancel_order(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    try:
        order_db_id = int(callback.data.split("_")[-1])
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute("SELECT status, memo_order_id FROM orders WHERE id = ?", (order_db_id,))
            order = await cursor.fetchone()
            if not order or order[0] != "pending":
                await callback.message.answer(MESSAGES["order_already_in_progress"], reply_markup=get_menu_keyboard(user_id))
                await callback.answer()
                return
            await conn.execute("DELETE FROM order_applications WHERE order_id = ?", (order_db_id,))
            await conn.commit()
        await callback.message.edit_text(f"📝 Набор для заказа #{order[1]} отменен.", reply_markup=None)
        await log_action("cancel_order", user_id, order_db_id, f"Набор для заказа #{order[1]} отменен")
        await callback.answer()
    except Exception as e:
        logger.error(f"Ошибка в cancel_order для {user_id}: {e}")
        await callback.message.answer(MESSAGES["error"], reply_markup=get_menu_keyboard(user_id))
        await callback.answer()

@dp.message(F.text == "🔐 Админ-панель")
async def admin_panel(message: types.Message):
    if not await check_access(message):
        return
    if not is_admin(message.from_user.id):
        await message.answer(MESSAGES["no_access"], reply_markup=get_menu_keyboard(message.from_user.id))
        return
    try:
        await message.answer("🔐 Админ-панель:", reply_markup=get_admin_keyboard())
    except Exception as e:
        logger.error(f"Ошибка в admin_panel для {message.from_user.id}: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_menu_keyboard(message.from_user.id))

@dp.message(F.text == "🏠 Добавить сквад")
async def add_squad(message: types.Message, state: FSMContext):
    if not await check_access(message):
        return
    if not is_admin(message.from_user.id):
        await message.answer(MESSAGES["no_access"], reply_markup=get_menu_keyboard(message.from_user.id))
        return
    try:
        await message.answer("🏠 Введите название нового сквада:", reply_markup=ReplyKeyboardRemove())
        await state.set_state(Form.squad_name)
    except Exception as e:
        logger.error(f"Ошибка в add_squad: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_admin_keyboard())

@dp.message(Form.squad_name)
async def process_squad_name(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    squad_name = message.text.strip()
    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            await conn.execute("INSERT INTO squads (name) VALUES (?)", (squad_name,))
            await conn.commit()
        await message.answer(f"🏠 Сквад '{squad_name}' добавлен!", reply_markup=get_admin_keyboard())
        await log_action("add_squad", user_id, None, f"Добавлен сквад '{squad_name}'")
        await state.clear()
    except aiosqlite.IntegrityError:
        await message.answer("⚠️ Сквад с таким названием уже существует.", reply_markup=get_admin_keyboard())
        await state.clear()
    except Exception as e:
        logger.error(f"Ошибка в process_squad_name для {user_id}: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_admin_keyboard())
        await state.clear()

@dp.message(F.text == "📋 Список сквадов")
async def list_squads(message: types.Message):
    if not await check_access(message):
        return
    if not is_admin(message.from_user.id):
        await message.answer(MESSAGES["no_access"], reply_markup=get_menu_keyboard(message.from_user.id))
        return
    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute("SELECT id, name FROM squads")
            squads = await cursor.fetchall()
        if not squads:
            await message.answer(MESSAGES["no_squads"], reply_markup=get_admin_keyboard())
            return
        response = "📋 Список сквадов:\n"
        for squad_id, name in squads:
            squad_info = await get_squad_info(squad_id)
            avg_rating = squad_info[4] / squad_info[5] if squad_info[5] > 0 else 0
            response += (
                f"\n🏠 {name}\n"
                f"- Участников: {squad_info[1]}\n"
                f"- Заказов: {squad_info[2]}\n"
                f"- Баланс: {squad_info[3]:.2f} руб.\n"
                f"- Рейтинг: {avg_rating:.2f} ⭐ ({squad_info[5]} оценок)\n"
            )
        await message.answer(response, reply_markup=get_admin_keyboard())
    except Exception as e:
        logger.error(f"Ошибка в list_squads: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_admin_keyboard())

@dp.message(F.text == "👤 Добавить сопровождающего")
async def add_escort(message: types.Message, state: FSMContext):
    if not await check_access(message):
        return
    if not is_admin(message.from_user.id):
        await message.answer(MESSAGES["no_access"], reply_markup=get_menu_keyboard(message.from_user.id))
        return
    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute("SELECT id, name FROM squads")
            squads = await cursor.fetchall()
        if not squads:
            await message.answer(MESSAGES["no_squads"], reply_markup=get_admin_keyboard())
            return
        response = "🏠 Доступные сквады:\n"
        for squad_id, name in squads:
            response += f"ID: {squad_id} - {name}\n"
        response += "\n👤 Введите данные: Telegram ID, Squad ID\nПример: 123456789, 1"
        await message.answer(response, reply_markup=ReplyKeyboardRemove())
        await state.set_state(Form.escort_info)
    except Exception as e:
        logger.error(f"Ошибка в add_escort: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_admin_keyboard())

@dp.message(Form.escort_info)
async def process_escort(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    try:
        telegram_id, squad_id = map(int, message.text.strip().split(","))
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute("SELECT id FROM squads WHERE id = ?", (squad_id,))
            if not await cursor.fetchone():
                await message.answer("⚠️ Сквад не найден.", reply_markup=get_admin_keyboard())
                await state.clear()
                return
            cursor = await conn.execute(
                "SELECT username FROM escorts WHERE telegram_id = ?",
                (telegram_id,)
            )
            existing_user = await cursor.fetchone()
            username = (await bot.get_chat(telegram_id)).username or "Unknown"
            if existing_user:
                await conn.execute(
                    "UPDATE escorts SET squad_id = ?, username = ? WHERE telegram_id = ?",
                    (squad_id, username, telegram_id)
                )
            else:
                await conn.execute(
                    '''
                    INSERT INTO escorts (telegram_id, username, squad_id)
                    VALUES (?, ?, ?)
                    ''',
                    (telegram_id, username, squad_id)
                )
            await conn.commit()
        await message.answer(
            f"👤 Сопровождающий @{username} добавлен в сквад!", reply_markup=get_admin_keyboard()
        )
        await log_action(
            "add_escort",
            user_id,
            None,
            f"Сопровождающий @{username} (ID: {telegram_id}) добавлен в сквад {squad_id}"
        )
        await state.clear()
    except ValueError:
        await message.answer(MESSAGES["invalid_format"], reply_markup=get_admin_keyboard())
        await state.clear()
    except Exception as e:
        logger.error(f"Ошибка в process_escort для {user_id}: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_admin_keyboard())
        await state.clear()

@dp.message(F.text == "🗑️ Удалить сопровождающего")
async def remove_escort(message: types.Message, state: FSMContext):
    if not await check_access(message):
        return
    if not is_admin(message.from_user.id):
        await message.answer(MESSAGES["no_access"], reply_markup=get_menu_keyboard(message.from_user.id))
        return
    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute("SELECT telegram_id, username FROM escorts")
            escorts = await cursor.fetchall()
        if not escorts:
            await message.answer(MESSAGES["no_escorts"], reply_markup=get_admin_keyboard())
            return
        response = "👤 Список сопровождающих (ID - username):\n"
        for telegram_id, username in escorts:
            response += f"{telegram_id} - @{username or-weather_id}\n"
        response += "\nВведите ID пользователя для удаления:"
        await message.answer(response, reply_markup=ReplyKeyboardRemove())
        await state.set_state(Form.remove_escort)
    except Exception as e:
        logger.error(f"Ошибка в remove_escort: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_admin_keyboard())

@dp.message(Form.remove_escort)
async def process_remove_escort(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    try:
        telegram_id = int(message.text.strip())
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute(
                "SELECT username FROM escorts WHERE telegram_id = ?",
                (telegram_id,)
            )
            user = await cursor.fetchone()
            if not user:
                await message.answer("⚠️ Пользователь не найден.", reply_markup=get_admin_keyboard())
                await state.clear()
                return
            username = user[0] or "Unknown"
            await conn.execute(
                "DELETE FROM escorts WHERE telegram_id = ?",
                (telegram_id,)
            )
            await conn.commit()
        await message.answer(
            f"🗑️ Сопровождающий @{username} удалён!", reply_markup=get_admin_keyboard()
        )
        await log_action(
            "remove_escort",
            user_id,
            None,
            f"Сопровождающий @{username} (ID: {telegram_id}) удалён"
        )
        await state.clear()
    except ValueError:
        await message.answer(MESSAGES["invalid_format"], reply_markup=get_admin_keyboard())
        await state.clear()
    except Exception as e:
        logger.error(f"Ошибка в process_remove_escort для {user_id}: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_admin_keyboard())
        await state.clear()

@dp.message(F.text == "💰 Балансы сопровождающих")
async def escort_balances(message: types.Message):
    if not await check_access(message):
        return
    if not is_admin(message.from_user.id):
        await message.answer(MESSAGES["no_access"], reply_markup=get_menu_keyboard(message.from_user.id))
    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute(
                '''
                SELECT telegram_id, username, balance
                FROM escorts
                WHERE balance > 0
                ORDER BY balance DESC
                '''
            )
            escorts = await cursor.fetchall()
        if not escorts:
            await message.answer("⚠️ Нет сопровождающих с положительным балансом.", reply_markup=get_admin_keyboard())
            return
        response = "💰 Балансы сопровождающих:\n"
        for telegram_id, username, balance in escorts:
            response += f"@{username or 'Unknown'} (ID: {telegram_id}): {balance:.2f} руб.\n"
        await message.answer(response, reply_markup=get_admin_keyboard())
    except Exception as e:
        logger.error(f"Ошибка в escort_balances: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_admin_keyboard())

@dp.message(F.text == "💸 Начислить")
async def add_balance(message: types.Message, state: FSMContext):
    if not await check_access(message):
        return
    if not is_admin(message.from_user.id):
        await message.answer(MESSAGES["no_access"], reply_markup=get_menu_keyboard(message.from_user.id))
        return
    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute("SELECT telegram_id, username FROM escorts")
            escorts = await cursor.fetchall()
        if not escorts:
            await message.answer(MESSAGES["no_escorts"], reply_markup=get_admin_keyboard())
            return
        response = "👤 Список пользователей (ID - username):\n"
        for telegram_id, username in escorts:
            response += f"{telegram_id} - @{username or 'Unknown'}\n"
        response += "\nВведите данные: Telegram ID, сумма\nПример: 123456789, 1000.50"
        await message.answer(response, reply_markup=ReplyKeyboardRemove())
        await state.set_state(Form.balance_amount)
    except Exception as e:
        logger.error(f"Ошибка в add_balance: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_admin_keyboard())

@dp.message(Form.balance_amount)
async def process_balance_amount(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    try:
        telegram_id, amount = map(float, message.text.strip().split(","))
        telegram_id = int(telegram_id)
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute(
                "SELECT username FROM escorts WHERE telegram_id = ?", (telegram_id,)
            )
            user = await cursor.fetchone()
            if not user:
                await message.answer("⚠️ Пользователь не найден.", reply_markup=get_admin_keyboard())
                await state.clear()
                return
            username = user[0] or "Unknown"
            await conn.execute(
                "UPDATE escorts SET balance = balance + ? WHERE telegram_id = ?",
                (amount, telegram_id)
            )
            await conn.commit()
        await message.answer(
            MESSAGES["balance_added"].format(amount=amount, user_id=telegram_id),
            reply_markup=get_admin_keyboard()
        )
        await log_action(
            "add_balance",
            user_id,
            None,
            f"Начислено {amount:.2f} руб. пользователю @{username} (ID: {telegram_id})"
        )
        await state.clear()
    except ValueError:
        await message.answer(MESSAGES["invalid_format"], reply_markup=get_admin_keyboard())
        await state.clear()
    except Exception as e:
        logger.error(f"Ошибка в process_balance_amount для {user_id}: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_admin_keyboard())
        await state.clear()

@dp.message(F.text == "📝 Добавить заказ")
async def add_order(message: types.Message, state: FSMContext):
    if not await check_access(message):
        return
    if not is_admin(message.from_user.id):
        await message.answer(MESSAGES["no_access"], reply_markup=get_menu_keyboard(message.from_user.id))
        return
    try:
        await message.answer(
            "📝 Введите данные заказа в формате: ID заказа, сумма, описание\n"
            "Пример: ORDER123, 1000.50, Сопровождение в Metro Royale",
            reply_markup=ReplyKeyboardRemove()
        )
        await state.set_state(Form.add_order)
    except Exception as e:
        logger.error(f"Ошибка в add_order: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_admin_keyboard())

@dp.message(Form.add_order)
async def process_add_order(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    try:
        order_id, amount, description = message.text.strip().split(",", 2)
        amount = float(amount.strip())
        order_id = order_id.strip()
        description = description.strip()
        async with aiosqlite.connect(DB_PATH) as conn:
            await conn.execute(
                '''
                INSERT INTO orders (memo_order_id, customer_info, amount)
                VALUES (?, ?, ?)
                ''',
                (order_id, description, amount)
            )
            await conn.commit()
        await message.answer(
            MESSAGES["order_added"].format(
                order_id=order_id, amount=amount, description=description, customer=description
            ),
            reply_markup=get_admin_keyboard()
        )
        await notify_admins(
            MESSAGES["order_added"].format(
                order_id=order_id, amount=amount, description=description, customer=description
            )
        )
        await log_action(
            "add_order",
            user_id,
            None,
            f"Добавлен заказ #{order_id}: {amount:.2f} руб., {description}"
        )
        await state.clear()
    except ValueError:
        await message.answer(MESSAGES["invalid_format"], reply_markup=get_admin_keyboard())
        await state.clear()
    except aiosqlite.IntegrityError:
        await message.answer("⚠️ Заказ с таким ID уже существует.", reply_markup=get_admin_keyboard())
        await state.clear()
    except Exception as e:
        logger.error(f"Ошибка в process_add_order для {user_id}: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_admin_keyboard())
        await state.clear()

@dp.message(F.text == "🚫 Бан навсегда")
async def ban_permanent(message: types.Message, state: FSMContext):
    if not await check_access(message):
        return
    if not is_admin(message.from_user.id):
        await message.answer(MESSAGES["no_access"], reply_markup=get_menu_keyboard(message.from_user.id))
        return
    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute("SELECT telegram_id, username FROM escorts")
            escorts = await cursor.fetchall()
        if not escorts:
            await message.answer(MESSAGES["no_escorts"], reply_markup=get_admin_keyboard())
            return
        response = "👤 Список пользователей (ID - username):\n"
        for telegram_id, username in escorts:
            response += f"{telegram_id} - @{username or 'Unknown'}\n"
        response += "\nВведите ID пользователя для блокировки:"
        await message.answer(response, reply_markup=ReplyKeyboardRemove())
        await state.set_state(Form.ban_permanent)
    except Exception as e:
        logger.error(f"Ошибка в ban_permanent: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_admin_keyboard())

@dp.message(Form.ban_permanent)
async def process_ban_permanent(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    try:
        telegram_id = int(message.text.strip())
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute(
                "SELECT username FROM escorts WHERE telegram_id = ?", (telegram_id,)
            )
            user = await cursor.fetchone()
            if not user:
                await message.answer("⚠️ Пользователь не найден.", reply_markup=get_admin_keyboard())
                await state.clear()
                return
            username = user[0] or "Unknown"
            await conn.execute(
                "UPDATE escorts SET is_banned = 1 WHERE telegram_id = ?", (telegram_id,)
            )
            await conn.commit()
        await message.answer(
            f"🚫 Пользователь @{username} заблокирован навсегда!", reply_markup=get_admin_keyboard()
        )
        await log_action(
            "ban_permanent",
            user_id,
            None,
            f"Пользователь @{username} (ID: {telegram_id}) заблокирован навсегда"
        )
        await state.clear()
    except ValueError:
        await message.answer(MESSAGES["invalid_format"], reply_markup=get_admin_keyboard())
        await state.clear()
    except Exception as e:
        logger.error(f"Ошибка в process_ban_permanent для {user_id}: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_admin_keyboard())
        await state.clear()

@dp.message(F.text == "⏰ Бан на время")
async def ban_temporary(message: types.Message, state: FSMContext):
    if not await check_access(message):
        return
    if not is_admin(message.from_user.id):
        await message.answer(MESSAGES["no_access"], reply_markup=get_menu_keyboard(message.from_user.id))
        return
    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute("SELECT telegram_id, username FROM escorts")
            escorts = await cursor.fetchall()
        if not escorts:
            await message.answer(MESSAGES["no_escorts"], reply_markup=get_admin_keyboard())
            return
        response = "👤 Список пользователей (ID - username):\n"
        for telegram_id, username in escorts:
            response += f"{telegram_id} - @{username or 'Unknown'}\n"
        response += "\nВведите ID пользователя и количество дней бана (например, 123456789, 7):"
        await message.answer(response, reply_markup=ReplyKeyboardRemove())
        await state.set_state(Form.ban_duration)
    except Exception as e:
        logger.error(f"Ошибка в ban_temporary: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_admin_keyboard())

@dp.message(Form.ban_duration)
async def process_ban_duration(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    try:
        telegram_id, days = map(int, message.text.strip().split(","))
        ban_until = (datetime.now() + timedelta(days=days)).isoformat()
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute(
                "SELECT username FROM escorts WHERE telegram_id = ?", (telegram_id,)
            )
            user = await cursor.fetchone()
            if not user:
                await message.answer("⚠️ Пользователь не найден.", reply_markup=get_admin_keyboard())
                await state.clear()
                return
            username = user[0] or "Unknown"
            await conn.execute(
                "UPDATE escorts SET ban_until = ? WHERE telegram_id = ?", (ban_until, telegram_id)
            )
            await conn.commit()
        await message.answer(
            f"⏰ Пользователь @{username} заблокирован до {ban_until}!", reply_markup=get_admin_keyboard()
        )
        await log_action(
            "ban_temporary",
            user_id,
            None,
            f"Пользователь @{username} (ID: {telegram_id}) заблокирован до {ban_until}"
        )
        await state.clear()
    except ValueError:
        await message.answer(MESSAGES["invalid_format"], reply_markup=get_admin_keyboard())
        await state.clear()
    except Exception as e:
        logger.error(f"Ошибка в process_ban_duration для {user_id}: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_admin_keyboard())
        await state.clear()

@dp.message(F.text == "⛔ Ограничить")
async def restrict_user(message: types.Message, state: FSMContext):
    if not await check_access(message):
        return
    if not is_admin(message.from_user.id):
        await message.answer(MESSAGES["no_access"], reply_markup=get_menu_keyboard(message.from_user.id))
        return
    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute("SELECT telegram_id, username FROM escorts")
            escorts = await cursor.fetchall()
        if not escorts:
            await message.answer(MESSAGES["no_escorts"], reply_markup=get_admin_keyboard())
            return
        response = "👤 Список пользователей (ID - username):\n"
        for telegram_id, username in escorts:
            response += f"{telegram_id} - @{username or 'Unknown'}\n"
        response += "\nВведите ID пользователя и количество дней ограничения (например, 123456789, 7):"
        await message.answer(response, reply_markup=ReplyKeyboardRemove())
        await state.set_state(Form.restrict_duration)
    except Exception as e:
        logger.error(f"Ошибка в restrict_user: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_admin_keyboard())

@dp.message(Form.restrict_duration)
async def process_restrict_duration(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    try:
        telegram_id, days = map(int, message.text.strip().split(","))
        restrict_until = (datetime.now() + timedelta(days=days)).isoformat()
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute(
                "SELECT username FROM escorts WHERE telegram_id = ?", (telegram_id,)
            )
            user = await cursor.fetchone()
            if not user:
                await message.answer("⚠️ Пользователь не найден.", reply_markup=get_admin_keyboard())
                await state.clear()
                return
            username = user[0] or "Unknown"
            await conn.execute(
                "UPDATE escorts SET restrict_until = ? WHERE telegram_id = ?", (restrict_until, telegram_id)
            )
            await conn.commit()
        await message.answer(
            f"⛔ Пользователь @{username} ограничен до {restrict_until}!", reply_markup=get_admin_keyboard()
        )
        await log_action(
            "restrict_user",
            user_id,
            None,
            f"Пользователь @{username} (ID: {telegram_id}) ограничен до {restrict_until}"
        )
        await state.clear()
    except ValueError:
        await message.answer(MESSAGES["invalid_format"], reply_markup=get_admin_keyboard())
        await state.clear()
    except Exception as e:
        logger.error(f"Ошибка в process_restrict_duration для {user_id}: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_admin_keyboard())
        await state.clear()

@dp.message(F.text == "💰 Обнулить баланс")
async def zero_balance(message: types.Message, state: FSMContext):
    if not await check_access(message):
        return
    if not is_admin(message.from_user.id):
        await message.answer(MESSAGES["no_access"], reply_markup=get_menu_keyboard(message.from_user.id))
        return
    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute("SELECT telegram_id, username FROM escorts")
            escorts = await cursor.fetchall()
        if not escorts:
            await message.answer(MESSAGES["no_escorts"], reply_markup=get_admin_keyboard())
            return
        response = "👤 Список пользователей (ID - username):\n"
        for telegram_id, username in escorts:
            response += f"{telegram_id} - @{username or 'Unknown'}\n"
        response += "\nВведите ID пользователя для обнуления баланса:"
        await message.answer(response, reply_markup=ReplyKeyboardRemove())
        await state.set_state(Form.zero_balance)
    except Exception as e:
        logger.error(f"Ошибка в zero_balance: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_admin_keyboard())

@dp.message(Form.zero_balance)
async def process_zero_balance(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    try:
        telegram_id = int(message.text.strip())
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute(
                "SELECT username FROM escorts WHERE telegram_id = ?", (telegram_id,)
            )
            user = await cursor.fetchone()
            if not user:
                await message.answer("⚠️ Пользователь не найден.", reply_markup=get_admin_keyboard())
                await state.clear()
                return
            username = user[0] or "Unknown"
            await conn.execute(
                "UPDATE escorts SET balance = 0 WHERE telegram_id = ?", (telegram_id,)
            )
            await conn.commit()
        await message.answer(
            MESSAGES["balance_zeroed"].format(user_id=telegram_id),
            reply_markup=get_admin_keyboard()
        )
        await log_action(
            "zero_balance",
            user_id,
            None,
            f"Баланс пользователя @{username} (ID: {telegram_id}) обнулен"
        )
        await state.clear()
    except ValueError:
        await message.answer(MESSAGES["invalid_format"], reply_markup=get_admin_keyboard())
        await state.clear()
    except Exception as e:
        logger.error(f"Ошибка в process_zero_balance для {user_id}: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_admin_keyboard())
        await state.clear()

@dp.message(F.text == "👥 Пользователи")
async def list_users(message: types.Message):
    if not await check_access(message):
        return
    if not is_admin(message.from_user.id):
        await message.answer(MESSAGES["no_access"], reply_markup=get_menu_keyboard(message.from_user.id))
        return
    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute(
                '''
                SELECT telegram_id, username, squad_id, pubg_id, balance, reputation,
                       completed_orders, rating, rating_count, is_banned, ban_until, restrict_until
                FROM escorts
                '''
            )
            escorts = await cursor.fetchall()
        if not escorts:
            await message.answer(MESSAGES["no_escorts"], reply_markup=get_admin_keyboard())
            return
        response = "👥 Список пользователей:\n"
        for escort in escorts:
            telegram_id, username, squad_id, pubg_id, balance, reputation, completed_orders, rating, rating_count, is_banned, ban_until, restrict_until = escort
            avg_rating = rating / rating_count if rating_count > 0 else 0
            async with aiosqlite.connect(DB_PATH) as conn:
                cursor = await conn.execute("SELECT name FROM squads WHERE id = ?", (squad_id,))
                squad = await cursor.fetchone()
            status = "Заблокирован" if is_banned else (
                f"Ограничен до {restrict_until}" if restrict_until and datetime.fromisoformat(restrict_until) > datetime.now()
                else "Активен"
            )
            response += (
                f"\n👤 @{username or 'Unknown'} (ID: {telegram_id})\n"
                f"- Сквад: {squad[0] if squad else 'не назначен'}\n"
                f"- PUBG ID: {pubg_id or 'не указан'}\n"
                f"- Баланс: {balance:.2f} руб.\n"
                f"- Репутация: {reputation}\n"
                f"- Заказов: {completed_orders}\n"
                f"- Рейтинг: {avg_rating:.2f} ⭐ ({rating_count} оценок)\n"
                f"- Статус: {status}\n"
            )
        await message.answer(response, reply_markup=get_admin_keyboard())
    except Exception as e:
        logger.error(f"Ошибка в list_users: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_admin_keyboard())

@dp.message(F.text == "📊 Все балансы")
async def all_balances(message: types.Message):
    if not await check_access(message):
        return
    if not is_admin(message.from_user.id):
        await message.answer(MESSAGES["no_access"], reply_markup=get_menu_keyboard(message.from_user.id))
        return
    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute(
                '''
                SELECT telegram_id, username, balance
                FROM escorts
                ORDER BY balance DESC
                '''
            )
            escorts = await cursor.fetchall()
        if not escorts:
            await message.answer("⚠️ Нет пользователей.", reply_markup=get_admin_keyboard())
            return
        response = "📊 Все балансы:\n"
        for telegram_id, username, balance in escorts:
            response += f"@{username or 'Unknown'} (ID: {telegram_id}): {balance:.2f} руб.\n"
        await message.answer(response, reply_markup=get_admin_keyboard())
    except Exception as e:
        logger.error(f"Ошибка в all_balances: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_admin_keyboard())

@dp.message(F.text == "📜 Журнал действий")
async def action_log(message: types.Message):
    if not await check_access(message):
        return
    if not is_admin(message.from_user.id):
        await message.answer(MESSAGES["no_access"], reply_markup=get_menu_keyboard(message.from_user.id))
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
            logs = await cursor.fetchall()
        if not logs:
            await message.answer("⚠️ Журнал действий пуст.", reply_markup=get_admin_keyboard())
            return
        response = "📜 Последние действия:\n"
        for action_type, user_id, order_id, description, action_date in logs:
            response += (
                f"[{action_date}] {action_type} (User ID: {user_id}, Order ID: {order_id or 'N/A'})\n"
                f"- {description}\n\n"
            )
        await message.answer(response, reply_markup=get_admin_keyboard())
    except Exception as e:
        logger.error(f"Ошибка в action_log: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_admin_keyboard())

@dp.message(F.text == "📤 Экспорт данных")
async def export_data(message: types.Message):
    if not await check_access(message):
        return
    if not is_admin(message.from_user.id):
        await message.answer(MESSAGES["no_access"], reply_markup=get_menu_keyboard(message.from_user.id))
        return
    try:
        filename = await export_orders_to_csv()
        if not filename:
            await message.answer(MESSAGES["no_data_to_export"], reply_markup=get_admin_keyboard())
            return
        with open(filename, 'rb') as f:
            await message.answer_document(types.FSInputFile(path=filename), caption=MESSAGES["export_success"].format(filename=filename))
        os.remove(filename)  # Удаляем файл после отправки
        await log_action("export_data", message.from_user.id, None, f"Экспорт данных в {filename}")
    except Exception as e:
        logger.error(f"Ошибка в export_data: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_admin_keyboard())

@dp.message(F.text == "📊 Отчет за месяц")
async def monthly_report(message: types.Message):
    if not await check_access(message):
        return
    if not is_admin(message.from_user.id):
        await message.answer(MESSAGES["no_access"], reply_markup=get_menu_keyboard(message.from_user.id))
        return
    try:
        start_date = (datetime.now() - timedelta(days=30)).isoformat()
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute(
                '''
                SELECT COALESCE(SUM(commission_amount), 0) as total_commission
                FROM orders
                WHERE completed_at >= ?
                ''', (start_date,)
            )
            total_commission = (await cursor.fetchone())[0]
        response = (
            f"📊 Отчет за последние 30 дней:\n"
            f"💰 Общая комиссия: {total_commission:.2f} руб.\n"
        )
        await message.answer(response, reply_markup=get_admin_keyboard())
    except Exception as e:
        logger.error(f"Ошибка в monthly_report: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_admin_keyboard())

@dp.message(F.text == "📈 Доход пользователя")
async def user_profit(message: types.Message, state: FSMContext):
    if not await check_access(message):
        return
    if not is_admin(message.from_user.id):
        await message.answer(MESSAGES["no_access"], reply_markup=get_menu_keyboard(message.from_user.id))
        return
    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute("SELECT telegram_id, username FROM escorts")
            escorts = await cursor.fetchall()
        if not escorts:
            await message.answer(MESSAGES["no_escorts"], reply_markup=get_admin_keyboard())
            return
        response = "👤 Список пользователей (ID - username):\n"
        for telegram_id, username in escorts:
            response += f"{telegram_id} - @{username or 'Unknown'}\n"
        response += "\nВведите ID пользователя для просмотра дохода:"
        await message.answer(response, reply_markup=ReplyKeyboardRemove())
        await state.set_state(Form.profit_user)
    except Exception as e:
        logger.error(f"Ошибка в user_profit: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_admin_keyboard())

@dp.message(Form.profit_user)
async def process_user_profit(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    try:
        telegram_id = int(message.text.strip())
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute(
                "SELECT username FROM escorts WHERE telegram_id = ?", (telegram_id,)
            )
            user = await cursor.fetchone()
            if not user:
                await message.answer("⚠️ Пользователь не найден.", reply_markup=get_admin_keyboard())
                await state.clear()
                return
            username = user[0] or "Unknown"
            cursor = await conn.execute(
                '''
                SELECT COALESCE(SUM(p.amount), 0) as total_payout
                FROM payouts p
                JOIN escorts e ON p.escort_id = e.id
                WHERE e.telegram_id = ?
                ''', (telegram_id,)
            )
            total_payout = (await cursor.fetchone())[0]
        response = (
            f"📈 Доход пользователя @{username} (ID: {telegram_id}):\n"
            f"💰 Общий доход: {total_payout:.2f} руб.\n"
        )
        await message.answer(response, reply_markup=get_admin_keyboard())
        await log_action(
            "view_user_profit",
            user_id,
            None,
            f"Просмотрен доход пользователя @{username} (ID: {telegram_id})"
        )
        await state.clear()
    except ValueError:
        await message.answer(MESSAGES["invalid_format"], reply_markup=get_admin_keyboard())
        await state.clear()
    except Exception as e:
        logger.error(f"Ошибка в process_user_profit для {user_id}: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_admin_keyboard())
        await state.clear()

@dp.message(F.text == "📖 Справочник админ-команд")
async def admin_help(message: types.Message):
    if not await check_access(message):
        return
    if not is_admin(message.from_user.id):
        await message.answer(MESSAGES["no_access"], reply_markup=get_menu_keyboard(message.from_user.id))
        return
    try:
        response = (
            "📖 Справочник админ-команд:\n"
            "🏠 Добавить сквад - Создать новый сквад\n"
            "📋 Список сквадов - Показать все сквады\n"
            "👤 Добавить сопровождающего - Добавить пользователя в сквад\n"
            "🗑️ Удалить сопровождающего - Удалить пользователя\n"
            "💰 Балансы сопровождающих - Показать балансы с ненулевым значением\n"
            "💸 Начислить - Начислить баланс пользователю\n"
            "📝 Добавить заказ - Создать новый заказ\n"
            "🚫 Бан навсегда - Заблокировать пользователя навсегда\n"
            "⏰ Бан на время - Заблокировать пользователя на определённое время\n"
            "⛔ Ограничить - Ограничить доступ пользователя к сопровождениям\n"
            "👥 Пользователи - Показать всех пользователей\n"
            "💰 Обнулить баланс - Сбросить баланс пользователя\n"
            "📊 Все балансы - Показать балансы всех пользователей\n"
            "📜 Журнал действий - Показать последние действия\n"
            "📤 Экспорт данных - Экспортировать данные заказов в CSV\n"
            "📊 Отчет за месяц - Показать комиссию за последние 30 дней\n"
            "📈 Доход пользователя - Показать доход конкретного пользователя\n"
        )
        await message.answer(response, reply_markup=get_admin_keyboard())
    except Exception as e:
        logger.error(f"Ошибка в admin_help: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_admin_keyboard())

@dp.message(F.text == "🔙 На главную")
async def back_to_main(message: types.Message):
    if not await check_access(message):
        return
    try:
        await message.answer("📌 Выберите действие:", reply_markup=get_menu_keyboard(message.from_user.id))
    except Exception as e:
        logger.error(f"Ошибка в back_to_main: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_menu_keyboard(message.from_user.id))

async def on_startup():
    # Проверка и создание директории для базы данных
    db_dir = os.path.dirname(DB_PATH)
    if db_dir and not os.path.exists(db_dir):
        os.makedirs(db_dir)
        logger.info(f"Создана директория для базы данных: {db_dir}")
    
    await init_db()
    scheduler.add_job(check_pending_orders, "interval", hours=1)
    scheduler.start()
    logger.info("Бот запущен")

async def main():
    try:
        await on_startup()
        await dp.start_polling(bot, polling_timeout=30)  # Увеличенный таймаут для Fly.io
    except Exception as e:
        logger.error(f"Ошибка при запуске бота: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(main())