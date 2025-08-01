import os
import logging
import asyncio
import aiosqlite
import traceback
import csv
from datetime import datetime, timedelta
from typing import Optional
from dotenv import load_dotenv
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import FSInputFile, InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup
from aiogram.utils.keyboard import ReplyKeyboardBuilder
from aiogram.exceptions import TelegramBadRequest, TelegramRetryAfter
from logging.handlers import RotatingFileHandler

# Настройка логирования
logging.basicConfig(
    filename="memo_bot.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    encoding='utf-8'
)
logger = logging.getLogger(__name__)
handler = RotatingFileHandler("memo_bot.log", maxBytes=10*1024*1024, backupCount=5)
handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
logger.addHandler(handler)

# Загрузка конфигурации
load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")
try:
    ADMIN_IDS = [int(x) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip()]
except ValueError as e:
    logger.error(f"Ошибка в ADMIN_IDS: {e}")
    raise ValueError("ADMIN_IDS содержит нечисловые значения")
DB_PATH = "database.db"

if not BOT_TOKEN or not ADMIN_IDS:
    logger.error("BOT_TOKEN или ADMIN_IDS не заданы в .env")
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
            await conn.execute("PRAGMA foreign_keys = ON")
            with open("schema.sql", "r", encoding="utf-8") as f:
                await conn.executescript(f.read())
            await conn.commit()
        logger.info("База данных инициализирована")
    except aiosqlite.Error as e:
        logger.error(f"Ошибка инициализации БД: {e}\n{traceback.format_exc()}")
        raise
    except FileNotFoundError:
        logger.error("Файл schema.sql не найден")
        raise FileNotFoundError("Файл schema.sql не найден")

# Проверка доступа
async def check_access(message: types.Message) -> bool:
    user_id = message.from_user.id
    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            async with conn.execute(
                "SELECT is_banned, ban_until, restrict_until FROM escorts WHERE telegram_id = ?",
                (user_id,)
            ) as cursor:
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
    except aiosqlite.Error as e:
        logger.error(f"Ошибка проверки доступа для {user_id}: {e}")
        await message.answer(MESSAGES["error"])
        return False

# Проверка администратора
def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS

# Клавиатуры
def get_admin_keyboard() -> ReplyKeyboardMarkup:
    builder = ReplyKeyboardBuilder()
    buttons = [
        "📝 Заказы", "🏠 Сквады", "👤 Сопровождающие",
        "🚫 Бан/ограничение", "💰 Балансы", "📈 Отчеты/справка",
        "🔙 Назад"
    ]
    for button in buttons:
        builder.button(text=button)
    builder.adjust(1)
    return builder.as_markup(resize_keyboard=True)

def get_orders_keyboard() -> ReplyKeyboardMarkup:
    builder = ReplyKeyboardBuilder()
    builder.button(text="📝 Добавить заказ")
    builder.button(text="🔙 Назад")
    builder.adjust(2)
    return builder.as_markup(resize_keyboard=True)

def get_squads_keyboard() -> ReplyKeyboardMarkup:
    builder = ReplyKeyboardBuilder()
    buttons = ["🏠 Добавить сквад", "📋 Список сквадов", "🗑️ Расформировать сквад", "🔙 Назад"]
    for button in buttons:
        builder.button(text=button)
    builder.adjust(2)
    return builder.as_markup(resize_keyboard=True)

def get_escorts_keyboard() -> ReplyKeyboardMarkup:
    builder = ReplyKeyboardBuilder()
    buttons = ["👤 Добавить сопровождающего", "🗑️ Удалить сопровождающего", "👥 Пользователи", "🔙 Назад"]
    for button in buttons:
        builder.button(text=button)
    builder.adjust(2)
    return builder.as_markup(resize_keyboard=True)

def get_ban_restrict_keyboard() -> ReplyKeyboardMarkup:
    builder = ReplyKeyboardBuilder()
    buttons = [
        "🚫 Бан навсегда", "⏰ Бан на время", "⛔ Ограничить",
        "🔒 Снять бан", "🔓 Снять ограничение", "🔙 Назад"
    ]
    for button in buttons:
        builder.button(text=button)
    builder.adjust(2)
    return builder.as_markup(resize_keyboard=True)

def get_balances_keyboard() -> ReplyKeyboardMarkup:
    builder = ReplyKeyboardBuilder()
    buttons = ["💰 Балансы сопровождающих", "💸 Начислить", "💰 Обнулить баланс", "🔙 Назад"]
    for button in buttons:
        builder.button(text=button)
    builder.adjust(2)
    return builder.as_markup(resize_keyboard=True)

def get_reports_keyboard() -> ReplyKeyboardMarkup:
    builder = ReplyKeyboardBuilder()
    buttons = ["📈 Отчет за месяц", "📤 Экспорт данных", "📜 Журнал действий", "📈 Доход пользователя", "🔙 Назад"]
    for button in buttons:
        builder.button(text=button)
    builder.adjust(2)
    return builder.as_markup(resize_keyboard=True)

def get_cancel_keyboard(admin: bool = False) -> ReplyKeyboardMarkup:
    builder = ReplyKeyboardBuilder()
    builder.button(text="🚫 Отмена")
    return builder.as_markup(resize_keyboard=True)

def get_menu_keyboard(user_id: int) -> ReplyKeyboardMarkup:
    builder = ReplyKeyboardBuilder()
    builder.button(text="📩 Поддержка")
    if is_admin(user_id):
        builder.button(text="📖 Админ-панель")
    builder.adjust(1)
    return builder.as_markup(resize_keyboard=True)

# Безопасная отправка сообщений
async def safe_send_message(chat_id: int, text: str, **kwargs) -> bool:
    try:
        await bot.send_message(chat_id, text, **kwargs)
        return True
    except TelegramRetryAfter as e:
        logger.warning(f"Rate limit: ждем {e.retry_after} секунд")
        await asyncio.sleep(e.retry_after)
        return await safe_send_message(chat_id, text, **kwargs)
    except TelegramBadRequest as e:
        logger.error(f"Ошибка отправки сообщения для chat_id {chat_id}: {e}")
        return False
    except Exception as e:
        logger.error(f"Неизвестная ошибка отправки для chat_id {chat_id}: {e}")
        return False

# Логирование действий
async def log_action(action_type: str, user_id: int, order_id: Optional[str], description: str):
    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            await conn.execute(
                "INSERT INTO action_log (action_type, user_id, order_id, description) VALUES (?, ?, ?, ?)",
                (action_type, user_id, order_id, description)
            )
            await conn.commit()
        logger.info(f"Действие '{action_type}' для user_id {user_id}: {description}")
    except aiosqlite.Error as e:
        logger.error(f"Ошибка логирования действия '{action_type}': {e}")

# Уведомление админов
async def notify_admins(message: str, reply_to_user_id: Optional[int] = None):
    tasks = []
    for admin_id in ADMIN_IDS:
        if reply_to_user_id:
            markup = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="Ответить", callback_data=f"reply_{reply_to_user_id}")]
            ])
            tasks.append(safe_send_message(admin_id, message, reply_markup=markup))
        else:
            tasks.append(safe_send_message(admin_id, message))
    await asyncio.gather(*tasks, return_exceptions=True)

# Уведомление сквада
async def notify_squad(squad_id: Optional[int], message: str):
    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            query = "SELECT telegram_id FROM escorts" if squad_id is None else \
                    "SELECT telegram_id FROM escorts WHERE squad_id = ?"
            params = () if squad_id is None else (squad_id,)
            async with conn.execute(query, params) as cursor:
                escorts = await cursor.fetchall()
        tasks = [safe_send_message(escort[0], message) for escort in escorts]
        await asyncio.gather(*tasks, return_exceptions=True)
    except aiosqlite.Error as e:
        logger.error(f"Ошибка уведомления сквада {squad_id}: {e}")

# Информация о скваде
async def get_squad_info(squad_id: int) -> Optional[tuple]:
    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            async with conn.execute(
                "SELECT name, total_orders, total_balance, rating, rating_count FROM squads WHERE id = ?",
                (squad_id,)
            ) as cursor:
                squad = await cursor.fetchone()
            if not squad:
                return None
            async with conn.execute("SELECT COUNT(*) FROM escorts WHERE squad_id = ?", (squad_id,)) as cursor:
                member_count = (await cursor.fetchone())[0]
        return (*squad, member_count)
    except aiosqlite.Error as e:
        logger.error(f"Ошибка получения информации о скваде {squad_id}: {e}")
        return None

# Экспорт заказов в CSV
async def export_orders_to_csv() -> Optional[str]:
    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            async with conn.execute("SELECT * FROM orders") as cursor:
                orders = await cursor.fetchall()
            if not orders:
                return None
        filename = f"orders_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        with open(filename, 'w', encoding='utf-8', newline='') as f:
            writer = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
            writer.writerow(["ID", "Memo Order ID", "Customer Info", "Amount", "Status", "Created At", 
                            "Commission Amount", "Escort ID"])
            for order in orders:
                writer.writerow([str(x) if x is not None else '' for x in order])
        return filename
    except (aiosqlite.Error, OSError) as e:
        logger.error(f"Ошибка экспорта заказов: {e}")
        return None

# Проверка незавершенных заказов
async def check_pending_orders():
    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            async with conn.execute(
                "SELECT memo_order_id, customer_info, amount FROM orders WHERE status = 'pending'"
            ) as cursor:
                orders = await cursor.fetchall()
        if orders:
            message = "⏰ Напоминание о незавершенных заказах:\n"
            for order_id, customer, amount in orders:
                message += f"📝 Заказ #{order_id}, клиент: {customer}, сумма: {amount:.2f} руб.\n"
            await notify_admins(message)
        logger.info("Проверка незавершенных заказов выполнена")
    except aiosqlite.Error as e:
        logger.error(f"Ошибка проверки незавершенных заказов: {e}")

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
            async with conn.execute(
                "SELECT username FROM escorts WHERE telegram_id = ?", (reply_to_user_id,)
            ) as cursor:
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
        logger.error(f"Ошибка в process_reply_callback для {user_id}: {e}")
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
            async with conn.execute(
                "SELECT username FROM escorts WHERE telegram_id = ?", (reply_to_user_id,)
            ) as cursor:
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
    except Exception as e:
        logger.error(f"Ошибка в process_reply_to_user для {user_id}: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_admin_keyboard())
        await state.clear()

# Команда /start
@dp.message(Command("start"))
async def cmd_start(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    await message.answer("👋 Добро пожаловать!", reply_markup=get_menu_keyboard(user_id))
    await state.clear()
    logger.info(f"Пользователь {user_id} запустил бота")

# Обработчик админ-панели
@dp.message(lambda message: message.text == "📖 Админ-панель")
async def admin_panel(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer(MESSAGES["no_access"], reply_markup=get_menu_keyboard(user_id))
        return
    await message.answer("📖 Админ-панель:", reply_markup=get_admin_keyboard())
    await state.clear()

# Обработчик группы "Заказы"
@dp.message(lambda message: message.text == "📝 Заказы")
async def orders_menu(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer(MESSAGES["no_access"], reply_markup=get_menu_keyboard(user_id))
        return
    await message.answer("📝 Меню заказов:", reply_markup=get_orders_keyboard())
    await state.clear()

# Обработчик группы "Сквады"
@dp.message(lambda message: message.text == "🏠 Сквады")
async def squads_menu(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer(MESSAGES["no_access"], reply_markup=get_menu_keyboard(user_id))
        return
    await message.answer("🏠 Меню сквадов:", reply_markup=get_squads_keyboard())
    await state.clear()

# Обработчик группы "Сопровождающие"
@dp.message(lambda message: message.text == "👤 Сопровождающие")
async def escorts_menu(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer(MESSAGES["no_access"], reply_markup=get_menu_keyboard(user_id))
        return
    await message.answer("👤 Меню сопровождающих:", reply_markup=get_escorts_keyboard())
    await state.clear()

# Обработчик группы "Бан/ограничение"
@dp.message(lambda message: message.text == "🚫 Бан/ограничение")
async def ban_restrict_menu(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer(MESSAGES["no_access"], reply_markup=get_menu_keyboard(user_id))
        return
    await message.answer("🚫 Меню бана/ограничений:", reply_markup=get_ban_restrict_keyboard())
    await state.clear()

# Обработчик группы "Балансы"
@dp.message(lambda message: message.text == "💰 Балансы")
async def balances_menu(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer(MESSAGES["no_access"], reply_markup=get_menu_keyboard(user_id))
        return
    await message.answer("💰 Меню балансов:", reply_markup=get_balances_keyboard())
    await state.clear()

# Обработчик группы "Отчеты/справка"
@dp.message(lambda message: message.text == "📈 Отчеты/справка")
async def reports_menu(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer(MESSAGES["no_access"], reply_markup=get_menu_keyboard(user_id))
        return
    await message.answer("📈 Меню отчетов:", reply_markup=get_reports_keyboard())
    await state.clear()

# Обработчик добавления сквада
@dp.message(lambda message: message.text == "🏠 Добавить сквад")
async def add_squad(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer(MESSAGES["no_access"], reply_markup=get_menu_keyboard(user_id))
        return
    await message.answer("🏠 Введите название нового сквада:", reply_markup=get_cancel_keyboard(True))
    await state.set_state(Form.squad_name)

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
            async with conn.execute("SELECT id FROM squads WHERE name = ?", (squad_name,)) as cursor:
                if await cursor.fetchone():
                    await message.answer(f"⚠️ Сквад '{squad_name}' уже существует.", reply_markup=get_cancel_keyboard(True))
                    return
            await conn.execute("INSERT INTO squads (name) VALUES (?)", (squad_name,))
            await conn.commit()
        await message.answer(f"🏠 Сквад '{squad_name}' успешно создан!", reply_markup=get_squads_keyboard())
        await log_action("add_squad", user_id, None, f"Создан сквад '{squad_name}'")
        await state.clear()
    except aiosqlite.Error as e:
        logger.error(f"Ошибка в process_squad_name для {user_id}: {e}")
        await message.answer("⚠️ Ошибка базы данных.", reply_markup=get_squads_keyboard())
        await state.clear()

# Обработчик списка сквадов
@dp.message(lambda message: message.text == "📋 Список сквадов")
async def list_squads(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer(MESSAGES["no_access"], reply_markup=get_menu_keyboard(user_id))
        return
    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            async with conn.execute("SELECT id, name FROM squads") as cursor:
                squads = await cursor.fetchall()
        if not squads:
            await message.answer(MESSAGES["no_squads"], reply_markup=get_squads_keyboard())
            return
        response = "🏠 Список сквадов:\n"
        for squad_id, name in squads:
            squad_info = await get_squad_info(squad_id)
            if squad_info:
                name, total_orders, total_balance, rating, rating_count, member_count = squad_info
                response += (
                    f"🏠 {name}\n📝 Заказов: {total_orders}\n💰 Баланс: {total_balance:.2f} руб.\n"
                    f"⭐ Рейтинг: {rating:.1f} ({rating_count} оценок)\n👥 Участников: {member_count}\n\n"
                )
        await message.answer(response, reply_markup=get_squads_keyboard())
    except aiosqlite.Error as e:
        logger.error(f"Ошибка в list_squads для {user_id}: {e}")
        await message.answer("⚠️ Ошибка базы данных.", reply_markup=get_squads_keyboard())

# Обработчик расформирования сквада
@dp.message(lambda message: message.text == "🗑️ Расформировать сквад")
async def delete_squad(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer(MESSAGES["no_access"], reply_markup=get_menu_keyboard(user_id))
        return
    await message.answer("🗑️ Введите название сквада для расформирования:", reply_markup=get_cancel_keyboard(True))
    await state.set_state(Form.delete_squad)

@dp.message(Form.delete_squad)
async def process_delete_squad(message: types.Message, state: FSMContext):
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
            async with conn.execute("SELECT id FROM squads WHERE name = ?", (squad_name,)) as cursor:
                squad = await cursor.fetchone()
                if not squad:
                    await message.answer(f"⚠️ Сквад '{squad_name}' не найден.", reply_markup=get_cancel_keyboard(True))
                    return
                squad_id = squad[0]
            await conn.execute("DELETE FROM squads WHERE id = ?", (squad_id,))
            await conn.execute("UPDATE escorts SET squad_id = NULL WHERE squad_id = ?", (squad_id,))
            await conn.commit()
        await message.answer(MESSAGES["squad_deleted"].format(squad_name=squad_name), reply_markup=get_squads_keyboard())
        await log_action("delete_squad", user_id, None, f"Расформирован сквад '{squad_name}'")
        await state.clear()
    except aiosqlite.Error as e:
        logger.error(f"Ошибка в process_delete_squad для {user_id}: {e}")
        await message.answer("⚠️ Ошибка базы данных.", reply_markup=get_squads_keyboard())
        await state.clear()

# Обработчик добавления сопровождающего
@dp.message(lambda message: message.text == "👤 Добавить сопровождающего")
async def add_escort(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer(MESSAGES["no_access"], reply_markup=get_menu_keyboard(user_id))
        return
    await message.answer(
        "👤 Введите данные сопровождающего (Telegram ID, @username, PUBG ID, Название сквада):",
        reply_markup=get_cancel_keyboard(True)
    )
    await state.set_state(Form.escort_info)

@dp.message(Form.escort_info)
async def process_escort_info(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if message.text == "🚫 Отмена":
        await message.answer(MESSAGES["cancel_action"], reply_markup=get_escorts_keyboard())
        await state.clear()
        return
    try:
        parts = [x.strip() for x in message.text.split(",", 3)]
        if len(parts) != 4:
            await message.answer(
                MESSAGES["invalid_format"] + "\nПример: 123456789, @username, PUBG123, Название сквада",
                reply_markup=get_cancel_keyboard(True)
            )
            return
        telegram_id, username, pubg_id, squad_name = parts
        telegram_id = int(telegram_id)
        if telegram_id == user_id:
            await message.answer("⚠️ Нельзя добавить самого себя!", reply_markup=get_cancel_keyboard(True))
            return
        async with aiosqlite.connect(DB_PATH) as conn:
            async with conn.execute("SELECT id FROM squads WHERE name = ?", (squad_name,)) as cursor:
                squad = await cursor.fetchone()
                if not squad:
                    await message.answer(f"⚠️ Сквад '{squad_name}' не найден.", reply_markup=get_cancel_keyboard(True))
                    return
                squad_id = squad[0]
            async with conn.execute("SELECT telegram_id FROM escorts WHERE telegram_id = ?", (telegram_id,)) as cursor:
                if await cursor.fetchone():
                    await message.answer(f"⚠️ Пользователь с Telegram ID {telegram_id} уже зарегистрирован.", 
                                       reply_markup=get_cancel_keyboard(True))
                    return
            await conn.execute(
                "INSERT INTO escorts (telegram_id, username, pubg_id, squad_id) VALUES (?, ?, ?, ?)",
                (telegram_id, username, pubg_id, squad_id)
            )
            await conn.commit()
        await message.answer(f"👤 Сопровождающий {username} успешно добавлен!", reply_markup=get_escorts_keyboard())
        await log_action("add_escort", user_id, None, f"Добавлен сопровождающий {username} (ID: {telegram_id})")
        await state.clear()
    except ValueError:
        await message.answer(
            MESSAGES["invalid_format"] + "\nПример: 123456789, @username, PUBG123, Название сквада",
            reply_markup=get_cancel_keyboard(True)
        )
    except aiosqlite.Error as e:
        logger.error(f"Ошибка в process_escort_info для {user_id}: {e}")
        await message.answer("⚠️ Ошибка базы данных.", reply_markup=get_escorts_keyboard())
        await state.clear()

# Обработчик удаления сопровождающего
@dp.message(lambda message: message.text == "🗑️ Удалить сопровождающего")
async def remove_escort(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer(MESSAGES["no_access"], reply_markup=get_menu_keyboard(user_id))
        return
    await message.answer("🗑️ Введите Telegram ID сопровождающего для удаления:", reply_markup=get_cancel_keyboard(True))
    await state.set_state(Form.remove_escort)

@dp.message(Form.remove_escort)
async def process_remove_escort(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if message.text == "🚫 Отмена":
        await message.answer(MESSAGES["cancel_action"], reply_markup=get_escorts_keyboard())
        await state.clear()
        return
    try:
        telegram_id = int(message.text.strip())
        if telegram_id == user_id:
            await message.answer("⚠️ Нельзя удалить самого себя!", reply_markup=get_cancel_keyboard(True))
            return
        async with aiosqlite.connect(DB_PATH) as conn:
            async with conn.execute("SELECT username FROM escorts WHERE telegram_id = ?", (telegram_id,)) as cursor:
                user = await cursor.fetchone()
                if not user:
                    await message.answer(f"⚠️ Пользователь с Telegram ID {telegram_id} не найден.", 
                                       reply_markup=get_cancel_keyboard(True))
                    return
                username = user[0]
            await conn.execute("DELETE FROM escorts WHERE telegram_id = ?", (telegram_id,))
            await conn.commit()
        await message.answer(f"🗑️ Сопровождающий @{username} удален.", reply_markup=get_escorts_keyboard())
        await log_action("remove_escort", user_id, None, f"Удален сопровождающий @{username} (ID: {telegram_id})")
        await state.clear()
    except ValueError:
        await message.answer("⚠️ Неверный формат Telegram ID.", reply_markup=get_cancel_keyboard(True))
    except aiosqlite.Error as e:
        logger.error(f"Ошибка в process_remove_escort для {user_id}: {e}")
        await message.answer("⚠️ Ошибка базы данных.", reply_markup=get_escorts_keyboard())
        await state.clear()

# Обработчик списка пользователей
@dp.message(lambda message: message.text == "👥 Пользователи")
async def list_escorts(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer(MESSAGES["no_access"], reply_markup=get_menu_keyboard(user_id))
        return
    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            async with conn.execute(
                '''
                SELECT telegram_id, username, pubg_id, squad_id, balance, reputation,
                       completed_orders, rating, rating_count, is_banned, ban_until, restrict_until
                FROM escorts
                '''
            ) as cursor:
                escorts = await cursor.fetchall()
        if not escorts:
            await message.answer(MESSAGES["no_escorts"], reply_markup=get_escorts_keyboard())
            return
        response = "👥 Список сопровождающих:\n"
        for escort in escorts:
            telegram_id, username, pubg_id, squad_id, balance, reputation, completed_orders, rating, rating_count, is_banned, ban_until, restrict_until = escort
            squad_info = await get_squad_info(squad_id) if squad_id else None
            squad_name = squad_info[0] if squad_info else "Нет"
            ban_status = "🚫 Забанен" if is_banned else (
                f"⏰ Бан до {datetime.fromisoformat(ban_until).strftime('%d.%m.%Y %H:%M')}" if ban_until else "✅ Активен"
            )
            restrict_status = (
                f"⛔ Ограничен до {datetime.fromisoformat(restrict_until).strftime('%d.%m.%Y %H:%M')}" 
                if restrict_until else "🔓 Без ограничений"
            )
            response += (
                f"👤 @{username} (ID: {telegram_id})\n🎮 PUBG ID: {pubg_id}\n🏠 Сквад: {squad_name}\n"
                f"💰 Баланс: {balance:.2f} руб.\n🌟 Репутация: {reputation}\n📝 Заказов: {completed_orders}\n"
                f"⭐ Рейтинг: {rating:.1f} ({rating_count} оценок)\n🔒 Статус: {ban_status}\n"
                f"⛔ Ограничения: {restrict_status}\n\n"
            )
        await message.answer(response, reply_markup=get_escorts_keyboard())
    except aiosqlite.Error as e:
        logger.error(f"Ошибка в list_escorts для {user_id}: {e}")
        await message.answer("⚠️ Ошибка базы данных.", reply_markup=get_escorts_keyboard())

# Обработчик добавления заказа
@dp.message(lambda message: message.text == "📝 Добавить заказ")
async def add_order(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer(MESSAGES["no_access"], reply_markup=get_menu_keyboard(user_id))
        return
    await message.answer(
        "📝 Введите данные заказа (ID заказа, описание клиента, сумма):",
        reply_markup=get_cancel_keyboard(True)
    )
    await state.set_state(Form.add_order)

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
                MESSAGES["invalid_format"] + "\nПример: ORDER123, Клиент Иванов, 5000",
                reply_markup=get_cancel_keyboard(True)
            )
            return
        order_id, customer, amount_str = parts
        amount = float(amount_str)
        if amount <= 0 or not order_id or not customer:
            await message.answer(
                "⚠️ ID заказа и описание не могут быть пустыми, сумма должна быть положительной.",
                reply_markup=get_cancel_keyboard(True)
            )
            return
        async with aiosqlite.connect(DB_PATH) as conn:
            async with conn.execute("SELECT id FROM orders WHERE memo_order_id = ?", (order_id,)) as cursor:
                if await cursor.fetchone():
                    await message.answer(f"⚠️ Заказ #{order_id} уже существует.", reply_markup=get_cancel_keyboard(True))
                    return
            await conn.execute(
                "INSERT INTO orders (memo_order_id, customer_info, amount) VALUES (?, ?, ?)",
                (order_id, customer, amount)
            )
            await conn.commit()
        await message.answer(
            MESSAGES["order_added"].format(order_id=order_id, customer=customer, amount=amount, description=customer),
            reply_markup=get_orders_keyboard()
        )
        await log_action("add_order", user_id, order_id, f"Добавлен заказ #{order_id} для {customer}, сумма: {amount:.2f}")
        await notify_admins(f"📝 Новый заказ #{order_id} добавлен!\nКлиент: {customer}\nСумма: {amount:.2f} руб.")
        await state.clear()
    except ValueError:
        await message.answer(
            MESSAGES["invalid_format"] + "\nПример: ORDER123, Клиент Иванов, 5000",
            reply_markup=get_cancel_keyboard(True)
        )
    except aiosqlite.Error as e:
        logger.error(f"Ошибка в process_add_order для {user_id}: {e}")
        await message.answer("⚠️ Ошибка базы данных.", reply_markup=get_orders_keyboard())
        await state.clear()

# Обработчик начисления баланса
@dp.message(lambda message: message.text == "💸 Начислить")
async def add_balance(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer(MESSAGES["no_access"], reply_markup=get_menu_keyboard(user_id))
        return
    await message.answer(
        "💸 Введите Telegram ID и сумму для начисления (через запятую):",
        reply_markup=get_cancel_keyboard(True)
    )
    await state.set_state(Form.balance_amount)

@dp.message(Form.balance_amount)
async def process_balance_amount(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if message.text == "🚫 Отмена":
        await message.answer(MESSAGES["cancel_action"], reply_markup=get_balances_keyboard())
        await state.clear()
        return
    try:
        parts = [x.strip() for x in message.text.split(",", 1)]
        if len(parts) != 2:
            await message.answer(
                MESSAGES["invalid_format"] + "\nПример: 123456789, 1000",
                reply_markup=get_cancel_keyboard(True)
            )
            return
        telegram_id, amount_str = parts
        telegram_id = int(telegram_id)
        amount = float(amount_str)
        if amount <= 0:
            await message.answer("⚠️ Сумма должна быть положительной.", reply_markup=get_cancel_keyboard(True))
            return
        async with aiosqlite.connect(DB_PATH) as conn:
            async with conn.execute("SELECT id FROM escorts WHERE telegram_id = ?", (telegram_id,)) as cursor:
                escort = await cursor.fetchone()
                if not escort:
                    await message.answer(f"⚠️ Пользователь с Telegram ID {telegram_id} не найден.", 
                                       reply_markup=get_cancel_keyboard(True))
                    return
            await conn.execute(
                "UPDATE escorts SET balance = balance + ? WHERE telegram_id = ?",
                (amount, telegram_id)
            )
            await conn.commit()
        await message.answer(
            MESSAGES["balance_added"].format(amount=amount, user_id=telegram_id),
            reply_markup=get_balances_keyboard()
        )
        await log_action("add_balance", user_id, None, f"Начислено {amount:.2f} руб. пользователю ID {telegram_id}")
        await safe_send_message(telegram_id, f"💸 Вам начислено {amount:.2f} руб.")
        await state.clear()
    except ValueError:
        await message.answer(
            MESSAGES["invalid_format"] + "\nПример: 123456789, 1000",
            reply_markup=get_cancel_keyboard(True)
        )
    except aiosqlite.Error as e:
        logger.error(f"Ошибка в process_balance_amount для {user_id}: {e}")
        await message.answer("⚠️ Ошибка базы данных.", reply_markup=get_balances_keyboard())
        await state.clear()

# Обработчик обнуления баланса
@dp.message(lambda message: message.text == "💰 Обнулить баланс")
async def zero_balance(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer(MESSAGES["no_access"], reply_markup=get_menu_keyboard(user_id))
        return
    await message.answer(
        "💰 Введите Telegram ID пользователя для обнуления баланса:",
        reply_markup=get_cancel_keyboard(True)
    )
    await state.set_state(Form.zero_balance)

@dp.message(Form.zero_balance)
async def process_zero_balance(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if message.text == "🚫 Отмена":
        await message.answer(MESSAGES["cancel_action"], reply_markup=get_balances_keyboard())
        await state.clear()
        return
    try:
        telegram_id = int(message.text.strip())
        if telegram_id == user_id:
            await message.answer("⚠️ Нельзя обнулить свой баланс!", reply_markup=get_cancel_keyboard(True))
            return
        async with aiosqlite.connect(DB_PATH) as conn:
            async with conn.execute("SELECT username FROM escorts WHERE telegram_id = ?", (telegram_id,)) as cursor:
                user = await cursor.fetchone()
                if not user:
                    await message.answer(f"⚠️ Пользователь с Telegram ID {telegram_id} не найден.", 
                                       reply_markup=get_cancel_keyboard(True))
                    return
            await conn.execute("UPDATE escorts SET balance = 0 WHERE telegram_id = ?", (telegram_id,))
            await conn.commit()
        await message.answer(
            MESSAGES["balance_zeroed"].format(user_id=telegram_id),
            reply_markup=get_balances_keyboard()
        )
        await log_action("zero_balance", user_id, None, f"Обнулен баланс пользователя ID {telegram_id}")
        await safe_send_message(telegram_id, "💰 Ваш баланс обнулен администратором.")
        await state.clear()
    except ValueError:
        await message.answer("⚠️ Неверный формат Telegram ID.", reply_markup=get_cancel_keyboard(True))
    except aiosqlite.Error as e:
        logger.error(f"Ошибка в process_zero_balance для {user_id}: {e}")
        await message.answer("⚠️ Ошибка базы данных.", reply_markup=get_balances_keyboard())
        await state.clear()

# Обработчик бана навсегда
@dp.message(lambda message: message.text == "🚫 Бан навсегда")
async def ban_permanent(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer(MESSAGES["no_access"], reply_markup=get_menu_keyboard(user_id))
        return
    await message.answer(
        "🚫 Введите Telegram ID пользователя для перманентного бана:",
        reply_markup=get_cancel_keyboard(True)
    )
    await state.set_state(Form.ban_permanent)

@dp.message(Form.ban_permanent)
async def process_ban_permanent(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if message.text == "🚫 Отмена":
        await message.answer(MESSAGES["cancel_action"], reply_markup=get_ban_restrict_keyboard())
        await state.clear()
        return
    try:
        telegram_id = int(message.text.strip())
        if telegram_id == user_id:
            await message.answer("⚠️ Нельзя забанить самого себя!", reply_markup=get_cancel_keyboard(True))
            return
        async with aiosqlite.connect(DB_PATH) as conn:
            async with conn.execute("SELECT username FROM escorts WHERE telegram_id = ?", (telegram_id,)) as cursor:
                user = await cursor.fetchone()
                if not user:
                    await message.answer(f"⚠️ Пользователь с Telegram ID {telegram_id} не найден.", 
                                       reply_markup=get_cancel_keyboard(True))
                    return
                username = user[0]
            await conn.execute(
                "UPDATE escorts SET is_banned = 1, ban_until = NULL WHERE telegram_id = ?",
                (telegram_id,)
            )
            await conn.commit()
        await message.answer(
            f"🚫 Пользователь @{username} заблокирован навсегда.",
            reply_markup=get_ban_restrict_keyboard()
        )
        await log_action("ban_permanent", user_id, None, f"Забанен пользователь @{username} (ID: {telegram_id}) навсегда")
        await safe_send_message(telegram_id, MESSAGES["user_banned"])
        await state.clear()
    except ValueError:
        await message.answer("⚠️ Неверный формат Telegram ID.", reply_markup=get_cancel_keyboard(True))
    except aiosqlite.Error as e:
        logger.error(f"Ошибка в process_ban_permanent для {user_id}: {e}")
        await message.answer("⚠️ Ошибка базы данных.", reply_markup=get_ban_restrict_keyboard())
        await state.clear()

# Обработчик бана на время
@dp.message(lambda message: message.text == "⏰ Бан на время")
async def ban_duration(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer(MESSAGES["no_access"], reply_markup=get_menu_keyboard(user_id))
        return
    await message.answer(
        "⏰ Введите Telegram ID и длительность бана в днях (через запятую):",
        reply_markup=get_cancel_keyboard(True)
    )
    await state.set_state(Form.ban_duration)

@dp.message(Form.ban_duration)
async def process_ban_duration(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if message.text == "🚫 Отмена":
        await message.answer(MESSAGES["cancel_action"], reply_markup=get_ban_restrict_keyboard())
        await state.clear()
        return
    try:
        parts = [x.strip() for x in message.text.split(",", 1)]
        if len(parts) != 2:
            await message.answer(
                MESSAGES["invalid_format"] + "\nПример: 123456789, 7",
                reply_markup=get_cancel_keyboard(True)
            )
            return
        telegram_id, days_str = parts
        telegram_id = int(telegram_id)
        days = int(days_str)
        if telegram_id == user_id:
            await message.answer("⚠️ Нельзя забанить самого себя!", reply_markup=get_cancel_keyboard(True))
            return
        if days <= 0:
            await message.answer("⚠️ Длительность бана должна быть положительной.", reply_markup=get_cancel_keyboard(True))
            return
        ban_until = (datetime.now() + timedelta(days=days)).isoformat()
        async with aiosqlite.connect(DB_PATH) as conn:
            async with conn.execute("SELECT username FROM escorts WHERE telegram_id = ?", (telegram_id,)) as cursor:
                user = await cursor.fetchone()
                if not user:
                    await message.answer(f"⚠️ Пользователь с Telegram ID {telegram_id} не найден.", 
                                       reply_markup=get_cancel_keyboard(True))
                    return
                username = user[0]
            await conn.execute(
                "UPDATE escorts SET is_banned = 1, ban_until = ? WHERE telegram_id = ?",
                (ban_until, telegram_id)
            )
            await conn.commit()
        formatted_date = datetime.fromisoformat(ban_until).strftime("%d.%m.%Y %H:%M")
        await message.answer(
            f"⏰ Пользователь @{username} заблокирован до {formatted_date}.",
            reply_markup=get_ban_restrict_keyboard()
        )
        await log_action("ban_duration", user_id, None, f"Забанен пользователь @{username} (ID: {telegram_id}) до {formatted_date}")
        await safe_send_message(telegram_id, MESSAGES["user_restricted"].format(date=formatted_date))
        await state.clear()
    except ValueError:
        await message.answer(
            MESSAGES["invalid_format"] + "\nПример: 123456789, 7",
            reply_markup=get_cancel_keyboard(True)
        )
    except aiosqlite.Error as e:
        logger.error(f"Ошибка в process_ban_duration для {user_id}: {e}")
        await message.answer("⚠️ Ошибка базы данных.", reply_markup=get_ban_restrict_keyboard())
        await state.clear()

# Обработчик ограничения
@dp.message(lambda message: message.text == "⛔ Ограничить")
async def restrict_user(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer(MESSAGES["no_access"], reply_markup=get_menu_keyboard(user_id))
        return
    await message.answer(
        "⛔ Введите Telegram ID и длительность ограничения в днях (через запятую):",
        reply_markup=get_cancel_keyboard(True)
    )
    await state.set_state(Form.restrict_duration)

@dp.message(Form.restrict_duration)
async def process_restrict_duration(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if message.text == "🚫 Отмена":
        await message.answer(MESSAGES["cancel_action"], reply_markup=get_ban_restrict_keyboard())
        await state.clear()
        return
    try:
        parts = [x.strip() for x in message.text.split(",", 1)]
        if len(parts) != 2:
            await message.answer(
                MESSAGES["invalid_format"] + "\nПример: 123456789, 7",
                reply_markup=get_cancel_keyboard(True)
            )
            return
        telegram_id, days_str = parts
        telegram_id = int(telegram_id)
        days = int(days_str)
        if telegram_id == user_id:
            await message.answer("⚠️ Нельзя ограничить самого себя!", reply_markup=get_cancel_keyboard(True))
            return
        if days <= 0:
            await message.answer("⚠️ Длительность ограничения должна быть положительной.", 
                               reply_markup=get_cancel_keyboard(True))
            return
        restrict_until = (datetime.now() + timedelta(days=days)).isoformat()
        async with aiosqlite.connect(DB_PATH) as conn:
            async with conn.execute("SELECT username FROM escorts WHERE telegram_id = ?", (telegram_id,)) as cursor:
                user = await cursor.fetchone()
                if not user:
                    await message.answer(f"⚠️ Пользователь с Telegram ID {telegram_id} не найден.", 
                                       reply_markup=get_cancel_keyboard(True))
                    return
                username = user[0]
            await conn.execute(
                "UPDATE escorts SET restrict_until = ? WHERE telegram_id = ?",
                (restrict_until, telegram_id)
            )
            await conn.commit()
        formatted_date = datetime.fromisoformat(restrict_until).strftime("%d.%m.%Y %H:%M")
        await message.answer(
            f"⛔ Пользователь @{username} ограничен до {formatted_date}.",
            reply_markup=get_ban_restrict_keyboard()
        )
        await log_action("restrict_user", user_id, None, f"Ограничен пользователь @{username} (ID: {telegram_id}) до {formatted_date}")
        await safe_send_message(telegram_id, MESSAGES["user_restricted"].format(date=formatted_date))
        await state.clear()
    except ValueError:
        await message.answer(
            MESSAGES["invalid_format"] + "\nПример: 123456789, 7",
            reply_markup=get_cancel_keyboard(True)
        )
    except aiosqlite.Error as e:
        logger.error(f"Ошибка в process_restrict_duration для {user_id}: {e}")
        await message.answer("⚠️ Ошибка базы данных.", reply_markup=get_ban_restrict_keyboard())
        await state.clear()

# Обработчик снятия бана
@dp.message(lambda message: message.text == "🔒 Снять бан")
async def unban_user(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer(MESSAGES["no_access"], reply_markup=get_menu_keyboard(user_id))
        return
    await message.answer(
        "🔒 Введите Telegram ID пользователя для снятия бана:",
        reply_markup=get_cancel_keyboard(True)
    )
    await state.set_state(Form.unban_user)

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
            async with conn.execute("SELECT username FROM escorts WHERE telegram_id = ?", (telegram_id,)) as cursor:
                user = await cursor.fetchone()
                if not user:
                    await message.answer(f"⚠️ Пользователь с Telegram ID {telegram_id} не найден.", 
                                       reply_markup=get_cancel_keyboard(True))
                    return
                username = user[0]
            await conn.execute(
                "UPDATE escorts SET is_banned = 0, ban_until = NULL WHERE telegram_id = ?",
                (telegram_id,)
            )
            await conn.commit()
        await message.answer(
            MESSAGES["user_unbanned"].format(username=username),
            reply_markup=get_ban_restrict_keyboard()
        )
        await log_action("unban_user", user_id, None, f"Снят бан с пользователя @{username} (ID: {telegram_id})")
        await safe_send_message(telegram_id, "🔒 Ваш бан снят. Вы снова можете использовать бота.")
        await state.clear()
    except ValueError:
        await message.answer("⚠️ Неверный формат Telegram ID.", reply_markup=get_cancel_keyboard(True))
    except aiosqlite.Error as e:
        logger.error(f"Ошибка в process_unban_user для {user_id}: {e}")
        await message.answer("⚠️ Ошибка базы данных.", reply_markup=get_ban_restrict_keyboard())
        await state.clear()

# Обработчик снятия ограничения
@dp.message(lambda message: message.text == "🔓 Снять ограничение")
async def unrestrict_user(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer(MESSAGES["no_access"], reply_markup=get_menu_keyboard(user_id))
        return
    await message.answer(
        "🔓 Введите Telegram ID пользователя для снятия ограничения:",
        reply_markup=get_cancel_keyboard(True)
    )
    await state.set_state(Form.unrestrict_user)

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
            async with conn.execute("SELECT username FROM escorts WHERE telegram_id = ?", (telegram_id,)) as cursor:
                user = await cursor.fetchone()
                if not user:
                    await message.answer(f"⚠️ Пользователь с Telegram ID {telegram_id} не найден.", 
                                       reply_markup=get_cancel_keyboard(True))
                    return
                username = user[0]
            await conn.execute(
                "UPDATE escorts SET restrict_until = NULL WHERE telegram_id = ?",
                (telegram_id,)
            )
            await conn.commit()
        await message.answer(
            MESSAGES["user_unrestricted"].format(username=username),
            reply_markup=get_ban_restrict_keyboard()
        )
        await log_action("unrestrict_user", user_id, None, f"Снято ограничение с пользователя @{username} (ID: {telegram_id})")
        await safe_send_message(telegram_id, "🔓 Ограничения с вас сняты. Вы снова можете использовать бота.")
        await state.clear()
    except ValueError:
        await message.answer("⚠️ Неверный формат Telegram ID.", reply_markup=get_cancel_keyboard(True))
    except aiosqlite.Error as e:
        logger.error(f"Ошибка в process_unrestrict_user для {user_id}: {e}")
        await message.answer("⚠️ Ошибка базы данных.", reply_markup=get_ban_restrict_keyboard())
        await state.clear()

# Обработчик балансов сопровождающих
@dp.message(lambda message: message.text == "💰 Балансы сопровождающих")
async def list_balances(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer(MESSAGES["no_access"], reply_markup=get_menu_keyboard(user_id))
        return
    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            async with conn.execute("SELECT telegram_id, username, balance FROM escorts") as cursor:
                escorts = await cursor.fetchall()
        if not escorts:
            await message.answer(MESSAGES["no_escorts"], reply_markup=get_balances_keyboard())
            return
        response = "💰 Балансы сопровождающих:\n"
        for telegram_id, username, balance in escorts:
            response += f"👤 @{username} (ID: {telegram_id}): {balance:.2f} руб.\n"
        await message.answer(response, reply_markup=get_balances_keyboard())
    except aiosqlite.Error as e:
        logger.error(f"Ошибка в list_balances для {user_id}: {e}")
        await message.answer("⚠️ Ошибка базы данных.", reply_markup=get_balances_keyboard())

# Обработчик отчета за месяц
@dp.message(lambda message: message.text == "📈 Отчет за месяц")
async def monthly_report(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer(MESSAGES["no_access"], reply_markup=get_menu_keyboard(user_id))
        return
    try:
        start_date = (datetime.now() - timedelta(days=30)).isoformat()
        async with aiosqlite.connect(DB_PATH) as conn:
            async with conn.execute(
                '''
                SELECT COUNT(*) as order_count, SUM(amount) as total_amount
                FROM orders
                WHERE created_at >= ?
                ''',
                (start_date,)
            ) as cursor:
                order_count, total_amount = await cursor.fetchone()
            async with conn.execute(
                '''
                SELECT COUNT(*) as payout_count, SUM(amount) as total_payout
                FROM payouts
                WHERE payout_date >= ?
                ''',
                (start_date,)
            ) as cursor:
                payout_count, total_payout = await cursor.fetchone()
        total_amount = total_amount or 0
        total_payout = total_payout or 0
        response = (
            f"📈 Отчет за последние 30 дней:\n"
            f"📝 Заказов: {order_count}\n💰 Сумма заказов: {total_amount:.2f} руб.\n"
            f"💸 Выплат: {payout_count}\n💰 Сумма выплат: {total_payout:.2f} руб.\n"
        )
        await message.answer(response, reply_markup=get_reports_keyboard())
    except aiosqlite.Error as e:
        logger.error(f"Ошибка в monthly_report для {user_id}: {e}")
        await message.answer("⚠️ Ошибка базы данных.", reply_markup=get_reports_keyboard())

# Обработчик экспорта данных
@dp.message(lambda message: message.text == "📤 Экспорт данных")
async def export_data(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer(MESSAGES["no_access"], reply_markup=get_menu_keyboard(user_id))
        return
    try:
        filename = await export_orders_to_csv()
        if not filename:
            await message.answer(MESSAGES["no_data_to_export"], reply_markup=get_reports_keyboard())
            return
        await message.answer(
            MESSAGES["export_success"].format(filename=filename),
            reply_markup=get_reports_keyboard()
        )
        await bot.send_document(user_id, FSInputFile(filename))
        await log_action("export_data", user_id, None, f"Экспортированы данные в {filename}")
        os.remove(filename)
    except (aiosqlite.Error, OSError) as e:
        logger.error(f"Ошибка экспорта данных для {user_id}: {e}")
        await message.answer("⚠️ Ошибка экспорта данных.", reply_markup=get_reports_keyboard())

# Обработчик журнала действий
@dp.message(lambda message: message.text == "📜 Журнал действий")
async def action_log(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer(MESSAGES["no_access"], reply_markup=get_menu_keyboard(user_id))
        return
    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            async with conn.execute(
                '''
                SELECT action_type, user_id, order_id, description, action_date
                FROM action_log
                ORDER BY action_date DESC
                LIMIT 50
                '''
            ) as cursor:
                actions = await cursor.fetchall()
        if not actions:
            await message.answer("⚠️ Журнал действий пуст.", reply_markup=get_reports_keyboard())
            return
        response = "📜 Журнал действий (последние 50):\n"
        for action_type, action_user_id, order_id, description, action_date in actions:
            formatted_date = datetime.fromisoformat(action_date).strftime("%d.%m.%Y %H:%M")
            response += (
                f"[{formatted_date}] {action_type} (ID: {action_user_id}, Заказ: {order_id or 'N/A'}): {description}\n"
            )
        await message.answer(response, reply_markup=get_reports_keyboard())
    except aiosqlite.Error as e:
        logger.error(f"Ошибка в action_log для {user_id}: {e}")
        await message.answer("⚠️ Ошибка базы данных.", reply_markup=get_reports_keyboard())

# Обработчик дохода пользователя
@dp.message(lambda message: message.text == "📈 Доход пользователя")
async def user_profit(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer(MESSAGES["no_access"], reply_markup=get_menu_keyboard(user_id))
        return
    await message.answer(
        "📈 Введите Telegram ID пользователя для отчета о доходе:",
        reply_markup=get_cancel_keyboard(True)
    )
    await state.set_state(Form.profit_user)

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
            async with conn.execute(
                "SELECT username, balance, completed_orders FROM escorts WHERE telegram_id = ?", (telegram_id,)
            ) as cursor:
                user = await cursor.fetchone()
                if not user:
                    await message.answer(f"⚠️ Пользователь с Telegram ID {telegram_id} не найден.", 
                                       reply_markup=get_cancel_keyboard(True))
                    return
                username, balance, completed_orders = user
            start_date = (datetime.now() - timedelta(days=30)).isoformat()
            async with conn.execute(
                '''
                SELECT COUNT(*) as order_count, SUM(amount) as total_amount
                FROM orders
                WHERE escort_id = ? AND created_at >= ? AND status = 'completed'
                ''',
                (telegram_id, start_date)
            ) as cursor:
                order_count, total_amount = await cursor.fetchone()
            async with conn.execute(
                '''
                SELECT SUM(amount) as total_payout
                FROM payouts
                WHERE user_id = ? AND payout_date >= ?
                ''',
                (telegram_id, start_date)
            ) as cursor:
                total_payout = (await cursor.fetchone())[0] or 0
        total_amount = total_amount or 0
        response = (
            f"📈 Доход пользователя @{username} (ID: {telegram_id}):\n"
            f"💰 Текущий баланс: {balance:.2f} руб.\n📝 Завершенных заказов за месяц: {order_count}\n"
            f"💸 Сумма заказов за месяц: {total_amount:.2f} руб.\n💵 Выплачено за месяц: {total_payout:.2f} руб.\n"
        )
        await message.answer(response, reply_markup=get_reports_keyboard())
        await log_action("view_user_profit", user_id, None, f"Просмотрен доход пользователя @{username} (ID: {telegram_id})")
        await state.clear()
    except ValueError:
        await message.answer("⚠️ Неверный формат Telegram ID.", reply_markup=get_cancel_keyboard(True))
    except aiosqlite.Error as e:
        logger.error(f"Ошибка в process_user_profit для {user_id}: {e}")
        await message.answer("⚠️ Ошибка базы данных.", reply_markup=get_reports_keyboard())
        await state.clear()

# Обработчик запросов в поддержку
@dp.message(lambda message: message.text == "📩 Поддержка")
async def support_request(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if not await check_access(message):
        return
    await message.answer(MESSAGES["support_request"], reply_markup=get_cancel_keyboard())
    await state.set_state(Form.support_message)

@dp.message(Form.support_message)
async def process_support_message(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if message.text == "🚫 Отмена":
        await message.answer(MESSAGES["cancel_action"], reply_markup=get_menu_keyboard(user_id))
        await state.clear()
        return
    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            async with conn.execute("SELECT username FROM escorts WHERE telegram_id = ?", (user_id,)) as cursor:
                user = await cursor.fetchone()
                username = user[0] if user else "Unknown"
        support_text = message.text.strip()
        if not support_text:
            await message.answer("⚠️ Запрос не может быть пустым.", reply_markup=get_cancel_keyboard())
            return
        await notify_admins(
            f"📩 Новый запрос в поддержку от @{username} (ID: {user_id}):\n{support_text}",
            reply_to_user_id=user_id
        )
        await message.answer(MESSAGES["support_sent"], reply_markup=get_menu_keyboard(user_id))
        await log_action("support_request", user_id, None, f"Отправлен запрос в поддержку: {support_text}")
        await state.clear()
    except aiosqlite.Error as e:
        logger.error(f"Ошибка в process_support_message для {user_id}: {e}")
        await message.answer("⚠️ Ошибка базы данных.", reply_markup=get_menu_keyboard(user_id))
        await state.clear()

# Обработчик команды /my_orders
@dp.message(Command("my_orders"))
async def my_orders(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if not await check_access(message):
        return
    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            async with conn.execute(
                '''
                SELECT memo_order_id, customer_info, amount, status, created_at
                FROM orders
                WHERE escort_id = ?
                ORDER BY created_at DESC
                LIMIT 10
                ''',
                (user_id,)
            ) as cursor:
                orders = await cursor.fetchall()
        if not orders:
            await message.answer(MESSAGES["no_orders"], reply_markup=get_menu_keyboard(user_id))
            return
        response = "📝 Ваши заказы (последние 10):\n"
        for order_id, customer, amount, status, created_at in orders:
            formatted_date = datetime.fromisoformat(created_at).strftime("%d.%m.%Y %H:%M")
            status_text = "⏳ В ожидании" if status == "pending" else "✅ Завершен"
            response += (
                f"📝 Заказ #{order_id}\n👤 Клиент: {customer}\n💰 Сумма: {amount:.2f} руб.\n"
                f"📅 Дата: {formatted_date}\n📊 Статус: {status_text}\n\n"
            )
        await message.answer(response, reply_markup=get_menu_keyboard(user_id))
        await log_action("view_my_orders", user_id, None, "Просмотрены свои заказы")
    except aiosqlite.Error as e:
        logger.error(f"Ошибка в my_orders для {user_id}: {e}")
        await message.answer("⚠️ Ошибка базы данных.", reply_markup=get_menu_keyboard(user_id))

# Обработчик возврата назад
@dp.message(lambda message: message.text == "🔙 Назад")
async def go_back(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    await message.answer("🔙 Возврат в главное меню.", reply_markup=get_menu_keyboard(user_id))
    await state.clear()

# Обработчик неизвестных команд
@dp.message()
async def unknown_command(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if not await check_access(message):
        return
    await message.answer("⚠️ Неизвестная команда. Используйте кнопки меню.", reply_markup=get_menu_keyboard(user_id))
    await state.clear()

# Запуск бота
async def main():
    try:
        await init_db()
        scheduler.add_job(check_pending_orders, "interval", hours=24)
        scheduler.start()
        logger.info("Бот запущен")
        await dp.start_polling(bot, skip_updates=True)
    except Exception as e:
        logger.error(f"Ошибка при запуске бота: {e}\n{traceback.format_exc()}")
        raise

if __name__ == "__main__":
    asyncio.run(main())
