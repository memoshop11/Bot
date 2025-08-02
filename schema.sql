-- Таблица пользователей
CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    tg_id INTEGER UNIQUE NOT NULL,
    username TEXT,
    balance INTEGER DEFAULT 0,
    rating INTEGER DEFAULT 0,
    is_worker INTEGER DEFAULT 0,
    registered_at TEXT DEFAULT CURRENT_TIMESTAMP
);

-- Таблица заказов
CREATE TABLE IF NOT EXISTS orders (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    customer_id INTEGER NOT NULL,
    customer_info TEXT,
    description TEXT NOT NULL,
    amount INTEGER,
    memo_order_id TEXT,
    status TEXT DEFAULT 'open',
    executor_id INTEGER,
    squad_id INTEGER,
    rating INTEGER,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    finished_at TEXT
);

-- Таблица сопровождающих
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
    rules_accepted INTEGER DEFAULT 0
);

-- Заявки на заказы
CREATE TABLE IF NOT EXISTS order_applications (
    order_id INTEGER,
    escort_id INTEGER,
    squad_id INTEGER,
    pubg_id TEXT,
    PRIMARY KEY (order_id, escort_id)
);

-- Назначенные сопровождающие на заказ
CREATE TABLE IF NOT EXISTS order_escorts (
    order_id INTEGER,
    escort_id INTEGER,
    pubg_id TEXT,
    PRIMARY KEY (order_id, escort_id)
);

-- Выплаты
CREATE TABLE IF NOT EXISTS payouts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id INTEGER,
    escort_id INTEGER,
    amount REAL,
    commission_amount REAL DEFAULT 0,
    payout_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Вывод средств
CREATE TABLE IF NOT EXISTS withdraws (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    amount INTEGER NOT NULL,
    status TEXT DEFAULT 'pending',
    requested_at TEXT DEFAULT CURRENT_TIMESTAMP,
    processed_at TEXT
);

-- Транзакции
CREATE TABLE IF NOT EXISTS transactions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    amount INTEGER NOT NULL,
    type TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

-- Логи действий
CREATE TABLE IF NOT EXISTS action_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    action_type TEXT,
    user_id INTEGER,
    order_id INTEGER,
    description TEXT,
    action_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Жалобы
CREATE TABLE IF NOT EXISTS complaints (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    text TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

-- Сквады
CREATE TABLE IF NOT EXISTS squads (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT UNIQUE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    rating REAL DEFAULT 0,
    rating_count INTEGER DEFAULT 0
);

-- Логи
CREATE TABLE IF NOT EXISTS logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    action TEXT,
    timestamp TEXT DEFAULT CURRENT_TIMESTAMP
);

-- Индексы
CREATE INDEX IF NOT EXISTS idx_orders_memo_order_id ON orders (memo_order_id);
CREATE INDEX IF NOT EXISTS idx_order_escorts_order_id ON order_escorts (order_id);
CREATE INDEX IF NOT EXISTS idx_order_applications_order_id ON order_applications (order_id);
CREATE INDEX IF NOT EXISTS idx_payouts_order_id ON payouts (order_id);
CREATE INDEX IF NOT EXISTS idx_action_log_action_date ON action_log (action_date);
CREATE INDEX IF NOT EXISTS idx_escorts_telegram_id ON escorts (telegram_id);
