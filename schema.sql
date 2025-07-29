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
    description TEXT NOT NULL,
    status TEXT DEFAULT 'open',
    executor_id INTEGER,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    finished_at TEXT
);

-- Таблица выводов
CREATE TABLE IF NOT EXISTS withdraws (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    amount INTEGER NOT NULL,
    status TEXT DEFAULT 'pending',
    requested_at TEXT DEFAULT CURRENT_TIMESTAMP,
    processed_at TEXT
);

-- Таблица транзакций
CREATE TABLE IF NOT EXISTS transactions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    amount INTEGER NOT NULL,
    type TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

-- Таблица логов
CREATE TABLE IF NOT EXISTS logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    action TEXT,
    timestamp TEXT DEFAULT CURRENT_TIMESTAMP
);

-- Таблица жалоб
CREATE TABLE IF NOT EXISTS complaints (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    text TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

-- Таблица squad (отряды)
CREATE TABLE IF NOT EXISTS squads (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    rating INTEGER DEFAULT 0,
    rating_count INTEGER DEFAULT 0,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

-- Таблица сопровождающих (эскортов)
CREATE TABLE IF NOT EXISTS escorts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    telegram_id INTEGER NOT NULL,
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
    FOREIGN KEY (squad_id) REFERENCES squads(id) ON DELETE SET NULL
);

-- Связь эскортов и заказов
CREATE TABLE IF NOT EXISTS order_escorts (
    order_id INTEGER,
    escort_id INTEGER,
    pubg_id TEXT,
    PRIMARY KEY (order_id, escort_id),
    FOREIGN KEY (order_id) REFERENCES orders(id) ON DELETE CASCADE,
    FOREIGN KEY (escort_id) REFERENCES escorts(id) ON DELETE CASCADE
);

-- Заявки на заказы
CREATE TABLE IF NOT EXISTS order_applications (
    order_id INTEGER,
    escort_id INTEGER,
    squad_id INTEGER,
    pubg_id TEXT,
    PRIMARY KEY (order_id, escort_id),
    FOREIGN KEY (order_id) REFERENCES orders(id) ON DELETE CASCADE,
    FOREIGN KEY (escort_id) REFERENCES escorts(id) ON DELETE CASCADE,
    FOREIGN KEY (squad_id) REFERENCES squads(id) ON DELETE SET NULL
);

-- Выплаты
CREATE TABLE IF NOT EXISTS payouts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id INTEGER,
    escort_id INTEGER,
    amount REAL,
    payout_date TEXT DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (order_id) REFERENCES orders(id) ON DELETE SET NULL,
    FOREIGN KEY (escort_id) REFERENCES escorts(id) ON DELETE SET NULL
);

-- Лог действий
CREATE TABLE IF NOT EXISTS action_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    action_type TEXT,
    user_id INTEGER,
    order_id INTEGER,
    description TEXT,
    action_date TEXT DEFAULT CURRENT_TIMESTAMP
);

-- Индексы
CREATE INDEX IF NOT EXISTS idx_escorts_telegram_id ON escorts(telegram_id);
