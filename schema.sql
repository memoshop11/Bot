-- Включение поддержки foreign keys
PRAGMA foreign_keys = ON;

-- Таблица сквадов
CREATE TABLE IF NOT EXISTS squads (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    total_orders INTEGER DEFAULT 0,
    total_balance REAL DEFAULT 0.0,
    rating REAL DEFAULT 0.0,
    rating_count INTEGER DEFAULT 0,
    CONSTRAINT unique_squad_name UNIQUE (name)
);

-- Таблица сопровождающих
CREATE TABLE IF NOT EXISTS escorts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    telegram_id INTEGER NOT NULL,
    username TEXT,
    pubg_id TEXT,
    squad_id INTEGER,
    balance REAL DEFAULT 0.0,
    reputation INTEGER DEFAULT 0,
    completed_orders INTEGER DEFAULT 0,
    rating REAL DEFAULT 0.0,
    rating_count INTEGER DEFAULT 0,
    is_banned INTEGER DEFAULT 0,
    ban_until TEXT,
    restrict_until TEXT,
    rules_accepted INTEGER DEFAULT 0,
    CONSTRAINT unique_telegram_id UNIQUE (telegram_id),
    CONSTRAINT fk_squad FOREIGN KEY (squad_id) REFERENCES squads(id) ON DELETE SET NULL
);

-- Таблица заказов
CREATE TABLE IF NOT EXISTS orders (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    memo_order_id TEXT NOT NULL,
    customer_info TEXT,
    amount REAL NOT NULL,
    status TEXT DEFAULT 'pending',
    created_at TEXT DEFAULT (datetime('now')),
    commission_amount REAL DEFAULT 0.0,
    escort_id INTEGER,
    CONSTRAINT unique_memo_order_id UNIQUE (memo_order_id),
    CONSTRAINT fk_escort FOREIGN KEY (escort_id) REFERENCES escorts(id) ON DELETE SET NULL
);

-- Таблица логов действий
CREATE TABLE IF NOT EXISTS action_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    action_type TEXT NOT NULL,
    user_id INTEGER,
    order_id TEXT,
    description TEXT,
    action_date TEXT DEFAULT (datetime('now')),
    CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES escorts(id) ON DELETE SET NULL,
    CONSTRAINT fk_order FOREIGN KEY (order_id) REFERENCES orders(memo_order_id) ON DELETE SET NULL
);

-- Таблица выплат
CREATE TABLE IF NOT EXISTS payouts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    escort_id INTEGER NOT NULL,
    amount REAL NOT NULL,
    payout_date TEXT DEFAULT (datetime('now')),
    CONSTRAINT fk_escort_payout FOREIGN KEY (escort_id) REFERENCES escorts(id) ON DELETE CASCADE
);

-- Индексы для оптимизации запросов
CREATE INDEX IF NOT EXISTS idx_escorts_telegram_id ON escorts(telegram_id);
CREATE INDEX IF NOT EXISTS idx_escorts_squad_id ON escorts(squad_id);
CREATE INDEX IF NOT EXISTS idx_orders_escort_id ON orders(escort_id);
CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status);
CREATE INDEX IF NOT EXISTS idx_action_log_action_date ON action_log(action_date);
CREATE INDEX IF NOT EXISTS idx_payouts_escort_id ON payouts(escort_id);
