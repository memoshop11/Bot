CREATE TABLE IF NOT EXISTS orders (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    customer_id INTEGER NOT NULL,
    description TEXT NOT NULL,
    status TEXT DEFAULT 'open',
    executor_id INTEGER,
    memo_order_id TEXT,  -- ВАЖНО: добавлено поле, которого не хватало
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    finished_at TEXT
);

CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    tg_id INTEGER UNIQUE NOT NULL,
    username TEXT,
    balance INTEGER DEFAULT 0,
    rating INTEGER DEFAULT 0,
    is_worker INTEGER DEFAULT 0,
    registered_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS withdraws (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    amount INTEGER NOT NULL,
    status TEXT DEFAULT 'pending',
    requested_at TEXT DEFAULT CURRENT_TIMESTAMP,
    processed_at TEXT
);

CREATE TABLE IF NOT EXISTS transactions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    amount INTEGER NOT NULL,
    type TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

-- Добавь остальные таблицы, если они есть в коде.
