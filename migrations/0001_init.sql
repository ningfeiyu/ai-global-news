CREATE TABLE IF NOT EXISTS sources (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    url TEXT NOT NULL,
    category TEXT NOT NULL,
    type TEXT NOT NULL DEFAULT 'rss',
    enabled INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS articles (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_id INTEGER,
    title TEXT NOT NULL,
    url TEXT NOT NULL UNIQUE,
    description TEXT,
    published_at TEXT,
    category TEXT,
    content_hash TEXT,
    importance INTEGER DEFAULT 0,
    ai_selected INTEGER DEFAULT 0,
    event_id INTEGER,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY(source_id) REFERENCES sources(id)
);

CREATE INDEX IF NOT EXISTS idx_articles_published
ON articles(published_at);

CREATE INDEX IF NOT EXISTS idx_articles_hash
ON articles(content_hash);

CREATE INDEX IF NOT EXISTS idx_articles_event
ON articles(event_id);

CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_key TEXT UNIQUE,
    title TEXT NOT NULL,
    category TEXT,
    importance INTEGER DEFAULT 0,
    summary TEXT,
    why_it_matters TEXT,
    market_impact TEXT,
    countries TEXT,
    companies TEXT,
    assets TEXT,
    source_urls TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_events_created
ON events(created_at);

CREATE TABLE IF NOT EXISTS daily_digest (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    digest_date TEXT NOT NULL UNIQUE,
    content TEXT NOT NULL,
    event_count INTEGER DEFAULT 0,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);