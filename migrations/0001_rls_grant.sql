CREATE TABLE IF NOT EXISTS rs (
    ticker    TEXT NOT NULL,
    date      TEXT NOT NULL,
    close     DOUBLE PRECISION,
    rs_raw    DOUBLE PRECISION,
    rs_rating INTEGER,
    PRIMARY KEY (ticker, date)
);
CREATE INDEX IF NOT EXISTS idx_rs_date ON rs(date);

CREATE TABLE IF NOT EXISTS tickers (
    ticker   TEXT PRIMARY KEY,
    sector   TEXT,
    industry TEXT
);

CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

ALTER TABLE rs ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS read_rs ON rs;
CREATE POLICY read_rs ON rs FOR SELECT USING (true);

ALTER TABLE tickers ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS read_tickers ON tickers;
CREATE POLICY read_tickers ON tickers FOR SELECT USING (true);

ALTER TABLE meta ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS read_meta ON meta;
CREATE POLICY read_meta ON meta FOR SELECT USING (true);

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'anonymous') THEN
        GRANT SELECT ON rs, tickers, meta TO anonymous;
    END IF;
END
$$;
