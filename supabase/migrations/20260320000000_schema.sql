-- IBD RS Rating Schema for Supabase PostgreSQL

CREATE TABLE IF NOT EXISTS price (
    ticker TEXT NOT NULL,
    date   TEXT NOT NULL,
    close  DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (ticker, date)
);
CREATE INDEX IF NOT EXISTS idx_price_date ON price(date);

CREATE TABLE IF NOT EXISTS rs (
    ticker    TEXT NOT NULL,
    date      TEXT NOT NULL,
    rs_raw    DOUBLE PRECISION NOT NULL,
    rs_rating INTEGER,
    PRIMARY KEY (ticker, date)
);
CREATE INDEX IF NOT EXISTS idx_rs_date ON rs(date);

CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

-- RLS: allow public read via Supabase REST API, writes only via direct connection
ALTER TABLE price ENABLE ROW LEVEL SECURITY;
ALTER TABLE rs ENABLE ROW LEVEL SECURITY;
ALTER TABLE meta ENABLE ROW LEVEL SECURITY;

CREATE POLICY "read_price" ON price FOR SELECT USING (true);
CREATE POLICY "read_rs" ON rs FOR SELECT USING (true);
CREATE POLICY "read_meta" ON meta FOR SELECT USING (true);
