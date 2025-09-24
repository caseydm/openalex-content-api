CREATE TABLE IF NOT EXISTS api_usage (
  api_key TEXT NOT NULL,
  day DATE NOT NULL,
  api_usage_content INTEGER DEFAULT 0,
  PRIMARY KEY (api_key, day)
);

CREATE INDEX idx_api_usage_day ON api_usage(day);
