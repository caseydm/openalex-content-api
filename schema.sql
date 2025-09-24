CREATE TABLE api_keys (
  -- Primary fields
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  api_key TEXT,

  -- Rate limiting
  max_per_second INTEGER DEFAULT 10,
  max_per_day INTEGER DEFAULT 10000,

  -- User info
  email TEXT,
  name TEXT,
  organization TEXT,

  -- Account type/status
  is_academic BOOLEAN DEFAULT FALSE,
  premium_domain TEXT, -- e.g., 'harvard.edu' for academic verification

  -- Timestamps
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  expires_at DATETIME, -- NULL means never expires

  -- Additional info
  notes TEXT, -- Admin notes, JSON metadata, etc.

  -- Payment info
  credit_card_on_file BOOLEAN DEFAULT FALSE
);

-- Indexes for performance
CREATE INDEX idx_api_key ON api_keys(api_key);
