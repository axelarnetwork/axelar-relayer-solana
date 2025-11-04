CREATE TABLE IF NOT EXISTS solana_refunds (
    refund_id   TEXT NOT NULL,
    signature	TEXT NOT NULL,
    updated_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    PRIMARY KEY (refund_id)
);

