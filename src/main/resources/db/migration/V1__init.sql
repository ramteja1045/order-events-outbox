CREATE TABLE orders (
    id           UUID PRIMARY KEY,
    customer_id  VARCHAR(100)   NOT NULL,
    sku          VARCHAR(100)   NOT NULL,
    quantity     INT            NOT NULL CHECK (quantity > 0),
    total        NUMERIC(19, 2) NOT NULL CHECK (total > 0),
    status       VARCHAR(16)    NOT NULL,
    created_at   TIMESTAMPTZ    NOT NULL
);

CREATE TABLE outbox_events (
    id            UUID PRIMARY KEY,
    aggregate_id  VARCHAR(100) NOT NULL,
    topic         VARCHAR(200) NOT NULL,
    payload       TEXT         NOT NULL,
    attempts      INT          NOT NULL DEFAULT 0,
    created_at    TIMESTAMPTZ  NOT NULL,
    published_at  TIMESTAMPTZ
);

-- The relay only ever scans unpublished rows, so a partial index keeps polling cheap.
CREATE INDEX idx_outbox_unpublished ON outbox_events (created_at) WHERE published_at IS NULL;

CREATE TABLE processed_events (
    event_id      UUID PRIMARY KEY,
    consumer      VARCHAR(100) NOT NULL,
    processed_at  TIMESTAMPTZ  NOT NULL
);
