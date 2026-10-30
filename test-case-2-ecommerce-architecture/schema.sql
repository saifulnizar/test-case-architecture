-- ============================================================================
-- E-COMMERCE PLATFORM ARCHITECTURE — CORE POSTGRESQL SCHEMA (DDL)
-- Target Workload: 10,000+ Concurrent Users
-- Architecture: 4 Core Domain Services (Logical Schema / DB Isolation per Service)
-- ============================================================================

CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- ============================================================================
-- 1. CATALOG, SEARCH & RECOMMENDATION DOMAIN
--    (Read-Heavy; synced to OpenSearch & cached in Redis)
-- ============================================================================

CREATE TABLE categories (
    category_id   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    parent_id     UUID REFERENCES categories(category_id),
    name          VARCHAR(120) NOT NULL,
    slug          VARCHAR(140) NOT NULL UNIQUE,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE products (
    sku_id        UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    category_id   UUID NOT NULL REFERENCES categories(category_id),
    sku_code      VARCHAR(64) NOT NULL UNIQUE,
    name          VARCHAR(255) NOT NULL,
    description   TEXT,
    price         NUMERIC(15, 2) NOT NULL CHECK (price >= 0),
    currency      CHAR(3) NOT NULL DEFAULT 'IDR',
    attributes    JSONB NOT NULL DEFAULT '{}'::jsonb,
    is_active     BOOLEAN NOT NULL DEFAULT TRUE,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_products_category_active ON products(category_id) WHERE is_active = TRUE;

-- Precomputed co-occurrence table for "Frequently Bought Together" recommendations
CREATE TABLE product_cooccurrences (
    sku_id             UUID NOT NULL REFERENCES products(sku_id) ON DELETE CASCADE,
    related_sku_id     UUID NOT NULL REFERENCES products(sku_id) ON DELETE CASCADE,
    cooccurrence_score INT NOT NULL DEFAULT 1,
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (sku_id, related_sku_id),
    CHECK (sku_id <> related_sku_id)
);

CREATE INDEX idx_cooccurrences_top_related ON product_cooccurrences(sku_id, cooccurrence_score DESC);


-- ============================================================================
-- 2. INVENTORY DOMAIN
--    (High Write Contention; Real-Time Stock Consistency & Anti-Oversell)
-- ============================================================================

CREATE TABLE inventory (
    sku_id        UUID PRIMARY KEY, -- Logical reference to products(sku_id)
    available_qty INT NOT NULL DEFAULT 0 CHECK (available_qty >= 0),
    reserved_qty  INT NOT NULL DEFAULT 0 CHECK (reserved_qty >= 0),
    version       BIGINT NOT NULL DEFAULT 1,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE inventory_reservations (
    reservation_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    order_id       UUID NOT NULL,
    sku_id         UUID NOT NULL REFERENCES inventory(sku_id),
    quantity       INT NOT NULL CHECK (quantity > 0),
    status         VARCHAR(20) NOT NULL DEFAULT 'PENDING'
                   CHECK (status IN ('PENDING', 'COMMITTED', 'RELEASED', 'EXPIRED')),
    expires_at     TIMESTAMPTZ NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (order_id, sku_id)
);

-- Partial index for the background TTL worker that releases expired reservations every 30s
CREATE INDEX idx_inventory_reservations_expiry
    ON inventory_reservations(expires_at)
    WHERE status = 'PENDING';


-- ============================================================================
-- 3. ORDER & TRACKING DOMAIN
--    (Write-Critical; Checkout Saga Orchestrator, Order Processing & Tracking)
-- ============================================================================

CREATE TABLE users (
    user_id       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    email         VARCHAR(255) NOT NULL UNIQUE,
    phone         VARCHAR(32),
    password_hash TEXT NOT NULL,
    full_name     VARCHAR(150) NOT NULL,
    role          VARCHAR(20) NOT NULL DEFAULT 'BUYER' CHECK (role IN ('BUYER', 'ADMIN')),
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE shipping_addresses (
    address_id     UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id        UUID NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    recipient_name VARCHAR(150) NOT NULL,
    phone          VARCHAR(32) NOT NULL,
    address_line   TEXT NOT NULL,
    city           VARCHAR(100) NOT NULL,
    postal_code    VARCHAR(16) NOT NULL,
    is_default     BOOLEAN NOT NULL DEFAULT FALSE,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_shipping_addresses_user ON shipping_addresses(user_id);

CREATE TABLE orders (
    order_id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id             UUID NOT NULL REFERENCES users(user_id),
    idempotency_key     VARCHAR(64) NOT NULL UNIQUE, -- Prevents duplicate checkout submissions
    status              VARCHAR(30) NOT NULL DEFAULT 'PENDING_PAYMENT'
                        CHECK (status IN (
                            'PENDING_PAYMENT',
                            'PAID',
                            'PROCESSING',
                            'SHIPPED',
                            'IN_TRANSIT',
                            'DELIVERED',
                            'CANCELLED',
                            'REFUNDED'
                        )),
    tracking_number     VARCHAR(64),
    total_amount        NUMERIC(15, 2) NOT NULL CHECK (total_amount >= 0),
    shipping_cost       NUMERIC(15, 2) NOT NULL DEFAULT 0 CHECK (shipping_cost >= 0),
    currency            CHAR(3) NOT NULL DEFAULT 'IDR',
    shipping_snapshot   JSONB NOT NULL, -- Immutable snapshot of recipient address at checkout time
    payment_expires_at  TIMESTAMPTZ NOT NULL,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_orders_user_created ON orders(user_id, created_at DESC);
CREATE INDEX idx_orders_status ON orders(status) WHERE status IN ('PENDING_PAYMENT', 'PAID', 'PROCESSING');

CREATE TABLE order_items (
    order_item_id  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    order_id       UUID NOT NULL REFERENCES orders(order_id) ON DELETE CASCADE,
    sku_id         UUID NOT NULL,
    product_name   VARCHAR(255) NOT NULL,
    unit_price     NUMERIC(15, 2) NOT NULL CHECK (unit_price >= 0),
    quantity       INT NOT NULL CHECK (quantity > 0),
    subtotal       NUMERIC(15, 2) NOT NULL CHECK (subtotal >= 0),
    UNIQUE (order_id, sku_id)
);

-- Chronological tracking log for Order Processing & Tracking (cached in Redis order:tracking:{order_id})
CREATE TABLE order_tracking_history (
    tracking_id    BIGSERIAL PRIMARY KEY,
    order_id       UUID NOT NULL REFERENCES orders(order_id) ON DELETE CASCADE,
    status         VARCHAR(30) NOT NULL,
    location       VARCHAR(120),
    description    TEXT NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_order_tracking_history_order ON order_tracking_history(order_id, created_at ASC);


-- ============================================================================
-- 4. PAYMENT DOMAIN
--    (PCI-DSS Boundary; Multi-Gateway Adapter & Idempotent Webhook Handling)
-- ============================================================================

CREATE TABLE payments (
    payment_id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    order_id             UUID NOT NULL,
    idempotency_key      VARCHAR(64) NOT NULL UNIQUE,
    gateway_name         VARCHAR(30) NOT NULL CHECK (gateway_name IN ('MIDTRANS', 'XENDIT', 'STRIPE')),
    gateway_reference_id VARCHAR(128),
    payment_method       VARCHAR(50) NOT NULL,
    amount               NUMERIC(15, 2) NOT NULL CHECK (amount >= 0),
    currency             CHAR(3) NOT NULL DEFAULT 'IDR',
    status               VARCHAR(20) NOT NULL DEFAULT 'PENDING'
                         CHECK (status IN ('PENDING', 'SUCCEEDED', 'FAILED', 'EXPIRED', 'REFUNDED')),
    payment_url          TEXT, -- Redirect/Snap URL (Zero raw PAN/CVV stored)
    paid_at              TIMESTAMPTZ,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_payments_order_id ON payments(order_id);
CREATE UNIQUE INDEX idx_payments_gateway_ref ON payments(gateway_name, gateway_reference_id)
    WHERE gateway_reference_id IS NOT NULL;

-- Ensures webhooks delivered multiple times by Midtrans/Xendit/Stripe are processed once
CREATE TABLE payment_webhook_logs (
    webhook_log_id    BIGSERIAL PRIMARY KEY,
    gateway_name      VARCHAR(30) NOT NULL,
    gateway_event_id  VARCHAR(128) NOT NULL,
    order_id          UUID,
    raw_payload       JSONB NOT NULL,
    processed_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (gateway_name, gateway_event_id)
);


-- ============================================================================
-- 5. TRANSACTIONAL OUTBOX TABLE (Present in Order, Inventory, and Payment Schemas)
--    Guarantees atomic DB state update + NATS JetStream event publication
-- ============================================================================

CREATE TABLE outbox_events (
    event_id       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    aggregate_type VARCHAR(50) NOT NULL,  -- e.g. 'ORDER', 'INVENTORY', 'PAYMENT'
    aggregate_id   UUID NOT NULL,
    subject        VARCHAR(100) NOT NULL, -- NATS JetStream subject, e.g. 'payment.succeeded'
    payload        JSONB NOT NULL,
    status         VARCHAR(20) NOT NULL DEFAULT 'PENDING'
                   CHECK (status IN ('PENDING', 'PUBLISHED', 'FAILED')),
    retry_count    INT NOT NULL DEFAULT 0,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    published_at   TIMESTAMPTZ
);

CREATE INDEX idx_outbox_events_pending
    ON outbox_events(created_at ASC)
    WHERE status = 'PENDING';

