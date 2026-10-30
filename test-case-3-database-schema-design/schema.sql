-- ============================================================================
-- MAJOO TAKE-HOME ASSESSMENT: TEST CASE 3 - DATABASE SCHEMA DESIGN
-- Platform: Social Media Platform
-- Database: PostgreSQL (Compatible with v14, v15, v16+)
-- Architecture: Normalized 3NF Relational Schema with Hybrid Stream Semantics
-- ============================================================================

-- Enable pgcrypto for UUID generation if needed (gen_random_uuid is native in PG13+)
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- Clean teardown (for fresh migration testing)
DROP TABLE IF EXISTS channel_read_positions CASCADE;
DROP TABLE IF EXISTS messages CASCADE;
DROP TABLE IF EXISTS channel_members CASCADE;
DROP TABLE IF EXISTS channels CASCADE;
DROP TABLE IF EXISTS reactions CASCADE;
DROP TABLE IF EXISTS activity_targets CASCADE;
DROP TABLE IF EXISTS activities CASCADE;
DROP TABLE IF EXISTS follows CASCADE;
DROP TABLE IF EXISTS feeds CASCADE;
DROP TABLE IF EXISTS feed_groups CASCADE;
DROP TABLE IF EXISTS users CASCADE;

-- ============================================================================
-- 1. USERS & PROFILES
-- ============================================================================

CREATE TABLE users (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    username      VARCHAR(50) NOT NULL,
    email         VARCHAR(255) NOT NULL,
    password_hash VARCHAR(255) NOT NULL,
    display_name  VARCHAR(100) NOT NULL,
    avatar_url    TEXT,
    bio           VARCHAR(500),
    is_private    BOOLEAN NOT NULL DEFAULT FALSE,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    -- Constraints
    CONSTRAINT chk_users_username_len CHECK (char_length(username) >= 3),
    CONSTRAINT chk_users_email_format CHECK (email ~* '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$')
);

-- Indexes for Users
-- Serves Query: User authentication, login by username/email, and profile lookup (Case-Insensitive)
CREATE UNIQUE INDEX idx_users_lower_username ON users (lower(username));
CREATE UNIQUE INDEX idx_users_lower_email ON users (lower(email));


-- ============================================================================
-- 2. FEEDS & SOCIAL GRAPH (FOLLOWERS / FOLLOWING)
-- ============================================================================

-- Namespace groups for feeds (e.g. 'user', 'timeline', 'notification', 'topic', 'community')
CREATE TABLE feed_groups (
    name        VARCHAR(32) PRIMARY KEY,
    description TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Generic registered feed instances
CREATE TABLE feeds (
    id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    group_name   VARCHAR(32) NOT NULL REFERENCES feed_groups(name) ON DELETE RESTRICT,
    instance_key VARCHAR(100) NOT NULL, -- e.g. user_id string, topic slug, or community name
    created_at   TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    -- Constraints: Exactly one feed per instance within a group namespace
    CONSTRAINT uq_feeds_group_instance UNIQUE (group_name, instance_key)
);

-- Generic Feed-to-Feed follow relationship (replaces separate followers/following tables)
CREATE TABLE follows (
    follower_feed_id UUID NOT NULL REFERENCES feeds(id) ON DELETE CASCADE,
    followed_feed_id UUID NOT NULL REFERENCES feeds(id) ON DELETE CASCADE,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    -- Primary Key: natural composite key preventing duplicate follow actions
    PRIMARY KEY (follower_feed_id, followed_feed_id),

    -- Integrity Check: Feed cannot follow itself
    CONSTRAINT chk_follows_no_self_follow CHECK (follower_feed_id <> followed_feed_id)
);

-- Indexes for Follows
-- Serves Query: "Who is following feed X?" (Follower list & fan-out write distribution)
-- Note: The PK already serves "Who is feed X following?" (Following list) via leading column.
CREATE INDEX idx_follows_followed_follower ON follows (followed_feed_id, follower_feed_id);


-- ============================================================================
-- 3. POSTS WITH MULTIMEDIA CONTENT & ACTIVITY FEEDS
-- ============================================================================

-- Master table for posts, multimedia content, and generic activities
CREATE TABLE activities (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id         UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    verb            VARCHAR(32) NOT NULL DEFAULT 'post',
    target_feed_id  UUID REFERENCES feeds(id) ON DELETE SET NULL, -- Optional wall/community destination
    extra           JSONB NOT NULL DEFAULT '{}'::jsonb,           -- Captions, media URLs, tags, dimensions
    created_at      TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    -- Constraints
    CONSTRAINT chk_activities_verb CHECK (verb IN ('post', 'share')),
    CONSTRAINT chk_activities_extra_not_null CHECK (extra IS NOT NULL)
);

-- Explicit activity distribution targets ("to" field / CC / Mentions / Direct Notifications)
CREATE TABLE activity_targets (
    activity_id UUID NOT NULL REFERENCES activities(id) ON DELETE CASCADE,
    feed_id     UUID NOT NULL REFERENCES feeds(id) ON DELETE CASCADE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    -- Primary Key: composite key guarantees 1NF and prevents duplicate delivery to same feed
    PRIMARY KEY (activity_id, feed_id)
);

-- Indexes for Activities & Targets
-- Serves Query: Author's personal timeline profile with keyset cursor (user_id, created_at DESC)
CREATE INDEX idx_activities_user_created ON activities (user_id, created_at DESC, id DESC);

-- Serves Query: Global public explore timeline (created_at DESC)
CREATE INDEX idx_activities_global_created ON activities (created_at DESC, id DESC);

-- Serves Query: Community / Target wall feed (target_feed_id, created_at DESC)
CREATE INDEX idx_activities_target_created ON activities (target_feed_id, created_at DESC, id DESC) 
    WHERE target_feed_id IS NOT NULL;

-- Serves Query: Notification inbox / explicit mention inbox retrieval (Reverse index on targets)
CREATE INDEX idx_activity_targets_feed_activity ON activity_targets (feed_id, activity_id);


-- ============================================================================
-- 4. COMMENTS & REACTIONS (UNIFIED SOCIAL INTERACTIONS)
-- ============================================================================

CREATE TABLE reactions (
    id                 UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    activity_id        UUID NOT NULL REFERENCES activities(id) ON DELETE CASCADE,
    user_id            UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    parent_reaction_id UUID REFERENCES reactions(id) ON DELETE CASCADE, -- Supports threaded comment replies
    kind               VARCHAR(32) NOT NULL,                           -- 'like', 'love', 'celebrate', 'comment'
    data               TEXT,                                           -- Comment text body
    created_at         TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    deleted_at         TIMESTAMPTZ,                                    -- Soft delete support

    -- Constraints
    CONSTRAINT chk_reactions_kind CHECK (kind IN ('like', 'love', 'celebrate', 'comment')),
    CONSTRAINT chk_reactions_comment_text CHECK (
        (kind = 'comment' AND data IS NOT NULL AND char_length(trim(data)) > 0)
        OR (kind <> 'comment')
    )
);

-- Partial Unique Index: A user can only give ONE reaction of each kind per activity (e.g. 1 like)
-- Comments are exempted from uniqueness (users can post multiple comments)
CREATE UNIQUE INDEX uq_reactions_user_activity_kind 
    ON reactions (activity_id, user_id, kind) 
    WHERE kind <> 'comment' AND deleted_at IS NULL;

-- Indexes for Reactions & Comments
-- Serves Query: Aggregating reaction counts per activity (like counts, reaction summary)
CREATE INDEX idx_reactions_activity_kind 
    ON reactions (activity_id, kind) 
    WHERE deleted_at IS NULL;

-- Serves Query: Top-level comments list on a post
CREATE INDEX idx_reactions_activity_toplevel_comments 
    ON reactions (activity_id, created_at ASC) 
    WHERE kind = 'comment' AND parent_reaction_id IS NULL AND deleted_at IS NULL;

-- Serves Query: Fetching nested comment replies (thread traversal)
CREATE INDEX idx_reactions_parent_replies 
    ON reactions (parent_reaction_id, created_at ASC) 
    WHERE parent_reaction_id IS NOT NULL AND deleted_at IS NULL;


-- ============================================================================
-- 5. PRIVATE MESSAGING SYSTEM (1:1 DIRECT & GROUP CHAT)
-- ============================================================================

-- Chat conversation room master
CREATE TABLE channels (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    channel_type    VARCHAR(16) NOT NULL, -- 'direct' or 'group'
    name            VARCHAR(100),         -- Display name (optional for direct, name for group)
    created_by      UUID REFERENCES users(id) ON DELETE SET NULL,
    last_message_at TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    -- Constraints
    CONSTRAINT chk_channels_type CHECK (channel_type IN ('direct', 'group'))
);

-- Participants in chat channels
CREATE TABLE channel_members (
    channel_id UUID NOT NULL REFERENCES channels(id) ON DELETE CASCADE,
    user_id    UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    role       VARCHAR(16) NOT NULL DEFAULT 'member', -- 'admin' or 'member'
    joined_at  TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    -- Primary Key: user can only join a channel once
    PRIMARY KEY (channel_id, user_id),
    CONSTRAINT chk_channel_members_role CHECK (role IN ('admin', 'member'))
);

-- Individual private messages
CREATE TABLE messages (
    id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY, -- Monotonically increasing for range queries
    channel_id   UUID NOT NULL REFERENCES channels(id) ON DELETE CASCADE,
    sender_id    UUID REFERENCES users(id) ON DELETE SET NULL,    -- Best practice: SET NULL preserves chat history
    message_text TEXT,
    attachments  JSONB DEFAULT '[]'::jsonb,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    -- Integrity Check: Message cannot be completely blank
    CONSTRAINT chk_messages_content CHECK (
        (message_text IS NOT NULL AND char_length(trim(message_text)) > 0)
        OR (attachments IS NOT NULL AND jsonb_array_length(attachments) > 0)
    )
);

-- Read receipts & unread tracking per user per channel
CREATE TABLE channel_read_positions (
    channel_id           UUID NOT NULL REFERENCES channels(id) ON DELETE CASCADE,
    user_id              UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    last_read_message_id BIGINT REFERENCES messages(id) ON DELETE SET NULL,
    last_read_at         TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (channel_id, user_id)
);

-- Indexes for Private Messaging
-- Serves Query: User's chat inbox list (all channels user belongs to, ordered by latest message)
CREATE INDEX idx_channel_members_user_channel ON channel_members (user_id, channel_id);
CREATE INDEX idx_channels_last_message ON channels (last_message_at DESC NULLS LAST);

-- Serves Query: Message history in channel with keyset pagination (scroll upwards)
CREATE INDEX idx_messages_channel_id_desc ON messages (channel_id, id DESC);

-- Serves Query: Unread message count calculation (messages in channel with id > last_read_message_id)
CREATE INDEX idx_messages_channel_id_asc ON messages (channel_id, id ASC);
