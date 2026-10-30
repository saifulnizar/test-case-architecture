-- ============================================================================
-- MAJOO TAKE-HOME ASSESSMENT: TEST CASE 3 - DATABASE SCHEMA DESIGN
-- File: queries.sql
-- Description: Complex SQL Queries for Feed Generation & Common Social Access Patterns
-- ============================================================================

-- ============================================================================
-- 1. COMPLEX FEED GENERATION QUERY: USER HOME TIMELINE (FAN-OUT ON READ)
-- ============================================================================
-- Description:
-- Generates the personalized home feed for a user ($current_user_id) by:
-- 1. Gathering all followed user/topic feeds + the user's own feed.
-- 2. Gathering activities routed explicitly to the user's notification feed ("to" field / mentions).
-- 3. Applying Keyset Pagination (Cursor) via (created_at, id) tuple comparison for O(1) performance.
-- 4. Joining author profile metadata.
-- 5. Using LATERAL subqueries to aggregate like/comment counts and check 'is_liked_by_me'
--    strictly for the 20 fetched rows (preventing table-wide aggregate locks).
--
-- Parameters:
--   $1: current_user_id (UUID)
--   $2: current_user_feed_id (UUID - feeds.id for group 'user' and instance_key = $current_user_id)
--   $3: current_user_notification_feed_id (UUID - feeds.id for group 'notification' and instance_key = $current_user_id)
--   $4: cursor_created_at (TIMESTAMPTZ, NULL for page 1)
--   $5: cursor_activity_id (UUID, NULL for page 1)
--   $6: page_limit (INT, default 20)

WITH user_followed_feeds AS (
    -- 1. Subquery all feeds followed by current user
    SELECT followed_feed_id AS feed_id 
    FROM follows 
    WHERE follower_feed_id = $2

    UNION ALL

    -- 2. Include user's own feed so personal posts appear in their home timeline
    SELECT $2 AS feed_id
),
relevant_activities AS (
    -- A. Activities originating from followed user feeds
    SELECT 
        a.id, 
        a.user_id, 
        a.verb, 
        a.target_feed_id, 
        a.extra, 
        a.created_at
    FROM activities a
    JOIN feeds f ON f.group_name = 'user' AND f.instance_key = a.user_id::text
    JOIN user_followed_feeds uff ON f.id = uff.feed_id
    WHERE (
        $4::timestamptz IS NULL 
        OR (a.created_at, a.id) < ($4::timestamptz, $5::uuid)
    )

    UNION

    -- B. Activities explicitly routed to the user's notification/inbox feed via "to" field
    SELECT 
        a.id, 
        a.user_id, 
        a.verb, 
        a.target_feed_id, 
        a.extra, 
        a.created_at
    FROM activities a
    JOIN activity_targets at ON a.id = at.activity_id
    WHERE at.feed_id = $3
      AND (
        $4::timestamptz IS NULL 
        OR (a.created_at, a.id) < ($4::timestamptz, $5::uuid)
      )
)
SELECT 
    ra.id AS activity_id,
    ra.verb,
    ra.extra AS payload,
    ra.created_at,
    
    -- Author Profile Enrichment
    u.id AS author_id,
    u.username AS author_username,
    u.display_name AS author_name,
    u.avatar_url AS author_avatar,

    -- Target context (if posted to a wall/community)
    ra.target_feed_id,
    
    -- Real-time Reaction & Comment Aggregations via Lateral Subquery
    COALESCE(r_stats.like_count, 0) AS total_likes,
    COALESCE(r_stats.comment_count, 0) AS total_comments,
    
    -- Personal interaction state
    EXISTS (
        SELECT 1 FROM reactions r_own 
        WHERE r_own.activity_id = ra.id 
          AND r_own.user_id = $1 
          AND r_own.kind = 'like' 
          AND r_own.deleted_at IS NULL
    ) AS is_liked_by_me

FROM relevant_activities ra
JOIN users u ON ra.user_id = u.id

-- Lateral subquery computes aggregates strictly for the sliced 20 rows
LEFT JOIN LATERAL (
    SELECT 
        COUNT(*) FILTER (WHERE kind = 'like') AS like_count,
        COUNT(*) FILTER (WHERE kind = 'comment') AS comment_count
    FROM reactions r
    WHERE r.activity_id = ra.id AND r.deleted_at IS NULL
) r_stats ON TRUE

ORDER BY ra.created_at DESC, ra.id DESC
LIMIT COALESCE($6, 20);


-- ============================================================================
-- 2. NOTIFICATIONS & MENTIONS INBOX FEED
-- ============================================================================
-- Serves Query: Activity feed generated specifically from direct mentions and tags.
-- Utilizes Index: idx_activity_targets_feed_activity (Index Scan)
SELECT 
    a.id AS activity_id,
    a.verb,
    a.extra AS content,
    a.created_at,
    u.id AS sender_id,
    u.username AS sender_username,
    u.avatar_url AS sender_avatar
FROM activities a
JOIN activity_targets at ON a.id = at.activity_id
JOIN users u ON a.user_id = u.id
WHERE at.feed_id = $1 -- current user's notification feed_id
  AND ($2::timestamptz IS NULL OR (a.created_at, a.id) < ($2::timestamptz, $3::uuid))
ORDER BY a.created_at DESC, a.id DESC
LIMIT 20;


-- ============================================================================
-- 3. THREADED COMMENTS & REPLIES RETRIEVAL (RECURSIVE CTE)
-- ============================================================================
-- Serves Query: Fetching top-level comments and all their nested replies up to depth 3
-- Utilizes Index: idx_reactions_activity_toplevel_comments & idx_reactions_parent_replies
WITH RECURSIVE comment_tree AS (
    -- Anchor member: Top-level comments on the post
    SELECT 
        r.id,
        r.activity_id,
        r.user_id,
        r.parent_reaction_id,
        r.data AS comment_text,
        r.created_at,
        1 AS depth,
        ARRAY[r.created_at] AS sort_path
    FROM reactions r
    WHERE r.activity_id = $1
      AND r.kind = 'comment'
      AND r.parent_reaction_id IS NULL
      AND r.deleted_at IS NULL

    UNION ALL

    -- Recursive member: Child replies to parent comments
    SELECT 
        c.id,
        c.activity_id,
        c.user_id,
        c.parent_reaction_id,
        c.data AS comment_text,
        c.created_at,
        ct.depth + 1,
        ct.sort_path || c.created_at
    FROM reactions c
    JOIN comment_tree ct ON c.parent_reaction_id = ct.id
    WHERE c.kind = 'comment' 
      AND c.deleted_at IS NULL
      AND ct.depth < 3 -- Cap tree depth at 3 levels
)
SELECT 
    ct.id,
    ct.parent_reaction_id,
    ct.depth,
    ct.comment_text,
    ct.created_at,
    u.username,
    u.display_name,
    u.avatar_url
FROM comment_tree ct
JOIN users u ON ct.user_id = u.id
ORDER BY ct.sort_path ASC;


-- ============================================================================
-- 4. PRIVATE MESSAGING INBOX LIST WITH LAST MESSAGE & UNREAD BADGE
-- ============================================================================
-- Serves Query: WhatsApp/Direct Message conversation list.
-- Shows each channel, the last message snippet, timestamp, and unread count.
-- Utilizes Indexes: idx_channel_members_user_channel, idx_channels_last_message
SELECT 
    c.id AS channel_id,
    c.channel_type,
    c.name AS channel_name,
    c.last_message_at,
    
    -- Counterpart info for 1:1 direct chat
    other_user.username AS counterpart_username,
    other_user.avatar_url AS counterpart_avatar,
    
    -- Last message preview
    last_msg.message_text AS last_message_preview,
    last_msg.created_at AS last_message_time,
    
    -- Dynamic unread messages calculation based on pointer
    COALESCE(unread.count, 0) AS unread_count

FROM channels c
JOIN channel_members cm ON c.id = cm.channel_id AND cm.user_id = $1 -- current user
LEFT JOIN channel_read_positions crp ON crp.channel_id = c.id AND crp.user_id = $1

-- Find counterpart for 1:1 direct message
LEFT JOIN channel_members other_cm ON c.id = other_cm.channel_id AND other_cm.user_id <> $1 AND c.channel_type = 'direct'
LEFT JOIN users other_user ON other_cm.user_id = other_user.id

-- Fetch snippet of the latest message
LEFT JOIN LATERAL (
    SELECT m.message_text, m.created_at
    FROM messages m
    WHERE m.channel_id = c.id
    ORDER BY m.id DESC
    LIMIT 1
) last_msg ON TRUE

-- Count unread messages strictly since user's last_read_message_id
LEFT JOIN LATERAL (
    SELECT COUNT(*) AS count
    FROM messages m
    WHERE m.channel_id = c.id
      AND (crp.last_read_message_id IS NULL OR m.id > crp.last_read_message_id)
      AND m.sender_id <> $1 -- don't count messages sent by current user
) unread ON TRUE

ORDER BY c.last_message_at DESC NULLS LAST
LIMIT 30;


-- ============================================================================
-- 5. SOCIAL GRAPH QUERIES (INDEX-ONLY SCANS)
-- ============================================================================

-- Query 5A: Who is following User X? (Followers List)
-- Utilizes Reverse Index: idx_follows_followed_follower
SELECT 
    u.id AS follower_id,
    u.username,
    u.display_name,
    u.avatar_url,
    f.created_at AS followed_since
FROM follows f
JOIN feeds f_follower ON f.follower_feed_id = f_follower.id
JOIN users u ON f_follower.instance_key = u.id::text
WHERE f.followed_feed_id = $1
ORDER BY f.created_at DESC
LIMIT 50;

-- Query 5B: Who is User X following? (Following List)
-- Utilizes Primary Key Index: follows_pkey (leading follower_feed_id)
SELECT 
    u.id AS followed_user_id,
    u.username,
    u.display_name,
    u.avatar_url,
    f.created_at AS following_since
FROM follows f
JOIN feeds f_followed ON f.followed_feed_id = f_followed.id
JOIN users u ON f_followed.instance_key = u.id::text
WHERE f.follower_feed_id = $1
ORDER BY f.created_at DESC
LIMIT 50;
