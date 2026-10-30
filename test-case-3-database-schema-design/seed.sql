-- ============================================================================
-- MAJOO TAKE-HOME ASSESSMENT: TEST CASE 3 - DATABASE SCHEMA DESIGN
-- File: seed.sql
-- Description: Realistic Sample Dataset for Testing Schema, Constraints, & Queries
-- ============================================================================

-- Clean existing data in reverse dependency order
TRUNCATE TABLE channel_read_positions CASCADE;
TRUNCATE TABLE messages CASCADE;
TRUNCATE TABLE channel_members CASCADE;
TRUNCATE TABLE channels CASCADE;
TRUNCATE TABLE reactions CASCADE;
TRUNCATE TABLE activity_targets CASCADE;
TRUNCATE TABLE activities CASCADE;
TRUNCATE TABLE follows CASCADE;
TRUNCATE TABLE feeds CASCADE;
TRUNCATE TABLE feed_groups CASCADE;
TRUNCATE TABLE users CASCADE;

-- 1. Seed Feed Groups (Namespaces)
INSERT INTO feed_groups (name, description) VALUES
('user', 'Individual user profile feeds'),
('timeline', 'Personalized user home timeline feeds'),
('notification', 'Direct mentions, tags, and notification inbox feeds'),
('topic', 'Community topic feeds (e.g. #golang, #architecture)');

-- 2. Seed Users
INSERT INTO users (id, username, email, password_hash, display_name, bio) VALUES
('a0000000-0000-0000-0000-000000000001', 'alice', 'alice@example.com', '$2a$12$e8n...hash', 'Alice Wonder', 'Senior Backend Engineer (Go)'),
('b0000000-0000-0000-0000-000000000002', 'bob', 'bob@example.com', '$2a$12$e8n...hash', 'Bob Builder', 'Cloud & Database Architect'),
('c0000000-0000-0000-0000-000000000003', 'charlie', 'charlie@example.com', '$2a$12$e8n...hash', 'Charlie Chaplin', 'DevOps & Site Reliability Engineer'),
('d0000000-0000-0000-0000-000000000004', 'david', 'david@example.com', '$2a$12$e8n...hash', 'David Copper', 'Distributed Systems Enthusiast');

-- 3. Seed Feeds
INSERT INTO feeds (id, group_name, instance_key) VALUES
('fa000000-0000-0000-0000-000000000001', 'user', 'a0000000-0000-0000-0000-000000000001'),
('fa000000-0000-0000-0000-000000000002', 'timeline', 'a0000000-0000-0000-0000-000000000001'),
('fa000000-0000-0000-0000-000000000003', 'notification', 'a0000000-0000-0000-0000-000000000001'),

('fb000000-0000-0000-0000-000000000001', 'user', 'b0000000-0000-0000-0000-000000000002'),
('fb000000-0000-0000-0000-000000000002', 'timeline', 'b0000000-0000-0000-0000-000000000002'),
('fb000000-0000-0000-0000-000000000003', 'notification', 'b0000000-0000-0000-0000-000000000002'),

('fc000000-0000-0000-0000-000000000001', 'user', 'c0000000-0000-0000-0000-000000000003'),
('fc000000-0000-0000-0000-000000000002', 'timeline', 'c0000000-0000-0000-0000-000000000003'),

('fd000000-0000-0000-0000-000000000001', 'user', 'd0000000-0000-0000-0000-000000000004'),
('fd000000-0000-0000-0000-000000000002', 'timeline', 'd0000000-0000-0000-0000-000000000004');

-- 4. Seed Social Graph (Alice follows Bob and Charlie)
INSERT INTO follows (follower_feed_id, followed_feed_id) VALUES
('fa000000-0000-0000-0000-000000000001', 'fb000000-0000-0000-0000-000000000001'),
('fa000000-0000-0000-0000-000000000001', 'fc000000-0000-0000-0000-000000000001');

-- 5. Seed Activities (Posts with Multimedia via JSONB extra)
INSERT INTO activities (id, user_id, verb, extra, created_at) VALUES
('11111111-0000-0000-0000-000000000001', 'b0000000-0000-0000-0000-000000000002', 'post', '{"caption": "Halo dunia dari Bob! Postingan pertama dengan foto", "media": ["https://cdn.example.com/bob1.jpg"]}'::jsonb, NOW() - INTERVAL '2 hours'),
('11111111-0000-0000-0000-000000000002', 'c0000000-0000-0000-0000-000000000003', 'post', '{"caption": "Tutorial arsitektur database modern di PostgreSQL 16", "media": []}'::jsonb, NOW() - INTERVAL '1 hour'),
('11111111-0000-0000-0000-000000000003', 'd0000000-0000-0000-0000-000000000004', 'post', '{"caption": "Halo @alice! Silakan cek spesifikasi baru sistem kita", "media": []}'::jsonb, NOW() - INTERVAL '30 minutes');

-- 6. Explicit routing / Tagging: David targets Alice via activity_targets ("to" field)
INSERT INTO activity_targets (activity_id, feed_id) VALUES
('11111111-0000-0000-0000-000000000003', 'fa000000-0000-0000-0000-000000000003');

-- 7. Seed Reactions & Threaded Comments
-- Alice likes Bob's post
INSERT INTO reactions (id, activity_id, user_id, kind, data, created_at) VALUES
('22222222-0000-0000-0000-000000000001', '11111111-0000-0000-0000-000000000001', 'a0000000-0000-0000-0000-000000000001', 'like', NULL, NOW() - INTERVAL '1 hour');

-- Charlie comments on Bob's post (depth 1)
INSERT INTO reactions (id, activity_id, user_id, kind, data, created_at) VALUES
('22222222-0000-0000-0000-000000000002', '11111111-0000-0000-0000-000000000001', 'c0000000-0000-0000-0000-000000000003', 'comment', 'Keren banget pembahasannya Bob!', NOW() - INTERVAL '50 minutes');

-- Bob replies to Charlie's comment (depth 2 - threaded)
INSERT INTO reactions (id, activity_id, user_id, parent_reaction_id, kind, data, created_at) VALUES
('22222222-0000-0000-0000-000000000003', '11111111-0000-0000-0000-000000000001', 'b0000000-0000-0000-0000-000000000002', '22222222-0000-0000-0000-000000000002', 'comment', 'Terima kasih Charlie!', NOW() - INTERVAL '40 minutes');

-- 8. Seed Private Messaging: 1:1 Direct Chat between Alice and Bob
INSERT INTO channels (id, channel_type, created_by, last_message_at) VALUES
('33333333-0000-0000-0000-000000000001', 'direct', 'b0000000-0000-0000-0000-000000000002', NOW() - INTERVAL '5 minutes');

INSERT INTO channel_members (channel_id, user_id, role) VALUES
('33333333-0000-0000-0000-000000000001', 'a0000000-0000-0000-0000-000000000001', 'member'),
('33333333-0000-0000-0000-000000000001', 'b0000000-0000-0000-0000-000000000002', 'member');

INSERT INTO messages (channel_id, sender_id, message_text, created_at) VALUES
('33333333-0000-0000-0000-000000000001', 'b0000000-0000-0000-0000-000000000002', 'Halo Alice, apakah sudah lihat draft skema DB?', NOW() - INTERVAL '10 minutes'),
('33333333-0000-0000-0000-000000000001', 'b0000000-0000-0000-0000-000000000002', 'Ada update penting tentang caching strategy nih.', NOW() - INTERVAL '5 minutes');

-- Alice read up to the first message (so message 2 is unread)
INSERT INTO channel_read_positions (channel_id, user_id, last_read_message_id, last_read_at)
SELECT 
    '33333333-0000-0000-0000-000000000001',
    'a0000000-0000-0000-0000-000000000001',
    MIN(id),
    NOW() - INTERVAL '8 minutes'
FROM messages WHERE channel_id = '33333333-0000-0000-0000-000000000001';
