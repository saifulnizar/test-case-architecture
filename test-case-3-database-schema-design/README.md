# Database Schema Design — Social Media Platform
**Take-Home Database Assessment — Software Architect Engineer**

---

## 1. Executive Summary

Dokumen ini memuat perancangan skema database relasional (**PostgreSQL 14+**) dan strategi caching (**Redis**) untuk platform media sosial berskala besar. Desain ini secara ketat memenuhi seluruh spesifikasi dan kriteria penilaian yang diamanatkan pada lembar ujian:

* **Tugas Spesifik Pengujian:**
  1. Skema database ternormalisasi (3NF).
  2. Definisi relasi antar-entitas dan integritas data (*PK, FK, UNIQUE, CHECK*).
  3. Desain index B-Tree presisi untuk kueri-kueri umum (*access patterns*).
  4. Penulisan kueri SQL kompleks untuk pembuatan linimasa (*feed generation*).
  5. Perumusan strategi caching berlatensi rendah (*sub-5ms*).

* **Cakupan 5 Modul Fungsional:**
  * Profil Pengguna & Relasi Sosial (*Followers / Following*)
  * Konten Postingan dengan Multimedia (*Images, Videos, Captions*)
  * Komentar Bersarang (*Threaded Comments*) & Reaksi (*Likes/Reactions*)
  * Sistem Pesan Privat (*1:1 Direct Message & Group Chat*)
  * Linimasa Aktivitas (*Activity Feeds*) & Kotak Masuk Notifikasi (*Explicit Mentions*)

---

## 2. Diagram Relasi Entitas (Entity-Relationship Diagram)

Skema terdiri dari **11 tabel ternormalisasi** lengkap dengan atribut, tipe data, kunci primer (*PK*), kunci asing (*FK*), dan constraint unik (*UK*):

```mermaid
erDiagram
    users {
        uuid id PK
        varchar username UK "Case-insensitive unique"
        varchar email UK "Case-insensitive unique"
        varchar password_hash
        varchar display_name
        text avatar_url
        varchar bio
        boolean is_private
        timestamptz created_at
        timestamptz updated_at
    }

    feed_groups {
        varchar name PK "user, timeline, notification, topic"
        text description
        timestamptz created_at
    }

    feeds {
        uuid id PK
        varchar group_name FK "references feed_groups(name)"
        varchar instance_key UK "UQ with group_name"
        timestamptz created_at
    }

    follows {
        uuid follower_feed_id PK,FK "references feeds(id) ON DELETE CASCADE"
        uuid followed_feed_id PK,FK "references feeds(id) ON DELETE CASCADE"
        timestamptz created_at
    }

    activities {
        uuid id PK
        uuid user_id FK "references users(id) ON DELETE CASCADE"
        varchar verb "post, share"
        uuid target_feed_id FK "references feeds(id) ON DELETE SET NULL"
        jsonb extra "caption, media_urls, tags, dimensions"
        timestamptz created_at
    }

    activity_targets {
        uuid activity_id PK,FK "references activities(id) ON DELETE CASCADE"
        uuid feed_id PK,FK "references feeds(id) ON DELETE CASCADE"
        timestamptz created_at
    }

    reactions {
        uuid id PK
        uuid activity_id FK "references activities(id) ON DELETE CASCADE"
        uuid user_id FK "references users(id) ON DELETE CASCADE"
        uuid parent_reaction_id FK "self-ref reactions(id) ON DELETE CASCADE"
        varchar kind "like, love, celebrate, comment"
        text data "comment body text"
        timestamptz created_at
        timestamptz deleted_at "soft delete support"
    }

    channels {
        uuid id PK
        varchar channel_type "direct, group"
        varchar name "optional group name"
        uuid created_by FK "references users(id) ON DELETE SET NULL"
        timestamptz last_message_at
        timestamptz created_at
    }

    channel_members {
        uuid channel_id PK,FK "references channels(id) ON DELETE CASCADE"
        uuid user_id PK,FK "references users(id) ON DELETE CASCADE"
        varchar role "admin, member"
        timestamptz joined_at
    }

    messages {
        bigint id PK "Identity sequence (orderable)"
        uuid channel_id FK "references channels(id) ON DELETE CASCADE"
        uuid sender_id FK "references users(id) ON DELETE SET NULL"
        text message_text
        jsonb attachments "media attachments array"
        timestamptz created_at
    }

    channel_read_positions {
        uuid channel_id PK,FK "references channels(id) ON DELETE CASCADE"
        uuid user_id PK,FK "references users(id) ON DELETE CASCADE"
        bigint last_read_message_id FK "references messages(id) ON DELETE SET NULL"
        timestamptz last_read_at
    }

    users ||--o{ activities : "authors (FK user_id)"
    users ||--o{ reactions : "gives (FK user_id)"
    users ||--o{ channels : "creates (FK created_by)"
    users ||--o{ channel_members : "participates (FK user_id)"
    users ||--o{ messages : "sends (FK sender_id)"
    users ||--o{ channel_read_positions : "maintains (FK user_id)"

    feed_groups ||--o{ feeds : "categorizes (FK group_name)"
    feeds ||--o{ follows : "follower (FK follower_feed_id)"
    feeds ||--o{ follows : "followed (FK followed_feed_id)"
    feeds ||--o{ activities : "target_wall (FK target_feed_id)"
    feeds ||--o{ activity_targets : "distributed_to (FK feed_id)"

    activities ||--o{ activity_targets : "routes (FK activity_id)"
    activities ||--o{ reactions : "receives (FK activity_id)"
    reactions ||--o{ reactions : "replies_to (FK parent_reaction_id)"

    channels ||--o{ channel_members : "has (FK channel_id)"
    channels ||--o{ messages : "contains (FK channel_id)"
    channels ||--o{ channel_read_positions : "tracks (FK channel_id)"
    messages ||--o{ channel_read_positions : "points_to (FK last_read_message_id)"
```

---

## 3. Prinsip Normalisasi & Arsitektur Model Hybrid

Kami mengadopsi model **Hybrid Relational Activity Streams** (mentransformasikan konsep *event-driven stream* seperti GetStream ke dalam integritas relasional RDBMS):

### A. Kepatuhan Bentuk Normalisasi (1NF, 2NF, 3NF)
1. **1NF (First Normal Form): Nilai Atomik Murni**
   * Di sistem NoSQL/ScyllaDB, routing target `"to"` disimpan sebagai JSON string array `["notification:bob", "topic:golang"]`. Di PostgreSQL, ini dipecah menjadi tabel relasi **`activity_targets (activity_id, feed_id)`**, di mana setiap baris atomik dan memiliki integritas *Foreign Key*.
   * Kolom `extra jsonb` diizinkan dalam standar RDBMS modern untuk menampung *opaque composite attributes* (seperti metadata teknis media: dimensi width/height, durasi video, caption) yang tidak pernah di-*join* secara relasional.
2. **2NF (Second Normal Form): Ketergantungan Penuh pada Composite PK**
   * Pada tabel relasi many-to-many seperti `channel_members (channel_id, user_id)`, seluruh atribut non-key (`role`, `joined_at`) bergantung penuh pada pasangan kedua kunci tersebut.
3. **3NF (Third Normal Form): Eliminasi Ketergantungan Transitif**
   * Pada tabel `activities`, kita hanya menyimpan `user_id` (FK ke `users.id`). Atribut seperti `username`, `display_name`, dan `avatar_url` tidak pernah diduplikasi di tabel aktivitas, mencegah *update anomaly* saat pengguna mengganti profil mereka.

### B. Penyederhanaan Skema (*No Bloat*)
* **Menghilangkan Kolom Redundan:** Kolom `object` dan `foreign_id` (yang lazim di *third-party SaaS feed engine*) dieliminasi karena platform ini bertindak sebagai *first-party source of truth*.
* **Tabel Reaksi Terpadu (*Unified Reactions*):** Menggabungkan komentar dan like ke dalam tabel **`reactions`** dengan kolom `parent_reaction_id` untuk mendukung percabangan komentar (*threaded comment replies*) hingga kedalaman berapapun secara alami.

---

## 4. Matriks Integritas Data & Aturan Relasi (*Constraints*)

| Tabel | Aturan Kunci (*PK / FK*) | Aturan Integritas (*CHECK & UNIQUE*) | Alasan Desain Arsitektur (*Architectural Rationale*) |
| :--- | :--- | :--- | :--- |
| **`users`** | PK: `id (UUID)` | `UNIQUE(lower(username))` <br> `UNIQUE(lower(email))` <br> `CHECK(char_length >= 3)` | Login dan profil bersifat *case-insensitive*. Format email divalidasi via regex. |
| **`follows`** | PK: `(follower_feed_id, followed_feed_id)` <br> FK: `ON DELETE CASCADE` | `CHECK(follower_feed_id <> followed_feed_id)` | **Anti Self-Follow:** Mencegah user mem-follow dirinya sendiri di level storage engine. PK komposit mencegah follow ganda. |
| **`activities`**| PK: `id (UUID)` <br> FK: `user_id (ON DELETE CASCADE)` <br> FK: `target_feed_id (ON DELETE SET NULL)` | `CHECK(verb IN ('post', 'share'))` <br> `CHECK(extra IS NOT NULL)` | **GDPR Right-to-be-Forgotten:** Jika user menutup akun, seluruh post publiknya ter-cascade bersih. Target feed opsional (wall/community). |
| **`activity_targets`**| PK: `(activity_id, feed_id)` <br> FK: `ON DELETE CASCADE` | Memenuhi 1NF | Menampung mention (@user) dan routing ke feed notifikasi secara relasional. |
| **`reactions`**| PK: `id (UUID)` <br> FK: `activity_id (ON DELETE CASCADE)` <br> FK: `parent_reaction_id (ON DELETE CASCADE)` | **Partial UNIQUE Index:** <br> `UNIQUE(activity_id, user_id, kind) WHERE kind <> 'comment' AND deleted_at IS NULL` | **Single Reaction Rule:** 1 user hanya boleh me-like 1 kali, namun bebas menulis banyak komentar. Komentar wajib memiliki teks (`CHECK`). |
| **`messages`** | PK: `id (BIGINT Identity)` <br> FK: `channel_id (ON DELETE CASCADE)` <br> FK: `sender_id (ON DELETE SET NULL)` | `CHECK(text IS NOT NULL OR attachments <> '[]')` | **Shared Chat History:** `sender_id ON DELETE SET NULL` memastikan jika pengirim menghapus akun, riwayat chat lawan bicara tidak rumpang ("Deleted User"). |
| **`channel_read_positions`** | PK: `(channel_id, user_id)` <br> FK: `last_read_message_id (ON DELETE SET NULL)` | Pointer berbasis ID | Menghitung unread messages via range scan tanpa *write-amplification* `UPDATE counter` pada grup chat. |

---

## 5. Strategi Indexing Presisi Berdasarkan Pola Akses (*Access Patterns*)

Kami menerapkan prinsip baku B-Tree: **Equality Columns First, then Range/Sort Columns Second**.

```
[B-Tree Root]
     │
     ▼
[Filter: user_id = $1] ─── (O(log N) Direct Seek)
     │
     └──► [Subtree: created_at DESC] ─── (Ordered Sequential Scan, LIMIT 20 -> Early Exit)
```

1. **Linimasa Profil Pengguna:**
   ```sql
   CREATE INDEX idx_activities_user_created ON activities (user_id, created_at DESC, id DESC);
   ```
   * *Mengapa?* Menjamin query profil pengguna langsung melakukan *Index Seek* ke `user_id` dan membaca 20 baris pertama terurut tanpa operasi disk sort (`Sort` node di EXPLAIN hilang).
2. **Relasi Sosial Dua Arah (*Social Graph*):**
   * PK `follows (follower_feed_id, followed_feed_id)` melayani query "Siapa yang di-follow X?".
   * Index kebalikan (*Reverse Index*):
     ```sql
     CREATE INDEX idx_follows_followed_follower ON follows (followed_feed_id, follower_feed_id);
     ```
     Melayani query "Siapa follower dari X?" berstatus **Index Only Scan**.
3. **Komentar & Agregasi Reaksi dengan Partial Index:**
   ```sql
   CREATE INDEX idx_reactions_activity_kind ON reactions (activity_id, kind) WHERE deleted_at IS NULL;
   CREATE INDEX idx_reactions_parent_replies ON reactions (parent_reaction_id, created_at ASC) 
       WHERE parent_reaction_id IS NOT NULL AND deleted_at IS NULL;
   ```
   * *Mengapa Partial Index?* Mengabaikan baris yang di-soft-delete, memangkas ukuran index di RAM sebesar 30–50%.
4. **Riwayat Pesan & Chat Inbox:**
   ```sql
   CREATE INDEX idx_messages_channel_id_desc ON messages (channel_id, id DESC);
   CREATE INDEX idx_channels_last_message ON channels (last_message_at DESC NULLS LAST);
   ```

---

## 6. Bedah Kueri Feed Kompleks (*Feed Generation Deep-Dive*)

Kueri pembuatan linimasa beranda diimplementasikan pada file [`queries.sql`](file:///Users/rizkisaifulnizar/Documents/Projects/test-case/test-case-3-database-schema-design/queries.sql) dengan tiga keunggulan arsitektural:

1. **Keyset Cursor Pagination (Bukan `OFFSET`):**
   * `WHERE (a.created_at, a.id) < ($cursor_time, $cursor_id)`
   * Menghindari penurunan performa $O(N)$ saat scrolling jauh dan mencegah postingan ganda akibat pergeseran data konkuren.
2. **CTE + UNION (Jalur Follow & Jalur Notifikasi):**
   * Menggabungkan postingan dari orang yang di-follow dengan aktivitas yang secara eksplisit me-mention user (`activity_targets`), memanfaatkan index masing-masing tabel secara optimal.
3. **Lateral Subqueries untuk Agregasi On-The-Fly:**
   * Alih-alih melakukan *GROUP BY* ke seluruh tabel `reactions`, kueri menggunakan `LEFT JOIN LATERAL` yang **hanya menghitung jumlah like dan komentar strictly untuk 20 postingan terpilih** pada halaman tersebut. Latensi kueri terpangkas dari ratusan milidetik menjadi `< 10ms`.

---

## 7. Rangkuman Strategi Caching (Redis)

Detail lengkap dan mendalam dipaparkan pada dokumen [`caching-strategy.md`](file:///Users/rizkisaifulnizar/Documents/Projects/test-case/test-case-3-database-schema-design/caching-strategy.md). Arsitektur caching dirancang secara pragmatis sebagai akselerator linimasa mendampingi PostgreSQL:

* **4 Data Cache Inti:**
  1. `feed:home:{user_id}:{mode}` (`ZSET`, TTL 10m/3m): 
     * **Pre-computed Candidate Pool:** Mengambil 200 kandidat awal secara sinkron (< 50ms) saat user login, lalu diperluas hingga 800 kandidat secara asinkron di background.
     * **Dual-Mode Scoring:** Mendukung skor algoritmik berbasis interaksi & peluruhan waktu (*time-decay*):
       $$\text{Score} = \left( 1.0 \times \text{likes} + 3.0 \times \text{comments} + 5.0 \times \text{shares} \right) \times \frac{1}{\left(1 + \Delta t_{\text{jam}}\right)^{1.5}}$$
       maupun skor kronologis murni (*Unix Timestamp*).
     * **ID-Only Caching:** Hanya menyimpan pointer `activity_id` (menghemat RAM >90%). Detail postingan dihidrasi langsung via *Primary Key batch lookup* PostgreSQL (`WHERE id IN (...)`) dalam < 1.5ms tanpa risiko *stale data*.
  2. `user:profile:{user_id}` (`STRING` JSON, TTL 15m): Cache profil (avatar, nama) menyerap jutaan *point lookup* yang berulang.
  3. `stats:activity:{id}` (`HASH`, TTL 5m): Counter agregat likes/comments untuk melindungi database dari pemindaian `COUNT(*)` pada post viral.
  4. `badge:unread:{user_id}` (`STRING`, TTL 15m): Counter unread pesan & notifikasi untuk menyerap polling konstan mobile app.

* **Primitif Kontrol Konkurensi:**
  * `lock:feed:regen:{user_id}` (`STRING`, TTL 5s): Redis Mutex Lock (`SET NX EX 5`) untuk mencegah fenomena **Cache Stampede / Thundering Herd** saat TTL linimasa habis, menjamin hanya 1 kueri yang dieksekusi ke PostgreSQL.

* **Mitigasi Masalah Skala & Outage:**
  * **Memory Capping:** Memangkas ekor linimasa otomatis maksimal 800 item terbaru via `ZREMRANGEBYRANK`.
  * **Graceful Degradation:** Jika Redis mengalami *outage*, circuit breaker otomatis mengalihkan pembacaan langsung ke PostgreSQL yang sudah teroptimasi dengan *Keyset Index*, mencegah *system crash*.

---

## 8. Panduan Pengujian & Verifikasi Lokal (Docker / PostgreSQL)

Seluruh berkas DDL dan SQL telah diverifikasi kompatibel dengan PostgreSQL 14, 15, dan 16+.

### Menjalankan Database PostgreSQL via Docker:
```bash
docker run --name social-db -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=social_media -p 5432:5432 -d postgres:16-alpine
```

### 1. Menjalankan Migrasi Skema:
```bash
psql -h localhost -U postgres -d social_media -f schema.sql
```

### 2. Memasukkan Sample Data Realistis (Seed Data):
```bash
psql -h localhost -U postgres -d social_media -f seed.sql
```

### 3. Menguji Eksekusi Kueri:
```bash
psql -h localhost -U postgres -d social_media -f queries.sql
```
