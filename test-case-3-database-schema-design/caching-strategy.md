# Caching Strategy: High-Throughput Social Media Platform

Dokumen ini memaparkan strategi *caching* berlapis (*multi-tier caching architecture*) menggunakan **Redis** untuk mendampingi database **PostgreSQL**. Desain ini dirumuskan secara pragmatis berdasarkan **karakteristik beban riil (*access patterns & database bottlenecks*)**, menghindari asumsi tak berdasar (*over-engineering*), dan menjamin latensi baca sub-5ms dengan perlindungan terhadap *cache stampede*.

---

## 1. Analisis Beban & Justifikasi Teknis: Mengapa Perlu Di-cache?

Dalam platform media sosial berskala besar, rasio trafik sangat timpang: **~95% Read vs ~5% Write**. Aturan emas perancangan arsitektur cache adalah:
> *"Jangan meng-cache data yang bisa dibaca cepat (< 2ms) dari database, kecuali kueri tersebut dieksekusi jutaan kali per detik atau melibatkan operasi agregasi/join yang membebani CPU database."*

Berdasarkan aturan tersebut, kami memfokuskan komponen menjadi **4 Data Cache Inti** dan **1 Primitif Kontrol Konkurensi**.

### Matriks Komponen Data Cache Inti

| Entitas Data | Struktur Redis | Pola Key | TTL Ideal | Justifikasi Rekayasa Teknis (*Engineering Rationale*) |
| :--- | :--- | :--- | :--- | :--- |
| **1. Home Timeline Feed** | **`ZSET` (Sorted Set)** | `feed:home:{user_id}:{mode}` | **10 Menit** (Full) <br> **3 Menit** (Partial) | **Penyelamat CPU Database:** Kueri paling mahal (menggabungkan ratusan following + CTE + Lateral Join). Tanpa cache, ribuan user refresh serentak akan melumpuhkan CPU PostgreSQL. |
| **2. Profil Pengguna** | **`STRING` (JSON)** | `user:profile:{user_id}` | **15 Menit** | **Penyelamat Point Lookup:** 1 layar feed memuat 20 post dari 20 author berbeda. Cache ini menyerap jutaan kueri pencarian nama dan avatar yang jarang berubah. |
| **3. Counter Reaksi Agregat**| **`HASH`** | `stats:activity:{activity_id}` | **5 Menit** | **Penyelamat Index Scan:** Pada post viral dengan 200.000 likes, kueri `SELECT count(*)` akan memindai 200.000 baris index per refresh. Counter Redis menyerap beban ini dalam $O(1)$. |
| **4. Unread Badge Counter** | **`STRING` (Integer)** | `badge:unread:{user_id}` | **15 Menit** | **Penyelamat Polling I/O:** Menyerap jutaan request *heartbeat / polling* dari mobile app untuk menampilkan bulatan merah "3 pesan baru". |

---

## 2. Primitif Kontrol Konkurensi: Pencegah *Cache Stampede*

Komponen ini **bukan entitas data**, melainkan **mekanisme sinkronisasi (Mutex Lock)** di Redis:

* **Key:** `lock:feed:regen:{user_id}`
* **Tipe:** `STRING` (`SET key 1 NX EX 5`)
* **TTL:** 5 Detik (auto-expire)

### Mengapa Mutex Lock Ini Mutlak Diperlukan?
Ketika cache linimasa seorang pengguna (`feed:home:alice`) *expired* (TTL habis):
1. **Tanpa Lock (*Fenomena Thundering Herd*):** Jika aplikasi melakukan 3–5 request serentak (refresh beranda, background sync, notifikasi), kelima request tersebut melihat "Cache Kosong" dan **bersama-sama mengeksekusi kueri CTE multi-join berat ke PostgreSQL**. Pada skala 5.000 user aktif, ribuan kueri berat yang redundan akan menghantam database hingga *pool connection* habis dan database *down*.
2. **Dengan Mutex Lock:**
   * Request #1 berhasil mengeksekusi `SET lock:feed:regen:alice 1 NX EX 5` $\rightarrow$ **Hanya Request #1 yang mengeksekusi kueri ke PostgreSQL**.
   * Request #2 s.d. #5 gagal memperoleh lock $\rightarrow$ Request menunggu sesaat (50ms) lalu membaca hasil yang sudah selesai diisi oleh Request #1 di Redis.
   * **Hasil:** Beban database berkurang drastis dari ribuan kueri redundan menjadi **tepat 1 kueri saja**.

---

## 3. Arsitektur Linimasa Feed (Home / Beranda) dengan Redis `ZSET`

Ketika pengguna login atau membuka aplikasi pertama kali, mereka langsung dihadapkan pada halaman default **Home / Beranda** (seperti Instagram atau Facebook). Linimasa ini menampung postingan dari seluruh akun/topik yang di-follow serta postingan pribadi pengguna.

Untuk menyajikan pengalaman beranda berlatensi ultra-rendah (< 50ms) tanpa membebani PostgreSQL, kita menerapkan **Arsitektur Pre-computed Ranked Feed ala `feed-service`**:

```
[User Login / Buka Beranda]
             │
             ▼
      Cek Redis Cache: `feed:home:{user_id}:{mode}`
             │
     ┌───────┴───────┐
   (Hit)           (Miss)
     │               │
     │               ▼
     │        Ambil Mutex Lock (`lock:feed:regen:{user_id}`)
     │               │
     │               ▼
     │        [PostgreSQL] Tarik 200 Row Kandidat Teratas (CTE + Keyset)
     │               │
     │               ▼
     │        [Ranking Engine] Hitung Skor Bobot & Simpan ke Redis ZSET (TTL 3m)
     │               │
     │               ▼ (Async Worker lanjutkan hingga 800 row, TTL 10m)
     ▼               │
Paginasi ZREVRANGEBYSCORE ──► Hydrate DB (WHERE id IN (20 IDs)) + MGET User Profile ──► Render Beranda
```

---

### A. Dua Mode Penilaian Skor pada Redis `ZSET` (*Dual-Mode Scoring*)
Struktur Redis `ZSET` memetakan `activity_id` (Member) ke sebuah nilai numerik (**Score**). Sistem mendukung 2 mode:

#### 1. Mode Algoritmik / Beranda Utama ("Untuk Anda" / Default Home):
Mengadopsi formula *time-decay* dan *engagement weight* dari `feed-service` Obsidian:
$$\text{Score} = \left( 1.0 \times \text{likes} + 3.0 \times \text{comments} + 5.0 \times \text{shares} \right) \times \frac{1}{\left(1 + \Delta t_{\text{jam}}\right)^{1.5}}$$
* Postingan yang banyak interaksi dan masih baru akan mendapatkan skor tertinggi dan berada di puncak beranda.
* Skor berupa angka desimal presisi tinggi yang disimpan langsung sebagai skor `ZSET`.

#### 2. Mode Kronologis Murni ("Terbaru"):
* **Score:** `Unix Timestamp (milidetik)` saat aktivitas dibuat.
* Digunakan saat pengguna secara eksplisit memilih filter tab "Terbaru / Following".

---

### B. Mekanisme *Candidate Pool* (Two-Phase Hybrid Caching)

Menghitung ribuan postingan secara langsung saat HTTP request akan menyebabkan aplikasi *freeze*. Oleh karena itu, kita membaginya ke dalam 2 fase:

1. **Fase 1: Fast Initial Render (Synchronous, Latensi < 50ms)**
   * Aplikasi mengambil batch pertama **200 baris postingan kandidat** teratas dari PostgreSQL melalui kueri linimasa kita (`queries.sql`).
   * Backend menghitung skor dan menyimpannya ke `feed:home:{user_id}:ranked` di Redis.
   * Key diberi **TTL 3 Menit (`PartialCacheTTL`)** dengan penanda `IsComplete: false`.
   * Halaman 1 beranda (20 post teratas) seketika ditampilkan ke layar pengguna.

2. **Fase 2: Background Pool Expansion (Asynchronous via Worker)**
   * Sistem mem-publish event ke background worker untuk melanjutkan pengambilan baris ke-201 hingga maksimal **800–1.000 baris kandidat** dari PostgreSQL.
   * Worker mengevaluasi skor sisa postingan, menambahkan ke ZSET (`ZADD`), dan memperpanjang **TTL menjadi 10 Menit (`FullCacheTTL`)** dengan penanda `IsComplete: true`.

---

### C. Scrolling Paginasi Tanpa Beban Kueri Berat (*Zero-Heavy-Query Pagination*)

Ketika pengguna melakukan *scroll down* (halaman 2, 3, 4, dst.):
1. **Paginasi di Memori RAM:** Aplikasi memanggil kueri Redis `ZREVRANGEBYSCORE`:
   ```text
   ZREVRANGEBYSCORE feed:home:{user_id}:ranked ({last_seen_score} -inf LIMIT 0 20
   ```
   *(Mengambil 20 ID postingan berikutnya dalam < 1ms).*
2. **Hidrasi Cepat Terpadu:**
   * Ambil teks & gambar 20 ID tersebut dari PostgreSQL via Primary Key Index lookup:
     ```sql
     SELECT * FROM activities WHERE id IN ($1, $2, ... $20);
     ```
     *(Hanya butuh 1-2ms di PostgreSQL via B-Tree Seek).*
   * Ambil nama & avatar penulis dari Redis via `MGET user:profile:{u1} user:profile:{u2} ...`.
3. **Hasil:** Pengguna dapat men-scroll hingga ratusan postingan dengan latensi konsisten sub-5ms **tanpa pernah memicu kueri CTE multi-join berat di PostgreSQL**.

---

### D. Pembatasan Memori Otomatis (*Memory Capping*)
Agar RAM Redis tetap terkendali pada saat menampung linimasa jutaan pengguna:
```text
ZREMRANGEBYRANK feed:home:{user_id}:ranked 0 -801
```
*(Secara otomatis memangkas ekor linimasa sehingga setiap user hanya menyimpan maksimal 800 kandidat teratas di memori RAM).*

---

## 4. Penanganan Mutasi Data, Undo / Rollback, dan Konsistensi

Sistem menerapkan prinsip **"Delete on Write"** (menghapus cache saat mutasi, bukan meng-update cache dari kode aplikasi) untuk mencegah *race condition* dan *stale data*:

### A. Kasus Reaksi: Like dan Undo Like (Unlike)
* **Saat User Menekan LIKE:**
  1. Tulis ke PostgreSQL: `INSERT INTO reactions ...`
  2. Tambahkan counter di Redis: `HINCRBY stats:activity:{id} like_count 1`
* **Saat User Menekan UNLIKE (Undo Like):**
  1. Update di PostgreSQL: `UPDATE reactions SET deleted_at = NOW() WHERE ...`
  2. Kurangi counter di Redis secara atomik: `HINCRBY stats:activity:{id} like_count -1`
  3. **Self-Healing Safeguard:** Counter `stats:activity:{id}` memiliki **TTL pendek (5 menit)**. Jika terjadi desinkronisasi akibat *network failure*, dalam 5 menit cache akan otomatis *expire* dan memuat ulang nilai pasti dari `COUNT(*)` PostgreSQL.

### B. Kasus Edit Postingan
* Penulis mengedit caption atau menambah media.
* **PostgreSQL:** Update baris di tabel `activities`.
* **Redis:** **NOL TINDAKAN KARENA TIDAK ADA CACHE DETAIL POST DI REDIS!**
  Saat follower membaca feed, proses *batch PK lookup* otomatis membaca baris terbaru langsung dari PostgreSQL. Zero cache invalidation bugs!

### C. Kasus Hapus Postingan & Pertahanan Hidrasi (*Hydration Drop Filter*)
* **PostgreSQL:** Hapus baris atau set `deleted_at = NOW()`.
* **Redis:** Publikasikan event async untuk membersihkan ZSET follower aktif (`ZREM`).
* **Pertahanan Lapis Ganda (Hydration Drop Filter):**
  Jika ada follower membuka aplikasi tepat sebelum worker selesai membersihkan ZSET follower, proses hidrasi backend mendeteksi bahwa baris di PostgreSQL sudah terhapus (`deleted_at IS NOT NULL` atau `record not found`). Backend otomatis **men-drop item tersebut dari response JSON**. Postingan yang sudah dihapus **tidak akan pernah bocor** ke pengguna.

### D. Kasus Relasi Sosial: Follow & Unfollow
* **Saat Follow:**
  * Simpan ke PostgreSQL `follows`.
  * Ambil 20 post terbaru dari author dan tambahkan ke `feed:home:{follower_id}` via `ZADD`.
* **Saat Unfollow:**
  * Hapus dari PostgreSQL `follows`.
  * **Integritas Mutlak:** Langsung hapus cache linimasa pengikut:
    ```text
    DEL feed:home:{follower_id}:*
    ```
    Saat pengikut membuka feed kembali, query PostgreSQL (CTE + Keyset) akan meregenerasi ZSET beranda yang murni hanya berisi orang-orang yang masih di-follow.

---

## 5. Alur Lengkap Baca (Read Path) & Tulis (Write Path)

```
[Client]
   │
   ├── (1) GET /feed/home ────────► [API Server]
   │                                     │
   │                              (2) ZREVRANGEBYSCORE feed:home:{user_id}:{mode}
   │                                     ▼
   │                              [Redis ZSET] ──(Hit: 20 IDs)──► PG: WHERE id IN (20 IDs) ──► Return JSON
   │                                     │
   │                                  (Miss)
   │                                     ▼
   │                              [PostgreSQL DB] ──► Regenerate ZSET (TTL 10m) ──► Return JSON
   │
   └── (3) POST /activities ──────► [API Server]
                                         │
                                  (4) Transaksional INSERT ke PostgreSQL
                                         ▼
                                  [PostgreSQL DB]
                                         │
                                  (5) Async Event (Fan-out Worker)
                                         ▼
                                  [Redis Cache] ──► ZADD to Active Followers' ZSET
```

---

## 6. Keandalan Sistem (*Resilience & Graceful Degradation*)

1. **Perlindungan *Penetration / Cache Null Values*:**
   * Jika ada kueri ke profil user acak yang tidak ada di database, simpan marker kosong di Redis: `SET user:profile:{invalid_id} "NULL" EX 60`. Mencegah serangan *denial of service* yang menembus database utama.
2. **Graceful Degradation saat Redis Mengalami Gangguan (*Outage*):**
   * Jika cluster Redis *down*, circuit breaker mendeteksi kegagalan dalam 100ms dan mengalihkan pembacaan langsung ke PostgreSQL.
   * Berkat indeks komposit `(user_id, created_at DESC, id DESC)` dan kueri keyset pagination yang efisien, PostgreSQL tetap stabil melayani kueri tanpa risiko *crash full-table scan*.
