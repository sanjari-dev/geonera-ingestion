# **Cetak Biru (Blueprint): Arsitektur Sistem Ingesti Data Dukascopy (BI5 to Parquet)**

Sistem ini dirancang untuk melakukan ekstraksi, transformasi, dan pemuatan (ETL) data *tick* dari Dukascopy ke Cloudflare R2 dalam format Apache Parquet menggunakan Go (Goroutines) dengan **ent (entgo) sebagai ORM**, dan PostgreSQL (Self-Managed HA Cluster) sebagai pengelola state.

Secara garis besar, implementasi cetak biru ini berwujud **aplikasi mandiri (standalone)** berbasis Go yang difokuskan sebagai mesin eksekusi utama (mengunduh data, konversi, agregasi, dan validasi) secara terpusat. Aplikasi ini bersifat *hybrid*: bertindak sebagai **Consumer persisten** yang mendengarkan event dari *Message Broker*, sekaligus melayani *request* RESTful API menggunakan framework **Fiber** sebagai pintu masuk (*entry point*) pemicu tugas.

## **1\. Komponen Utama Arsitektur**

* **Compute Engine (Dedicated VPS):** Aplikasi berbasis **Go**. Deployment sangat direkomendasikan menggunakan **Dedicated VPS**. Penggunaan VPS memberikan *runtime* aplikasi yang persisten sehingga aplikasi dapat berjalan sebagai *daemon always-on* untuk mengkonsumsi antrean dari Message Broker kapan pun event tiba. Sistem menerapkan **Pipeline Konkurensi Asimetris**: proses ekstraksi/download data BI5 **(termasuk HTTP re-check ke API)** dibatasi ketat maksimal 12 Goroutines statis (via Bounded Semaphore terpusat) demi mencegah *error 429 Too Many Requests* dari server Dukascopy. Sementara itu, proses konversi Parquet dan pengunggahan ke R2 dijalankan secara konkuren namun **dilindungi oleh mekanisme *back-pressure* menggunakan *Bounded Semaphore* yang berbeda** (misal: maksimum 50 goroutines global) untuk mencegah lonjakan konsumsi memori (*Out-of-Memory*/OOM) pada instrumen dengan volume tick yang sangat tinggi (seperti EURUSD). Menggunakan framework **ent (entgo) ORM** untuk pemodelan skema *type-safe*, migrasi otomatis, dan manajemen transaksi database.  
* **Message Broker (Messaging System):** Infrastruktur antrian pesan (seperti RabbitMQ, NATS, atau Redis Streams) yang bertindak sebagai pintu masuk (pemicu) utama. Penggunaan broker mencegah isu *timeout* pada pemicuan HTTP dan menangani lonjakan beban (*spike*) secara asinkron sebelum masuk ke *Compute Engine*.  
* **Cloudflare R2 (Storage):** Penyimpanan objek untuk file Parquet. Mendukung struktur folder hierarkis untuk Ticks dan Candles.  
* **PostgreSQL (Self-Managed HA Cluster):** Manajemen state terpusat yang di-deploy secara mandiri menggunakan **minimal dua Virtual Private Server (VPS)** dalam topologi Primary-Standby. Mendukung *connection pooling* (via PgBouncer). Sistem menggunakan **Dual Connection Strategy** untuk memisahkan koneksi berdurasi panjang (*long-lived lock*) dari koneksi transaksi cepat (lihat Poin 4.A). Sistem dilengkapi mekanisme failover otomatis dan monitoring ketat untuk mencegah *Single Point of Failure* (SPOF). Semua tabel wajib menggunakan **UUID** (gen\_random\_uuid()) sebagai Primary Key.  
* **OpenTelemetry (APM & Tracing Backend):** Standar instrumentasi (menggunakan OTel SDK di Go) yang mengirimkan *Traces, Metrics,* dan *Logs* ke backend APM (seperti Jaeger, Grafana Tempo, SigNoz, atau Datadog) untuk observabilitas menyeluruh dan mitigasi *blind-spot* saat proses latar belakang berjalan.

## **2\. Skema Database (Ent Schema Definition)**

Struktur database didefinisikan menggunakan *Schema-as-Code* di entgo (misal: ent/schema/instrument.go).

### **Schema: Instrument**

* **ID:** UUID (Default uuid.New())  
* **Name:** string (Unique) \- Contoh: xauusd  
* **Description:** string \- Penjelasan detail instrumen (Contoh: Gold vs US Dollar).  
* **AssetClass:** string \- Contoh: forex, commodity, crypto.  
* **IsActive:** bool (Default: true) \- Status operational instrument.  
* **Divider:** int \- Faktor pembagi harga dari format integer BI5.  
* **StartDate:** time.Time (UTC) \- Titik awal data historis yang diinginkan. **Wajib UTC:** Segala input *timezone* lokal harus dikonversi atau dipaksa ke UTC sebelum masuk ke database, baik melalui *hook* ORM maupun validasi API, untuk menghindari *off-by-hours* yang merusak kalkulasi *gap detection* harian pada proses *Seeder*.  
* **IsPause:** bool \- **Mutex Column:** Diaktifkan otomatis jika ada ketidaksinkronan data per instrumen. Jika true, proses reguler instrumen tersebut berhenti dan hanya membiarkan status tetap PENDING.

### **Schema: Timeframe**

Daftar 19 timeframe baku (Solid, tidak boleh diubah/ditambah):

* **ID:** UUID (Default uuid.New())  
* **Name:** string \- (m1, m2, m3, m4, m5, m6, m10, m12, m15, m20, m30, h1, h2, h3, h4, h6, h8, h12, d1)  
* **Minutes:** int \- Total durasi dalam menit (Contoh: m1=1, h1=60, d1=1440).  
* **IsActive:** bool (Default: true)

### **Schema: State (Consolidated)**

* **ID:** UUID  
* **Edge/Relation:** Instrument (O2M)  
* **JobType:** enum \- TICK, CANDLE  
* **Timestamp:** time.Time (UTC).  
* **Status:** enum \- PENDING, PROCESSED, NOT\_FOUND, FAILED, COMPLETED, BROKEN, CONFIRMED, ABANDONED  
  * **PENDING:** Antrean awal.  
  * **PROCESSED:** Sedang dikerjakan (In-progress).  
  * **NOT\_FOUND:** Data tidak ditemukan di sumber API. (Bersifat sementara/antrean untuk pengecekan ulang jika terjadi *delay* rilis).  
  * **FAILED:** Gagal saat download/konversi karena error sistem atau jaringan.  
  * **COMPLETED:** Berhasil diunggah ke R2 (Menunggu validasi fisik).  
  * **BROKEN:** File di R2 korup atau gagal validasi fisik berdasar kriteria ketat.  
  * **CONFIRMED:** Terminal State (Satu-satunya status akhir yang valid). Berarti file fisik 100% valid, ATAU telah dipastikan sebagai data kosong/libur permanen setelah melewati batas ambang NotFoundStreak dan sukses memvalidasi file *Zero-Row Parquet*.  
  * **ABANDONED:** Terminal State (Kegagalan permanen). Mengindikasikan job telah gagal (FAILED/BROKEN) berulang kali dan mencapai batas maksimal *retry* (misal: \>= 5). Membutuhkan intervensi manual dari engineer.  
* **PreviousStatus:** enum (Nullable) \- Berisi nilai status tepat sebelum status saat ini. Wajib digeser secara otomatis pada setiap transaksi pembaruan status. Digunakan sebagai konteks historis cepat (misal: untuk mendeteksi demosi/penurunan kasta dari CONFIRMED ke BROKEN).  
* **IsHoliday:** bool (Default: false) \- Flag eksplisit penanda hari libur. Diset menjadi true HANYA ketika sistem secara sengaja merilis *Zero-Row Parquet*.  
* **ResolvedTickCount:** int (Default: 0\) \- Counter harian khusus untuk job CANDLE. Menggabungkan jumlah file TICK yang berstatus CONFIRMED di hari tersebut.  
* **RetryCount:** int (Default: 0\) \- Bertambah (+1) jika terjadi FAILED/BROKEN. Khusus untuk data CANDLE, jika ada task berstatus PROCESSED yang stuck dan diturunkan kembali ke PENDING, nilai RetryCount juga ditambah 1. Seluruh modifikasi field ini **wajib** menggunakan operasi atomik.  
* **NotFoundStreak:** int (Default: 0\) \- Bertambah (+1) secara atomik HANYA saat sumber API mengembalikan 404 (data kosong). **Wajib di-reset ke 0** jika pada pengulangan berikutnya data ternyata ditemukan. Digunakan untuk menentukan ambang batas pelepasan *Zero-Row Parquet* (libur permanen) secara presisi tanpa tumpang tindih dengan error jaringan.  
* **IsDeleted:** bool (Default: false) \- Penanda *soft-delete* untuk menggaransi proses *2-Phase Commit* saat siklus pembersihan/pruning.  
* **UpdatedAt:** time.Time  
* **Index:** Unique constraint gabungan (instrument\_id, timestamp, job\_type)

### **Schema: SyncTask (Outbox Pattern)**

Digunakan untuk menampung event perubahan status terminal secara transaksional guna memastikan sinkronisasi ResolvedTickCount tidak hilang (*lost update*).

* **ID:** UUID  
* **InstrumentID:** UUID (FK ke Instrument)  
* **TargetDate:** time.Time (Tanggal 00:00:00 dari Tick yang bersangkutan)  
* **Status:** enum \- PENDING, PROCESSED  
* **CreatedAt:** time.Time  
* **Index:** Unique gabungan (instrument\_id, target\_date) untuk mencegah penumpukan event pada hari yang sama.

## **3\. Spesifikasi Format Parquet & Penyimpanan (Cloudflare R2)**

### **A. Ticks Parquet**

**Path:** ingestion/dukascopy/ticks/{instrument}/{YYYY}/{MM}/ticks-{instrument}-{YYYY-MM-DD}-{HH}.parquet

*(Catatan: Untuk jam di mana pasar tutup/libur, sistem akan membuat "Zero-Row Parquet" yaitu file dengan ekstensi dan skema persis di bawah ini, namun dengan jumlah row \= 0).*

* **timestamp:** datetime (UTC)  
* **instrument:** string  
* **bid:** Decimal (18, 5\)  
* **ask:** Decimal (18, 5\)  
* **bid\_volume:** bigint  
* **ask\_volume:** bigint

### **B. Candles Parquet (Daily File)**

**Path:** ingestion/dukascopy/candles/{instrument}/{YYYY}/candles-{instrument}-{YYYY-MM-DD}.parquet

Satu file harian berisi **seluruh 19 timeframe**.

* **timestamp:** datetime (UTC)  
* **instrument:** string  
* **timeframe:** string  
* **open, high, low, close, vwap, min\_spread, max\_spread, avg\_spread:** Decimal (18, 5\)  
* **tick\_count, total\_bid\_volume, total\_ask\_volume:** bigint

## **4\. Logika Operasional & Konkurensi**

### **A. Mekanisme Locking Global & Strategi Split Connection**

Untuk memastikan skalabilitas (*multi-instance*) bebas konflik namun tetap kompatibel dengan arsitektur **PgBouncer** (Transaction Mode), sistem menerapkan **Split Connection Strategy (Jalur Koneksi Terpisah)**:

1. **Jalur Utama (Direct PostgreSQL Port \- Bypass PgBouncer):** Hanya digunakan oleh satu buah fungsi *Parent Handler* (Orchestrator) untuk menahan pg\_try\_advisory\_xact\_lock. Mengingat proses ETL bisa berjalan lama, koneksi langsung ini menjamin *advisory lock* tidak terlepas diam-diam oleh rotasi *pooler* dan tidak menyandera/memblokir koneksi PgBouncer dari klien lain.  
2. **Jalur Eksekutor (Via PgBouncer Port):** Digunakan oleh seluruh *Worker/Goroutines* anakan (T-0, T-1, Backfill) untuk melakukan *query*, *update*, dan klaim data singkat (FOR UPDATE SKIP LOCKED). Karena transaksinya dalam hitungan milidetik, ini sangat efisien ditangani oleh PgBouncer.

**Guardrail Enforcer di Level Kode:**

Untuk memastikan desain *Split Connection* ini diimplementasikan dengan benar dan mencegah *human error* (misal: *developer* tak sengaja memanggil koneksi direct di dalam eksekusi *worker*), sistem menerapkan struktur DataAccessLayer (DAL) sebagai pagar pengaman (*guardrail*). Dengan mengenkapsulasi (*private fields*) objek klien koneksi ke dalam sebuah *struct*, kesalahan penggunaan akan langsung memicu *compile error*, alih-alih berujung pada *runtime bug*.

* **Alokasi Lock ID (Konstanta):** Menggunakan konstanta *integer* untuk mencegah penggunaan *magic numbers*.  
  * **LockIDTick (1001):** Digunakan eksklusif oleh endpoint ingesti *Tick* (Regular maupun Backfill).  
  * **LockIDCandle (1002):** Digunakan eksklusif oleh endpoint agregasi *Candles* (Regular maupun Backfill).  
  * **LockIDMaintenance (1003):** Digunakan eksklusif oleh endpoint *Auto-Seeder* dan *Pruning*.  
  * **LockIDSync (1004):** Digunakan eksklusif oleh endpoint *Outbox Worker*.  
* **Logika Transport Layer & Eksekusi Handler:**

// 1\. Konstanta Global          
const LockIDTick int64 \= 1001

// 2\. DataAccessLayer (DAL): Guardrail / Enforcer Pemisahan Koneksi  
type DAL struct {  
    lockConn \*ent.Client // private: Direct connection (Bypass PgBouncer), KHUSUS ambil lock  
    pool     \*ent.Client // private: PgBouncer connection, KHUSUS worker/klaim data  
}

// AcquireAdvisoryLock hanya digunakan oleh Parent Handler.   
// Mengembalikan transaksi yang terikat pada direct connection untuk menahan Advisory Lock.  
func (d \*DAL) AcquireAdvisoryLock(ctx context.Context) (\*ent.Tx, error) {  
    return d.lockConn.Tx(ctx)   
}

// ExecuteInPool adalah satusatunya cara bagi goroutine worker untuk berinteraksi dengan database.  
// Memaksa eksekusi secara transaksional melalui koneksi PgBouncer.  
func (d \*DAL) ExecuteInPool(ctx context.Context, fn func(tx \*ent.Tx) error) error {  
    return d.pool.Tx(ctx, fn)  
}

// 3A. Transport Layer: Pemanggilan via Message Broker (Metode Utama)        
func setupMessageQueueConsumer(broker MQClient, dal \*DAL) {        
    broker.Subscribe("jobs.ticks.regular", func(msg \[\]byte, headers map\[string\]string) {        
        ctx := otel.GetTextMapPropagator().Extract(context.Background(), propagation.MapCarrier(headers))        
        go runTickParentHandler(ctx, "REGULAR", dal)        
    })        
    broker.Subscribe("jobs.ticks.backfill", func(msg \[\]byte, headers map\[string\]string) {        
        ctx := otel.GetTextMapPropagator().Extract(context.Background(), propagation.MapCarrier(headers))        
        go runTickParentHandler(ctx, "BACKFILL", dal)        
    })       
}

// 3B. Transport Layer: Pemanggilan via HTTP Fiber (Alternatif / Fallback)            
app.Post("/api/v1/ticks/regular", func(c \*fiber.Ctx) error {            
    ctx := otel.GetTextMapPropagator().Extract(context.Background(), propagation.HeaderCarrier(c.GetReqHeaders()))          
    go runTickParentHandler(ctx, "REGULAR", appDAL)            
    return c.Status(202).JSON(fiber.Map{"status": "triggered\_via\_http"})            
})

// 4\. Core Handler: Parent Handler (Satu Pintu Utama untuk mengambil Lock)            
func runTickParentHandler(ctx context.Context, mode string, dal \*DAL) {            
    // Memulai Root Span untuk proses ETL latar belakang          
    ctx, span := tracer.Start(ctx, fmt.Sprintf("runTickParentHandler\_%s", mode))          
    defer span.End()

    // Membuka transaksi MURNI via Guardrail DAL (Direct Connection)  
    tx, err := dal.AcquireAdvisoryLock(ctx)            
    if err \!= nil {             
        span.RecordError(err)          
        return             
    }            
                
    // PROTEKSI MUTLAK: defer ini menggaransi transaksi pasti akan di-rollback (melepaskan lock)     
    // jika fungsi ini selesai, crash, atau context dibatalkan.    
    defer tx.Rollback()

    var locked bool            
    // Mencoba mengklaim Transaction-Level Lock menggunakan Konstanta LockIDTick (Satu Pintu)      
    err \= tx.QueryRowContext(ctx, "SELECT pg\_try\_advisory\_xact\_lock($1)", LockIDTick).Scan(\&locked)            
    if err \!= nil || \!locked {            
        span.AddEvent("Lock already acquired by another process/instance (Regular/Backfill), skipping.")          
        return             
    }

    // Hak eksklusif berhasil didapatkan\!            
              
    // PROTEKSI JARINGAN (LOCK HEALTH MONITOR):          
    // Heartbeat untuk mendeteksi hilangnya koneksi database di jalur Direct          
    lockCtx, cancelETL := context.WithCancel(ctx) // lockCtx juga mewarisi OTel TraceID          
    defer cancelETL() 

    go func() {          
        ticker := time.NewTicker(30 \* time.Second)          
        defer ticker.Stop()          
        for {          
            select {          
            case \<-lockCtx.Done():          
                return           
            case \<-ticker.C:          
                var alive bool          
                  
                // Tambahkan timeout spesifik agar heartbeat tidak hang saat database lambat ekstrem  
                hbCtx, hbCancel := context.WithTimeout(lockCtx, 5\*time.Second)  
                err := tx.QueryRowContext(hbCtx, "SELECT true").Scan(\&alive)  
                hbCancel() // Selalu bebaskan context timeout

                if err \!= nil {          
                    span.RecordError(fmt.Errorf("lost lock connection on direct port: %w", err))          
                    cancelETL() // Membatalkan seluruh proses worker secara otomatis          
                    return          
                }          
            }          
        }          
    }()

    // Fork: Memecah proses (Child Concurrency) HANYA SETELAH Lock dipegang penuh.    
    // Worker WAJIB memanggil koneksi pool melalui parameter DAL.  
    var wg sync.WaitGroup      
          
    if mode \== "REGULAR" {      
        wg.Add(3)      
        go runT0Phase(lockCtx, dal, \&wg) // Eksekusi T-0 Asinkron      
        go runT1Phase(lockCtx, dal, \&wg) // Eksekusi T-1 Asinkron      
        go runT2Phase(lockCtx, dal, \&wg) // Eksekusi T-2 Asinkron      
    } else if mode \== "BACKFILL" {      
        wg.Add(3)      
        go runBackfillIngestionGroup(lockCtx, dal, \&wg) // Eksekusi Grup Utama Asinkron      
        go runBackfillResetGroup(lockCtx, dal, \&wg)     // Eksekusi Grup Reset Asinkron      
        go runBackfillValidationGroup(lockCtx, dal, \&wg)// Eksekusi Grup Validasi Asinkron      
    }      
          
    // Join: Menunggu seluruh sub-proses konkuren selesai sebelum melepaskan lock.      
    wg.Wait()      
}

**Aturan Mutlak Goroutine Anakan: Panic Recovery & WaitGroup Guarantee**

Untuk mencegah anomali di mana fungsi anakan (seperti fase T-2) mengalami *panic* tak terduga (misal karena *file corruption parsing*) yang mengakibatkan wg.Done() tidak pernah dipanggil — yang akan menyebabkan wg.Wait() pada *Parent Handler* tertahan selamanya dan menyandera kunci *Advisory Lock* — maka **SELURUH goroutine anakan wajib mendaftarkan mekanisme *defer panic recovery* berurutan di awal fungsinya**.

// Contoh Implementasi WAJIB untuk semua worker (T-0, T-1, T-2, Backfill, dll)  
func runT2Phase(ctx context.Context, dal \*DAL, wg \*sync.WaitGroup) {  
    // 1\. GARANSI UNBLOCK: Harus selalu dipanggil, diletakkan paling awal   
    // agar dieksekusi terakhir setelah panic-recovery selesai.  
    defer wg.Done() 

    // 2\. PROTEKSI PANIC: Menangkap panic agar tidak meruntuhkan aplikasi   
    // dan menjamin eksekusi mengalir hingga ke defer wg.Done()  
    defer func() {  
        if r := recover(); r \!= nil {  
            // Ekstrak span dari context (OTel) untuk korelasi Trace  
            span := trace.SpanFromContext(ctx)  
            span.RecordError(fmt.Errorf("panic in runT2Phase: %v", r))  
              
            // Opsional (Fallback Logger)  
            log.Printf("FATAL: Panic tertangkap di goroutine T-2, TraceID: %s | Error: %v", span.SpanContext().TraceID().String(), r)  
        }  
    }()

    // ... logika utama eksekusi ETL T-2 ...  
    // Jika terjadi panic di sini, defer recovery akan menangkapnya,  
    // lalu defer wg.Done() akan melepaskan antrean Parent Handler.  
}

### **B. Mekanisme Atomic Job Claiming & Updates**

Di dalam implementasi sub-proses dari Goroutine di atas, antrean pekerjaan aktual diambil berdasarkan prioritas status dan urutan waktu menggunakan *Query Modifier* di ent untuk mengimplementasikan FOR UPDATE SKIP LOCKED. Mekanisme ini **wajib** dilakukan di dalam satu blok transaksi database *yang berbeda dari transaksi penahan Lock* (dan dijalankan via PgBouncer) agar eksekusi sangat cepat.

**Aturan Emas Modifikasi Counter & Status Historis:** 1\. Segala perubahan terhadap kolom RetryCount maupun NotFoundStreak **dilarang keras** dilakukan dengan membaca nilai ke memori Go dan menuliskannya kembali. Semua modifikasi wajib dieksekusi secara atomik di sisi SQL.

2\. Setiap kali mengeksekusi transisi status (misal dari PENDING ke PROCESSED, atau dari COMPLETED ke BROKEN), **status lama WAJIB disalin ke dalam kolom PreviousStatus** pada blok UpdateOneID yang sama.

// Di dalam executeETLLogic() Goroutine Child (Async)            
var claimedState \*ent.State

// PENTING: Menggunakan metode Guardrail ExecuteInPool milik DAL  
err := dal.ExecuteInPool(ctx, func(tx \*ent.Tx) error {            
    var errTx error            
                
    // 1\. Pilih baris dengan Lock di DALAM transaksi PgBouncer.             
    claimedState, errTx \= tx.State.Query().              
        Where(              
            state.JobTypeEQ(jobType),              
            state.StatusIn(statuses...),              
            state.HasInstrumentWith(          
                instrument.IsPauseEQ(false),          
                instrument.IsActiveEQ(true), // Proteksi instrumen yang dinonaktifkan          
            ),              
            state.IsDeletedEQ(false), // Proteksi baris yang masuk fase pruning          
        ).              
        Order(              
            func(s \*sql.Selector) {              
                s.OrderExpr(sql.ExprFunc(func(b \*sql.Builder) {              
                    b.WriteString(\`CASE status WHEN 'PENDING' THEN 1 WHEN 'PROCESSED' THEN 2 WHEN 'FAILED' THEN 3 WHEN 'BROKEN' THEN 4 WHEN 'NOT\_FOUND' THEN 5 WHEN 'COMPLETED' THEN 6 ELSE 7 END ASC\`)              
                }))              
            },              
            ent.Asc(state.FieldTimestamp),              
        ).              
        Modify(func(s \*sql.Selector) {              
            s.ForUpdate(sql.WithLockAction(sql.SkipLocked))              
        }).              
        First(ctx)            
                    
    if errTx \!= nil {            
        return errTx            
    }

    // 2\. Langsung update status ke PROCESSED di dalam transaksi yang sama.  
    // ATURAN MUTLAK: Selalu geser nilai Status yang diklaim ke PreviousStatus  
    claimedState, errTx \= tx.State.UpdateOneID(claimedState.ID).              
        SetPreviousStatus(claimedState.Status). // Track history\!  
        SetStatus(state.StatusPROCESSED).              
        SetUpdatedAt(time.Now().UTC()).              
        Save(ctx)

    return errTx            
})

### **C. Auto-Seeder & Pruning (Idempotent, Batched & Per-Instrument)**

**Target Waktu:** Dipicu melalui **Event (Message Broker / HTTP)** yang diatur dari luar sistem (misalnya dikirim setiap 5 menit dimulai dari menit ke-02).

1. **Maintenance Lock:** Ambil LockIDMaintenance (1003) via *Direct Connection*. Transport layer mendelegasikan pemanggilan ke *background handler*.  
2. **Forward Seeding (Batched):** Iterasi per instrumen aktif (IsActive \= true) dan per job\_type. Untuk mencegah *timeout*, memori membludak (*OOM*), dan *long table lock* di PostgreSQL (khususnya untuk instrumen baru), **proses seeding wajib menerapkan batasan (*batching*)**.  
   * Cari MAX(timestamp) terakhir **dengan filter wajib per job\_type untuk mencegah ambiguitas**.  
   * Buat tumpukan (*chunk*) penambahan baris ke masa depan dengan batas maksimal **misal 30 hari (720 jam)** per satu kali iterasi eksekusi.  
   * Lakukan *Upsert* (INSERT ... ON CONFLICT DO NOTHING) sampai batas target *batch* tercapai atau mentok di time.Now().UTC(). Sisa hari yang tertinggal akan dikerjakan pada pemicuan 5 menit berikutnya.  
3. **Historical Sync & Gap Detection (Batched):** Bandingkan MIN(timestamp) di mana instrument\_id \= ID AND job\_type \= 'TICK' AND status \= 'CONFIRMED' dengan start\_date instrumen.  
   * **Edge Case (Instrumen Baru):** Jika hasil MIN() adalah NULL (diimplementasikan di Go dengan mengecek pointer minTime \== nil), ini berarti instrumen belum di-ingesti sama sekali. Sistem harus membiarkan proses seeding terus bergerak dari start\_date menuju waktu saat ini secara parsial dari waktu ke waktu **tanpa** mengaktifkan status IsPause.  
   * **Isi Gap Masa Lampau:** Jika MIN \> start\_date, sisipkan (*Upsert*) baris kosong menuju masa lalu secara berangsur-angsur (*backward batched insertion*).  
   * **Isolasi Terukur (IsPause Trigger):** Jika hasil MIN() bukan NULL dan menunjukkan MIN \> start\_date, sistem harus memverifikasi apakah ini dikarenakan data memang belum di-*seed*, atau karena baris gagal diproses (stuck).  
     Sistem menjalankan fungsi deterministik isBackfillComplete(instrumentID, startDate, minConfirmedTime):  
     1. Hitung durasi jam aktual yang diekspektasikan (*Expected Hours Count*) antara start\_date dan MIN(timestamp).  
     2. Lakukan evaluasi secara spesifik terhadap baris-baris dalam rentang tersebut (antara start\_date dan MIN). **Hitung baris yang secara nyata mengalami masalah (*stuck*)** seperti berstatus FAILED, BROKEN, ABANDONED, atau baris PROCESSED/PENDING yang tertinggal terlampau usang **(didefinisikan secara tegas: memiliki nilai UpdatedAt lebih lama dari 24 jam dari waktu saat ini)**. Baris NOT\_FOUND yang sedang dalam siklus tunggu wajar (belum mencapai *threshold* NotFoundStreak \>= 3\) **tidak boleh** dihitung sebagai baris yang telah terdaftar secara permanen.  
     3. **Evaluasi:** Jika ditemukan baris-baris *error/stuck* tersebut yang menghalangi mundurnya nilai MIN(timestamp) menuju start\_date, hal ini berarti terdapat korupsi data persisten di hulu. Jika kondisi kemacetan riil ini terpenuhi, maka aktifkan **IsPause \= true** secara eksplisit pada instrumen tersebut agar proses re-try yang sia-sia tidak menyumbat antrean.  
4. **Pruning (2-Phase Commit & Batched):** Menghapus entitas dari Database dan Cloudflare R2 secara bersamaan tidak mungkin dijamin atomik. Jika Database terhapus lebih dulu namun R2 *error*, akan terbentuk *permanent orphaned files* yang memboroskan *storage*. Proses ini dipecah menjadi **Mark & Sweep**:  
   * **Fase 1 \- Mark (Soft Delete):** Jalankan proses UPDATE untuk menandai IsDeleted \= true pada sekumpulan (*batch*) record di mana timestamp \< start\_date (dan IsDeleted \= false). Record yang ditandai ini akan langsung diabaikan oleh worker ETL manapun.  
   * **Fase 2 \- Sweep (Hard Delete & Object Removal):** Ambil antrean record dari database dengan kondisi IsDeleted \= true secara *batched*. Untuk setiap record, *Worker* akan memicu perintah penghapusan di Cloudflare R2 untuk menyingkirkan file fisiknya terlebih dahulu.  
   * **Finalisasi:** **HANYA JIKA** R2 merespons dengan indikasi sukses (termasuk jika merespons *NoSuchKey* yang berarti file memang sudah lenyap), barulah sistem menjalankan perintah DELETE fisik (*hard-delete*) di PostgreSQL. Jika R2 merespons *timeout* atau gagal, baris tersebut tetap tersimpan di DB dengan IsDeleted \= true dan akan di-*retry* pada pemanggilan Pruning berikutnya (Idempotent).  
5. **Resume:** Set IsPause \= false jika MIN(timestamp) instrumen (yang berstatus CONFIRMED) sudah stabil mencakup start\_date.

### **D. Ticks Reguler (Offset T-0, T-1, T-2)**

Proses reguler dipicu melalui **Event (Message Broker / HTTP)** yang secara eksternal diharapkan tiba setiap jam **tepat pada menit ke-05** (contoh: 10:05 UTC) untuk memproses data dari instrumen yang aktif.

*(Catatan Konkurensi: Ketiga offset waktu di bawah ini dijalankan secara konkuren/paralel oleh **Parent Handler tunggal**. Parent handler bertugas mengambil Global Lock LockIDTick, dan baru memecah eksekusinya (forking goroutine) ke T-0, T-1, dan T-2 setelah hak eksklusif diamankan).*

1. **T-0 (Current Hour \- Ingesti Real-time):**  
   * **Target Waktu:** Jam berjalan (misal: eksekusi jam 10:05, target timestamp adalah 10:00:00 UTC).  
   * **Kondisi Claim:** Hanya mengambil record states dengan JobType \= 'TICK', Status \= 'PENDING', dan instrumen terkait memiliki IsPause \= false **serta IsActive \= true**.  
   * **Aksi Compute Engine:** Proses ETL (Ekstraksi, Transformasi, Load) ini dipisahkan secara arsitektural dalam hal manajemen konkurensi:  
     1. **Klaim baris secara atomik:** Mengubah status antrean ke PROCESSED.  
     2. **Unduh file BI5 mentah dari API Dukascopy:** Proses ekstraksi ini **dibatasi secara ketat maksimal 12 goroutines yang berjalan bersamaan (Worker Pool / Bounded Semaphore)**. Pembatasan kaku pada fase *download* ini adalah mekanisme pertahanan utama demi menghindari *Error 429 Too Many Requests* dari server Dukascopy.  
     3. **Konversi format:** Mengubah format integer file BI5 yang telah diunduh ke format Parquet *Decimal/BigInt* (mengacu pada atribut *divider* instrument). Proses CPU-bound ini berjalan konkuren di luar batasan 12 goroutines di atas, **namun tetap dikendalikan oleh *Bounded Semaphore*** agar sistem tidak OOM saat memproses puluhan instrumen.  
     4. **Unggah hasil Parquet ke Cloudflare R2:** Sesuai format path. Proses *I/O-bound* ini juga beroperasi secara konkuren dan berbagi kuota *Bounded Semaphore* yang sama dengan proses konversi guna menjaga kestabilan alokasi RAM.  
   * **Hasil Akhir:** Status diperbarui ke COMPLETED (jika unggahan sukses), FAILED (jika error unduh/konversi), atau NOT\_FOUND (diset menggunakan instruksi **inkremen atomik AddNotFoundStreak(1)**. Jika secara situasional nilainya langsung menembus batas \>= 3, sistem **wajib menyiapkan *Zero-Row Parquet* di memori, mengamankan status IsHoliday \= true ke database dalam satu transaksi atomik terlebih dahulu**, dan baru setelah DB terupdate file tersebut diunggah ke R2 untuk akhirnya ditetapkan sebagai CONFIRMED).  
2. **T-1 (Previous Hour \- Recovery & Retry):**  
   * **Target Waktu:** Satu jam sebelumnya (misal: target timestamp 09:00:00 UTC).  
   * **Kondisi Claim:** Mengambil record dengan Status IN ('PENDING', 'PROCESSED', 'FAILED', 'NOT\_FOUND') dan IsPause \= false **serta IsActive \= true**.  
   * **Aksi Compute Engine:** Berfungsi sebagai *safety net* untuk pemulihan. Status **PROCESSED secara langsung disapu dan di-reset menjadi PENDING**. Untuk baris dengan status FAILED, sistem akan meng-inkremen secara atomik AddRetryCount(1). **Jika nilai RetryCount mencapai batas maksimal (misal: \>= 5), status langsung diubah menjadi ABANDONED untuk mencegah *infinite loop***. Jika API sumber merespons data kosong (404), sistem akan meng-inkremen secara atomik AddNotFoundStreak(1). Jika setelah inkremen nilai NotFoundStreak mencapai batas \>= 3, **T-1 akan langsung mengambil alih prosedur pembuatan Zero-Row Parquet** secara mandiri dengan alur yang ketat demi mencegah *partial failure loop* (siapkan file 0 baris di memori \-\> **Update DB IsHoliday \= true terlebih dahulu** \-\> unggah ke R2 \-\> validasi \-\> promosi ke CONFIRMED).  
   * **Hasil Akhir:** Diperbarui ke COMPLETED, FAILED, ABANDONED, NOT\_FOUND (jika belum mencapai ambang batas \>= 3), atau **CONFIRMED** (jika batas streak tercapai, *Zero-Row* valid, dan *SyncTask* terkirim). Jika pada saat proses ternyata data ditemukan (tidak kosong), maka status NotFoundStreak **wajib di-reset menjadi 0**.  
3. **T-2 (Two Hours Ago \- Validasi Fisik & Promosi):**  
   * **Target Waktu:** Dua jam sebelumnya (misal: target timestamp 08:00:00 UTC).  
   * **Kondisi Claim:** Mengambil record dengan Status \= 'COMPLETED' (untuk instrumen yang IsActive \= true). Worker T-2 **hanya** bertugas memvalidasi file yang sudah berhasil diunggah. Seluruh status PROCESSED yang menggantung (*stray/stuck*) tidak ditangani oleh T-2, melainkan sepenuhnya menjadi tanggung jawab proses Reset/Backfill agar pemulihan status tetap terpusat dan tidak tumpang tindih dengan validasi fisik.  
   * **Aksi Compute Engine:** Tidak melakukan unduhan ke sumber data. Sistem membaca objek langsung dari Cloudflare R2 dan menjalankan **Validasi Fisik File Parquet (sesuai metrik Poin 6.3)**.  
   * **Hasil Akhir:**  
     * **LULUS:** Mengubah status menjadi CONFIRMED dan **menyisipkan SyncTask di dalam transaksi database yang sama (Poin 6.1)** untuk mengatur counter agregasi.  
     * **GAGAL:** Status diubah menjadi BROKEN agar nantinya dikerjakan ulang pada siklus eksekusi *Backfill Ticks*. **Sistem juga wajib menyisipkan SyncTask (memicu rekalkulasi hitungan agregasi harian) secara transaksional jika nilai PreviousStatus \== CONFIRMED (kasus terjadinya demosi data yang sebelumnya dianggap final)**.
     * **Catatan Demotion:** Jalur penurunan status dari CONFIRMED menjadi BROKEN (yang men-trigger pembuatan SyncTask demosi) **hanya** terjadi melalui eksekusi **Worker Backfill / Manual Reset**. Worker validasi reguler T-2 didesain khusus hanya untuk memproses status COMPLETED, sehingga tidak memvalidasi ulang baris CONFIRMED secara rutin.

### **E. Backfill Ticks (Setiap 10 Menit)**

Proses *Backfill* berfungsi sebagai "penyapu" (*sweeper*) di mana pemicu event dipanggil melalui **Event (Message Broker / HTTP)** (ideal diset dari luar setiap 10 menit). Tujuannya adalah membereskan data historis yang tertinggal, merecover kegagalan, atau memvalidasi pekerjaan yang terlewat oleh siklus T-2. Sistem memprioritaskan pekerjaan ke dalam tiga kelompok (semua wajib untuk instrumen yang IsActive \= true dan IsPause \= false):

*(Catatan Konkurensi: Sama halnya dengan Ticks Reguler, ketiga grup backfill di bawah ini dijalankan paralel oleh **Parent Handler tunggal** yang mengunci Global Lock LockIDTick dari satu pintu).*

1. **Grup Ingesti Utama (Menyelesaikan PENDING):**  
   * **Kondisi Claim:** Baris dengan Status \= 'PENDING'. Biasanya ini merupakan baris masa lampau yang di-*generate* oleh proses *Seeder*, atau baris yang baru saja di-reset dari status *error*.  
   * **Aksi Compute Engine:** Melakukan siklus ETL standar (Unduh BI5 mentah dengan maksimal 12 worker \-\> Konversi ke format Parquet secara bebas terkontrol Semaphore \-\> Unggah ke R2).  
   * **Hasil Akhir:** Diperbarui menjadi COMPLETED (sukses), FAILED (gagal proses), atau NOT\_FOUND (dengan inkremen atomik AddNotFoundStreak(1). Sekali lagi, jika menyentuh \>= 3, alur pengamanan *Zero-Row* dieksekusi dengan update DB mendahului upload file).  
2. **Grup Reset & Pemulihan (Menangani PROCESSED, FAILED, BROKEN):**  
   * **Kondisi Claim:** Baris dengan status FAILED, BROKEN (file korup/gagal validasi), atau **PROCESSED**. Seluruh baris PROCESSED yang tertangkap oleh siklus Backfill ini dijamin merupakan proses yang gagal (zombie worker), karenanya **langsung diklaim tanpa mengecek durasi UpdatedAt**.  
   * **Aksi Compute Engine:** Tidak ada proses *download/upload* berat pada tahapan ini. Sistem sekadar melakukan "cuci gudang" dengan mereset statusnya. Untuk baris PROCESSED diretur ke PENDING. Untuk FAILED dan BROKEN akan di-inkremen secara atomik AddRetryCount(1). **Jika nilai RetryCount pasca-inkremen mencapai ambang batas \>= 5, baris tersebut langsung ditransisikan ke status ABANDONED**, memberhentikannya dari siklus antrean tak berujung.  
   * **Hasil Akhir:** Status diubah menjadi PENDING, atau ABANDONED (jika *max retry limit* terlampaui).  
3. **Grup Validasi Final (Mengevaluasi COMPLETED, NOT\_FOUND):**  
   * **Kondisi Claim:** Baris dengan status COMPLETED (yang belum sempat dipromosikan oleh reguler T-2) atau **NOT\_FOUND (tanpa batasan nilai spesifik, seluruhnya dievaluasi untuk menjamin tidak ada record stuck)**.  
   * **Aksi Compute Engine:**  
     * **Untuk COMPLETED:** Sistem membaca file dari Cloudflare R2 untuk menjalankan **Validasi Fisik File Parquet secara komprehensif** (Sesuai metrik Poin 6.3).  
     * **Untuk NOT\_FOUND:** Sistem mengecek ulang ke sumber data API. Pengecekan ulang bertindak untuk memastikan apakah data sekadar terlambat (*delay rilis*) atau memang libur permanen. **PENTING (Rate-Limiter): Panggilan HTTP untuk mengecek ulang data API ini WAJIB menggunakan pool Bounded Semaphore (maksimal 12\) yang sama dengan jalur unduhan BI5 utama.** Tanpa perlindungan ini, memproses ratusan record NOT\_FOUND masa lampau secara bersamaan akan memicu serangan *Error 429 Too Many Requests* ke server Dukascopy.  
   * **Hasil Akhir:**  
     * **LULUS / DATA TERSEDIA & VALID:** Jika data yang tadinya NOT\_FOUND ternyata sudah rilis, sistem akan mengunduh, konversi, unggah ke R2, memvalidasi nya, mereset NotFoundStreak menjadi 0, dan merubah status ke CONFIRMED (beserta SyncTask).  
     * **TETAP KOSONG (API \= 404):** Sistem melakukan inkremen atomik AddNotFoundStreak(1). Jika hitungan aktual di database **mencapai atau melewati** batas toleransi kelambatan (yaitu NotFoundStreak \>= 3), data dipastikan sebagai Hari Libur Permanen. Untuk **mencegah *infinite loop*** akibat aplikasi *crash* (kondisi *race* di mana file kosong terunggah namun DB gagal mengunci flag), sistem wajib menerapkan urutan operasi yang tidak bisa dipisahkan:  
       1. Siapkan struktur *Zero-Row Parquet* di dalam memori lokal.  
       2. **Lakukan transaksi atomik ke PostgreSQL untuk mengubah status IsHoliday menjadi true TERLEBIH DAHULU.**  
       3. **HANYA JIKA transaksi DB berhasil (commit)**, worker mengunggah file memori tersebut ke Cloudflare R2.  
          Setelah diunggah, file tersebut **wajib dibaca kembali dan divalidasi fisiknya** menggunakan kriteria Poin 6.3. Jika validasi lulus, status diubah menjadi **CONFIRMED** (Terminal State) dan sistem menyisipkan event SyncTask.  
     * **GAGAL / FILE KORUP:** Status diturunkan menjadi BROKEN (untuk memicu reset pada siklus berikutnya) dan menyisipkan event SyncTask secara transaksional **jika nilai PreviousStatus \== CONFIRMED (kasus demosi)**.

### **F. Candles Reguler (Seeding & Agregasi Utama)**

Proses pembentukan file *Candles* (agregasi dari data *Ticks* ke 19 timeframe) bertindak sebagai rantai paling ujung di sistem. Proses ini tidak mengunduh data dari Dukascopy, melainkan mengagregasi data dari file Ticks Parquet yang sudah diverifikasi di R2.

1. **Seeding Harian (Inisialisasi PENDING):**  
   * **Target Waktu:** Dijalankan via **Event (Message Broker / HTTP)** sekali sehari setiap pukul 00:00 UTC.  
   * **Kondisi Claim:** Seluruh instrumen yang berstatus aktif (IsActive \= true) dan tidak sedang di-pause (IsPause \= false).  
   * **Aksi Compute Engine:** Menjalankan query Upsert (OnConflict Do Nothing) untuk membooking baris dengan JobType \= 'CANDLE', timestamp pada 00:00:00 hari tersebut, Status \= 'PENDING', dan ResolvedTickCount \= 0\.  
2. **Eksekusi Reguler (Agregasi Utama 05:08 UTC):**  
   * **Target Waktu:** Dipicu via **Event (Message Broker / HTTP)** pukul 05:08 UTC setiap harinya. Jeda waktu ekstra diberikan untuk memberi ruang agar 24 jam data *Tick* hari sebelumnya selesai di-ingesti dan divalidasi penuh oleh sistem.  
   * **Kondisi Claim:** Baris dengan JobType \= 'CANDLE', Status \= 'PENDING', instrumen aktif (IsActive \= true), tidak sedang di-pause (IsPause \= false), dan **syarat mutlak:** ResolvedTickCount \= 24 (Artinya 24 jam siklus *Tick* di hari tersebut telah mencapai fase akhir/terminal CONFIRMED).  
   * **Aksi Compute Engine (Di Background Goroutine):** 1\. Klaim baris secara atomik (Ubah ke PROCESSED).  
    2\. **Stream-based Aggregation (O(1) Memory Footprint):** *Worker* dilarang keras mengunduh seluruh 24 file Ticks sekaligus ke dalam memori atau melakukan penumpukan sementara di disk (*disk-staging*). Sistem wajib memproses file secara *streaming* dan bergiliran. *Worker* membaca satu persatu file Ticks dari R2, mem-*parsing* barisnya untuk mengkomputasi dan mengakumulasikan nilai *Open, High, Low, Close, VWAP, Spread,* dan *Volume* ke dalam struktur agregasi di memori (*OHLCV accumulators* untuk 19 timeframe baku). Setelah satu file selesai diproses, sistem wajib membersihkan baris mentah (*raw rows*) tersebut dari memori sebelum lanjut mengunduh file jam berikutnya. *(Catatan Penting: Karena semua status CONFIRMED kini selalu memiliki file fisik Parquet, worker **TIDAK BOLEH mengabaikan error NoSuchKey dari R2 secara sembarangan**. Jika terjadi NoSuchKey, worker wajib mengecek flag IsDeleted pada baris TICK di database. Jika IsDeleted \= true (efek dari proses pruning), file tersebut dapat diabaikan secara aman. Namun, jika IsDeleted \= false, itu adalah indikasi nyata BROKEN Storage dan job harus BROKEN).*  
     3\. Lakukan proses *streaming* agregasi komputasi tersebut secara terus-menerus hingga seluruh 24 jam selesai dibaca. Jika saat memproses suatu file ditemukan file tersebut adalah *Zero-Row Parquet* (libur), sistem langsung melewatinya dari kalkulasi harga namun *looping* tetap dilanjutkan.  
     4\. Setelah seluruh data diakumulasikan, bentuk *accumulators* tersebut menjadi **1 (satu) file Parquet harian** terpadu (meskipun isinya kosong/flat jika libur sehari penuh).  
     5\. Unggah file hasil agregasi ke struktur folder *Candles* di R2.  
  * **Hasil Akhir:** Status diperbarui ke COMPLETED (berhasil diunggah), FAILED (gagal proses agregasi/unggah), atau BROKEN (khusus anomali storage seperti NoSuchKey pada file TICK yang seharusnya ada dan IsDeleted \= false). Status COMPLETED akan diiringi verifikasi instan untuk menaikkannya menjadi CONFIRMED.

### **G. Backfill Candles (Setiap 20 Menit)**

Proses *Backfill* ini bertindak sebagai *sweeper* di latar belakang khusus untuk pekerjaan CANDLE guna menangani kegagalan atau antrean yang tertinggal dari proses reguler.

1. **Backfill & Recovery:**  
   * **Target Waktu:** Dipicu via **Event (Message Broker / HTTP)** (ideal diset dari luar setiap 20 menit mulai dari menit ke-04 yakni pada menit ke-04, 24, 44, dst).  
   * **Kondisi Claim:** Mengambil baris CANDLE milik instrumen aktif (IsActive \= true) dan tidak sedang di-pause (IsPause \= false) dengan:  
     * Status FAILED, BROKEN, atau **PROCESSED** (langsung diklaim dan di-reset tanpa batasan batas waktu).  
     * Status PENDING masa lampau (tertinggal) di mana syarat agregasi sudah terpenuhi (ResolvedTickCount \= 24).  
   * **Aksi Compute Engine:**  
     * **Reset:** Baris *error* / *stuck* (termasuk yang tersangkut di PROCESSED) diturunkan kembali ke PENDING dengan RetryCount secara atomik ditambah 1 (AddRetryCount(1)). **Sama halnya dengan TICK, jika RetryCount Candles mencapai batas \>= 5, statusnya langsung diubah menjadi ABANDONED.**  
     * **Eksekusi Susulan:** Baris PENDING yang tertinggal namun sudah lengkap (*count* 24\) akan langsung diproses mengikuti alur **Eksekusi Reguler** (menggunakan *Stream-based Aggregation* \-\> Agregasi \-\> Unggah).  
   * **Hasil Akhir:** Proses susulan akan berakhir di status COMPLETED (lalu menuju CONFIRMED), FAILED dengan mekanisme *Overwrite Policy* wajib di iterasi berikutnya, atau ABANDONED jika melebih batas *retry*.

## **5\. Manajemen Alur Status (State Transition) Sistem Ticks**

Dokumen ini menjelaskan alur perubahan status (State Transition) untuk pemrosesan Ticks, baik dalam mode Reguler maupun Backfill.

### **5.1. Aturan Dasar & Konkurensi**

* **Status Awal:** Semua job/task selalu dimulai dengan status PENDING.  
* **Mutual Eksklusif Mode:** Mode Reguler dan Backfill tidak boleh berjalan secara bersamaan. Hanya salah satu yang aktif pada satu waktu (menggunakan pg\_try\_advisory\_xact\_lock secara global via koneksi Direct).  
* **Konkurensi Terpusat (Fork-Join):** Proses-proses offset (T-0, T-1, T-2 pada mode Reguler) maupun sub-grup pada mode Backfill berjalan secara paralel/konkuren secara individual. **Namun**, mereka wajib dipicu (*fork*) dari **satu pintu utama (Parent Handler)** yang sebelumnya telah berhasil mengamankan *Global Lock* (LockIDTick). Pendekatan ini menjamin mode Reguler dan Backfill tidak akan pernah mengalami *race condition* atau saling *overwrite*, namun operasi internalnya tetap secepat mungkin secara asinkron.

### **5.2. Matriks Transisi Status per Proses**

#### **A. Fase Ingesti (Reguler T-0 & Backfill Ingesti)**

Tugas utama fase ini adalah mengambil data mentah.

| **Status Awal** | **Action saat Diambil** | **Kondisi Eksekusi** | **Status Akhir** | **Keterangan** |

| PENDING | Update ke PROCESSED | Ingesti Berhasil | COMPLETED | Data berhasil diproses. |

| PENDING | Update ke PROCESSED | Data tidak ada | NOT\_FOUND | Data tidak ditemukan. Melakukan **Inkremen Atomik (+1) pada NotFoundStreak**. |

| PENDING | Update ke PROCESSED | Terjadi Error | FAILED | Error tertangkap (handled error). |

| PENDING | Update ke PROCESSED | Error tak tertangkap | PROCESSED | Stuck (akan diatasi oleh Fase Reset). |

#### **B. Fase Reset & Pemulihan (Reguler T-1 & Backfill Reset)**

Tugas utama fase ini adalah memulihkan job yang stuck, gagal, atau perlu dicoba ulang.

| **Status Awal** | **Kondisi Eksekusi** | **Status Akhir** | **Perubahan Counter (Atomik)** | **Keterangan** |

| PENDING | Ditemukan | PENDING | Tidak ada | Refresh/Reset state. |

| PROCESSED | Ditemukan (Stuck) | PENDING | Tidak ada | Memulihkan job dari T-0 yang stuck. |

| FAILED | Ditemukan (RetryCount \< 5\) | PENDING | RetryCount (+1) | Mencoba ulang job yang gagal. |

| FAILED | Ditemukan (RetryCount \>= 5\) | **ABANDONED** | RetryCount (+1) | Limit retry error tercapai. |

| BROKEN | Ditemukan (RetryCount \< 5\) | PENDING | RetryCount (+1) | Khusus Backfill Reset, coba ulang data korup. |

| BROKEN | Ditemukan (RetryCount \>= 5\) | **ABANDONED** | RetryCount (+1) | Limit retry error tercapai. |

| NOT\_FOUND | Ditemukan (Ternyata ada) | PENDING | **Reset NotFoundStreak \= 0** | Data yang sebelumnya kosong kini tersedia, reset hitungan kemangkiran ke awal. |

| NOT\_FOUND | Tetap tidak ditemukan | NOT\_FOUND | NotFoundStreak (+1) | Data masih belum tersedia, tambah hitungan *streak*. |

**Catatan Khusus CANDLE:** Pada alur umum TICK, transisi PROCESSED \-\> PENDING untuk memulihkan job stuck tidak menaikkan RetryCount. Namun pada CANDLE, transisi PROCESSED \-\> PENDING di fase Backfill/Reset dianggap sebagai indikasi agregasi harian yang sempat tersangkut, sehingga RetryCount wajib dinaikkan secara atomik (+1) sebagai pengecualian dari perilaku TICK.

#### **C. Fase Validasi (Reguler T-2 & Backfill Validate)**

Tugas utama fase ini adalah memvalidasi hasil akhir data (misal: integritas file).

| **Status Awal** | **Kondisi Eksekusi** | **Status Akhir** | **Keterangan** |

| COMPLETED | Validasi Sukses | CONFIRMED | Data valid dan final. |

| COMPLETED | File Parquet Corrupt | BROKEN | Menandakan file rusak (akan direset Backfill). |

| NOT\_FOUND | NotFoundStreak \< 3 | NOT\_FOUND | Pengecekan ulang ke API dilakukan. Jika tetap kosong, nilai diinkremen secara atomik. |

| NOT\_FOUND | NotFoundStreak \>= 3 & Validasi Sukses | CONFIRMED | Toleransi *streak* tercapai, dipastikan libur, diakhiri secara sistem (Zero-Row). |

| NOT\_FOUND | NotFoundStreak \>= 3 & (Parquet Corrupt / Ada Row) | BROKEN | Anomali: Status NOT\_FOUND tapi file corrupt atau berisi data tak wajar (tidak sinkron). |

### **5.3. Diagram Alur State (State Diagram)**

Diagram di bawah ini memvisualisasikan bagaimana sebuah data berpindah dari satu status ke status lainnya berdasarkan aktor prosesnya.

graph TD      
    %% Styling      
    classDef pending fill:\#f9f0ff,stroke:\#d8b4e2,stroke-width:2px;      
    classDef inprogress fill:\#fff3cd,stroke:\#ffeeba,stroke-width:2px;      
    classDef success fill:\#d4edda,stroke:\#c3e6cb,stroke-width:2px;      
    classDef error fill:\#f8d7da,stroke:\#f5c6cb,stroke-width:2px;      
    classDef final fill:\#cce5ff,stroke:\#b8daff,stroke-width:2px;  
    classDef abandoned fill:\#e2e3e5,stroke:\#383d41,stroke-width:2px;

    %% States      
    Start((Mulai)) \--\> PENDING      
          
    PENDING(\[PENDING\]):::pending      
    PROCESSED(\[PROCESSED\]):::inprogress      
    COMPLETED(\[COMPLETED\]):::success      
    NOT\_FOUND(\[NOT\_FOUND\]):::inprogress      
    FAILED(\[FAILED\]):::error      
    BROKEN(\[BROKEN\]):::error      
    CONFIRMED(\[CONFIRMED\]):::final  
    ABANDONED(\[ABANDONED\]):::abandoned

    %% T-0 / Ingesti Flow      
    PENDING \-- "T-0 / B-Ingesti\\n(Ambil Job)" \--\> PROCESSED      
    PROCESSED \-- "T-0 / B-Ingesti\\n(Sukses)" \--\> COMPLETED      
    PROCESSED \-- "T-0 / B-Ingesti\\n(Kosong, Streak Atomik++)" \--\> NOT\_FOUND      
    PROCESSED \-- "T-0 / B-Ingesti\\n(Error)" \--\> FAILED      
    PROCESSED \-. "T-0 / B-Ingesti\\n(Error Tak Tertangkap)" .-\> PROCESSED

    %% T-1 / Reset Flow (Recoveries to PENDING or ABANDONED)      
    PROCESSED \-- "T-1 / B-Reset\\n(Recover Stuck)" \--\> PENDING      
    FAILED \-- "T-1 / B-Reset\\n(RetryCount \< 5, Atomik++)" \--\> PENDING  
    FAILED \-- "T-1 / B-Reset\\n(RetryCount \>= 5)" \--\> ABANDONED      
    BROKEN \-- "B-Reset\\n(RetryCount \< 5, Atomik++)" \--\> PENDING  
    BROKEN \-- "B-Reset\\n(RetryCount \>= 5)" \--\> ABANDONED      
    PENDING \-- "T-1 / B-Reset\\n(Refresh)" \--\> PENDING      
          
    %% T-1 / Reset (Special handling for NOT\_FOUND returning to process)      
    NOT\_FOUND \-- "T-1 / B-Reset\\n(Ternyata Ada, Reset Streak=0)" \--\> PENDING      
    NOT\_FOUND \-- "T-1 / B-Reset\\n(Tetap Kosong, Streak Atomik++)" \--\> NOT\_FOUND

    %% T-2 / Validate Flow      
    COMPLETED \-- "T-2 / B-Validate\\n(Valid)" \--\> CONFIRMED      
    COMPLETED \-- "T-2 / B-Validate\\n(Corrupt)" \--\> BROKEN      
          
    %% T-2 Validation for NOT\_FOUND threshold      
    NOT\_FOUND \-- "T-2 / B-Validate\\n(Check API, jika kosong: Streak++)" \--\> NOT\_FOUND      
    NOT\_FOUND \-- "T-2 / B-Validate\\n(Streak \>= 3 & Sukses)" \--\> CONFIRMED      
    NOT\_FOUND \-- "T-2 / B-Validate\\n(Streak \>= 3 & Corrupt/Ada Row)" \--\> BROKEN

**Catatan Diagram:** Panah PROCESSED \-\> PENDING menggambarkan perilaku umum TICK tanpa perubahan RetryCount. Untuk CANDLE, panah yang sama pada Backfill/Reset disertai kenaikan RetryCount (+1) secara atomik.

## **6\. Integritas Data & Sinkronisasi**

### **6.1. Transactional Outbox Pattern (Sinkronisasi Count Aktual)**

Untuk menjamin *reliability* dan menghindari *lost updates* (karena container *crash*) maupun *race conditions* saat melakukan *recompute*, sistem menggunakan pola **Outbox Pattern**.

Alih-alih langsung memicu perhitungan di dalam Goroutine, sistem akan menyisipkan *event* perubahan status ke dalam tabel SyncTask **di dalam transaksi database yang sama** dengan perubahan status TICK.

#### **Tahap 1: Pencatatan Event Secara Atomik (Publishing)**

Ketika sebuah file TICK mencapai fase akhir/terminal (status CONFIRMED) **atau ketika mengalami demosi (turun dari status CONFIRMED ke BROKEN)** di dalam Goroutine T-2 atau Backfill:

// PENTING: Worker wajib memanggil dal.ExecuteInPool  
err := dal.ExecuteInPool(ctx, func(tx \*ent.Tx) error {              
    // 1\. Ubah status TICK (menjadi CONFIRMED, baik karena ada data maupun karena libur permanen)  
    // ATAU menjadi BROKEN jika terjadi demosi kegagalan validasi.  
    tick, err := tx.State.UpdateOneID(tickID).  
        SetPreviousStatus(stateLama). // Rekam jejak status sebelum ini  
        SetStatus(statusBaru).           
        Save(ctx)              
    if err \!= nil { return err }

    // 2\. Sisipkan SyncTask (Outbox Event) dalam Tx yang sama.
    // Implementasi memakai raw SQL INSERT ... ON CONFLICT ... DO UPDATE agar
    // event lama di hari yang sama selalu dikembalikan ke PENDING.
    // Ini mencegah Lost Update ketika worker SyncTask sedang memproses event,
    // tetapi ada event TICK baru yang membutuhkan recompute ulang.
    targetDate := tick.Timestamp.Truncate(24 \* time.Hour)              
    err = tx.ExecContext(ctx, `
        INSERT INTO ingestion.sync_tasks (id, instrument_id, target_date, status, created_at)
        VALUES ($1, $2, $3, $4, NOW())
        ON CONFLICT (instrument_id, target_date)
        DO UPDATE SET status = EXCLUDED.status
    `, uuid.New(), tick.InstrumentID, targetDate, synctask.StatusPENDING)
    return err
})

#### **Tahap 2: Outbox Worker (Consumer Idempotent via Message Broker / HTTP)**

Event pemrosesan dipanggil secara berkala oleh trigger eksternal. Handler menerima instruksi dan memproses sinkronisasi menggunakan LockIDSync (1004).

Untuk menghindari *Lost Update* akibat penghapusan task secara prematur (saat ada event *TICK* baru yang mengubahnya kembali menjadi PENDING), worker melakukan klaim menggunakan **atomic Common Table Expression (CTE)** di dalam satu transaksi tunggal. CTE tersebut memilih task berstatus PENDING dengan `FOR UPDATE SKIP LOCKED`, langsung mengubahnya menjadi PROCESSED pada statement yang sama, lalu mengembalikan data task untuk diproses tanpa validasi ulang (*no read-after-read*).

// Di dalam Goroutine background khusus Sync              
func processSyncTask(ctx context.Context, dal \*DAL) error {              
    // Menggunakan Guardrail ExecuteInPool    
    return dal.ExecuteInPool(ctx, func(tx \*ent.Tx) error {              
        // 1\. Klaim task secara atomik dan ubah statusnya menjadi PROCESSED
        // dalam statement yang sama. Worker lain akan melewati baris terkunci.
        task, err := claimOneSyncTaskInTx(ctx, tx)
        if err \!= nil { return err }
        if task == nil { return nil }

        startOfDay := task.TargetDate              
        endOfDay := startOfDay.Add(24 \* time.Hour)

        // 3\. Hitung ulang secara aktual (Idempotent Recompute)              
        // Kita HANYA menghitung status terminal tunggal: CONFIRMED          
        actualCount, err := tx.State.Query().              
            Where(              
                state.JobTypeEQ(state.JobTypeTICK),              
                state.HasInstrumentWith(instrument.IDEQ(task.InstrumentID)),              
                state.TimestampGTE(startOfDay),              
                state.TimestampLT(endOfDay),              
                state.StatusEQ(state.StatusCONFIRMED),              
            ).              
            Count(ctx)              
                      
        if err \!= nil { return err }

        // 4\. Sinkronisasikan hitungan pada CANDLE di hari tersebut          
        // Menggunakan time.Now().UTC() untuk menjaga konsistensi zona waktu.          
        updateQuery := tx.State.Update().              
            Where(              
                state.JobTypeEQ(state.JobTypeCANDLE),              
                state.HasInstrumentWith(instrument.IDEQ(task.InstrumentID)),              
                state.TimestampEQ(startOfDay),              
            ).              
            SetResolvedTickCount(actualCount). // Menggunakan field ResolvedTickCount            
            SetUpdatedAt(time.Now().UTC())

        // Mencegah candle corrupt atau stale jika file mentah ternyata di-reset (akibat demosi).  
        if actualCount \< 24 {              
            updateQuery.SetStatus(state.StatusPENDING)              
        }

        if err := updateQuery.Exec(ctx); err \!= nil { return err }

        // 5\. Hapus (Delete) bersyarat HANYA JIKA status masih PROCESSED.    
        // MITIGASI LOST UPDATE: Jika sewaktu proses 3 dan 4 berjalan ada event TICK    
        // yang melakukan Upsert dan mengubah baris ini kembali ke PENDING (Tahap 1),    
        // maka proses Delete di bawah ini TIDAK AKAN mengeksekusi apapun (mengabaikan hapus).    
        // Sehingga event SyncTask yang telah di-reset ke PENDING tersebut tetap     
        // eksis untuk diproses ulang pada iterasi berikutnya.    
        \_, err \= tx.SyncTask.Delete().    
            Where(    
                synctask.IDEQ(task.ID),    
                synctask.StatusEQ(synctask.StatusPROCESSED),    
            ).    
            Exec(ctx)              
                
        return err    
    })              
}

### **6.2. Overwrite Policy**

Setiap proses ingesti ulang pada status FAILED atau BROKEN wajib melakukan *overwrite* file di R2 untuk memastikan data terbaru yang benar yang tersimpan.

### **6.3. Kriteria Validasi Fisik File Parquet (Syarat Promosi ke CONFIRMED)**

Status CONFIRMED adalah penanda bahwa data sudah dijamin keamanannya secara fisik maupun logikal, dan siap diagregasi menjadi file Candles. Oleh karena itu, sekadar memeriksa keberadaan file di R2 tidak diizinkan. Proses validasi pada fase T-2 atau Backfill menggunakan library Go Parquet Reader wajib mencakup metrik verifikasi berikut:

1. **File Existence & Non-Empty Size:** Memastikan file target tersedia di R2 dan ukuran *bytes*\-nya lebih dari 0 (mencegah *empty shell*). File *Zero-Row Parquet* juga dijamin memiliki *bytes* yang valid karena membawa *header*, *footer*, dan struktur *schema* di dalamnya.  
2. **Parquet Magic Number & Footer Integrity:** Membaca *header* dan *footer* file untuk memastikan formatnya memiliki *magic number* (PAR1) dan metadata bisa di-*parse* oleh library *Parquet Reader* tanpa *error* korupsi data.  
3. **Schema Matching:** Mencocokkan skema *internal file* dengan spesifikasi sistem. Jumlah kolom, nama kolom, dan presisi tipe datanya (Decimal, bigint) harus 100% sama dengan definisi pada **Poin 3.A**.  
4. **Data Continuity & Boundary Check (Context-Aware Validation):** Jika file memiliki baris (row count \> 0), sistem membaca nilai timestamp dari baris pertama dan terakhir. Timestamp tersebut harus berkesinambungan dan berada di dalam batas jendela 1 jam (HH) yang sesuai dengan representasi barisnya pada database states.  
   **Pengecualian Khusus (Zero-Row Context):** Untuk file *Zero-Row Parquet*, nilai row count diizinkan berjumlah 0 **HANYA JIKA** flag IsHoliday pada entitas database terkait bernilai true. Jika file kosong namun IsHoliday bernilai false (indikasi *bug parser* pada file yang sebetulnya harus berisi data), file tersebut wajib ditolak. Jika lolos kondisi konteks ini, sistem hanya akan memvalidasi *Schema Matching* dan *Magic Number* tanpa mengecek batas *timestamp*.
   **Pengecualian Khusus Candle Harian Libur Penuh:** Jika satu hari penuh merupakan hari libur dan file Candles harian berisi 0 row, file Parquet boleh kosong (*zero-row*) selama magic number, footer, dan schema tetap valid. Untuk kasus *zero-row* ini, validasi kelengkapan 19 timeframe akan dilewati (*bypassed*) karena tidak ada baris timeframe yang dapat diverifikasi.

// Di dalam validator, implementasikan parameter konteks stateRecord:        
func validateParquet(ctx context.Context, stateRecord \*ent.State, fileBytes \[\]byte) error {        
    rowCount := readRowCount(fileBytes)        
    if rowCount \== 0 && \!stateRecord.IsHoliday {        
        // File kosong tapi BUKAN holiday yang dikonfirmasi secara sistem \-\> BUG \-\> BROKEN        
        return ErrUnexpectedEmptyFile        
    }        
    // ... lanjutkan eksekusi boundary check & validasi normal        
    return nil        
}

Jika **salah satu** tahapan validasi ini gagal, file tersebut akan langsung ditandai dengan status BROKEN, memicu sistem untuk melakukan proses *re-ingestion* di siklus berikutnya melalui mekanisme *Overwrite Policy*. File baru dinyatakan lulus dan di-*update* statusnya menjadi CONFIRMED jika seluruh tahapan di atas terpenuhi sempurna.

## **7\. Ketersediaan Tinggi, Observability & Toleransi Kesalahan**

Mengingat infrastruktur dikelola mandiri secara penuh menggunakan VPS, PostgreSQL menjadi tulang punggung utama (*source of truth*) untuk manajemen *state* dan *locking*. Untuk mencegah risiko sistematis akibat *Single Point of Failure* (SPOF) yang menyebabkan "Silent Failure", infrastruktur harus dilindungi dengan arsitektur observabilitas mutakhir:

### **7.1. High Availability (HA) & Automated Failover**

* **Replikasi Streaming (Primary-Standby):** VPS Database minimal menggunakan 2 *node* server terpisah (1 Primary, 1 Standby) dengan implementasi *asynchronous/synchronous streaming replication*.  
* **Patroni / repmgr untuk Automated Failover:** Jika server Primary mengalami kegagalan, alat otomatisasi akan langsung mempromosikan server Standby menjadi Primary baru.  
* **Transisi Kunci (Locks Transition):** Karena pg\_try\_advisory\_xact\_lock terikat pada koneksi TCP (yang diakses via Direct Connection), sebuah *failover* akan memutus koneksi dan melepaskan seluruh *lock*. Ini akan dideteksi oleh *Lock Health Monitor* (Goroutine Heartbeat di Poin 4.A) yang kemudian memicu pembatalan (cancel context) pekerjaan ETL secara elegan tanpa *deadlock*.

### **7.2. Observability & Monitoring (Disk / WAL)**

Sistem ETL dengan *throughput* tinggi sangat rentan terhadap penumpukan *Write-Ahead Logging* (WAL).

* **Metrics Monitoring:** Mengimplementasikan **Prometheus & Grafana** menggunakan *node\_exporter* dan *postgres\_exporter*.  
* **Alerting Cepat (Paging):** Peringatan ke Slack/Telegram jika Penggunaan Disk melampaui 80% atau jika *streaming replication* macet.

### **7.3. Distributed Tracing & Application Observability (OpenTelemetry)**

Karena seluruh operasi ETL dieksekusi di latar belakang (*asynchronous goroutines*) yang dilepas oleh Message Consumer atau Fiber, pelacakan *bug*, *latency*, dan identifikasi baris data yang "tersangkut" tidak mungkin dilakukan hanya dengan *print log* biasa. Sistem wajib mengimplementasikan standar instrumentasi **OpenTelemetry (OTel)**.

* **Context Propagation (Silsilah Trace):** Endpoint Fiber maupun Message Consumer wajib mengekstrak TraceID (atau memulainya dari awal) lalu menyuntikkannya ke context.Background(). Context ber-TraceID ini kemudian dilewatkan pada inisialisasi Goroutine, *Lock Claiming*, hingga ke dalam lockCtx dan executeETLLogic(ctx). Jika operasi dibatalkan oleh *Lock Health Monitor*, *span error* akan tercatat pada ID pelacakan yang sama.  
* **Instrumentasi Database (entgo):** Mengaktifkan *built-in OTel integration* (seperti ent/dialect/sql/schema) pada ORM ent. Hal ini memungkinkan sistem memvisualisasikan durasi *query* klaim data (Skip Locked) hingga proses *update state*.  
* **Instrumentasi External (API & Cloudflare R2):** Seluruh HTTP Call ke Dukascopy API dan SDK AWS (untuk R2 Object Storage) harus dibungkus dengan OTel Carrier. Ini memudahkan identifikasi masalah jika *rate limit* Dukascopy tercapai (menghasilkan *span log error*) atau jika latensi R2 Storage sedang tinggi.  
* **Log to Trace Correlation:** Setiap kali status berubah menjadi FAILED atau BROKEN, sebuah *structured log* wajib dicetak dengan menyertakan TraceID. Sehingga, engineer hanya perlu menyalin *TraceID* tersebut dan mencarinya di APM Backend (seperti Jaeger/Tempo) untuk melihat rentetan eksekusi fungsi dari titik awal pemicuan event, penguncian database, hingga titik eksak di mana proses ETL tersebut gagal secara visual.
