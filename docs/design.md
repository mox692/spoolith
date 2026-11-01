# spoolith 設計書: ディスク永続化付きリングバッファ

Status: Draft / 2026-10-31 作成、2026-11-01 改訂

改訂履歴:

- 2026-11-01: ring thread は tokio の `LocalRuntime` を前提にし、`Ring::open` が `(Ring, RingDriver)` を返す形にした（driver は user が `spawn_local` する。§2、§7.2）。flush のトリガは周期 tick から deadline 方式に変えた（§7.1）。flush のリトライと `Failed` 状態、シャットダウンの API も決めた（§13）。
- 2026-11-01: ディスク上の ring（最大 256 GB 程度）はメモリに乗らない前提に変更した。メモリは直近の window だけを持ち、遅れた reader はディスクから読む（cold read、§9.4）。Linux 向けの I/O 最適化（sync_file_range / O_DIRECT / io_uring）を §12 に追加した。確認が取れた前提を §18 に決定事項としてまとめた。

---

## 0. 要点 (TL;DR)

- **スレッド構成**: producer threads →(bounded mpsc)→ **ring thread**（tokio の `LocalRuntime`。current_thread runtime + `LocalSet` でもよい）→(job)→ **I/O backend**（Phase 1 は専用の `std::thread`。Linux では O_DIRECT / io_uring に差し替える）。reader は ring thread 上の local task として動き、必要なら channel で他の runtime にデータを渡す。ライブラリは runtime を作らず、task も spawn しない。`Ring::open` が返す `RingDriver` を、user が local task として spawn する。
- **ring の状態はすべて ring thread が単独で所有する**（`Rc` + `Cell`）。書き込み位置・読み出し位置の更新に lock は不要で、atomic RMW も使わない。
- **2 層構成**: 本体はディスク上の ring（`D·S`、最大 256 GB 程度）。メモリ上の ring（`M·S`、例 1 GiB）はその直近部分だけを持つ window で、書き込みバッファと hot cache を兼ねる。reader は window 内ならメモリから zero-copy で読み、それより古ければディスクから読む（**cold read**）。遅れた reader がいても、writer は止まらずメモリも増えない。
- **永続化は「範囲の貸し出し」で lock-free にする**: ring thread は未永続化の範囲を I/O backend に *貸し出し*、I/O backend は ring のメモリから直接書く（中間バッファへのコピーは無い）。貸し出し中の範囲を writer が上書きしないことは位置の不変条件で保証するので、**Mutex は不要**。
- **差分 flush**: ring は追記のみなので、dirty 領域は常に `[durable, write)` という連続範囲になる。1 回の flush で書くのはこの差分と 4 KiB のヘッダだけで、サイズにも上限を設ける。
- **クラッシュ一貫性**: ヘッダは A/B 2 スロット + CRC。データとヘッダを書いてから fdatasync を 1 回だけ行う。直近 flush 範囲の CRC をヘッダに持たせ、復旧時に検証する。**runway（予約セグメント）** を設けて、書き込み中のデータが「ディスク上で有効とされている領域」を上書きしないようにする。復旧時に読むのは直近の flush 範囲と現在のセグメントだけで、256 GB 全体は読まない。
- **セグメント分割**: ring を固定長のセグメントに分割し、レコードはセグメントを跨がないようにする。tail の前進、zero-copy lease の確認、writer の slow path はすべてセグメント単位で行うので、per-record の hot path は「比較 1 回 + 8B ヘッダ書き込み + memcpy」だけになる。
- **cold read の正しさ**: ディスク上のセグメントは、writer が一周してくると上書きされる。cold read は完了時に「読んだセグメントがまだ tail 以上か」を ring thread で検査し、上書きと競合した可能性のあるデータは捨てる（seqlock と同じ考え方で、lock は要らない）。
- **Linux 本番の推奨**: O_DIRECT（ring メモリから直接 DMA し、page cache を使わない）+ 事前に zero-fill したファイル（または raw block device）。io_uring は I/O backend の中身を差し替える形で段階的に入れる（§12）。

---

## 1. 要件と前提

### 1.1 要件

| # | 要件 |
|---|---|
| R1 | ring buffer のデータを定期的に disk に書き出す（メモリ上のバッファは使ってよい） |
| R2 | 入力は他 thread から channel 経由で届く |
| R3 | どこまで disk に書き出したかをヘッダに保存し、ヘッダも永続化する |
| R4 | 書き込み位置に加えて読み出し位置もトラックする（reader task から読めるようにする） |
| R5 | 読み出しは可能な限り lock を取らない。ring の操作は tokio の local runtime 上の同一 thread で行う |
| R6 | disk への永続化で他の task を block しない |
| R7 | パフォーマンス最優先。不要な memcpy を避け、flush 時間を短く抑える（差分 flush）。書き込み・読み出し位置は lock-free にする |
| R8 | reader が読んだデータは channel で他の runtime に渡せる。CPU 負荷の高い処理は ring に入れない |
| R9 | ディスク上の ring は 256 GB 程度になることがあり、メモリには乗らない。reader は大きく遅延することがある |

### 1.2 前提（確認済み。詳細は §18）

- **A1**: ring が一杯になったら最古のデータを上書きする（ring buffer の semantics）。読み遅れた reader は `Lagged` を受け取る。「読み終わるまで上書きしない」キュー型の reader も option で選べる。
- **A2**: レコードは可変長の bytes で、最大サイズはセグメントサイズ未満。
- **A3**: ディスク上の ring 全体はメモリに乗らない。メモリには直近の window だけを置き、遅れた reader はディスクから読む。
- **A4**: 本番環境は Linux。開発は macOS でも動くようにする（Linux 専用の最適化は cfg と設定で切り替える）。
- **A5**: 電源断まで耐えることを目標とする。
- **A6**: 永続化する読み出し位置は commit（下流での処理が完了した位置）。

### 1.3 Non-goals

- 複数プロセスから同じファイルへの同時アクセス
- ring thread 内での CPU 負荷の高い処理（デコードや集計は下流の runtime で行う）
- レプリケーション

---

## 2. 全体アーキテクチャ

```
 ┌─────────────┐
 │ producer th │──┐  tokio::sync::mpsc (bounded)
 └─────────────┘  │  ※ 満杯なら producer が待つ = backpressure
 ┌─────────────┐  │
 │ producer th │──┤
 └─────────────┘  ▼
 ┌──────────────── Ring thread (LocalRuntime) ──────────────────────────────────────┐
 │                                                                                 │
 │  ingest task ──append──▶ RingCore (Rc, !Send) ◀──read── reader task(s) ───┐     │
 │                          memory window (M·S)                              │     │
 │  flush driver ── FlushJob（範囲の貸し出し + header snapshot）               │     │
 │  reader ──────── ReadJob（cold read: window より古いデータ）                │     │
 │  control task (subscribe / stats / shutdown)                               │     │
 └──────┼──────────────────────────────▲──────────────────────────────────────┼────┘
        ▼                              │ 完了通知                             ▼
 ┌──────────────── I/O backend ─────────────────────────┐          mpsc → 他の runtime
 │ flush:     ring メモリから直接 write → header → sync  │          (CPU heavy な処理)
 │ cold read: ディスク → 読み出し用バッファ              │
 └──────────────────────────┬───────────────────────────┘
                            ▼
     file / block device: [header A][header B][data region: D·S（最大 256 GB 程度）]
```

| コンポーネント | 実体 | 役割 |
|---|---|---|
| producer | 任意の thread / runtime | `RingSender` でレコードを送る |
| ring thread | `std::thread` + tokio の `LocalRuntime`（`Builder::new_current_thread().enable_time().build_local(..)`、worker 1 本）。current_thread runtime + `LocalSet` でもよい。runtime は user が作る | ring の状態を単独所有する。ingest、flush の指揮、reader（hot read と cold read の発行）、control を担当 |
| I/O backend | Phase 1: flush 用の `std::thread` 1 本（`ring-flush`。`Ring::open` が起動し、ring が close したら join する）+ cold read 用の `std::thread` 数本。Linux では io_uring に差し替えられる（§12.4） | blocking I/O を ring thread の外で行う。flush と cold read は別の queue にして、cold read が詰まっても flush が待たされないようにする |
| consumer | 任意の runtime | reader から channel 経由でデータを受け取り、CPU heavy な処理を行う |

ring thread の起動イメージ:

```rust
std::thread::Builder::new().name("ring".into()).spawn(move || -> io::Result<()> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_time()                                  // RingDriver は timer を使う
        .build_local(tokio::runtime::LocalOptions::default())?;
    // 復旧と I/O thread の起動。file I/O で block するので、runtime が動き出す前に呼ぶ
    let (ring, driver) = Ring::open(cfg)?;
    rt.block_on(async move {
        let driver = tokio::task::spawn_local(driver.run());     // flush を担当する
        let mut batch = Vec::with_capacity(256);
        while rx.recv_many(&mut batch, 256).await > 0 {
            for msg in batch.drain(..) {
                ring.append(&msg).await.map_err(io::Error::other)?;  // runway が一杯のときだけ待つ
            }
        }
        ring.shutdown().await?;   // 残りを flush し、CLEAN_SHUTDOWN を書き、I/O thread を join する
        driver.await.unwrap()
    })
})?;
```

- `Ring` は `Rc` を包んだ `!Send` の handle で、clone して複数の local task（ingest、reader、監視など）で共有できる。最後の handle が drop されると、`shutdown` と同じ手順で close する。
- `RingDriver` は tokio-postgres の `Connection` と同じ考え方の future で、ライブラリ自身は task を spawn しない。どの runtime（`LocalRuntime` か `LocalSet`）で動かすかは user が決める。driver が poll されないと flush が起きないので、close 前に drop されたら ring を `Failed` にする（§13）。
- 入力 channel と ingest loop は、現時点では user 側のコードで、`Ring::append` を呼ぶ。§6.1–6.2 の `Msg` / barrier をデータ channel に流す方式をライブラリで提供するかは未定。
- 動く例: `examples/local_runtime.rs`（CAN 風のフレームを producer thread から送る）、`tests/local_runtime.rs`。

---

## 3. 位置モデル

### 3.1 論理位置

- 位置はすべて **単調増加する論理バイトオフセット `u64`** で表す（wrap しない）。1 GB/s で書き続けても u64 が溢れるまで約 584 年かかる。
- 物理位置はメモリとディスクで別々に求める（メモリは `pos & (M·S − 1)`、ディスクは §5.1）。
- この方式の利点:
  - 比較が `a < b` だけで済み、ABA 問題も起きない
  - reader の lag は `write - reader.pos` で求まる
  - 位置をそのままレコード ID / offset として使える（commit に使う）

### 3.2 パラメータ

| 記号 | 意味 | 制約 |
|---|---|---|
| `S` | segment size | 2 の冪、4 KiB の倍数 |
| `M` | memory segments（メモリ window のセグメント数） | 2 の冪。ヘッダには保存せず、再起動時に変えてよい |
| `D` | disk segments（ディスク上の ring のセグメント数） | 任意の整数（2 の冪でなくてよい）。作成時に決めてヘッダに保存する |
| `R` | runway segments（未永続化データの最大量、セグメント単位） | `1 ≤ R ≤ M − 1` |
| `H` | `D − 1 − R`（ディスク上で読める履歴として保証するセグメント数） | `H ≥ M − 1` |

例: `S = 1 MiB`、`M = 1024`（メモリ 1 GiB）、`D = 262 144`（ディスク 256 GiB）、`R = 64`。

- `D` を 2 の冪に限らないのは、ディスク容量をそのまま使えるようにするため。ディスク上の物理位置の計算（剰余）は flush の区間や cold read の job ごとに 1 回だけ行い、レコードごとには行わないので、コストは無視できる。
- `R ≤ M − 1` はメモリ安全性、`H = D − 1 − R` はクラッシュ一貫性のための制約（§8.2）。

### 3.3 位置の種類

| 名前 | 意味 | 所有者 / 型 |
|---|---|---|
| `write` | 次に書き込む位置 | ring thread / `Cell<u64>` |
| `durable` | sync まで完了した位置（= ディスク上のヘッダの `head`） | ring thread / `Cell<u64>`（完了通知を受けて更新） |
| `inflight_end` | I/O backend に貸し出し中の範囲の終端 | ring thread / `Cell<Option<u64>>` |
| `tail` | 読める最古の位置（ディスク上。必ずセグメント境界） | ring thread / `Cell<u64>` |
| `mem_low` | メモリ window の下限（セグメント境界）。これより古い位置は cold read になる | ring thread / `Cell<u64>`（セグメント移動時に更新） |
| `limit` | fast path で書いてよい上限（キャッシュ値） | ring thread / `Cell<u64>` |
| `reader[i].pos` | 各 reader の読み出し位置 | ring thread / `Cell<u64>` |
| `reader[i].committed` | 下流での処理が完了した位置（永続化の対象） | 任意 thread / `AtomicU64` |

```
 論理位置 →
 tail                          mem_low               durable   inflight_end     write    durable+R·S
  │◀──── cold（ディスクから読む）────▶│◀──────────── hot（メモリから読む）──────────────▶│
  │                                   │                    │◀─ I/O 中 ─▶│◀─ 未 flush ─▶│
  │                                   │                    │◀────────── runway (≤ R·S) ─────────▶│
  │◀──────────────────── Visibility::Written で読める範囲 ──────────────────────────────▶│
  │◀───────────── Visibility::Durable で読める範囲 ─────────▶│
      最大 ≒ H·S（例 256 GiB）          最大 (M − 1)·S（例 1 GiB）
```

### 3.4 tail と mem_low の規則

```
tail(p) = max(tail_floor, seg_start(p) − H·S)                    // saturating
mem_low = max(mem_valid_low, seg_start(write) − (M − 1)·S)        // saturating
```

**tail**

- 位置 `p` を含むセグメントの先頭から H セグメント分を履歴として扱う。値はセグメント境界になるので、必ずレコード境界でもある。
- writer 側にもディスクのヘッダにも同じ規則を当てはめる。writer 側は `seg_start(p)` を「writer がいるセグメントの先頭」に、ヘッダは「`head` の直前のバイトを含むセグメントの先頭」（`last_seg_start(head) = seg_start(head − 1)`）に置き換える。
  - レコードがセグメントをちょうど埋めると `write` は次のセグメントの先頭に一致するが、writer はまだそのセグメントに入っていない（§6.3）。`seg_start(head)` を使うと、`head` がこの位置のときにヘッダの tail が writer 側より 1 セグメント進んでしまう。
  - 上の置き換えにより、ヘッダの tail は writer 側の tail 以下になる。つまり **reader が見えていた durable なデータは、再起動しても消えない**。
- その代わり、ディスクのうち R セグメント分は読めない（runway として使う）。実効的な履歴は `H·S`。
- `tail_floor` は復旧時にヘッダから読んだ tail（単調性を保つため）。

**mem_low**

- writer がセグメント g に入ると、メモリスロット `g mod M` にあった論理セグメント `g − M` は消える。したがってメモリにあるのはセグメント `g − M + 1 .. g` の M 個で、その先頭が `mem_low`。
- `mem_valid_low` は、復旧直後にメモリに読み込んであるデータの下限（§8.3）。writer が M セグメント進めば効かなくなる。
- `R ≤ M − 1` と I2 から `durable ≥ write − R·S ≥ seg_start(write) − (M − 1)·S` なので、**`mem_low ≤ durable` が常に成り立つ**。つまり、メモリから消えるデータは必ずディスク上で durable になっている。cold read が読む範囲 `[tail, mem_low)` は、常に durable で、かつ完結したセグメントだけからなる。
- `H ≥ M − 1` なので `tail ≤ mem_low` も常に成り立つ。

### 3.5 不変条件

| # | 不変条件 |
|---|---|
| I1 | `tail ≤ mem_low ≤ durable ≤ inflight_end ≤ write` |
| I2 (runway) | `write − durable ≤ R·S`：未永続化データは常に runway に収まる |
| I3 | writer がセグメント g に入るとき、メモリスロット `g mod M` の lease 数は 0 で、すべての blocking reader の `pos` が `tail(g·S)` 以上 |
| I4 | ヘッダの `head` / `prev_head` は常にレコード境界 |
| I5 | in-flight な FlushJob は同時に高々 1 つ |
| I6 (cold read) | cold read の結果を使ってよいのは、ring thread が完了を受け取った時点で「読んだセグメントの先頭 ≥ tail」のときだけ |

I2 だけで「I/O backend が読むメモリを writer が上書きしない」（メモリ安全性、M についての議論）と「flush がディスク上の有効データを壊さない」（クラッシュ一貫性、D についての議論）の両方を保証できる（§8.2）。I6 は cold read が writer による上書きと競合しないことを保証する（§9.4）。

---

## 4. メモリレイアウトとレコードフォーマット

### 4.1 RingMemory

```rust
/// M·S バイト、2 MiB aligned。ring thread / I/O backend / SharedSlice 保持者で共有する。
pub(crate) struct RingMemory { ptr: NonNull<u8>, len: usize }
unsafe impl Send for RingMemory {}
unsafe impl Sync for RingMemory {}
// SAFETY: 書き込み範囲と、他 thread が参照する範囲が disjoint であること（§3.5 の不変条件）に依存する。
```

- 確保: Linux では anonymous mmap + `MADV_HUGEPAGE`（THP）で、2 MiB 境界に揃える。GB 単位の領域に memcpy し続けても TLB miss が増えにくく、O_DIRECT のページ pin も安くなる。その他の OS では `std::alloc`（align 4096）を使う。
- 起動時に全ページを touch して prefault しておき（Linux なら `MAP_POPULATE` でもよい）、hot path で page fault が起きないようにする。必要なら `mlock` も使う。
- 整列済みなので、O_DIRECT の書き込み元や io_uring の fixed buffer としてそのまま使える（§12）。
- **バッファ全体を指す `&mut [u8]` は決して作らない。** 他 thread が一部を `&[u8]` で参照しているので、aliasing UB になる。書き込みは、書く範囲だけに `ptr::copy_nonoverlapping` で行う。
- `Arc<RingMemory>` として共有する。

### 4.2 Frame

```
 0         4         8                         8+len     align 8
 ┌─────────┬─────────┬─────────────────────────┬────────┐
 │ len u32 │ crc u32 │ payload (len bytes)     │ pad    │
 └─────────┴─────────┴─────────────────────────┴────────┘
```

- frame は 8 byte 境界に整列する。payload も 8B aligned になるので、consumer 側で zero-copy デコードしやすい。
- `len == u32::MAX` は **セグメント終端マーカー** を表し、reader はそこから次のセグメント先頭へジャンプする。整列により残り領域は常に 0 か 8 以上なので、マーカーは必ず入る。
- `crc` は `ChecksumMode::PerRecord` のときだけ使う（§8.4）。デフォルトは 0。復旧時の検証はヘッダの `range_crc` で行い、hot path から CRC 計算を外す。
- レコードはセグメントを跨がないので、payload の最大長は `S − 8`。padding による無駄はセグメントあたり最大でもレコード 1 個分。
- メモリ上でもディスク上でも同じフォーマットなので、hot read と cold read で parse のコードを共有できる。
- 固定長レコードしか扱わないなら、frame ヘッダを省いた特殊化も可能（将来の拡張）。

---

## 5. ディスクレイアウトとヘッダ

### 5.1 ファイルレイアウト

```
 file offset
 0x0000   Header slot A (4 KiB)
 0x1000   Header slot B (4 KiB)
 0x2000   Data region (D·S bytes、例: 256 GiB)

 disk_off(pos) = 0x2000 + (seg(pos) mod D)·S + (pos & (S − 1))
```

- 剰余はレコードごとではなく、flush の区間と cold read の job ごとに 1 回だけ計算する。
- 通常のファイルの代わりに、専用のパーティションや block device をそのまま使ってもよい（`path` に block device を指定する）。ファイルシステムのメタデータが一切絡まないので、最も挙動が読みやすい（§12.3）。
- ファイルサイズは作成時に固定し、運用中は変えない。fdatasync がファイルサイズの更新（メタデータの journal 書き込み）を伴わないようにするため。

**事前確保**（256 GB 級では方式の選択が効いてくる）:

| 方式 | 作成にかかる時間 | 運用中の挙動 |
|---|---|---|
| `ZeroFill`: 実際に 0 を書く（デフォルト） | 長い（256 GB ÷ 書き込み帯域。2 GB/s で 2 分強） | 最初の一周から、すべて既に書かれた extent への上書きになる。fdatasync はメタデータ更新を伴わず、O_DIRECT + DSYNC の FUA 最適化も効く（§12.3） |
| `Fallocate`: fallocate のみ | ほぼ一瞬 | 最初の一周は unwritten extent への書き込みになり、extent の変換が journal に乗るので fdatasync が重い。二周目以降は ZeroFill と同じ |
| `WriteZeroes`: `FALLOC_FL_WRITE_ZEROES`（Linux 6.17〜、ext4 などの FS 側の対応も必要、§12.3） | device が write zeroes を高速に処理できれば短い | ZeroFill と同じ |
| block device | 不要 | 常に一定 |

- 作成時間を短くしたい場合は `Fallocate` を選べるようにする。macOS では `F_PREALLOCATE` を使う。
- どの方式でも、ヘッダが有効と示す `[tail, head)` 以外は読まないので、未初期化の領域を誤って解釈することはない。

### 5.2 ヘッダ

```rust
/// 4096 byte。メモリ表現を transmute せず、little endian で明示的に encode/decode する。
struct Header {
    magic: [u8; 8],            // b"RBUFV2\0\0"
    version: u32,
    header_size: u32,          // 4096
    instance_id: [u8; 16],     // 作成時に乱数で決める。ファイルの取り違え検出用
    seq: u64,                  // ヘッダの世代。書き込み先は slot = seq % 2
    segment_size: u32,         // S（作成時に固定）
    disk_segments: u32,        // D（作成時に固定）
    runway_segments: u32,      // R
    flags: u32,                // CLEAN_SHUTDOWN, CHECKSUM_PER_RECORD, ...
    head: u64,                 // 永続化済みの書き込み位置（レコード境界）
    prev_head: u64,            // この flush の開始位置（= 直前のヘッダの head）
    tail: u64,                 // 有効な最古の位置（単調増加）
    range_crc: u32,            // data[prev_head, head) の crc32c
    reader_count: u32,
    readers: [ReaderSlot; MAX_READERS],   // { name: [u8; 32], committed: u64 } × 64 個 ≒ 2.5 KiB
    header_crc: u32,           // ここまでの crc32c
}
```

- **A/B 2 スロット方式**: `seq % 2` のスロットに交互に書き込む。ヘッダの書き込みが途中で切れて壊れても、もう片方のスロットが残る。
- open 時は両スロットを読み、magic / version / `header_crc` が正しいもののうち `seq` が最大のものを採用する。次に書くヘッダは `seq + 1` にする。これで採用しなかった側（古いか壊れたスロット）が上書きされる。
- M はヘッダに保存しない。メモリ量は再起動時に変えてよい。
- R を変える場合は、データを書く前にヘッダだけの flush を 1 回行い、新しい R で計算した tail を確定させる。R を大きくすると、古い R で書かれたヘッダが有効とみなしている範囲を新しい flush が上書きしうるため。

---

## 6. 書き込みパス (ingest)

### 6.1 入力 channel

- `tokio::sync::mpsc::channel::<Msg>(capacity)` を bounded で使う。tokio の mpsc は lock-free（block linked list + AtomicWaker）。sync thread からは `blocking_send` で送れる。
- ring への memcpy 1 回は、channel を経由する以上避けられない最小コスト。それ以外のコピーはしない。

```rust
pub enum Msg {
    One(Bytes),
    /// 小さいレコードを大量に送るなら、producer 側で 1 つのバッファに詰めて送る。
    /// alloc と channel 操作が償却される
    Batch { buf: Vec<u8>, ends: Vec<u32> },
    /// flush barrier。データと同じ channel を通すことで、同じ producer が先に送ったレコードとの順序が保証される
    Barrier(oneshot::Sender<io::Result<u64>>),
}
```

> flush barrier を control channel に流すと、データ channel との間で順序が保証されない。「送ってから `flush()`」が正しく動くように、barrier は **データ channel に流す**。

- 必要に応じて、producer → ring で送った `Vec` を return channel で返却して再利用する（producer 側で alloc し ring thread で free するという cross-thread free を避ける）。

### 6.2 ingest loop

```rust
async fn ingest_loop(core: Rc<RingCore>, mut rx: mpsc::Receiver<Msg>) {
    let mut batch = Vec::with_capacity(core.cfg.ingest_batch);
    while rx.recv_many(&mut batch, core.cfg.ingest_batch).await > 0 {
        for msg in batch.drain(..) {
            for rec in msg.records() {
                loop {
                    match core.try_append(rec) {
                        Ok(_pos) => break,
                        // wait_stall は先に after_batch() を呼ぶ（reader の wake や flush の kick）。
                        // そうしないと blocking reader が永久に進まず deadlock する
                        Err(stall) => core.wait_stall(stall).await,
                    }
                }
            }
            if let Msg::Barrier(tx) = msg { core.add_flush_waiter(core.write(), tx) }
        }
        // write の atomic mirror の公開、待機中 reader の wake、flush 閾値の判定はバッチごとに 1 回だけ行う
        core.after_batch();
    }
    // すべての producer が drop した → shutdown シーケンスへ
}
```

### 6.3 fast path / slow path

```rust
#[inline]
fn try_append(&self, payload: &[u8]) -> Result<u64, Stall> {
    let need = align8(FRAME_HDR + payload.len()) as u64;
    if self.write.get() + need > self.limit.get() {
        self.slow_path(need)?;              // セグメントの移動、または Stall
    }
    let pos = self.write.get();
    unsafe { self.mem.write_frame(pos, payload) };   // 8B ヘッダ + memcpy
    self.write.set(pos + need);
    Ok(pos)
}
```

- writer がいるセグメントの終端 `seg_end` を明示的に持つ。`seg_end(write)` から求めると、レコードがセグメントをちょうど埋めたとき（`write` が次のセグメントの先頭に一致）に「次のセグメントに入った」と区別できない。`write == seg_end` は「使い切ったが、次のセグメントにはまだ入っていない」状態で、次の append が slow path で入る。
- `limit = min(seg_end, durable + R·S)` をキャッシュしておくので、per-record のチェックは比較 1 回で済む。
- `slow_path` が呼ばれるのはセグメントあたり高々数回:
  1. 現在のセグメントに収まる場合（`write + need ≤ seg_end`）: `write + need > durable + R·S` なら `Stall::Durability` を返す（flush を即座に kick する）
  2. 収まらない場合は、次のセグメント g に入れるかを **移動前に** 確認する:
     - `seg_start(g) ≤ durable + R·S` でなければ `Stall::Durability`（移動した時点で I2 が破れないように、移動後の位置で判定する）
     - `leases[g mod M] == 0`（Acquire load）でなければ `Stall::Lease`
     - `tail(seg_start(g)) ≤ min(blocking reader の pos)` でなければ `Stall::Reader`
     - すべて満たしたら、現在位置に終端マーカーを書き（`write < seg_end` のとき）、`write = seg_start(g)` にして `tail` と `mem_low` を更新する
     - 移動した後で `seg_start(g) + need > durable + R·S` なら `Stall::Durability`。移動の条件を `seg_start(g) + need ≤ durable + R·S` にすると、R = 1 で `write == durable` のときに flush するものが無く、永久に進めなくなる。先に移動しておけば、終端マーカーまでを flush した時点で `durable = seg_start(g)` となり、必ず収まる。
  3. `limit` を再計算する
- `limit` は flush 完了時（`durable` が進んだとき）にも再計算する。
- メモリスロットの再利用には、reader がそのセグメントを読み終えていることを要求しない。中身はディスク上で durable になっているので（§3.4）、遅れた reader は cold read に切り替わるだけ。writer を止めうるのは lease と、ディスクを一周分遅れた blocking reader だけ。

### 6.4 Stall の待ち方（lock-free）

| Stall | 待つ対象 | 起こし方 |
|---|---|---|
| `Durability` | flush の完了 | flush driver が完了処理の中で local waker を wake する |
| `Lease(slot)` | 他 thread で `SharedSlice` が drop されること | `AtomicWaker`。`fetch_sub` で 1 → 0 になったら wake する（writer は waker を登録した **後に** lease 数をもう一度確認し、wake の取りこぼしを防ぐ） |
| `Reader` | blocking reader が前進すること（ディスクを一周分遅れたときだけ起きる） | reader が pos を更新したときに local waker を wake する |

Stall している間は ingest が止まるので channel が詰まり、producer は `send().await` で待つことになる。これが自然な backpressure になる。
（option: `OnFull::DropNewest` を選ぶと、Stall 時に新着レコードを捨ててカウンタだけ増やす。古いデータを捨てて逃げることはメモリ安全性の制約上できない。）

---

## 7. 永続化パス (flush)

### 7.1 トリガ

- **時間**: 未 flush のデータが `flush_interval`（例: 5〜50 ms）より長く残らないようにする。周期 tick ではなく deadline 方式で、データが無い間は wake しない。deadline は「最初の未 flush データが来た時刻 + `flush_interval`」、flush 完了後は「その flush の開始時刻 + `flush_interval`」（flush 中に来たデータは開始時刻より後に来ているので、上限は守られる）。
- **量**: `write − durable ≥ flush_threshold` になったら、deadline を待たずに flush する。`flush_threshold = 0` なら、データが来るたびに flush する（in-flight 中に来た分は次の 1 回にまとめるので、ディスクの速度で group commit になる）。
- **明示**: `Ring::flush()`（barrier）。呼んだ時点の `write` が durable になったら resolve する（future を最初に poll した時点ではない）。
- **急ぎ**: runway が一杯で append が止まったとき（Stall）と shutdown のとき。すぐに flush する。
- **commit のみ**（未実装）: reader の committed だけが変わった場合は、`flush_interval` ごとに header-only flush（データなし）を行う。
- （option）**write-behind**: flush を待たずにデータだけ先に書き出しておく（§12.2）。

### 7.2 flush driver task

`RingDriver::run` の骨格:

```rust
loop {
    kicked = false;
    match state {
        Failed => return Err(failure),
        Closing if core.clean_shutdown_written() => { finish_close(); return Ok(()) }  // I/O thread を join
        _ => {}
    }
    let pending = write - durable;
    if pending == 0 { deadline = None } else if deadline.is_none() { deadline = Some(now + flush_interval) }
    let due = state == Closing || flush_target > durable                   // 急ぎ・明示
        || pending > 0 && (pending >= flush_threshold || deadline <= now); // 量・時間
    if due && !backoff {
        if let Some(job) = core.prepare_flush() {
            kick_at = u64::MAX;                       // flush 中は append から kick しない
            let res = io.submit_flush(job).await;     // oneshot を待つ。その間も ingest と reader は動き続ける
            match core.complete_flush(res) {          // durable の更新、limit の再計算
                Ok(_) => deadline = Some(now + flush_interval),
                Err(e) if failures > max_flush_retries => { fail(e); return Err(..) }
                Err(_) => retry_at = Some(now + flush_interval),
            }
            waiters.wake_all();                       // Stall した append、barrier、shutdown
            continue;                                 // 溜まっていれば続けて flush する（group commit）
        }
    }
    // 次に起きる条件を置いてから待つ
    kick_at = if backoff { u64::MAX } else if pending == 0 { durable + 1 } else { durable + flush_threshold };
    wait(kicked || sleep_until(if backoff { retry_at } else { deadline })).await;
}
```

- **append 側の判定は比較 1 回**: driver は待つ前に `kick_at` を置き、append は `write >= kick_at` のときだけ driver を wake する。データが無ければ `durable + 1`（最初の 1 件で deadline を始めるため）、あれば `durable + flush_threshold`、flush 中とリトライ待ちの間は `u64::MAX`。kick は `kick_at = u64::MAX` にするので、wake は 1 回の待ちにつき高々 1 回。
- **barrier / Stall / shutdown** は `flush_target` を上げるか state を変えてから kick する。driver はループの先頭で全条件を評価し直すので、待っている間に来た kick を取りこぼさない。
- **wake の仕組み**: `tokio::sync::Notify` は使わない。driver の waker は `Cell<Option<Waker>>` + `kicked` フラグ、flush の完了を待つ task は ring thread 内の waker list で起こす（§7.4）。

`prepare_flush` の中身:

```
start  = durable
end    = min(write, seg_start(start) + max_flush_bytes)   // max_flush_bytes は S の倍数
         // write で切るか、セグメント境界で切る → どちらでもレコード境界になる (I4)
pieces = [start, end) を「メモリ上でもディスク上でも連続な区間」に分けたもの
         // メモリは M·S ごと、ディスクは D·S ごとに wrap する。区間長 < M·S ≤ D·S なので
         // 切れ目は高々 2 つ → 最大 3 区間（D が M の倍数なら両者の wrap が一致するので最大 2 区間）
header = Header { seq: next_seq, head: end, prev_head: start,
                  tail: max(prev_tail, tail(end)), readers: commit の snapshot, .. }
inflight_end = Some(end)
```

### 7.3 I/O backend（Phase 1: thread）

```rust
fn flush_thread(storage: impl Storage, mem: Arc<RingMemory>, rx: std::sync::mpsc::Receiver<FlushJob>) {
    let mut hdr_buf = AlignedBuf::new(4096);                // 再利用する
    while let Ok(mut job) = rx.recv() {
        let res = (|| {
            let mut crc = 0u32;
            for p in job.pieces() {                         // 最大 3 区間
                // SAFETY: [durable, inflight_end) は貸し出し中で、writer は触らない (I2)
                let src = unsafe { mem.slice(p.mem_off, p.len) };   // ring メモリを直接参照する（コピーなし）
                storage.write_at(src, p.disk_off)?;
                crc = crc32c::crc32c_append(crc, src);
            }
            job.header.range_crc = crc;
            job.header.encode_into(&mut hdr_buf);
            storage.write_at(&hdr_buf, slot_offset(job.header.seq))?;
            storage.sync()?;                                 // fdatasync は 1 回だけ
            Ok(job.header.head)
        })();
        let _ = job.done.send(res);                          // tokio oneshot で ring thread を wake する
    }
}
```

- **`tokio::fs` は使わない。** 内部で spawn_blocking を使い、内部バッファへのコピーも発生するため。
- **`spawn_blocking` ではなく専用 thread にする。** fd を保持し続けられ、job ごとの thread pool への handoff もなく、処理順序も自明になる。CPU affinity を設定することもできる。
- cold read 用の thread（数本）は flush 用とは別に持ち、`crossbeam-channel` で ReadJob を配る（§9.4）。cold read がディスクの帯域を使っていても、flush の job は待たされない。
- I/O は job 単位の `IoBackend` trait の裏に隠す（FlushJob / ReadJob を受け取り、完了を通知する）。実装は次の 3 つ:
  - `ThreadBackend`: pwrite / pread / fdatasync。全 OS で動く
  - `UringBackend`: Linux のみ（§12.4）
  - `SimBackend`: テスト用で、クラッシュと遅延を注入できる
- pwrite / pread / fdatasync / fcntl / sync_file_range には `rustix` を使う。O_DIRECT のときの整列と bounce buffer の扱いは §12.3。

### 7.4 なぜ Mutex が要らないか

共有されうるものと、それぞれの解決方法:

1. **ring メモリ（writer と I/O backend の間）**: 範囲を貸し出す。I/O backend が読むのは `[durable, inflight_end)` だけで、writer が書くのは `write` より後ろと、新しく入るセグメントだけ。I2 により、この 2 つは交わらない（§8.2）。happens-before は、job の送信（channel）と完了の oneshot が release/acquire になることで成立する。
2. **ヘッダ**: ring thread が値として snapshot を作り、job に move する。共有された可変のヘッダは存在しない。
3. **位置**: ring thread の `Cell`。他 thread へは `AtomicU64` の mirror を Release store で公開する（観測専用）。
4. **commit**: 任意の thread から `AtomicU64::fetch_max` で更新し、ring thread が snapshot 時に Acquire load する。
5. **lease**: `AtomicU32` + `AtomicWaker` で管理する。
6. **cold read のバッファ**: ReadJob に move して I/O backend へ渡し、完了通知で戻ってくる。同時に 2 者が触ることはない。

→ data path 上の Mutex は **ゼロ**。
（`tokio::sync::Notify` は内部で mutex を使うので hot path では使わない。ring thread 内の wake には、single-thread 用の `Cell<Option<Waker>>` を自作して使う。）

### 7.5 flush 時間を抑える工夫

- **差分のみ書く**: dirty 領域は連続範囲。ヘッダは 4 KiB を 1 回書くだけ。
- **上限を設ける**: 1 回の flush は `max_flush_bytes` までなので、1 flush の所要時間の上限を見積もれる（`max_flush_bytes / 書き込み帯域 + sync の時間`）。
- **group commit**: in-flight 中に溜まった分は次の 1 回にまとめて書く。fsync の回数がディスクの速度に自動的に追従する。
- **fdatasync は 1 flush につき 1 回**（2 回にしない理由は §8.1）。
- **ファイルサイズを固定し、事前に確保する**ので、fdatasync がメタデータ更新を伴わない（§5.1）。
- **flush と cold read の queue を分ける**ので、遅れた reader がいても flush が待たされない。
- Linux ではさらに write-behind、O_DIRECT、io_uring で削れる（§12）。

### 7.6 SyncMode

```rust
pub enum SyncMode {
    /// write のみ。buffered なら page cache まで、O_DIRECT なら device まで届く。
    /// process crash には耐えるが、OS crash / 電源断には耐えない。最速
    None,
    /// fdatasync。推奨デフォルト
    Data,
    /// fsync。macOS では F_FULLFSYNC
    Full,
    /// (Linux) write ごとに RWF_DSYNC を付ける。FUA が効く device では Data より速いことがある（§12.3）
    Dsync,
}
```

- 守りたいのが process crash だけなら、`None` で十分。page cache は process が死んでも残る。
- **macOS の注意点**: `fsync(2)` はドライブのキャッシュまでは flush しない。また Rust std の `File::sync_data()` は Apple 環境では `F_FULLFSYNC` を使うので非常に重い。開発機でベンチを取ると Linux と桁が違う結果になるので注意する。dev 用に `F_BARRIERFSYNC` を選べるようにしてもよい。

---

## 8. クラッシュ一貫性と復旧

### 8.1 書き込み順序

flush n の手順:

1. data `[h_{n−1}, h_n)` を write する
2. header n を write する（slot `n % 2`、`prev_head = h_{n−1}`、`range_crc` 付き）
3. fdatasync する
4. ring thread に完了を通知し、`durable = h_n` にする

fdatasync が 1 回なので、1 と 2 のどちらが先に永続化されるかは保証されない。header n だけが残って data n が欠けることがありうるが、これは `range_crc` で検出して `prev_head` にフォールバックする。`h_{n−1}` までは flush n−1 の fdatasync が完了している（I5 により in-flight は 1 つだけ）ので、フォールバック先は安全。
（data → fdatasync → header → fdatasync の 2 回方式にすれば `range_crc` は不要になるが、fsync の回数が 2 倍になる。Linux では同じ順序を DSYNC write で安く実現できる場合がある、§12.3。）

### 8.2 runway が保証すること

**主張 1（クラッシュ一貫性、D についての議論）**: どの時点でクラッシュしても、ディスク上の採用されうるヘッダ（n または n−1）が有効とする範囲 `[tail(h), h)` は、flush によって上書きされていない。

証明の概略:

- flush n が書くのは `[h_{n−1}, h_n)` で、I2 より `h_n − h_{n−1} ≤ R·S`。よって触れるのは論理セグメント `k .. k+R`（`k = seg(h_{n−1})`）だけ。
- ディスク上で同じ場所にあった旧データは、論理セグメント `k−D .. k+R−D = k−H−1`。
- 一方、ヘッダ n−1 の有効範囲はセグメント `k−H` 以降。したがって両者は交わらない。
- セグメント k の中の `[seg_start(k), h_{n−1})` には触れない（buffered ならバイト単位で書くので、そもそも触れない。O_DIRECT でブロック境界に切り下げて書く場合も、同じ内容を書き直すだけなので壊れない、§12.3）。
- ヘッダ n は `h_n ≥ h_{n−1}` なので有効範囲はさらに新しく、同じ議論が成り立つ。

**主張 2（メモリ安全性、M についての議論）**: writer が論理セグメント j に入るとき、slow path の確認により `durable ≥ seg_start(j) − R·S` なので、in-flight の範囲は論理セグメント `j−R` 以降に収まる。j とメモリ上で衝突するのは `j−M` だが、`M > R` なのでここには含まれない。

**主張 3（cold read）**: §9.4。

### 8.3 復旧手順

1. 両スロットを読み、magic / version / `header_crc` が正しいもののうち `seq` が最大のものを採用する（どちらも無ければ新規ファイルとして扱うか、エラーにする）。
2. data `[prev_head, head)` を読み、`range_crc` を検証する。不一致なら `head = prev_head` にする（失った分をログと metrics に出す）。
   - `CLEAN_SHUTDOWN` フラグがあっても検証は省略しない。読むのは高々 R·S なので、省略しても得るものが小さい。フラグを data と同じ flush で立てると、sync が 1 回なのでフラグ付きのヘッダだけが残りうる。省略するなら、フラグはヘッダだけの flush で立てる必要がある（§13）。
   - 検証には ring メモリではなく一時バッファを使う。M を前回の R 以下に縮めて再起動しても検証できる。
   - `head − prev_head` が、ヘッダに記録された R の `R·S` を超えていれば、ヘッダが壊れているとみなす。
3. `tail = min(header.tail, head)` とする（`tail_floor` にもこの値を使う）。
4. `head` を含むセグメントの `[seg_start(head), head)` をメモリスロットに読み込み、`mem_valid_low = max(tail, seg_start(head))` とする。
   - writer はこのセグメントの続きに追記するので、ここは必ず読む。O_DIRECT でブロックを書き直すときにも、メモリに中身が必要になる（§12.3）。
   - option `warm_segments = W`: 直前の W セグメント（`W ≤ M − 1`）も読み込み、その分 `mem_valid_low` を下げる。再起動直後の reader がすぐ hot に乗れる。
   - **256 GB 全体は読まない。** それより古いデータは cold read で必要になったときに読む。
5. `write = durable = head`、`next_seq = 採用したヘッダの seq + 1` とする。
6. reader の committed を `[tail, head]` に clamp して復元する。

復旧時間はおよそ `(max_flush_bytes + (W + 1)·S) / 読み込み帯域` で、ディスク容量 D·S に依存しない。

フォールバック後に書いた header n+1 が永続化される前にもう一度クラッシュしても、header n が再び採用され、`range_crc` が不一致になって `prev_head` に戻る。結果は一貫する。

### 8.4 per-record CRC（option）

ビット腐敗を検出したい場合や、`range_crc` が不一致のときに `prev_head` より先をレコード単位で救済したい場合に使う。CRC の初期値に論理位置を混ぜるので、前の周回の残骸を有効なレコードと誤認しない。cold read の多重の防御にもなる（§9.4）。hot path のコストが増えるので、デフォルトは off。

---

## 9. 読み出しパス

### 9.1 reader の状態

```rust
struct ReaderState {                 // RingCore.readers: RefCell<Slab<ReaderState>>（ring thread 専用）
    pos: Cell<u64>,
    kind: ReaderKind,                // Lossy | Blocking
    visibility: Visibility,          // Written | Durable
    cold: RefCell<Option<ColdCursor>>,   // cold read 中だけ Some（§9.4）
    waker: Cell<Option<Waker>>,
    slot: usize,                     // Shared.commits[slot]（永続化される commit 位置）
}
```

- `pos` は ring thread の `Cell` なので、読み出し位置の更新に lock も atomic も要らない。
- 外部から観測できるように、バッチごとに `Shared.reader_pos[slot]`（`AtomicU64`、Relaxed）へ mirror する（lag の metrics 用）。
- `RefCell` の borrow は await を跨いで保持しない。`Waker::wake` は task をスケジュールするだけで同期実行しないので、re-entrancy は起きない。

### 9.2 kind と visibility

| | 挙動 |
|---|---|
| `ReaderKind::Lossy`（デフォルト） | writer を止めない。`pos < tail` になったら次の read で `Err(Lagged { skipped_bytes })` を返し、`pos = tail` にする（tokio broadcast と同じ方式）。tail はレコード境界なので、そこから再同期できる。メモリ window から外れただけでは Lagged にならず、cold read に切り替わる |
| `ReaderKind::Blocking` | writer はこの reader の pos を越えて tail を進めない（キュー semantics）。writer が止まるのは、この reader がディスクを一周分（約 H·S）遅れたときだけで、メモリ window を押さえることはない。blocking reader 1 つ + commit の永続化で「永続キュー」になる |
| `Visibility::Written` | `write` まで読める。レイテンシは低いが、クラッシュで消えるデータを下流が見る可能性がある |
| `Visibility::Durable` | `durable` まで読める。下流に渡したものは復旧後も必ず存在する。flush 完了時に wake される |

### 9.3 hot read と cold read

```
 mem_low ≤ pos           → hot:  メモリから zero-copy で読む
 tail ≤ pos < mem_low    → cold: ディスクから chunk 単位で読む（§9.4）
 pos < tail              → Lossy: Lagged（pos = tail へ）／ Blocking: I3 により起きない
```

- 判定は read のたびに比較 1〜2 回。`mem_low` と `tail` はセグメント移動時にしか変わらない。
- hot と cold で API は変わらない（§9.5）。frame のフォーマットも同じなので、parse のコードも共有する。

### 9.4 cold read

**読み出しの単位**

- chunk = 1 セグメントの残り `[blk_floor(pos), seg_end(pos))`（`blk_floor` はブロック境界への切り下げ）。2 つ目以降はセグメント全体になる。レコードはセグメントを跨がないので、chunk は自己完結する。
- `[tail, mem_low)` は完結した durable なセグメントだけからなるので（§3.4）、chunk の中身はすべて有効なレコードと終端マーカーになる。
- S が大きい場合は `cold_chunk`（< S）に分割してもよい。chunk の末尾で切れたレコードは、次の chunk でそのレコードの先頭（を含むブロック）から読み直す。この場合は `cold_chunk ≥ 最大レコード長 + ブロックサイズ` が必要。

**先読みと帯域**

- reader ごとに `cold_readahead` 個の chunk を in-flight にしておく。シーケンシャルに読むので、ディスクの帯域を出しやすい。
- 発行してよいのは、発行時点で `mem_low` より前にあるセグメントだけ。`mem_low` は単調増加するので、発行したセグメントは完了まで「完結していて durable」なままになる。`mem_low` 以降はメモリから読めるので、cold read する必要もない。
- 全体の同時 in-flight 数は `cold_max_inflight` で抑え、flush の帯域を食いつぶさないようにする。
- 同じセグメントを複数の reader が cold read する場合は、`ColdCache`（セグメント → `Weak<ColdChunk>`）で読んだ結果を共有できる（option）。

**バッファと受け渡し**

```rust
struct ReadJob {
    disk_off: u64,
    len: u32,
    buf: AlignedBuf,                                  // ColdBufPool から取る。I/O backend へ move する
    done: oneshot::Sender<io::Result<AlignedBuf>>,    // 完了したらバッファごと戻ってくる
}

struct ColdCursor {
    inflight: VecDeque<(u64, oneshot::Receiver<io::Result<AlignedBuf>>)>,  // (chunk の先頭, 完了)。発行順に消費する
    next_issue: u64,                                  // 次に発行する chunk の先頭（論理位置）
    current: Option<Arc<ColdChunk>>,                  // いま parse している chunk
}

struct ColdChunk { start: u64, len: u32, buf: AlignedBuf }    // drop でバッファを pool に返す
```

- バッファは固定サイズ・整列済み（O_DIRECT にそのまま使える）で、`ColdBufPool` で再利用する。pool への返却は他 thread から起こりうるので、lock-free な queue（`crossbeam-queue::ArrayQueue`）を使う。
- 完了処理（ring thread）:
  1. **I6 の検査**: `seg_start(chunk.start) ≥ tail` でなければ、その chunk と後続の先読みを捨てる。Lossy reader なら `Lagged` を返して `pos = tail` から読み直す。
  2. 検査を通れば `ColdChunk` にして reader に渡す。
- 一度検査を通った chunk の中身は以後変わらない（独立したバッファにある）ので、後から tail が追い越しても、そのまま読み切ってよい。

**hot への復帰**

- chunk を読み進めて `pos ≥ mem_low` になったら hot に切り替え、余分な先読みは捨てる。
- `mem_low` も前進し続けるので、reader が hot に追いつけるのは、cold read の帯域と下流の処理速度が書き込みレートを上回る場合だけ。追いつけない Lossy reader は、いずれ Lagged になる。

**正しさ（I6）**

- ディスク上のセグメント c の場所を上書きするのは、論理セグメント `c + D` の flush だけ。
- その flush が発行されるのは、writer がセグメント `c + D` に入った後。その時点で `tail ≥ seg_start(c + D) − H·S = (c + 1 + R)·S > c·S` になっている。
- tail は単調増加で、tail の更新、flush の発行、cold read の完了処理はすべて ring thread 上で順に行われる。
- したがって、完了処理の時点で `tail ≤ c·S` なら、それまでに c を上書きする書き込みは発行されていない。read はすでに完了しているので、読んだデータは上書きと競合していない。
- 検査に落ちた chunk は torn な可能性があるので、中身を一切使わない。per-record CRC（§8.4）を有効にすれば、多重の防御になる。
- Blocking reader では I3 により tail がその reader を越えないので、この検査は常に通る。

**page cache との関係**

- buffered I/O（Phase 1）では、最近 flush したデータは page cache に残っていることが多く、cold read が速い。その代わり、古いデータを大きく scan すると page cache を汚す。
- O_DIRECT では毎回 device から読むが、page cache を汚さず、DMA でバッファに直接入るのでコピーも無い（§12.3）。

### 9.5 local reader の API（ring thread 内）

```rust
impl RingReader {                                // !Send
    /// 読めるデータが来るまで待つ（local waker を登録する）。cold なら chunk の完了も待つ
    pub async fn ready(&mut self) -> Result<(), Lagged>;

    /// zero-copy の借用。closure は同期なので中で await できない。
    /// → 借用している間に writer が走らないことが型で保証される
    pub fn read_with<R>(&mut self, f: impl FnOnce(Record<'_>) -> R) -> Result<Option<R>, Lagged>;

    /// 最大 max_bytes 分をまとめて処理する（境界チェックと wake を償却）
    pub fn read_batch_with(&mut self, max_bytes: usize, f: impl FnMut(Record<'_>)) -> Result<usize, Lagged>;

    /// 他 thread に zero-copy で渡せるスライス（セグメント内の連続したレコード列）
    pub fn next_shared(&mut self, max_bytes: usize) -> Result<Option<SharedSlice>, Lagged>;

    pub fn position(&self) -> u64;
    pub fn seek(&mut self, pos: u64) -> Result<(), SeekError>;   // レコード境界のみ（commit 値など）
    pub fn commit_handle(&self) -> CommitHandle;                  // Send + Clone
}

pub struct Record<'a> { pub pos: u64, pub data: &'a [u8] }
impl Record<'_> { pub fn next_pos(&self) -> u64; }               // commit にはこの値を使う
```

- `read_with` は、hot ならメモリから、cold なら reader が持っている `ColdChunk` から借用する。呼び出し側からは区別しない。
- `&[u8]` を返す API にしない理由: 借用を await 越しに保持できてしまい、その間に writer がセグメントを再利用するとデータが壊れるため。closure 形式にすれば、借用を同期区間の中に閉じ込められる。

### 9.6 他の runtime への受け渡し

| 方法 | コピー回数 | writer への影響 | 向いている用途 |
|---|---|---|---|
| `read_batch_with` の中で `BytesMut` にコピーし、channel で送る | 1 回 | なし | 小さいレコード、下流が遅い場合、Lossy |
| `SharedSlice`（hot: lease 付き）を channel で送る | 0 回 | lease を保持している間、そのメモリスロットの再利用を待たせる | 大きいデータ、高スループット |
| `SharedSlice`（cold: 読み出し用バッファを共有）を channel で送る | 0 回（ディスク → バッファの後はコピーなし） | なし（バッファは ring メモリと独立） | 遅れた reader の追いつき |
| local でそのまま処理する（軽い処理のみ） | 0 回 | なし | フィルタ、ルーティング |

```rust
pub struct SharedSlice {            // Send + Sync
    backing: Backing,
    start: u64,                     // 論理位置（同一セグメント内、レコード境界）
    end: u64,
}
enum Backing {
    Hot { mem: Arc<RingMemory>, shared: Arc<Shared>, slot: u32 },   // メモリスロットの lease を持つ
    Cold(Arc<ColdChunk>),                                            // バッファを共有するだけ
}
impl SharedSlice {
    pub fn records(&self) -> impl Iterator<Item = Record<'_>>;
    pub fn end_pos(&self) -> u64;   // commit 用
}
impl Drop for SharedSlice {
    fn drop(&mut self) {
        if let Backing::Hot { shared, slot, .. } = &self.backing {
            if shared.leases[*slot as usize].fetch_sub(1, Ordering::Release) == 1 {
                shared.lease_waker.wake();      // writer が待っていなければほぼコストはかからない
            }
        }
    }
}
```

- lease の取得（`fetch_add`）は ring thread がスライスごとに 1 回行う（バッチで償却される）。lease の配列はメモリスロット（M 個）の分だけあればよい。
- writer は **セグメントに入るときだけ** `leases[slot].load(Acquire) == 0` を確認する。同じセグメント内では、lease されているバイトは常に `write` より前にあり、そのスロットが再利用されるまで書き換わらない。
- Release / Acquire により「consumer の読み取り → writer の上書き」の happens-before が成立する。
- hot の lease は長く保持しないこと。下流は処理が終わったらすぐ drop し、長期間保持したいなら `to_vec` する。保持し続けると、いずれ writer が止まる（lease 保持時間を metrics にする）。cold の `SharedSlice` は writer に影響しない。

### 9.7 remote reader（pump パターン）

```rust
// RingHandle（Send）から呼ぶ
let mut sub = handle.subscribe(ReaderOptions {
    name: "indexer".into(),
    kind: ReaderKind::Lossy,
    visibility: Visibility::Durable,
    start: StartAt::Committed,
    delivery: Delivery::Shared { max_batch_bytes: 256 << 10 },   // or Delivery::Copy
}).await?;

// 任意の runtime 上で動かす
while let Some(ev) = sub.recv().await {
    match ev {
        Event::Data(slice) => {
            for r in slice.records() { /* CPU heavy な処理 */ }
            sub.commit(slice.end_pos());
        }
        Event::Lagged(n) => { /* ... */ }
    }
}
```

- 内部の流れ: control channel で ring thread に登録要求を送る → ring thread 上で pump task を `spawn_local` する → bounded mpsc で `Event` を流す。下流が詰まると pump が await で止まる。Lossy なら reader は cold に落ち、さらにディスクを一周分遅れると `Lagged` になる。Blocking ならディスクを一周分遅れたところで writer が止まる。
- local reader を外から起動するための `handle.spawn_local(|ctx| async move { .. })` も用意する（closure は Send、生成される future は !Send でよい。`tokio_util::task::LocalPoolHandle::spawn_pinned` と同じ形）。

### 9.8 読み出し位置の永続化

- `CommitHandle::commit(pos)` は `Shared.commits[slot].fetch_max(pos, Release)` を行う。どの thread からでも lock-free に呼べる。
- commit の値は flush 時のヘッダ snapshot に含まれる。データが無くても、commit が変われば header-only flush を行う。
- 復旧後は `StartAt::Committed` で committed から再開する（at-least-once）。committed がメモリ window より古ければ、cold read から始まる。
- reader 名とスロットの対応はヘッダに保存する（最大 `MAX_READERS` 個）。
- 「Blocking reader が commit 位置まで tail を止める」option（再起動後の replay を保証する）は必要になったら追加する。

---

## 10. 同期のまとめ

| データ | 書く人 | 読む人 | 手段 |
|---|---|---|---|
| ring メモリ | ring thread | ring thread (reader)、I/O backend、SharedSlice (hot) 保持者 | raw ptr + 範囲の不変条件 |
| write / tail / mem_low / durable / inflight_end / limit | ring thread | ring thread | `Cell<u64>` |
| 同上（観測用の mirror） | ring thread | 任意 | `AtomicU64`（Release store） |
| reader pos | ring thread | ring thread | `Cell<u64>` |
| reader committed | 任意 thread | ring thread（snapshot） | `AtomicU64::fetch_max` |
| メモリスロットの lease | ring thread (+1)、任意 thread (−1) | ring thread | `AtomicU32` + `AtomicWaker` |
| FlushJob / header snapshot | ring thread | I/O backend | move（`std::sync::mpsc`） |
| ReadJob / cold read のバッファ | ring thread ⇄ I/O backend | ring thread、reader、consumer | move（job と oneshot）、`Arc<ColdChunk>` |
| バッファの pool への返却 | 任意 thread | ring thread | `crossbeam-queue::ArrayQueue` |
| flush 完了 | I/O backend | ring thread | `tokio::sync::oneshot` |
| 入力 | producer | ring thread | `tokio::sync::mpsc`（bounded） |
| ring thread 内の wake | ring thread | ring thread | `Cell<Option<Waker>>` |

`Shared` に置く atomic は `CachePadded` で包み、false sharing を避ける。

---

## 11. パフォーマンス上の工夫

- **hot path**: 比較 1 回 + 8B ヘッダ書き込み + memcpy だけ。CRC、atomic、lock は使わない。
- **バッチ化**: `recv_many` でまとめて受け取り、wake・atomic の公開・flush の判定はバッチごとに 1 回だけ行う。
- **セグメント単位の slow path**: lease、blocking reader、tail、mem_low の確認と更新はセグメントに入るときだけ。
- **位置計算**: メモリ上の位置は mask だけで求まる（M は 2 の冪）。ディスク上の位置の剰余は I/O の単位ごとに 1 回だけ。
- **prefault・huge page・事前確保**: メモリは huge page で prefault し、ファイルは最終サイズまで事前に確保する。
- **差分 flush**: 上限付きで、group commit でまとめる。
- **コピー**: buffered I/O では pwrite でユーザ空間 → page cache のコピーが 1 回あるが、I/O thread 側で行われ ring thread には乗らない。O_DIRECT ならこのコピーも無くなる（§12.3）。
- **cold read の分離**: 遅れた reader の I/O は flush と別の queue で、同時実行数にも上限がある。writer の hot path には影響しない。
- **下流への受け渡し**: zero-copy（SharedSlice）か、コピー 1 回。
- **公平性**: tokio の coop budget（mpsc の recv も参加する）に加え、大きいバッチの途中で `yield_now` して、reader や flush driver が飢餓状態にならないようにする。ring thread は CPU core に pin するとジッタが減る。
- **スケール**: ring thread 1 本で足りなくなったら、ring を shard する（thread とファイルを複数持つ）。順序は shard 内でのみ保証される。
- **allocator**: producer で alloc し ring thread で free するパターンが多いなら、mimalloc などを使うか、バッファを返却して再利用する。

---

## 12. Linux 向け I/O 最適化（sync_file_range / O_DIRECT / io_uring）

本番は Linux なので、I/O backend を段階的に最適化する。どれも `IoBackend` の裏側の変更で、ring thread の hot path（§6.3）と位置の不変条件（§3.5）は変わらない。変わるのは FlushJob の区間の丸め（O_DIRECT）と、I/O の発行方法だけ。必要な kernel version は機能ごとに書く（本番 kernel の確認は §18.2）。

### 12.1 出発点: buffered pwrite + fdatasync の課題

Phase 1（§7.3）の構成は全 OS で動くが、256 GB 級のファイルでは次の点が効いてくる:

| 課題 | 内容 |
|---|---|
| コピー | pwrite でユーザ空間 → page cache のコピーが 1 回入る。I/O thread 上で行われるが、メモリ帯域を倍使う |
| page cache の二重持ち | 書いたデータが page cache にも残る。memory window と同じデータを kernel も持ち、ファイル全体（256 GB）を流していくうちに、他の用途の cache を追い出す |
| writeback の集中 | dirty page は fdatasync のときにまとめて書かれるので、flush のレイテンシが dirty 量に比例する。`SyncMode::None` では dirty page が kernel の閾値まで溜まり、書き込み側が throttling で止められることもある |
| cold read | 最近のデータは page cache に当たって速いが、古いデータを大きく scan すると page cache を汚す |

以下の 3 つ（12.2〜12.4）は独立に選べる。推奨の組み合わせと導入順序は §12.5。

### 12.2 sync_file_range: buffered I/O のまま writeback を制御する

`sync_file_range(fd, offset, nbytes, flags)` は、指定範囲の dirty page の writeback を開始する、または完了を待つ。

- **耐久性の保証にはならない**: メタデータも device の write cache も flush しない（man page にも「extremely dangerous」と書かれている）。fdatasync の代わりではなく、writeback の **タイミングを早める** 道具として使う。
- フラグ: `SYNC_FILE_RANGE_WRITE` は、まだ writeback が始まっていない dirty page の writeback を開始し、基本的には待たずに返る（device の request queue が一杯だと block することがあるので、I/O thread から呼ぶ）。`SYNC_FILE_RANGE_WAIT_BEFORE` / `WAIT_AFTER` は、writeback の完了を待つ。
- O_DIRECT では page cache を使わないので、意味がない。

使いどころ:

1. **write-behind（先行書き出し）**: flush を待たずに、溜まったデータを少しずつ書き出しておく。
   - 位置 `written`（`durable ≤ written ≤ write`）を追加し、`write − written ≥ write_behind_bytes` になったら `[written, x)` の pwrite + `SYNC_FILE_RANGE_WRITE` を I/O thread に依頼する。ヘッダは書かず、sync もしない。flush と同じ queue に順番に流すので、I5 は保たれる。
   - flush 本体は、残りのデータとヘッダを書いて fdatasync するだけになる。fdatasync の時点で device に届いていないデータが減るので、レイテンシが安定する。RocksDB の `bytes_per_sync` と同じ考え方。
   - 先行書き出しの範囲は `[durable, write)` に含まれる。メモリ安全性（I2）とクラッシュ一貫性（§8.2 の議論は「runway 内の範囲を書く」ことしか使っていない）はそのまま成り立つ。`range_crc` は `[prev_head, head)` 全体に対して計算する（データはまだメモリにある）。
   - write-behind は O_DIRECT でも使える。その場合は write した時点で device に届くので、sync_file_range は要らない。
2. **`SyncMode::None` のときに dirty page を抑える**: fsync はしないが、`SYNC_FILE_RANGE_WRITE` で writeback を早めに始めて、dirty page の蓄積と throttling を防ぐ。
3. **page cache の解放**: durable になって十分経った範囲（例: memory window より古い範囲）に `posix_fadvise(POSIX_FADV_DONTNEED)` を発行して clean page を捨て、page cache の使用量を抑える。dirty page には効かないので、sync 済みの範囲にだけ使う。cold read が page cache に当たらなくなる、というトレードオフがある。

### 12.3 O_DIRECT

**効果**

| | buffered | O_DIRECT |
|---|---|---|
| flush のコピー | 1 回（→ page cache） | 0 回（ring メモリから DMA） |
| page cache | 使う（二重持ち、汚染） | 使わない。memory window が唯一の cache |
| flush のレイテンシ | writeback と reclaim の影響を受ける | 書いた量だけで決まり、予測しやすい |
| cold read | page cache に当たれば速い | 毎回 device から読む（自前の先読みが前提、§9.4）。DMA でバッファに直接入る |
| 整列 | 不要 | buffer のアドレス・file offset・長さを整列する必要がある |

256 GB 級のディスクを、メモリ上の自前の window と組み合わせて使うこの設計では、page cache を挟む利点がほとんど無い。**本番の推奨は O_DIRECT**。

**整列**

- 必要な整列は `statx(STATX_DIOALIGN)`（6.1〜）の `stx_dio_mem_align` / `stx_dio_offset_align` で取得する。block device にも使える。取れない場合は logical block size（`BLKSSZGET`）か 4096 を使う。以下、この値を `BLK` と書く。
- RingMemory（2 MiB 整列）、セグメント（S は 4 KiB の倍数）、ヘッダスロット（4 KiB）、data region の開始位置（0x2000）は、すでに 4 KiB に揃っている。
- flush の範囲 `[start, end)` はレコード境界なので、BLK に揃っているとは限らない。次のように丸める:
  - **先頭**: `blk_floor(start)` まで切り下げる。`[blk_floor(start), start)` は、前回の flush で書いたのと同じバイト列を ring メモリからもう一度書くことになる。device は logical block 単位で atomic に書く（NVMe の電源断時の atomic write 単位は最小でも 1 logical block）。途中で切れても各 block は新旧どちらかになり、既に durable な部分は新旧で同じ内容なので壊れない。
  - **末尾**: `[blk_floor(end), end)` を BLK 長の **bounce buffer** にコピーし、残りを 0 で埋めて書く。ring メモリの `end` より後ろは writer が並行して書き込んでいる最中なので、DMA の読み出し元にしない。中身は head より後ろなのでどうせ使われないが、書き換え中のメモリを device に渡すのは避ける（DIO 中のバッファ書き換えで checksum 不一致を起こす FS もある）。コピー量は flush あたり BLK 未満。
  - 次の flush は `blk_floor(end)` から ring メモリで書き直すので、bounce で書いたブロックも正しい内容で上書きされる。
  - 小さい flush を頻繁に行うと、flush あたり最大 2 block 分の余分な書き込みが発生する（buffered でも writeback はページ単位なので、ほぼ同じ）。
- 先頭を切り下げると、I/O backend が読む ring メモリは `[blk_floor(durable), …)` に広がる。それでもセグメントは BLK 整列なので、同じセグメント内に収まる。§8.2 の主張 2 はそのまま成り立つ。
- ヘッダの読み書き、復旧時の読み込み、cold read も、同じ fd で O_DIRECT で行う。同じファイルに buffered と direct を混ぜると、page cache との一貫性の問題が起きる。

**耐久性**

- O_DIRECT は page cache を通らないだけで、**device の volatile write cache には残りうる**。電源断に耐えるには、引き続き fdatasync（または DSYNC write）が要る。
- written extent への上書きでファイルサイズも変わらない場合、fdatasync は journal commit を伴わず、device の cache flush だけになる。
- PLP（power loss protection）付きの enterprise NVMe は、volatile write cache を持たないと報告する（`/sys/block/<dev>/queue/write_cache` が `write through`）。この場合 cache flush はほぼ no-op になる。fdatasync のコストは device に大きく依存するので、§12.6 の方法で事前に測る。

**`SyncMode::Dsync`（RWF_DSYNC）**

- `pwritev2(..., RWF_DSYNC)`（4.7〜）は、その write 1 回だけに O_DSYNC の意味を持たせる。io_uring の write にも同じフラグを付けられる。
- iomap を使う FS（XFS、ext4）の direct I/O では、written extent への純粋な上書き（メタデータ更新が要らない）で、device が FUA に対応しているか volatile write cache を持たなければ、完了後の cache flush の代わりに **FUA 付きの write** になる（`fs/iomap/direct-io.c`）。unwritten extent への書き込みや、メタデータの更新が必要な場合は、通常の sync にフォールバックする。
- これを使うと、2 通りの flush ができる:
  - **(a) 並行**: data と header を並行して DSYNC で書き、両方の完了で flush 完了とする。fdatasync は不要。header と data の永続化順序は保証されないので、`range_crc` は引き続き必要。
  - **(b) 順序付き**: data を DSYNC で書き、その **完了後に** header を DSYNC で書く。header が永続化されていれば data も永続化済みなので、`range_crc` が無くても一貫する。§8.1 の 2 回 fsync 方式と同じ順序を、FUA で安く実現する形になる。device への往復は 2 回になる。
- FUA に対応していない device では、kernel が write ごとに cache flush を発行するので、かえって遅くなりうる。`Data`（fdatasync 1 回）と測り比べて選ぶ。

**事前確保が効いてくる**

- fallocate しただけのファイルは unwritten extent のまま。最初の一周はそこへの書き込みになるので、extent の変換（メタデータの更新）が journal に乗り、fdatasync が重くなる。FUA の最適化も効かない。§5.1 の `ZeroFill`（または `WriteZeroes`、block device）を使う。
- `FALLOC_FL_WRITE_ZEROES`: device の write zeroes コマンドを使って、0 で埋めた written extent を割り当てる fallocate のモード。Linux 6.17 で追加された（ext4 などが対応）。NVMe の DEAC などを使えば、実際に 0 を書かずに済む。使えなければ `ZeroFill` に fallback する。

**ファイルシステム**

- ext4 / XFS、または raw block device を推奨する。
- Btrfs（data checksum 有効）は、DIO 中にバッファが書き換わると checksum 不一致を起こしうるので推奨しない。
- テストで使う tmpfs などの O_DIRECT 非対応の FS では、`direct = false` に fallback できるようにしておく。

**ページの pin**

- O_DIRECT の I/O ごとに、kernel はユーザページを pin する。huge page で確保していれば、pin するページ数が減る。io_uring の fixed buffer にすれば、登録時に 1 度 pin するだけで済む（§12.4）。

### 12.4 io_uring

**この設計にもたらすもの**

1. **submit をまとめられる**: 1 回の flush（data の区間 + bounce block + header、さらに fsync）を、1 回の `io_uring_enter` で投入できる。pwrite を順に呼ぶより syscall が減り、区間どうしも並行に処理される。
2. **fixed buffer**: RingMemory、ヘッダ用バッファ、bounce buffer、cold read の pool を起動時に登録する（`IORING_REGISTER_BUFFERS`）。`WRITE_FIXED` / `READ_FIXED` を使えば、I/O ごとのページ pin を省ける。ring のメモリは長寿命で固定サイズなので、相性がよい。
   - 登録できるのは 1 バッファあたり最大 1 GiB。RingMemory は 1 GiB ごとに分けて登録する。
   - 登録したメモリは pin され、CAP_IPC_LOCK が無ければ RLIMIT_MEMLOCK に計上される。systemd なら `LimitMEMLOCK=` を設定する。
3. **cold read を高い queue depth で出せる**: thread pool なしで、複数の reader の先読みを同時に in-flight にできる。
4. **I/O thread を無くせる**（下の構成 B）: ring thread が直接 SQE を積み、完了を受け取る。thread 間の handoff と context switch が無くなる。

**flush job から SQE への対応と、順序の注意**

```
data piece 0..k  (WRITE_FIXED、互いに独立)  ┐
bounce block     (WRITE_FIXED)              ├ まとめて投入し、並行に処理させる
header           (WRITE_FIXED)              ┘
  ↓ すべての CQE が成功したら
fsync            (IORING_OP_FSYNC + IORING_FSYNC_DATASYNC)
```

- io_uring の fsync は、**同時に in-flight な write の完了を待たない**。write の CQE をすべて受け取ってから fsync を投入する。syscall が 1 回増えるが、単純で確実。
- `IOSQE_IO_LINK` を使うと、「前の SQE が成功したら次を開始する」chain を組める。途中で失敗すると、残りは `-ECANCELED` で完了する。chain は直列なので、data → header → fsync を chain にすると、区間どうしの並行性は失われる。
- `IOSQE_IO_DRAIN` を付けた SQE は、それより前に投入した **すべての** SQE が完了するまで開始されない。また、それより後の SQE も、DRAIN の SQE が完了するまで開始されない。cold read と同じ ring に投入している場合に付けると、flush が cold read を待たされる。使わないか、flush 用と cold read 用で ring を分ける。
- §12.3 の `Dsync` (b) は、data の CQE をすべて待ってから header を投入すれば実現できる（data が 1 区間なら `IOSQE_IO_LINK` で `data(DSYNC) → header(DSYNC)` と書ける）。

**構成の選択肢**

| | 構成 | 利点 | 欠点 |
|---|---|---|---|
| A | I/O thread の中で io_uring を使う。job を受け取り、SQE を積み、CQE を待ってから oneshot で返す | 変更は `IoBackend` の内側だけ。submit のまとめ、fixed buffer、cold read の並行化の効果はすべて得られる | thread 間の handoff は残る |
| B | ring thread が直接 io_uring を操作する。完了は eventfd（`IORING_REGISTER_EVENTFD`）を tokio の `AsyncFd` で待つ | handoff と context switch が無くなる。flush の完了から reader の wake までが最短になる | submit の syscall と、O_DIRECT の block layer への投入処理が ring thread 上で走る。CRC の置き場所を考え直す必要がある（下記） |
| C | io_uring ベースの runtime（tokio-uring / monoio / glommio）に置き換える | — | runtime ごと替わり、tokio の mpsc / time / LocalSet と混ぜにくい。採用しない（§15） |

- **推奨: まず A**。O_DIRECT + fixed buffer の効果を A で測る。プロファイルで thread 間の handoff（flush 完了の通知、cold read の完了）が目立つ場合にだけ、B に進む。
- **B の詳細**:
  - `IORING_SETUP_SINGLE_ISSUER`（6.0〜。submit するのは ring thread だけ）と `IORING_SETUP_DEFER_TASKRUN`（6.1〜。完了処理を、ring thread が回収しに行くまで遅らせる。hot path が割り込まれない）を使う。
  - DEFER_TASKRUN では、eventfd で起きた時点ではまだ CQE が CQ に載っていない。`io_uring_enter(GETEVENTS)` で遅延された完了処理を走らせてから、CQ を読む。
  - submit は `io_uring_enter(to_submit, min_complete = 0)` で行い、完了を待たない。O_DIRECT の write は、まず block せずに投入を試み、block しそうなら kernel 内の io-wq worker に回される。ring thread が I/O の完了を待って止まることはない。
  - `range_crc` の計算が ring thread に移る。選択肢は 2 つ。
    - ingest の `after_batch` で、memcpy した直後（cache に乗っている）のデータに crc32c をインクリメンタルに掛ける。ハードウェア命令で十分速いが、ingest のスループットにはいくらか効く（要計測）。
    - `Dsync` (b) を使い、`range_crc` 自体を不要にする。
  - SQPOLL（`IORING_SETUP_SQPOLL`）を使うと submit の syscall も消えるが、kernel thread が CPU を 1 つ使い続ける。プロファイルで syscall が目立つときだけ使う。

**安全性（Rust）**

- SQE が参照するバッファは、CQE を受け取るまで有効でなければならない。
  - ring メモリ: 範囲の貸し出し（I2）により、in-flight 中の範囲は writer が書かない。RingMemory の解放は、すべての CQE を回収してから行う。shutdown では drain し、drain できない異常系では解放せずに leak する。
  - pool のバッファ / bounce buffer / ヘッダ用バッファ: in-flight の表が所有し、CQE を受け取ってから返す。
- I/O の寿命を Future の drop に結びつけない。Future を drop して cancel したつもりでも kernel は I/O を続ける、という io_uring runtime によくある落とし穴を避けるため、in-flight の操作は表で明示的に管理する。

**運用上の注意**

- 環境によっては io_uring が使えない。`kernel.io_uring_disabled` sysctl で無効化されている場合、コンテナの seccomp で拒否されている場合、組織の方針で無効化されている場合などがある。起動時に `io_uring_setup` を試し、失敗したら ThreadBackend に fallback してログに出す。
- crate は low-level な `io-uring`（tokio-rs）を使う。

### 12.5 推奨構成と導入順序

| Phase | 構成 | 主な効果 | 前提 |
|---|---|---|---|
| 1 | ThreadBackend + buffered + fdatasync（option: write-behind、sync_file_range、fadvise） | 全 OS で動く基準線 | なし |
| 2 | + O_DIRECT + `ZeroFill`（または raw block device） | flush のコピーが 0。page cache を使わない。レイテンシが安定する | ext4 / XFS / block device |
| 3 | UringBackend（構成 A）+ fixed buffer。cold read も io_uring で行う | syscall とページ pin の削減、cold read の並行化 | io_uring が使える kernel と環境 |
| 4 | 構成 B、`Dsync`、SQPOLL など | handoff の除去、sync の削減 | プロファイルで必要と分かった場合のみ |

- Phase ごとに §12.6 の計測を行い、次に進むかを決める。
- どの Phase でも、ring thread の hot path（§6.3）とクラッシュ一貫性の議論（§8）は変わらない。

### 12.6 事前確認と計測

device と kernel の確認:

```sh
uname -r
cat /sys/block/nvme0n1/queue/logical_block_size /sys/block/nvme0n1/queue/physical_block_size
cat /sys/block/nvme0n1/queue/write_cache      # write back（volatile cache あり）/ write through
nvme id-ctrl /dev/nvme0 | grep -iE 'vwc|awupf'  # volatile write cache の有無、電源断時の atomic write 単位
findmnt -no FSTYPE,OPTIONS -T /path/to/ring
```

fio で、sync の方式ごとのレイテンシを先に測る（`--overwrite=1` で先にファイルを書いておき、unwritten extent の影響を除く）:

```sh
# O_DIRECT write + 毎回 fdatasync（SyncMode::Data に相当）
fio --name=data --filename=/path/ring.test --size=8G --overwrite=1 --direct=1 \
    --rw=write --bs=64k --ioengine=io_uring --iodepth=1 --fdatasync=1
# O_DSYNC write（FUA が効くか。SyncMode::Dsync に相当）
fio --name=dsync --filename=/path/ring.test --size=8G --overwrite=1 --direct=1 \
    --rw=write --bs=64k --ioengine=io_uring --iodepth=1 --sync=dsync
```

crate 側の計測:

- flush レイテンシのヒストグラム（p50 / p99 / p99.9）を、flush サイズごとに取る。
- ingest のスループットと、ring thread の CPU の内訳（perf で memcpy / syscall / CRC の割合を見る）。
- cold reader を 0 / 1 / 4 本動かしたときの、flush レイテンシと ingest スループットへの影響。
- Phase どうしの比較は、同じ device・同じ FS で行う（macOS の数値は参考にしない、§7.6）。

---

## 13. エラー処理とシャットダウン

- **flush の I/O エラー**: 完了通知が `Err` なら `durable` を進めない。データはメモリに残っていて上書きされないので（I2）、`flush_interval` 後にリトライする（その間は barrier や Stall でも急がない）。連続して `max_flush_retries` 回を超えて失敗したら `Failed` 状態にする。
  - `Failed` では append が `AppendError::Failed` を返し、`flush` / `shutdown` / `RingDriver::run` は原因のエラーを返す。hot read はそのまま使える。
  - （未実装）Linux は fsync に失敗した後、dirty page を clean として扱うことがある。そのため、同じ fd で fsync を再試行して成功しても信用しない。ファイルを開き直して `[durable, write)` を書き直し、それから sync する。O_DIRECT なら page cache を介さないので、書き直しの元はもともと ring メモリになる。
- **cold read の I/O エラー**: その reader に `Err(ReadError::Io)` を返す（何回かリトライした後）。writer と他の reader には影響しない。reader 側で、リトライするか、seek して飛ばすかを決める。
- **ディスクフル**: ファイルを事前に確保しているので、運用中は発生しない。
- **シャットダウン**: `Ring::shutdown()` が呼ばれるか、すべての `Ring` handle が drop されたら、次の順に進める（入力 channel は user のものなので、channel の close を見て `shutdown` を呼ぶのは user 側）。`shutdown()` の future は 4 が終わったら resolve する。
  1. append を拒否し（`AppendError::Closed`）、新しい cold read の発行を止める
  2. in-flight の cold read の完了を待つ
  3. 最終 flush を行う。data が durable になった後、ヘッダだけの flush で `CLEAN_SHUTDOWN` フラグを立てる（data と同じ flush で立てると、フラグ付きのヘッダだけが残りうる、§8.3）。次の起動後の最初の flush でフラグは消える。
  4. I/O backend を close して join する
  5. RingMemory を解放する（すべての I/O が終わった後）
- **driver の停止**: `RingDriver` が close 前に drop されたら（spawn し忘れ、runtime の drop など）、ring を `Failed` にする。flush の完了を待っている task が永久に待たないようにするため。
- **ring thread の panic**: I/O thread は channel が close されたことで終了する。ディスク上は、最後に完了した flush の時点で一貫している。io_uring で in-flight の操作が残っている可能性がある場合は、RingMemory を解放しない（§12.4）。

---

## 14. 設定とサイジング

```rust
pub struct Config {
    pub path: PathBuf,                // 通常のファイルか block device
    pub segment_size: u32,            // S: 2 の冪。例 1 MiB（作成時のみ）
    pub disk_segments: u32,           // D: 任意。例 262_144 → 256 GiB（作成時のみ）
    pub memory_segments: u32,         // M: 2 の冪。例 1024 → 1 GiB（再起動時に変更可）
    pub runway_segments: u32,         // R ≤ M − 1。例 64 → 未永続化データは最大 64 MiB
    pub flush_interval: Duration,     // 未 flush データが残る時間の上限。リトライの間隔も兼ねる。例 10 ms
    pub flush_threshold: u64,         // これだけ溜まったらすぐ flush する（0 = 毎回）。R·S より十分小さくする。例 4 MiB
    pub max_flush_retries: u32,       // 連続でこの回数を超えて flush に失敗したら Failed。例 5
    pub max_flush_bytes: u64,         // S の倍数かつ ≤ R·S。例 16 MiB
    pub write_behind_bytes: Option<u64>,  // §12.2。例 Some(1 MiB)
    pub sync: SyncMode,               // None | Data | Full | Dsync
    pub io: IoConfig,
    pub checksum: ChecksumMode,       // None | PerRecord
    pub cold_chunk: u32,              // cold read の単位。デフォルト S
    pub cold_readahead: u32,          // reader ごとの先読み数。例 4
    pub cold_max_inflight: u32,       // 全体の cold read の同時 in-flight 数。例 16
    pub warm_segments: u32,           // 起動時にメモリへ読み込む直前のセグメント数。例 0
    pub ingest_batch: usize,          // recv_many の上限。例 256
    pub channel_capacity: usize,
    pub max_readers: usize,           // ヘッダに保存する reader の最大数
}

pub struct IoConfig {
    pub backend: Backend,             // Thread | Uring { sqpoll: bool }（Uring が使えなければ Thread に fallback）
    pub direct: bool,                 // O_DIRECT（Linux。使えない FS なら fallback）
    pub preallocate: Preallocate,     // ZeroFill | Fallocate | WriteZeroes（block device では無視）
}
```

サイジングの指針:

- **メモリ使用量**: `M·S` + cold read のバッファ（最大 `cold_max_inflight × cold_chunk`）+ ヘッダと bounce 用の小さなバッファ。reader の数や遅れには比例しない。
- **M（hot window）**: 大きいほど、遅れた reader が cold に落ちにくい。runway より十分大きくする（例: `M ≥ 4R`）。
- **R（runway）**: `R·S ≥ 入力レート × (flush_interval + flush 所要時間の p99) × 2〜4`。足りないと `Stall::Durability` が発生する。
- **D**: `ディスク容量 / S`。保証される履歴量は `H·S = (D − 1 − R)·S`。
- **S**: 大きくすると slow path の頻度と padding 率が下がり、cold read の単位も大きくなる。小さくすると tail や lease の粒度が細かくなる。最大レコードサイズよりも十分大きくする。
- **cold read の帯域**: 遅れた reader が追いつくには、cold read の帯域（と下流の処理速度）が入力レートを上回る必要がある。足りない分は `cold_readahead` と `cold_max_inflight` で調整する。
- **1 flush の時間**: 最大でおよそ `max_flush_bytes / ディスクの書き込み帯域 + sync の時間`。

例（256 GiB のディスク、メモリ 1 GiB 程度）:

| 項目 | 値 |
|---|---|
| S | 1 MiB |
| D | 262 144（256 GiB） |
| M | 1024（1 GiB） |
| R | 64（64 MiB） |
| 履歴 `H·S` | 約 256 GiB − 65 MiB |
| cold read のバッファ | 16 × 1 MiB = 16 MiB |
| メモリ合計 | 約 1.1 GiB |

---

## 15. 検討した代替案

| 案 | 採用しなかった理由 |
|---|---|
| ring 全体をメモリに持つ（改訂前の前提） | 256 GB はメモリに乗らない |
| 遅れた reader が読むまで、メモリのデータを解放しない | メモリ使用量が reader の遅れに比例して増える。データはディスクにあるので、cold read で読む方が安い |
| `Arc<Mutex<Ring>>` | 読み書きのたびに lock を取る。flush 中は lock を長く保持するか、コピーが必要になる |
| ダブルバッファ（flush 時に別バッファへコピー） | ring thread 上で余計な memcpy が発生する。貸し出し方式ならコピーは要らない |
| ファイルを mmap して ring にする | 256 GB の file を mmap すると、どこをメモリに置くかを kernel に委ねることになり、reader の page fault が hot path のジッタになる。writeback 後のページに書くと write-protect fault が起き、I/O エラーは SIGBUS になる |
| `tokio::fs` | 内部で spawn_blocking + バッファへのコピーを行う |
| flush ごとに `spawn_blocking` | handoff のコストがかかり、順序の保証も自前で行う必要がある |
| io_uring ベースの runtime に置き換える（tokio-uring / monoio / glommio） | runtime ごと替わり、tokio の mpsc / time / LocalSet と混ぜにくい。`io-uring` crate を I/O backend の中で直接使えば、効果のほとんどは得られる（§12.4） |
| cold read を page cache に任せ続ける（buffered pread のみ） | Phase 1 ではこれを使う。256 GB の scan で page cache を汚し、レイテンシが読みにくいので、本番は O_DIRECT + 自前の先読みに移る（§12.3） |
| producer が ring に直接 claim する（Disruptor 型の MPSC） | copy-in を無くせるが、producer 側で atomic RMW が必要になり、channel 前提の要件とも合わない。ring thread のスループットが限界になったら検討する |
| fsync 2 回方式（data → header） | `range_crc` が不要で単純になるが、fsync の回数が 2 倍になる。Linux + FUA 対応 device なら、`Dsync` (b) で同じ順序を安く実現できる（§12.3） |

---

## 16. モジュール構成と実装計画

### 16.1 モジュール構成

```
src/
  lib.rs          公開 API の re-export                                             [済]
  config.rs       Config, IoConfig, SyncMode, ChecksumMode, Preallocate            [済]
  pos.rs          位置の演算（seg_start, mem_off, disk_off, tail / mem_low の規則）— 純粋関数  [済]
  frame.rs        Frame のエンコード / デコード（§4.2）                             [済]
  memory.rs       RingMemory（アラインされた ring バッファ、frame の読み書き）       [済]
  header.rs       ヘッダ（A/B 面、CRC、ReaderSlot）（§5.2）                         [済]
  core.rs         RingCore: append（fast / slow path）、flush の準備 / 完了、hot read、不変条件  [済]
  recovery.rs     復旧手順（§8.3）、range_crc                                       [済]
  ring.rs         公開ハンドル Ring / RingDriver（tokio の LocalSet 上で動く）       [一部]
  io/
    mod.rs        Storage / IoBackend trait、FlushJob                               [済]
    thread.rs     FileStorage, ThreadBackend（専用 I/O thread）                     [済]
    prealloc.rs   事前確保（ZeroFill / Fallocate）                                  [済]
    sim.rs        SimDisk / SimBackend（クラッシュシミュレーション用）               [済]
  reader.rs       reader の状態、Lossy / Blocking、remote pump、commit              [未]
  cold.rs         cold read（ディスクからの読み出し）                               [未]
  shared.rs       SharedSlice（zero-copy の受け渡し、lease の寿命管理）             [未]
  io/direct.rs    O_DIRECT backend、sync_file_range / fadvise                       [未]
  io/uring.rs     io_uring backend                                                  [未]
```

テストは `src/tests/`（`core_basic` / `ring` / `file_backend` / `crash_sim`）と `tests/local_runtime.rs`、例は `examples/local_runtime.rs`。
`[済]` は実装済み、`[一部]` は一部のみ、`[未]` は未着手。

### 16.2 実装計画と進捗

| 区切り | Step | 内容 | 状態 |
|---|---|---|---|
| M1 | 1 | コア: 位置演算、frame、メモリ、ヘッダ、config、`RingCore`（append / flush の準備と完了 / hot read） | 完了 |
| M1 | 2 | ThreadBackend での flush、ヘッダ書き込み、クラッシュからの復旧、SimBackend によるクラッシュシミュレーション | 完了 |
| M2 | 3 | tokio への組み込み: ingest、barrier（`flush`）、`shutdown`、`RingDriver` | 一部（下記） |
| M2 | 4 | reader（Lossy / Blocking）、remote pump、commit の永続化 | 未着手 |
| M3 | 5 | cold read | 未着手 |
| M3 | 6 | SharedSlice（zero-copy の受け渡し） | 未着手 |
| 後回し | 7 | O_DIRECT、事前確保の Linux 向けの方式、sync_file_range / fadvise | 一部（`Preallocate::Fallocate` は実装済み） |
| 後回し | 8 | io_uring | 未着手 |

#### Step 3 の内訳

- [x] `Ring` / `RingDriver`（`!Send`、`LocalRuntime` または `LocalSet` 上で動かす）
- [x] `append`（`try_append` と、stall を待つ async 版）
- [x] `flush`（barrier）、`shutdown`（clean shutdown ヘッダ）
- [x] driver による定期 flush（`flush_interval` / `flush_threshold`）、flush の再試行（`max_flush_retries`）と失敗時の Failed 状態
- [x] `read_hot`（貸し出し方式）と、読んでいる間の append の保留
- [x] example / 結合テスト（`examples/local_runtime.rs`, `tests/local_runtime.rs`）
- [ ] 入力 channel を `Ring` 側で受ける ingest task（現状は利用者が channel から `append` を呼ぶ）
- [ ] 複数 reader の登録（Step 4 と合わせて）

#### Step 4 の内訳

- [ ] reader の状態と登録 / 解除（ヘッダの `ReaderSlot` の形式だけは実装済み）
- [ ] Lossy reader（`Lagged`）
- [ ] Blocking reader（メモリ window の位置で writer を止める）
- [ ] remote reader（pump パターン）
- [ ] commit の永続化（header-only flush。§7.1）

#### 区切りの意味

- **M1**（完了）: 同期 API だけのコア。append、flush（ThreadBackend）、ヘッダ、クラッシュからの復旧まで。tokio は使わない。
- **M2**: tokio に組み込み、ライブラリとして使える形にする。ingest、barrier、reader（Lossy / Blocking）、remote pump、commit の永続化まで。cold read がまだ無いので、メモリ window より遅れた reader は暫定的に次の扱いになる。
  - Lossy reader は `Lagged` を受け取る。
  - Blocking reader は、メモリ window の位置で writer を止める。
  - ディスク容量がメモリに乗る範囲の用途なら、この段階でも実用になる。
- **M3**: cold read と SharedSlice が入る。設計のうち buffered I/O の部分（§12.5 の Phase 1）が揃う。
- **後回し**: O_DIRECT と io_uring（Step 7, 8）。Step 7 のうち、sync_file_range / fadvise と事前確保の Fallocate は buffered I/O でも使え、規模も小さいので、M3 の後に単独で入れてもよい。

#### この区切りにした理由

- M1 で区切る: いちばん壊れやすいのはクラッシュ一貫性。async と切り離しておけば、SimBackend を使ったクラッシュシミュレーションを決定的に回して固められる。
- M2 で区切る: ここで end-to-end に使えるようになる。
- M3 で区切る: cold read と SharedSlice は、どちらも「読み出し用バッファと lease の寿命管理」の話で、テスト（cold read 競合テストと loom）も近いので、まとめた方が楽。

進め方は、M1 を一度レビューしてから M2、M3 に進む。
