# MiniDB

![language](https://img.shields.io/badge/language-C-blue)
![platform](https://img.shields.io/badge/platform-macOS%20%7C%20Linux-lightgrey)
![dependencies](https://img.shields.io/badge/dependencies-none-brightgreen)

A small SQLite-style storage engine written in C, built from the disk up:
**paged file I/O → LRU page cache → write-ahead log → slotted pages → B+Tree.**

About 1.7k lines of plain C with no dependencies. Every layer is hand-written, including the hash table behind the page cache.

---

## Features

| Layer | What it does |
| --- | --- |
| **Paged storage** | The database file is a sequence of fixed 4 KB pages. Page 0 holds a header (magic number, page size, page count). |
| **LRU page cache** | A 16-page buffer pool backed by a chained hash table (O(1) lookup) and a doubly linked list (O(1) eviction). Dirty pages are written back when evicted. |
| **Write-ahead log** | Every page change is appended to the WAL as a full page image and `fsync`'d before the page is marked dirty. The log is replayed on startup and can be checkpointed into the data file. |
| **Slotted pages** | Variable-length records. The slot array grows forward from the header and record data grows backward from the end of the page. |
| **B+Tree index** | Ordered insert, point lookup, leaf splits, and range scans over leaves linked through `next_leaf`. |
| **Metrics** | Counts cache hits and misses, disk reads and writes, and WAL appends and replays, for benchmarking. |

## Architecture

```
          btree_insert · btree_search · btree_range_scan
                               │
                    ┌──────────▼──────────┐
                    │       B+Tree        │  btree.c
                    └──────────┬──────────┘
                    ┌──────────▼──────────┐
                    │    Slotted page     │  page_layout.c
                    └──────────┬──────────┘
                    ┌──────────▼──────────┐
                    │  Pager + LRU cache  │  pager.c
                    └─────┬──────────┬────┘
                          │          │
                  ┌───────▼───┐  ┌───▼───────┐
                  │    WAL    │  │ data file │
                  │  (*.wal)  │  │  (*.db)   │  wal.c / pager.c
                  └───────────┘  └───────────┘
```

**Read path** (`pager_get_page`)

1. Look up the page in the LRU cache. On a hit, move it to the head of the list and return it.
2. On a miss, evict the least recently used page if the cache is full, writing it back first if it is dirty.
3. Check the WAL for the newest copy of the page.
4. If the WAL has no copy, read the page from the data file.

**Write path**

1. The B+Tree changes the page in the cache.
2. `wal_append_page` appends a `[page number][4 KB page image]` frame to the log and calls `fsync`.
3. The page is marked dirty. It reaches the `.db` file on eviction, on `pager_close`, or at checkpoint.

**Recovery and checkpoint**

- `wal_recover` runs on open. It replays every frame in order, so the last write wins, and then truncates the log.
- `wal_checkpoint` writes the log back into the data file, `fsync`s it, and truncates the log so it does not keep growing during a long session.

## On-disk format

### Database file (`*.db`)

| Page | Content |
| --- | --- |
| 0 | `DBHeader`: `magic = 0x4D444230` ("MDB0"), `page_size = 4096`, `page_count` |
| 1 … N | B+Tree nodes, each stored as one slotted page |

### WAL file (`*.wal`)

A flat sequence of fixed-size frames. A partial frame at the end of the file is ignored.

```
┌────────────┬──────────────────────────┬────────────┬──────────────────────────┬─────
│ pgno (u32) │   page image (4096 B)    │ pgno (u32) │   page image (4096 B)    │ ...
└────────────┴──────────────────────────┴────────────┴──────────────────────────┴─────
```

### Slotted page

```
0             16                       free_start         free_end                4096
┌─────────────┬────────────────────────┬──────────────────┬──────────────────────────┐
│ PageHeader  │ Slot 0 │ Slot 1 │ ...  │    free space    │ ... │ record 1 │ record 0 │
└─────────────┴────────────────────────┴──────────────────┴──────────────────────────┘
                 slots grow →                                       ← records grow
```

| `PageHeader` field | Size | Meaning |
| --- | --- | --- |
| `node_type` | u16 | `0` = leaf, `1` = internal |
| `slot_count` | u16 | number of records |
| `free_start` | u16 | end of the slot array |
| `free_end` | u16 | start of record data |
| `leftmost_child` | u32 | internal nodes only: the leftmost child page |
| `next_leaf` | u32 | leaf nodes only: the right sibling page (`0` = none) |

Each slot is `{offset u16, length u16}`. The slot array is kept sorted by key. On insert, the record bytes go wherever there is free space and only the 4-byte slot entries are shifted.

**Record encoding**

```
leaf:      [klen u16][key bytes][vlen u16][value bytes]
internal:  [klen u16][separator key][right child pgno u32]
```

Internal nodes store `n` separators and `n + 1` children. The extra child is `leftmost_child` in the header. A key smaller than the first separator goes to `leftmost_child`. A key with `sep[i] ≤ key < sep[i+1]` goes to the right child of `sep[i]`.

## Getting started

Requires a POSIX system (macOS or Linux) and `clang`. On Windows, use WSL.

```bash
git clone https://github.com/irisonly9716/my_sqlite.git
cd my_sqlite

make            # build ./mini_db (the test runner)
./mini_db       # run the tests (creates test.db / test.wal)

make bench      # build ./mini_bench and run it on a fresh bench.db
make clean
```

To use gcc instead: `make CC=gcc`.

### Using the API

```c
Pager pager;
WAL   wal;
BTree tree;

pager_open(&pager, "my.db");
wal_open(&wal, "my.wal");
wal_recover(&wal, &pager);            // redo whatever is left in the log
btree_init(&tree, &pager, &wal, 1);   // root starts on page 1

btree_insert(&tree, "user01", "alice");
btree_insert(&tree, "user02", "bob");

char     out[256];
uint16_t len;
if (btree_search(&tree, "user01", out, &len) == 0)
    printf("%s\n", out);              // alice

btree_range_scan(&tree, "user00", "user09");   // prints key -> value

wal_checkpoint(&wal, &pager);         // fold the WAL into my.db
wal_close(&wal);
pager_close(&pager);
```

## Tests

`src/main.c` is a small self-checking test runner. It prints `[PASS]` / `[FAIL]` for each check and exits non-zero if anything fails. It covers:

1. Basic insert and point lookup, including a missing key
2. Inserting enough 64-byte values to split the root leaf into a two-level tree
3. Lookups after the split, including keys inserted before it
4. Range scans that cross leaf boundaries through `next_leaf`
5. WAL checkpoint

## Benchmark

`make bench` runs `src/benchmark.c`:

1. Inserts 200 records (`user000` … `user199`, 64-byte values)
2. Reads all 200 back and measures the cache hit rate for reads alone
3. Range-scans `user050 → user099`

It then prints these counters:

| Metric | Meaning |
| --- | --- |
| `cache_hits` / `cache_misses` | page requests served from or missing the LRU cache |
| `disk_reads` / `disk_writes` | page-sized reads and writes against the `.db` file |
| `wal_appends` | frames written to the log |
| `wal_replays` | recovery passes that replayed a non-empty log |
| `cache hit rate` / `read hit rate` | overall hit rate, and the hit rate for the read phase alone |

## Project structure

```
src/
├── pager.h / pager.c              page I/O, DB header, LRU cache + hash table, metrics
├── wal.h / wal.c                  log append, lookup, recovery, checkpoint
├── page_layout.h / page_layout.c  slotted page header, slot array, record insert/read
├── btree.h / btree.c              record encoding, ordered insert, splits, search, range scan
├── main.c                         test runner  → ./mini_db
└── benchmark.c                    benchmark    → ./mini_bench
Makefile
```

## Current limitations

MiniDB is a learning project, and some of SQLite's hard parts are not implemented yet:

- **Tree height is capped at 2.** Leaves split, but a full internal root does not split yet, so inserts start failing once the root runs out of space.
- **Reopening a database is not supported yet.** The root page number is not saved in the header, and `btree_init` re-initializes the root on every open.
- **Splits are not atomic.** Each page of a split is logged as a separate frame, so a crash partway through a split can leave the tree inconsistent.
- **There are no delete or update operations.** Inserting a duplicate key adds a second record.
- **Several operations are linear scans.** Search inside a page is linear, and a cache miss scans the whole WAL for the newest copy of the page.
- **Records have size limits.** Keys must be under 256 bytes and records under 512 bytes.
- **There are no frame checksums or concurrency control.** The engine is single-threaded and calls `fsync` on every write.

## Roadmap

- [ ] Split internal nodes recursively so the tree can grow to any height
- [ ] Store the root page in the header and support reopening an existing database
- [ ] Binary search inside pages
- [ ] Delete and update, with page compaction
- [ ] Transactions: group a split's frames behind a commit record
- [ ] An in-memory WAL index (page → latest frame) to avoid scanning the log
- [ ] Frame checksums
- [ ] A small query layer or REPL on top of the B+Tree

## References

- [SQLite: Architecture](https://www.sqlite.org/arch.html)
- [SQLite: Database File Format](https://www.sqlite.org/fileformat.html)
- [SQLite: Write-Ahead Logging](https://www.sqlite.org/wal.html)
