#include <stdio.h>
#include <string.h>
#include "pager.h"
#include "wal.h"
#include "btree.h"

#define BENCH_N 200

int main(void) {
    Pager pager;
    WAL wal;
    BTree tree;

    if (pager_open(&pager, "bench.db") != 0) { printf("open db failed\n"); return 1; }
    if (wal_open(&wal, "bench.wal") != 0) { printf("open wal failed\n"); return 1; }
    if (wal_recover(&wal, &pager) != 0) { printf("wal recover failed\n"); return 1; }
    if (btree_init(&tree, &pager, &wal, 1) != 0) { printf("btree init failed\n"); return 1; }

    char bigval[64];
    memset(bigval, 'x', sizeof(bigval) - 1);
    bigval[sizeof(bigval) - 1] = '\0';

    printf("=== BENCHMARK: %d records ===\n\n", BENCH_N);

    // 第一轮：插入
    printf("[1] inserting %d records...\n", BENCH_N);
    for (int i = 0; i < BENCH_N; i++) {
        char key[32];
        sprintf(key, "user%03d", i);
        btree_insert(&tree, key, bigval);
    }
    printf("    done. cache_hits=%llu  cache_misses=%llu  disk_writes=%llu\n\n",
        pager.cache_hits, pager.cache_misses, pager.disk_writes);

    // 重置计数器，单独测读
    uint64_t hits_before  = pager.cache_hits;
    uint64_t misses_before = pager.cache_misses;
    uint64_t reads_before  = pager.disk_reads;

    // 第二轮：重复读同一批 key，测 cache hit rate
    printf("[2] reading %d records...\n", BENCH_N);
    for (int i = 0; i < BENCH_N; i++) {
        char key[32];
        char out[256];
        uint16_t out_len = 0;
        sprintf(key, "user%03d", i);
        btree_search(&tree, key, out, &out_len);
    }

    uint64_t read_hits   = pager.cache_hits   - hits_before;
    uint64_t read_misses = pager.cache_misses  - misses_before;
    uint64_t read_disk   = pager.disk_reads    - reads_before;
    uint64_t read_total  = read_hits + read_misses;

    printf("    done. cache_hits=%llu  cache_misses=%llu  disk_reads=%llu\n\n",
        read_hits, read_misses, read_disk);

    // 第三轮：range scan
    printf("[3] range scan user050 -> user099...\n");
    btree_range_scan(&tree, "user050", "user099");
    printf("\n");

    // 总 metrics
    printf("=== TOTAL METRICS ===\n");
    printf("cache_hits      = %llu\n", pager.cache_hits);
    printf("cache_misses    = %llu\n", pager.cache_misses);
    printf("disk_reads      = %llu\n", pager.disk_reads);
    printf("disk_writes     = %llu\n", pager.disk_writes);
    printf("wal_appends     = %llu\n", wal.wal_appends);
    printf("wal_replays     = %llu\n", wal.wal_replays);

    uint64_t total = pager.cache_hits + pager.cache_misses;
    if (total > 0) {
        printf("cache hit rate  = %llu%%\n", pager.cache_hits * 100 / total);
    }
    if (read_total > 0) {
        printf("read hit rate   = %llu%%\n", read_hits * 100 / read_total);
    }

    if (wal_checkpoint(&wal, &pager) != 0) { printf("checkpoint failed\n"); return 1; }

    wal_close(&wal);
    pager_close(&pager);
    return 0;
}