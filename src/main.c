#include <stdio.h>
#include <string.h>
#include "pager.h"
#include "wal.h"
#include "btree.h"

static int passed = 0;
static int failed = 0;

static void check(const char *label, int ok) {
    if (ok) {
        printf("  [PASS] %s\n", label);
        passed++;
    } else {
        printf("  [FAIL] %s\n", label);
        failed++;
    }
}

int main(void) {
    Pager pager;
    WAL wal;
    BTree tree;

    if (pager_open(&pager, "test.db") != 0) { printf("open db failed\n"); return 1; }
    if (wal_open(&wal, "test.wal") != 0) { printf("open wal failed\n"); return 1; }
    if (wal_recover(&wal, &pager) != 0) { printf("wal recover failed\n"); return 1; }
    if (btree_init(&tree, &pager, &wal, 1) != 0) { printf("btree init failed\n"); return 1; }

    char out[256];
    uint16_t out_len = 0;

    // -------------------------------------------------------
    printf("\n[1] basic insert + search\n");
    // -------------------------------------------------------
    check("insert key_a", btree_insert(&tree, "key_a", "val_a") == 0);
    check("insert key_b", btree_insert(&tree, "key_b", "val_b") == 0);
    check("insert key_c", btree_insert(&tree, "key_c", "val_c") == 0);

    check("search key_a", btree_search(&tree, "key_a", out, &out_len) == 0 && strcmp(out, "val_a") == 0);
    check("search key_b", btree_search(&tree, "key_b", out, &out_len) == 0 && strcmp(out, "val_b") == 0);
    check("search key_c", btree_search(&tree, "key_c", out, &out_len) == 0 && strcmp(out, "val_c") == 0);
    check("search missing key", btree_search(&tree, "key_z", out, &out_len) == -1);

    // -------------------------------------------------------
    printf("\n[2] insert enough to trigger root leaf split\n");
    // -------------------------------------------------------
    char bigval[64];
    memset(bigval, 'x', sizeof(bigval) - 1);
    bigval[sizeof(bigval) - 1] = '\0';

    for (int i = 0; i < 20; i++) {
        char key[32];
        sprintf(key, "user%02d", i);
        btree_insert(&tree, key, bigval);
    }

    check("search after split user00", btree_search(&tree, "user00", out, &out_len) == 0);
    check("search after split user09", btree_search(&tree, "user09", out, &out_len) == 0);
    check("search after split user19", btree_search(&tree, "user19", out, &out_len) == 0);

    // -------------------------------------------------------
    printf("\n[3] insert enough to trigger child leaf split\n");
    // -------------------------------------------------------
    for (int i = 20; i < 60; i++) {
        char key[32];
        sprintf(key, "user%02d", i);
        btree_insert(&tree, key, bigval);
    }

    check("search after child split user20", btree_search(&tree, "user20", out, &out_len) == 0);
    check("search after child split user45", btree_search(&tree, "user45", out, &out_len) == 0);
    check("search after child split user59", btree_search(&tree, "user59", out, &out_len) == 0);
    check("original keys still findable key_a", btree_search(&tree, "key_a", out, &out_len) == 0 && strcmp(out, "val_a") == 0);

    // -------------------------------------------------------
    printf("\n[4] range scan\n");
    // -------------------------------------------------------
    printf("  range scan user10 -> user14:\n");
    int rc = btree_range_scan(&tree, "user10", "user14");
    check("range scan returns 0", rc == 0);

    printf("  range scan key_a -> key_c:\n");
    rc = btree_range_scan(&tree, "key_a", "key_c");
    check("range scan original keys returns 0", rc == 0);

    // -------------------------------------------------------
    printf("\n[5] WAL checkpoint\n");
    // -------------------------------------------------------
    check("wal checkpoint", wal_checkpoint(&wal, &pager) == 0);

    // -------------------------------------------------------
    printf("\n=== RESULT: %d passed, %d failed ===\n\n", passed, failed);

    wal_close(&wal);
    pager_close(&pager);
    return failed > 0 ? 1 : 0;
}