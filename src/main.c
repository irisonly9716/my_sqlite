#include <stdio.h>
#include <string.h>
#include "pager.h"
#include "wal.h"
#include "btree.h"

// range scan 测试
int main(void) {
    Pager pager;
    WAL wal;
    BTree tree;

    if (pager_open(&pager, "test.db") != 0) {
        printf("open db failed\n");
        return 1;
    }
    if (wal_open(&wal, "test.wal") != 0) {
        printf("open wal failed\n");
        return 1;
    }
    if (wal_recover(&wal, &pager) != 0) {
        printf("wal recover failed\n");
        return 1;
    }

    if (btree_init(&tree, &pager, &wal, 1) != 0) {
        printf("btree init failed\n");
        return 1;
    }

    char bigval[50];
    memset(bigval, 'x', sizeof(bigval) - 1);
    bigval[sizeof(bigval) - 1] = '\0';

    for (int i = 0; i < 25; i++) {
        char key[32];
        snprintf(key, sizeof(key), "key%02d", i);

        if (btree_insert(&tree, key, bigval) != 0) {
            printf("insert failed at %d\n", i);
            break;
        }
        printf("inserted %s\n", key);
    }

    printf("root page now = %u\n", tree.root_page);

    char out[256];
    uint16_t out_len = 0;

    if (btree_search(&tree, "key00", out, &out_len) == 0) {
        printf("found key00\n");
    }
    if (btree_search(&tree, "key18", out, &out_len) == 0) {
        printf("found key18\n");
    }
    if (btree_search(&tree, "key24", out, &out_len) == 0) {
        printf("found key24\n");
    }

    printf("range scan from key10:\n");
    if (btree_range_scan(&tree, "key10", "key15") != 0) {
        printf("range scan failed\n");
    }

    if (wal_checkpoint(&wal, &pager) != 0) {
        printf("checkpoint failed\n");
        return 1;
    }

    wal_close(&wal);
    pager_close(&pager);
    return 0;
}