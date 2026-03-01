#include <stdio.h>
#include <string.h>
#include "pager.h"
#include "wal.h"

int main(void) {
    Pager pager;
    WAL wal;

    if (pager_open(&pager, "test.db") != 0) {
        printf("open db failed\n");
        return 1;
    }
    if (wal_open(&wal, "test.wal") != 0) {
        printf("open wal failed\n");
        return 1;
    }

    // on startup: recover from WAL
    if (wal_recover(&wal, &pager) != 0) {
        printf("wal recovery failed\n");
        return 1;
    }

    printf("opened, pages=%u\n", pager.num_pages);

    // make sure page1 exists
    if (pager.num_pages < 2) {
        pager_allocate_page(&pager);
    }

    // write "hello" into page1 via WAL (not directly)
    unsigned char buf[PAGE_SIZE];
    memset(buf, 0, PAGE_SIZE);
    strcpy((char*)buf, "hello via WAL");

    if (wal_append_page(&wal, 1, buf) != 0) {
        printf("wal append failed\n");
        return 1;
    }

    // read page1: prefer WAL latest
    unsigned char out[PAGE_SIZE];
    int hit = wal_read_latest_page(&wal, 1, out);
    if (hit < 0) {
        printf("wal read failed\n");
        return 1;
    }
    if (hit == 0) {
        if (pager_read_page(&pager, 1, out) != 0) {
            printf("pager read failed\n");
            return 1;
        }
    }

    printf("page1 says: %s\n", out);

    wal_close(&wal);
    pager_close(&pager);
    return 0;
}
