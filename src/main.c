#include <stdio.h>
#include <string.h>
#include "pager.h"
#include "wal.h"

static int db_read_page(Pager *pager, WAL *wal, uint32_t pgno, void *out_buf) {
    int hit = wal_read_latest_page(wal, pgno, out_buf);
    if (hit < 0) return -1;
    if (hit == 1) return 0;
    return pager_read_page(pager, pgno, out_buf);
}

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

    // 启动时先恢复（Day5）
    if (wal_recover(&wal, &pager) != 0) {
        printf("wal recover failed\n");
        return 1;
    }

    printf("opened, pages=%u\n", pager.num_pages);

    // 确保 page1 存在
    if (pager.num_pages < 2) {
        int pg = pager_allocate_page(&pager);
        printf("allocated page %d\n", pg);
    }

    // 1) 先读一次 page1（此时应该来自data file）
    unsigned char out[PAGE_SIZE];
    memset(out, 0, PAGE_SIZE);
    if (db_read_page(&pager, &wal, 1, out) != 0) {
        printf("read failed\n");
        return 1;
    }
    printf("before write, page1: %s\n", out);

    // 2) 写入 WAL（不立即写data file）
    unsigned char buf[PAGE_SIZE];
    memset(buf, 0, PAGE_SIZE);
    strcpy((char*)buf, "hello from WAL (same run)");

    if (wal_append_page(&wal, 1, buf) != 0) {
        printf("wal append failed\n");
        return 1;
    }

    // 3) 立刻再读 page1（此时必须命中WAL，读到新内容）
    memset(out, 0, PAGE_SIZE);
    if (db_read_page(&pager, &wal, 1, out) != 0) {
        printf("read failed\n");
        return 1;
    }
    printf("after write, page1: %s\n", out);

    wal_close(&wal);
    pager_close(&pager);
    return 0;
}
