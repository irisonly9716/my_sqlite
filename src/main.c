#include <stdio.h>
#include <string.h>
#include "pager.h"
#include "wal.h"
#include "page_layout.h"

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

    // 启动恢复
    if (wal_recover(&wal, &pager) != 0) {
        printf("wal recover failed\n");
        return 1;
    }

    printf("opened, pages=%u\n", pager.num_pages);

    if (pager.num_pages < 2) {
        int pg = pager_allocate_page(&pager);
        printf("allocated page %d\n", pg);
    }

    // 通过 pager cache 获取 page1
    Page *page = pager_get_page(&pager, &wal, 1);
    if (!page) {
        printf("get page failed\n");
        return 1;
    }

    printf("before write, page1: %s\n", page->data);

    // 修改 page
    strcpy((char *)page->data, "hello from WAL (same run)");

    // 先写 WAL，保证 durability
    if (wal_append_page(&wal, 1, page->data) != 0) {
        printf("wal append failed\n");
        return 1;
    }

    // 标脏
    if (pager_mark_dirty(page) != 0) {
        printf("mark dirty failed\n");
        return 1;
    }

    printf("after write, page1: %s\n", page->data);
    printf("doing checkpoint...\n");
    if (wal_checkpoint(&wal, &pager) != 0) {
        printf("checkpoint failed\n");
        return 1;
    }
    printf("checkpoint done\n");

    wal_close(&wal);
    pager_close(&pager);
    return 0;
}
