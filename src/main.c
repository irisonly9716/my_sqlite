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

    // 确保 page1 存在
    if (pager.num_pages < 2) {
        int pg = pager_allocate_page(&pager);
        printf("allocated page %d\n", pg);
    }

    // 获取 page1
    Page *page = pager_get_page(&pager, &wal, 1);
    if (!page) {
        printf("get page failed\n");
        return 1;
    }

    // 初始化为 slotted page
    slotted_page_init(page);

    // 插入两条 record
    int slot1 = slotted_page_insert(page, "alice", 5);
    int slot2 = slotted_page_insert(page, "bob", 3);

    if (slot1 < 0 || slot2 < 0) {
        printf("insert failed\n");
        return 1;
    }

    printf("inserted slots: %d, %d\n", slot1, slot2);

    // 读回 slot1
    char buf[100];
    uint16_t len = 0;

    if (slotted_page_read(page, slot1, buf, &len) != 0) {
        printf("read slot1 failed\n");
        return 1;
    }
    buf[len] = '\0';
    printf("slot %d = %s\n", slot1, buf);

    // 读回 slot2
    if (slotted_page_read(page, slot2, buf, &len) != 0) {
        printf("read slot2 failed\n");
        return 1;
    }
    buf[len] = '\0';
    printf("slot %d = %s\n", slot2, buf);

    // 修改后的整页写入 WAL
    if (wal_append_page(&wal, 1, page->data) != 0) {
        printf("wal append failed\n");
        return 1;
    }

    // 标记 dirty
    if (pager_mark_dirty(page) != 0) {
        printf("mark dirty failed\n");
        return 1;
    }

    // checkpoint
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
