#include <stdio.h>
#include <string.h>
#include "pager.h"

int main(void) {
    Pager pager;

    if (pager_open(&pager, "test.db") != 0) {
        printf("open failed\n");
        return 1;
    }

    printf("opened, pages=%u\n", pager.num_pages);

    // 如果只有 header，则分配 page1
    if (pager.num_pages < 2) {
        int pg = pager_allocate_page(&pager);
        printf("allocated page %d\n", pg);
    }

    // 写入 page1
    unsigned char buf[PAGE_SIZE];
    memset(buf, 0, PAGE_SIZE);
    strcpy((char*)buf, "hello from page1");

    if (pager_write_page(&pager, 1, buf) != 0) {
        printf("write failed\n");
        return 1;
    }

    // 读回
    unsigned char buf2[PAGE_SIZE];
    memset(buf2, 0, PAGE_SIZE);

    if (pager_read_page(&pager, 1, buf2) != 0) {
        printf("read failed\n");
        return 1;
    }

    printf("page1 says: %s\n", buf2);

    pager_close(&pager);
    return 0;
}
