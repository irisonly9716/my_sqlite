#pragma once
#include <stdint.h>
#include "pager.h"

typedef struct {
    uint16_t slot_count; // 当前有多少条记录
    uint16_t free_start; // slot array 结束为止（向右增长）
    uint16_t free_end; // record data 起始位置 （向左增长）
} PageHeader;

typedef struct {
    uint16_t offset; // record 在page内的起始偏移（位置
    uint16_t length; // record 的长度
} Slot;

void slotted_page_init(Page *page);
int slotted_page_insert(Page *page, const void *record, uint16_t record_len);
int slotted_page_read(Page *page, uint16_t slot_index, void *out_buf, uint16_t *out_len);
