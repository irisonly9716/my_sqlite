#include "page_layout.h"
#include <string.h>
#include <stdio.h>

// ------------------------helper-------------------------------
// 为了方便从page->data里面取出header和slot
static PageHeader *get_header(Page *page) {
    return (PageHeader *)page->data;
}

static Slot *get_slot(Page *page, uint16_t index) {
    return (Slot *)(page->data + sizeof(PageHeader) + index * sizeof(Slot));
}

// ------------------------初始化slotted page-------------------------------
// 初始化优化成leaf page，新建一个page时默认是leaf page，后续我们会在分裂成internal page时修改它的node_type
// 新增了leftmost_child和next_leaf字段，先初始化成0； next_leaf字段在我们实现scan range时会用到，先初始化成0表示没有右兄弟了
void slotted_page_init(Page *page) {
    PageHeader *header = get_header(page);
    header->node_type = NODE_LEAF;
    header->slot_count = 0;
    header->free_start = sizeof(PageHeader);
    header->free_end = PAGE_SIZE;
    header->leftmost_child = 0;
    header->next_leaf = 0;
}

// 插入record
int slotted_page_insert(Page *page, const void *record, uint16_t record_len) {
    PageHeader *header = get_header(page);

    uint16_t need = sizeof(Slot) + record_len;
    if (header->free_start + need > header->free_end) {
        return -1; // not enough space
    }

    // 1. record data 从右往左放
    header->free_end -= record_len;
    memcpy(page->data + header->free_end, record, record_len);

    // 2. 写 slot
    Slot *slot = get_slot(page, header->slot_count);
    slot->offset = header->free_end;
    slot->length = record_len;

    // 3. 更新 header
    header->slot_count += 1;
    header->free_start += sizeof(Slot);

    return header->slot_count - 1; // 返回 slot index
}


// 读取record

int slotted_page_read(Page *page, uint16_t slot_index, void *out_buf, uint16_t *out_len) {
    PageHeader *header = get_header(page);

    if (slot_index >= header->slot_count) {
        return -1;
    }

    Slot *slot = get_slot(page, slot_index);
    memcpy(out_buf, page->data + slot->offset, slot->length);

    if (out_len) {
        *out_len = slot->length;
    }

    return 0;
}
