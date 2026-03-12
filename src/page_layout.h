#pragma once
#include <stdint.h>
#include "pager.h"


#define NODE_LEAF 0
#define NODE_INTERNAL 1


typedef struct {
    // 0 = leaf, 1 = internal；为了区分是internal节点还是leaf
    // internal只装载 separator key + child pgno，leaf 才装载真正的 key/value record
    uint16_t node_type; 

    uint16_t slot_count; // 当前有多少条记录
    uint16_t free_start; // slot array 结束为止（向右增长）
    uint16_t free_end; // record data 起始位置 （向左增长）
    uint32_t leftmost_child;  // internal node 才有的字段，指向最左边的 child page；leaf node 这个字段不使用
    uint32_t next_leaf;      // 优化：为了可以scan range 增加一个属性 知道这个叶子的右兄弟是谁 leaf node only, 0 means none

} PageHeader;

typedef struct {
    uint16_t offset; // record 在page内的起始偏移（位置
    uint16_t length; // record 的长度
} Slot;

void slotted_page_init(Page *page);
int slotted_page_insert(Page *page, const void *record, uint16_t record_len);
int slotted_page_read(Page *page, uint16_t slot_index, void *out_buf, uint16_t *out_len);
