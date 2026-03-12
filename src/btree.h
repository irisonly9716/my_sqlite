#pragma once
#include <stdint.h>
#include "pager.h"
#include "wal.h"
#include "page_layout.h"

typedef struct {
    Pager *pager;
    WAL *wal;
    uint32_t root_page;
} BTree;

// 初始化一棵最小 B+Tree
// 如果 root page 不存在，会自动分配并初始化
int btree_init(BTree *tree, Pager *pager, WAL *wal, uint32_t root_page);

// 插入一个 key/value
int btree_insert(BTree *tree, const char *key, const char *value);

// 按 key 查找 value
// 成功返回 0，失败返回 -1
int btree_search(BTree *tree, const char *key, char *out_value, uint16_t *out_len);

// 实现 range scan，扫描从 start_key 开始, end_key 结束的所有 key/value，打印出来（先不返回数据）
int btree_range_scan(BTree *tree, const char *start_key,const char *end_key);



