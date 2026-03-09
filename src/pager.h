#pragma once
#include <stdint.h>

#define PAGE_SIZE 4096
#define DB_MAGIC  0x4D444230u
#define MAX_CACHE_PAGES 16
#define HASH_SIZE 32

typedef struct WAL WAL;

typedef struct {
    uint32_t magic;
    uint32_t page_size;
    uint32_t page_count;
} DBHeader;

typedef struct Page {
    uint32_t pgno;	//本页页号
    unsigned char data[PAGE_SIZE];    //页内容
    int is_dirty;	//是否被修改但是未刷盘

    struct Page *prev;		// LRU双向链表
    struct Page *next;
} Page;

typedef struct PageNode {
    uint32_t pgno;
    Page *page;
    struct PageNode *next;
} PageNode;

typedef struct {
    int fd;
    uint32_t num_pages;
    				// C中没有内置hashmap所以要自己实现桶哈希
    PageNode *page_table[HASH_SIZE];
    				// LRU 页管理
    Page *lru_head;
    Page *lru_tail;

    int cache_count;	// cache内的size
    int max_cache_pages;	// 允许的最大size 要是超过 必须pop链表尾部节点
} Pager;

int pager_open(Pager *pager, const char *filename);
int pager_close(Pager *pager);

int pager_read_page(Pager *pager, uint32_t page_num, void *buffer);
int pager_write_page(Pager *pager, uint32_t page_num, void *buffer);
int pager_allocate_page(Pager *pager);
// 新增的API
Page *pager_get_page(Pager *pager, WAL *wal, uint32_t page_num);
int pager_mark_dirty(Page *page);
int pager_flush_page(Pager *pager, Page *page);
int pager_flush_all(Pager *pager);
