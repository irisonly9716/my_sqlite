#pragma once
#include <stdint.h>

#define PAGE_SIZE 4096
#define DB_MAGIC 0x4D444230u  // 'MDB0'

typedef struct {
    int fd;
    uint32_t num_pages;
} Pager;

int pager_open(Pager *pager, const char *filename);
int pager_close(Pager *pager);

int pager_read_page(Pager *pager, uint32_t page_num, void *buffer);
int pager_write_page(Pager *pager, uint32_t page_num, void *buffer);
int pager_allocate_page(Pager *pager);
