#pragma once
#include <stdint.h>
#include "pager.h"

typedef struct WAL {
    int fd;
} WAL;


int wal_open(WAL *wal, const char *wal_filename);
int wal_close(WAL *wal);

// append one page update to WAL, fsync WAL
int wal_append_page(WAL *wal, uint32_t page_num, const void *page_data);

// if WAL contains a page update for page_num, load the latest into out_buf and return 1.
// if not found, return 0. on error, return -1.
int wal_read_latest_page(WAL *wal, uint32_t page_num, void *out_buf);

// replay WAL into data file, then truncate WAL to 0
int wal_recover(WAL *wal, Pager *pager);

// 主动把wal里的data写回pager，并清空WAL，防止在一次启动中不断变大的情况
int wal_checkpoint(WAL *wal, Pager *pager);
