#include "wal.h"
#include "pager.h"
#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <stdio.h>
#include <sys/stat.h>
#include <stdlib.h> // for malloc/free


// -----------------------header helper -------------------
static int write_header(int fd, const DBHeader *h) {
    if (lseek(fd, 0, SEEK_SET) < 0) return -1;
    ssize_t n = write(fd, h, sizeof(*h));
    return (n == (ssize_t)sizeof(*h)) ? 0 : -1;
}

static int read_header(int fd, DBHeader *h) {
    if (lseek(fd, 0, SEEK_SET) < 0) return -1;
    ssize_t n = read(fd, h, sizeof(*h));
    return (n == (ssize_t)sizeof(*h)) ? 0 : -1;
}

// for LRU cache hashmap: manully hashing
static uint32_t page_hash(uint32_t pgno) {
    return pgno % HASH_SIZE;
}


// --------------------LRU helper -----------------------------------
// 在pager.cache中找有没有这个page 有的话返回这个*page 没有返回Null
static Page *pager_lookup_page(Pager *pager, uint32_t pgno) {
    uint32_t h = page_hash(pgno);
    PageNode *node = pager->page_table[h];

    while (node) {
        if (node->pgno == pgno) {
            return node->page;
        }
        node = node->next;
    }

    return NULL;
}

// 把新装入的page装进新的node 然后把node放进hash table
static int pager_table_insert(Pager *pager, Page *page) {
    uint32_t h = page_hash(page->pgno); // 这里hash出了一个hash key 

    PageNode *node = malloc(sizeof(PageNode));
    if (!node) return -1;

    node->pgno = page->pgno;
    node->page = page;
    node->next = pager->page_table[h];
    pager->page_table[h] = node;

    return 0;
}

// 通过node里面的pano 删掉hashtable中的节点 注意hashtable里面哈希冲突的值是链表
// 所以要删掉hashtable里面的node的话 还得在同个桶里面找到这个node
static void pager_table_remove(Pager *pager, uint32_t pgno) {
    uint32_t h = page_hash(pgno);

    PageNode *curr = pager->page_table[h];
    PageNode *prev = NULL;

    while (curr) {
        if (curr->pgno == pgno) {
            if (prev) {
                prev->next = curr->next;
            } else {
                pager->page_table[h] = curr->next;
            }
            free(curr);
            return;
        }

        prev = curr;
        curr = curr->next;
    }
}

// 把最新使用到的节点放到头部
static void lru_add_to_head(Pager *pager, Page *page) {
    page->prev = NULL;
    page->next = pager->lru_head;

    if (pager->lru_head) {
        pager->lru_head->prev = page;
    }

    pager->lru_head = page;

    if (pager->lru_tail == NULL) {
        pager->lru_tail = page;
    }
}

// 从cache中移掉节点
static void lru_remove(Pager *pager, Page *page) {
    if (page->prev) {
        page->prev->next = page->next;
    } else {
        pager->lru_head = page->next;
    }

    if (page->next) {
        page->next->prev = page->prev;
    } else {
        pager->lru_tail = page->prev;
    }

    page->prev = NULL;
    page->next = NULL;
}

// 移动page到头部
static void lru_move_to_head(Pager *pager, Page *page) {
    if (pager->lru_head == page) return;

    lru_remove(pager, page);
    lru_add_to_head(pager, page);
}

// -----------------------------pager static------------------------

// 如果cache满了 淘汰最久没用的page 也就是lru_tail
static int pager_evict(Pager *pager) {
    Page *victim = pager->lru_tail;
    if (!victim) return -1;

    // dirty page 先刷盘（写回data file）
    if (victim->is_dirty) {
        if (pager_write_page(pager, victim->pgno, victim->data) != 0) {
            return -1;
        }
    }

    // 从 LRU 链表删掉
    lru_remove(pager, victim);

    // 从 hash table 删掉
    pager_table_remove(pager, victim->pgno);

    free(victim);
    pager->cache_count--;

    return 0;
}



// ---------------------pager------------------------------


// 核心使用LRU的功能 把链路变成LRU - WAL - data file
// 现在这里是LRU层 如果cache里面没有这页 把这页数据读出来放进LRU cache

Page *pager_get_page(Pager *pager, WAL *wal, uint32_t pgno) {
    // 1. 先查 cache
    Page *page = pager_lookup_page(pager, pgno);
    if (page) {
        // 新增：如果命中chche 了 就把这个page移动到LRU头部 因为它是最新被访问的了
        // 并且记录名命中次数
        pager->cache_hits++;
        lru_move_to_head(pager, page);
        return page;
    }

    // 2. cache miss，满了就淘汰
    pager->cache_misses++; // 记录cache miss次数
    if (pager->cache_count >= pager->max_cache_pages) {
        if (pager_evict(pager) != 0) {
            return NULL;
        }
    }

    // 3. 分配新的 Page 对象
    page = malloc(sizeof(Page));
    if (!page) return NULL;

    page->pgno = pgno;
    page->is_dirty = 0;
    page->prev = NULL;
    page->next = NULL;

    // 4. 先查 WAL overlay
    int hit = wal_read_latest_page(wal, pgno, page->data);
    if (hit < 0) {
        free(page);
        return NULL;
    }

    // 5. WAL miss -> 从 data file 读
    if (hit == 0) {
        if (pager_read_page(pager, pgno, page->data) != 0) {
            free(page);
            return NULL;
        }
    }

    // 6. 插入 hash table
    if (pager_table_insert(pager, page) != 0) {
        free(page);
        return NULL;
    }

    // 7. 插入 LRU 头部
    lru_add_to_head(pager, page);
    pager->cache_count++;

    return page;
}


// 把page标记成脏页（有更新但未写入的
int pager_mark_dirty(Page *page) {
    if (!page) return -1;
    page->is_dirty = 1;
    return 0;
}

// 把脏页写进磁盘（先写WAL） 然后把是否脏页还原为否
int pager_flush_page(Pager *pager, Page *page) {
    if (!pager || !page) return -1;

    if (!page->is_dirty) {
        return 0;
    }

    if (pager_write_page(pager, page->pgno, page->data) != 0) {
        return -1;
    }

    page->is_dirty = 0;
    return 0;
}

// 把当前cache里的所有脏页写进磁盘
int pager_flush_all(Pager *pager) {
    if (!pager) return -1;

    Page *curr = pager->lru_head;
    while (curr) {
        if (pager_flush_page(pager, curr) != 0) {
            return -1;
        }
        curr = curr->next;
    }

    return 0;
}


int pager_open(Pager *pager, const char *filename) {
    memset(pager, 0, sizeof(*pager));

    int fd = open(filename, O_RDWR | O_CREAT, 0644);
    if (fd < 0) {
        perror("open");
        return -1;
    }
    pager->fd = fd;
    pager->cache_hits = 0;
    pager->cache_misses = 0;
    pager->disk_reads = 0;
    pager->disk_writes = 0;

    struct stat st;
    if (fstat(fd, &st) != 0) {
        perror("fstat");
        close(fd);
        return -1;
    }

    // 新文件：长度为 0
    if (st.st_size == 0) {
        DBHeader h = {0};
        h.magic = DB_MAGIC;
        h.page_size = PAGE_SIZE;
        h.page_count = 1; // page 0: header itself

        if (write_header(fd, &h) != 0) {
            perror("write_header");
            close(fd);
            return -1;
        }

        // 把文件扩到至少 1 页大小
        if (ftruncate(fd, PAGE_SIZE) != 0) {
            perror("ftruncate");
            close(fd);
            return -1;
        }

        pager->num_pages = 1;   
        pager->lru_head = NULL;
        pager->lru_tail = NULL;
        pager->cache_count = 0;
        pager->max_cache_pages = MAX_CACHE_PAGES;
        return 0;
    }

    // 旧文件：读 header
    DBHeader h;
    if (read_header(fd, &h) != 0) {
        perror("read_header");
        close(fd);
        return -1;
    }

    if (h.magic != DB_MAGIC) {
        fprintf(stderr, "Invalid DB magic\n");
        close(fd);
        return -1;
    }

    if (h.page_size != PAGE_SIZE) {
        fprintf(stderr, "Unsupported page size: %u\n", h.page_size);
        close(fd);
        return -1;
    }

    pager->num_pages = h.page_count;

    pager->lru_head = NULL;
    pager->lru_tail = NULL;
    pager->cache_count = 0;
    pager->max_cache_pages = MAX_CACHE_PAGES;

    for (int i = 0; i < HASH_SIZE; i++) {
        pager->page_table[i] = NULL;
    }
 
    return 0;
}

// 更新close 因为我们现在有了脏页 在我们关闭数据库的时候要刷掉这些脏页
// （把有更新的page写进磁盘（已封装先顺序写WAL checkpoint或重启时再写data file）
int pager_close(Pager *pager) {
    if (!pager) return 0;

    pager_flush_all(pager);

    Page *curr = pager->lru_head;
    while (curr) {
        Page *next = curr->next;
        free(curr);
        curr = next;
    }

    // 叫你malloc 必须得free所有
    for (int i = 0; i < HASH_SIZE; i++) {
        PageNode *node = pager->page_table[i];
        while (node) {
            PageNode *next = node->next;
            free(node);
            node = next;
        }
        pager->page_table[i] = NULL;
    }

    if (pager->fd >= 0) close(pager->fd);

    pager->fd = -1;  
    pager->num_pages = 0; // 别忘了刷pager + 清空LRU cache
    pager->cache_count = 0;
    pager->lru_head = NULL;
    pager->lru_tail = NULL;

    return 0;
}

int pager_read_page(Pager *pager, uint32_t page_num, void *buffer) {
    if (!pager || pager->fd < 0 || !buffer) return -1;

    off_t offset = (off_t)page_num * (off_t)PAGE_SIZE;

    if (lseek(pager->fd, offset, SEEK_SET) < 0) {
        perror("lseek(read)");
        return -1;
    }

    ssize_t n = read(pager->fd, buffer, PAGE_SIZE);
    if (n != PAGE_SIZE) {
        perror("read");
        return -1;
    }

    pager->disk_reads++; // 记录磁盘读次数
    return 0;
}


int pager_write_page(Pager *pager, uint32_t page_num, void *buffer) {
    if (!pager || pager->fd < 0 || !buffer) return -1;

    off_t offset = (off_t)page_num * (off_t)PAGE_SIZE;

    if (lseek(pager->fd, offset, SEEK_SET) < 0) {
        perror("lseek(write)");
        return -1;
    }

    ssize_t n = write(pager->fd, buffer, PAGE_SIZE);
    if (n != PAGE_SIZE) {
        perror("write");
        return -1;
    }

    // 为了验证持久化，先简单 fsync
    if (fsync(pager->fd) != 0) {
        perror("fsync");
        return -1;
    }
    pager->disk_writes++; // 记录磁盘写次数

    return 0;
}

int pager_allocate_page(Pager *pager) {
    if (!pager || pager->fd < 0) return -1;

    uint32_t new_pgno = pager->num_pages;

    off_t new_size = (off_t)(new_pgno + 1) * (off_t)PAGE_SIZE;

    if (ftruncate(pager->fd, new_size) != 0) {
        perror("ftruncate");
        return -1;
    }

    // 2) 初始化新页为 0（确保内容一致）
    unsigned char zero[PAGE_SIZE];
    memset(zero, 0, PAGE_SIZE);
    if (pager_write_page(pager, new_pgno, zero) != 0) {
        fprintf(stderr, "init new page failed\n");
        return -1;
    }

    DBHeader h;
    if (read_header(pager->fd, &h) != 0) return -1;

    h.page_count = new_pgno + 1;

    if (write_header(pager->fd, &h) != 0) return -1;

    if (fsync(pager->fd) != 0) return -1;

    pager->num_pages = h.page_count;

    return new_pgno;
}
