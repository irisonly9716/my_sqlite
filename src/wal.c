#include "wal.h"
#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <stdio.h>
#include <sys/stat.h>

static int write_all(int fd, const void *buf, size_t n) {
    const unsigned char *p = (const unsigned char*)buf;
    size_t off = 0;
    while (off < n) {
        ssize_t w = write(fd, p + off, n - off);
        if (w <= 0) return -1;
        off += (size_t)w;
    }
    return 0;
}

static int read_all(int fd, void *buf, size_t n) {
    unsigned char *p = (unsigned char*)buf;
    size_t off = 0;
    while (off < n) {
        ssize_t r = read(fd, p + off, n - off);
        if (r <= 0) return -1;
        off += (size_t)r;
    }
    return 0;
}

int wal_open(WAL *wal, const char *wal_filename) {
    memset(wal, 0, sizeof(*wal));
    wal->fd = open(wal_filename, O_RDWR | O_CREAT, 0644);
    if (wal->fd < 0) {
        perror("open wal");
        return -1;
    }
    return 0;
}

int wal_close(WAL *wal) {
    if (!wal) return 0;
    if (wal->fd >= 0) close(wal->fd);
    wal->fd = -1;
    return 0;
}

int wal_append_page(WAL *wal, uint32_t page_num, const void *page_data) {
    if (!wal || wal->fd < 0 || !page_data) return -1;

    // append to end
    if (lseek(wal->fd, 0, SEEK_END) < 0) return -1;

    if (write_all(wal->fd, &page_num, sizeof(page_num)) != 0) return -1;
    if (write_all(wal->fd, page_data, PAGE_SIZE) != 0) return -1;

    // ensure durability of the log
    if (fsync(wal->fd) != 0) return -1;
    return 0;
}

int wal_read_latest_page(WAL *wal, uint32_t page_num, void *out_buf) {
    if (!wal || wal->fd < 0 || !out_buf) return -1;

    struct stat st;
    if (fstat(wal->fd, &st) != 0) return -1;

    off_t pos = 0;
    int found = 0;

    // scan from start; keep overwriting out_buf when match found, thus last match wins
    if (lseek(wal->fd, 0, SEEK_SET) < 0) return -1;

    while (pos + (off_t)sizeof(uint32_t) + (off_t)PAGE_SIZE <= st.st_size) {
        uint32_t pg;
        if (read_all(wal->fd, &pg, sizeof(pg)) != 0) return -1;

        unsigned char tmp[PAGE_SIZE];
        if (read_all(wal->fd, tmp, PAGE_SIZE) != 0) return -1;

        if (pg == page_num) {
            memcpy(out_buf, tmp, PAGE_SIZE);
            found = 1;
        }

        pos += (off_t)sizeof(uint32_t) + (off_t)PAGE_SIZE;
    }

    return found;
}

int wal_recover(WAL *wal, Pager *pager) {
    if (!wal || wal->fd < 0 || !pager) return -1;

    struct stat st;
    if (fstat(wal->fd, &st) != 0) return -1;

    if (st.st_size == 0) return 0; // nothing to recover

    if (lseek(wal->fd, 0, SEEK_SET) < 0) return -1;

    off_t pos = 0;
    while (pos + (off_t)sizeof(uint32_t) + (off_t)PAGE_SIZE <= st.st_size) {
        uint32_t pg;
        unsigned char buf[PAGE_SIZE];

        if (read_all(wal->fd, &pg, sizeof(pg)) != 0) return -1;
        if (read_all(wal->fd, buf, PAGE_SIZE) != 0) return -1;

        // ensure data file has enough pages
        while (pager->num_pages <= pg) {
            if (pager_allocate_page(pager) < 0) return -1;
        }

        // apply to data file (redo)
        if (pager_write_page(pager, pg, buf) != 0) return -1;

        pos += (off_t)sizeof(uint32_t) + (off_t)PAGE_SIZE;
    }

    // truncate WAL after successful replay
    if (ftruncate(wal->fd, 0) != 0) return -1;
    if (fsync(wal->fd) != 0) return -1;
    return 0;
}

int wal_checkpoint(WAL *wal, Pager *pager) {
    if (!wal || wal->fd < 0 || !pager) return -1;

    struct stat st;
    if (fstat(wal->fd, &st) != 0) return -1;

    if (st.st_size == 0) return 0;  // nothing to checkpoint

    if (lseek(wal->fd, 0, SEEK_SET) < 0) return -1;

    off_t pos = 0;

    while (pos + (off_t)sizeof(uint32_t) + (off_t)PAGE_SIZE <= st.st_size) {
        uint32_t pg;
        unsigned char buf[PAGE_SIZE];

        if (read_all(wal->fd, &pg, sizeof(pg)) != 0) return -1;
        if (read_all(wal->fd, buf, PAGE_SIZE) != 0) return -1;

        while (pager->num_pages <= pg) {
            if (pager_allocate_page(pager) < 0) return -1;
        }

        if (pager_write_page(pager, pg, buf) != 0) return -1;

        pos += (off_t)sizeof(uint32_t) + (off_t)PAGE_SIZE;
    }

    // 确保 data file 已持久化
    if (fsync(pager->fd) != 0) return -1;

    // 清空 WAL
    if (ftruncate(wal->fd, 0) != 0) return -1;
    if (fsync(wal->fd) != 0) return -1;

    return 0;
}
