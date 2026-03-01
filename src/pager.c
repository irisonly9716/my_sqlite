#include "pager.h"
#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <stdio.h>
#include <sys/stat.h>

typedef struct {
    uint32_t magic;
    uint32_t page_size;
    uint32_t page_count;
} DBHeader;



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

int pager_open(Pager *pager, const char *filename) {
    memset(pager, 0, sizeof(*pager));

    int fd = open(filename, O_RDWR | O_CREAT, 0644);
    if (fd < 0) {
        perror("open");
        return -1;
    }
    pager->fd = fd;

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
    return 0;
}

int pager_close(Pager *pager) {
    if (!pager) return 0;
    if (pager->fd >= 0) close(pager->fd);
    pager->fd = -1;
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
