#include "page_layout.h"
#include "btree.h"
#include <string.h>
#include <stdio.h>

// decode and encode functions for key/value records in leaf nodes
/// 这个函数把 key/value 编码成二进制格式，写入 buf 中，并返回总长度
static int encode_record(
    const char *key,
    const char *value,
    unsigned char *buf,
    uint16_t *out_len
) {
    uint16_t klen = strlen(key);
    uint16_t vlen = strlen(value);

    uint16_t pos = 0;

    memcpy(buf + pos, &klen, sizeof(uint16_t));
    pos += sizeof(uint16_t);

    memcpy(buf + pos, key, klen);
    pos += klen;

    memcpy(buf + pos, &vlen, sizeof(uint16_t));
    pos += sizeof(uint16_t);

    memcpy(buf + pos, value, vlen);
    pos += vlen;

    *out_len = pos;

    return 0;
}
// 这个函数从 buf 中解码出 key/value，分别写入 key 和 value 中
static int decode_record(
    const unsigned char *buf,
    char *key,
    char *value
) {
    uint16_t pos = 0;

    uint16_t klen;
    memcpy(&klen, buf + pos, sizeof(uint16_t));
    pos += sizeof(uint16_t);

    memcpy(key, buf + pos, klen);
    key[klen] = '\0';
    pos += klen;

    uint16_t vlen;
    memcpy(&vlen, buf + pos, sizeof(uint16_t));
    pos += sizeof(uint16_t);

    memcpy(value, buf + pos, vlen);
    value[vlen] = '\0';

    return 0;
}

//       --------helpers 用来实现key ordered insert 和 search-------

// 取出 record 中的 key 部分，写入 key这个string中
static int record_get_key(
    const unsigned char *record,
    char *key
) {
    uint16_t pos = 0;
    uint16_t klen;

    memcpy(&klen, record + pos, sizeof(uint16_t));
    pos += sizeof(uint16_t);

    memcpy(key, record + pos, klen);
    key[klen] = '\0';

    return 0;
}

// 这个函数在 page 中找到合适的位置插入 key，返回 slot index
static int find_insert_pos(Page *page, const char *key)
{
    PageHeader *header = (PageHeader*)page->data;

    for (uint16_t i = 0; i < header->slot_count; i++) {

        unsigned char buf[512];
        uint16_t len;

        if (slotted_page_read(page, i, buf, &len) != 0)
            continue;

        char existing[256];
        record_get_key(buf, existing);

        if (strcmp(key, existing) < 0)
            return i;
    }

    return header->slot_count;
}

// ---------------------------helpers 用来帮助分裂tree------------------------------
// 给 internal page node 的初始化单独一个static方法来帮助创建 internal page node
// 优化：把最左子页初始化出来 这样我们每次分裂就是往右边分裂了 就不需要动左边的 child pgno
// 只需要在 internal node 的 record 里写 separator key 和右子页的 pgno 就行了
static void init_internal_page(Page *page, uint32_t leftmost_child) {
    memset(page->data, 0, PAGE_SIZE);

    PageHeader *header = (PageHeader *)page->data;
    header->node_type = NODE_INTERNAL;
    header->slot_count = 0;
    header->free_start = sizeof(PageHeader);
    header->free_end = PAGE_SIZE;
    header->leftmost_child = leftmost_child;
    header->next_leaf = 0;
}
// 后续我们会实现内部节点 这个函数会用到 
static int compare_record_key(const unsigned char *record, const char *key) {
    char existing[256];
    record_get_key(record, existing);
    return strcmp(existing, key);
}

// 为了 split，我们需要把 leaf page 里的所有 record 先收集出来。
// 这个函数把 page 中的 record 读出来，写入 records 数组中，返回 record 数量
typedef struct {
    unsigned char data[512];
    uint16_t len;
} TempRecord;

static int collect_leaf_records(Page *page, TempRecord *records, int max_records) {
    PageHeader *header = (PageHeader *)page->data;
    if (header->slot_count > max_records) return -1;

    for (uint16_t i = 0; i < header->slot_count; i++) {
        if (slotted_page_read(page, i, records[i].data, &records[i].len) != 0) {
            return -1;
        }
    }

    return header->slot_count;
}

// 把 我们暂时拿出来的 records 数组中的 record 按 key 的字典序排序
static void sort_temp_records(TempRecord *records, int count) {
    for (int i = 0; i < count; i++) {
        for (int j = i + 1; j < count; j++) {
            char key_i[256], val_i[256];
            char key_j[256], val_j[256];

            decode_record(records[i].data, key_i, val_i);
            decode_record(records[j].data, key_j, val_j);

            if (strcmp(key_i, key_j) > 0) {
                TempRecord tmp = records[i];
                records[i] = records[j];
                records[j] = tmp;
            }
        }
    }
}

// ----------------------------helpers for分裂节点------------------------------
// 先重置一个 leaf page，把它变成空的 slotted page
static void reset_leaf_page(Page *page) {
    memset(page->data, 0, PAGE_SIZE);
    slotted_page_init(page);
}

// 把records数组里面的record全都encode成内部节点的格式，写入buf中，并返回总长度
// 优化只encode右边的 child pgno 和 separator key，左边的 child pgno 放在 internal node header 的 leftmost_child 字段里
static int encode_internal_record(
    const char *sep_key,
    uint32_t right_pgno,
    unsigned char *buf,
    uint16_t *out_len
) {
    uint16_t klen = strlen(sep_key);
    uint16_t pos = 0;

    memcpy(buf + pos, &klen, sizeof(uint16_t));
    pos += sizeof(uint16_t);

    memcpy(buf + pos, sep_key, klen);
    pos += klen;

    memcpy(buf + pos, &right_pgno, sizeof(uint32_t));
    pos += sizeof(uint32_t);

    *out_len = pos;
    return 0;
}

// -----------------------------helpers for 内部节点----------------------------------
// 把buf中的内部节点的record解码出 separator key 和左右子页的 pgno，分别写入变量sep_key、right_pgno 中
// 解码内部节点 拿到sep_key 和右子页的 pgno 这个函数会在我们实现内部节点的搜索时用到
// 优化：不写左子页 只向右边分裂了，所以左子页的 pgno 放在 internal node header 的 leftmost_child 字段里
static int decode_internal_record(
    const unsigned char *buf,
    char *sep_key,
    uint32_t *right_pgno
) {
    uint16_t pos = 0;
    uint16_t klen = 0;

    memcpy(&klen, buf + pos, sizeof(uint16_t));
    pos += sizeof(uint16_t);

    memcpy(sep_key, buf + pos, klen);
    sep_key[klen] = '\0';
    pos += klen;

    memcpy(right_pgno, buf + pos, sizeof(uint32_t));

    return 0;
}

// 从 root page 这个 internal node 出发，根据 key 的值选择应该往哪个子页走，返回 child pgno
// 只有leftmost和右边节点后 找节点变成了找第一个key<separator key 如果是第0条 返回leftmost 如果没找到 返回最后一个rightpgno
// [leftmost, right1, right2, right3 ....]
static int choose_child_from_root(Page *root, const char *key, uint32_t *child_pgno) {
    PageHeader *header = (PageHeader *)root->data;

    if (header->node_type != NODE_INTERNAL) {
        return -1;
    }

    uint32_t current_child = header->leftmost_child;

    for (uint16_t i = 0; i < header->slot_count; i++) {
        unsigned char buf[512];
        uint16_t len = 0;

        if (slotted_page_read(root, i, buf, &len) != 0) {
            return -1;
        }

        char sep_key[256];
        uint32_t right_pgno = 0;
        decode_internal_record(buf, sep_key, &right_pgno);

        if (strcmp(key, sep_key) < 0) {
            *child_pgno = current_child;
            return 0;
        }

        current_child = right_pgno;
    }

    *child_pgno = current_child;
    return 0;
}

//  ------------------------helper for在分裂节点时搜索-------------------------------
// 在 leaf page 中搜索 key，找到就把 value 写入 out_value，设置 out_len，并返回 0；找不到返回 -1
// 目前就2个页，root page 和 leaf page，后续我们会有更多层的 internal node，这个函数也会被我们在分裂节点时用来搜索
static int leaf_search_page(Page *page, const char *key, char *out_value, uint16_t *out_len) {
    PageHeader *header = (PageHeader *)page->data;

    for (uint16_t i = 0; i < header->slot_count; i++) {
        unsigned char buf[512];
        uint16_t len = 0;

        if (slotted_page_read(page, i, buf, &len) != 0) {
            continue;
        }

        char k[256], v[256];
        decode_record(buf, k, v);

        if (strcmp(k, key) == 0) {
            strcpy(out_value, v);
            *out_len = strlen(v);
            return 0;
        }
    }

    return -1;
}

// 在 leaf page 中插入 key/value，保持 key 有序，如果 page 满了返回 -1
// 这个是把插入key/value的逻辑抽出来单独成一个函数 因为在leaf 才需要插入值
static int leaf_insert_page(BTree *tree, Page *page, uint32_t pgno, const char *key, const char *value) {
    unsigned char record[512];
    uint16_t len = 0;

    encode_record(key, value, record, &len);

    int pos = find_insert_pos(page, key);
    int slot = slotted_page_insert(page, record, len);

    if (slot < 0) {
        return -1; // page full
    }

    PageHeader *header = (PageHeader *)page->data;
    Slot *slots = (Slot *)(page->data + sizeof(PageHeader));

    Slot new_slot = slots[header->slot_count - 1];

    for (int i = header->slot_count - 1; i > pos; i--) {
        slots[i] = slots[i - 1];
    }

    slots[pos] = new_slot;

    if (wal_append_page(tree->wal, pgno, page->data) != 0) {
        return -1;
    }

    if (pager_mark_dirty(page) != 0) {
        return -1;
    }

    return 0;
}

// 实现分裂 root leaf 的函数，后续我们会在插入时调用它
static int btree_split_root_leaf(BTree *tree, const char *new_key, const char *new_value) {
    Page *left = pager_get_page(tree->pager, tree->wal, tree->root_page);
    if (!left) return -1;

    TempRecord records[128];
    int count = collect_leaf_records(left, records, 128); // 先把原来 leaf page 中的 record 全部收集出来，放在 records 数组中
    if (count < 0) return -1;

    // 新 record 也放进去
    unsigned char new_record[512];
    uint16_t new_len;
    encode_record(new_key, new_value, new_record, &new_len);

    memcpy(records[count].data, new_record, new_len); // 把新的 record 也放到 records 数组中
    records[count].len = new_len;
    count++;

    // 排序
    sort_temp_records(records, count); // 把 records 数组中的 record 按 key 的字典序排序

    // 分配右页
    int right_pgno = pager_allocate_page(tree->pager);
    if (right_pgno < 0) return -1;

    Page *right = pager_get_page(tree->pager, tree->wal, right_pgno);
    if (!right) return -1;

    // 重置左右 leaf
    reset_leaf_page(left);
    reset_leaf_page(right);

    // 左右分裂
    int mid = count / 2;
    // 把 records 数组中前半部分的 record 插入左页，后半部分的 record 插入右页
    for (int i = 0; i < mid; i++) {
        if (slotted_page_insert(left, records[i].data, records[i].len) < 0) return -1;
    }

    for (int i = mid; i < count; i++) {
        if (slotted_page_insert(right, records[i].data, records[i].len) < 0) return -1;
    }

    // 右页第一个 key 作为 separator
    char sep_key[256], sep_val[256];
    decode_record(records[mid].data, sep_key, sep_val);

    // ← 在这里插入 next_leaf 链表维护
    PageHeader *lh = (PageHeader *)left->data;
    PageHeader *rh = (PageHeader *)right->data;
    rh->next_leaf = lh->next_leaf;  // 右页继承左页原来的 next
    lh->next_leaf = right_pgno;     // 左页的 next 指向新右页

    // 分配新 root page，把 separator 和左右页的 pgno 写入新 root page
    int new_root_pgno = pager_allocate_page(tree->pager);
    if (new_root_pgno < 0) return -1;
    // 这里我们先简单处理，把 root page 也当成 slotted page，后续我们会加 node type 来区分内部节点和叶子节点
    Page *new_root = pager_get_page(tree->pager, tree->wal, new_root_pgno);
    if (!new_root) return -1;
    // 把新 root page 初始化成 slotted page 
    // 这里优化成了interval page node；和普通的叶子节点区分开来 她只存 separator key + child pgno
    init_internal_page(new_root, tree->root_page);
    // 把 separator key 和左右页的 pgno 编码成内部节点的 record，插入新 root page
    unsigned char internal_record[512];
    uint16_t internal_len;
    encode_internal_record(sep_key, right_pgno, internal_record, &internal_len);

    if (slotted_page_insert(new_root, internal_record, internal_len) < 0) return -1;

    // WAL + dirty
    if (wal_append_page(tree->wal, left->pgno, left->data) != 0) return -1;
    if (wal_append_page(tree->wal, right->pgno, right->data) != 0) return -1;
    if (wal_append_page(tree->wal, new_root_pgno, new_root->data) != 0) return -1;

    if (pager_mark_dirty(left) != 0) return -1;
    if (pager_mark_dirty(right) != 0) return -1;
    if (pager_mark_dirty(new_root) != 0) return -1;

    tree->root_page = new_root_pgno;
    return 0;
}


// -----------------------helper for 分裂叶子节点 + 把新的节点插入root internal node---------------------------
// 在 child_pgno 指向的 leaf page 中插入 key/value，如果 page 满了就分裂成两个 leaf page
// 并把 separator key 插入 root internal node 中
static int btree_split_child_leaf(
    BTree *tree,
    uint32_t child_pgno,
    const char *key,
    const char *value
) {
    // 先拿到 child page 也就是要插入的那个 leaf page
    Page *child = pager_get_page(tree->pager, tree->wal, child_pgno);
    if (!child) return -1;
    // 把 child page 中的 record 全部收集出来，放在 records 数组中
    PageHeader *header = (PageHeader*)child->data;

    int total = header->slot_count + 1;

    unsigned char records[128][512];
    uint16_t lens[128];
    char keys[128][256];

    // collect existing
    for (int i = 0; i < header->slot_count; i++) {
        slotted_page_read(child, i, records[i], &lens[i]);
        decode_record(records[i], keys[i], NULL);
    }

    // new record
    unsigned char newrec[512];
    uint16_t newlen;
    encode_record(key, value, newrec, &newlen);
    // 把新的 record 也放到 records 数组中，keys 数组中也存一下 key，方便后续排序
    memcpy(records[header->slot_count], newrec, newlen);
    lens[header->slot_count] = newlen;
    strcpy(keys[header->slot_count], key);

    // sort records by key 找出 separator key 也就是中间那个 key 方便后续插入到 internal node 中
    for (int i = 0; i < total - 1; i++) {
        for (int j = i + 1; j < total; j++) {
            if (strcmp(keys[i], keys[j]) > 0) {

                char tk[256];
                strcpy(tk, keys[i]);
                strcpy(keys[i], keys[j]);
                strcpy(keys[j], tk);

                uint16_t tl = lens[i];
                lens[i] = lens[j];
                lens[j] = tl;

                unsigned char tr[512];
                memcpy(tr, records[i], lens[j]);
                memcpy(records[i], records[j], lens[i]);
                memcpy(records[j], tr, lens[j]);
            }
        }
    }
    // 分裂成两个 leaf page，前半部分的 record 插入左页，后半部分的 record 插入右页
    int mid = total / 2;

    // reset left page 数据已经取出 把原来的 leaf page 重置一下 变成空的 slotted page
    uint32_t old_next = ((PageHeader *)child->data)->next_leaf;
    reset_leaf_page(child);
    // 把 records 数组中前半部分的 record 插入左页，后半部分的 record 插入右页
    for (int i = 0; i < mid; i++) {
        slotted_page_insert(child, records[i], lens[i]);
    }

    // allocate right page
    int right_pgno = pager_allocate_page(tree->pager);

    Page *right = pager_get_page(tree->pager, tree->wal, right_pgno);

    reset_leaf_page(right);

    for (int i = mid; i < total; i++) {
        slotted_page_insert(right, records[i], lens[i]);
    }

    // 把叶子节点的链表连接上 维护 next_leaf 链表
    PageHeader *ch = (PageHeader *)child->data;
    PageHeader *rh = (PageHeader *)right->data;

    rh->next_leaf = old_next;
    ch->next_leaf = right_pgno;

    // sep key
    char sep_key[256];
    strcpy(sep_key, keys[mid]);

    // insert sep into root internal 取到root页 然后把sep_key编码 插入root internal node 中
    Page *root = pager_get_page(tree->pager, tree->wal, tree->root_page);

    unsigned char rec[512];
    uint16_t len;

    encode_internal_record(sep_key, right_pgno, rec, &len);
    // 在 root internal node 中找到合适的位置插入 separator key，保持 key 有序
    int pos = find_insert_pos(root, sep_key);

    int slot = slotted_page_insert(root, rec, len);

    if (slot < 0) {
        printf("internal root full (not implemented yet)\n");
        return -1;
    }
    // 把 root internal node 中插入 separator key 后面的 record 往后挪一下，给 separator key 腾出位置
    // PageHeader *rh = (PageHeader*)root->data;
    Slot *slots = (Slot*)(root->data + sizeof(PageHeader));
    // 这里我们直接把最后一个 record 复制一份放到最后面，然后再往后挪，最后把 separator key 放到 pos 位置
    PageHeader *root_header = (PageHeader *)root->data;
    Slot new_slot = slots[root_header->slot_count - 1];

    for (int i = root_header->slot_count - 1; i > pos; i--) {
        slots[i] = slots[i - 1];
    }

    slots[pos] = new_slot;
    // 记录 WAL，标记 dirty
    wal_append_page(tree->wal, child_pgno, child->data);
    wal_append_page(tree->wal, right_pgno, right->data);
    wal_append_page(tree->wal, tree->root_page, root->data);

    pager_mark_dirty(child);
    pager_mark_dirty(right);
    pager_mark_dirty(root);

    return 0;
}



// 查找应该从哪个 leaf page 开始搜索，这个函数会在我们实现搜索range时用到
static Page *btree_find_start_leaf(BTree *tree, const char *key) {
    Page *root = pager_get_page(tree->pager, tree->wal, tree->root_page);
    if (!root) return NULL;

    PageHeader *header = (PageHeader *)root->data;

    if (header->node_type == NODE_LEAF) {
        return root;
    }

    if (header->node_type == NODE_INTERNAL) {
        uint32_t child_pgno = 0;
        if (choose_child_from_root(root, key, &child_pgno) != 0) {
            return NULL;
        }

        return pager_get_page(tree->pager, tree->wal, child_pgno);
    }

    return NULL;
}

int btree_init(BTree *tree, Pager *pager, WAL *wal, uint32_t root_page) {
    if (!tree || !pager || !wal) return -1;

    tree->pager = pager;
    tree->wal = wal;
    tree->root_page = root_page;

    // 确保 root page 存在
    while (pager->num_pages <= root_page) {
        if (pager_allocate_page(pager) < 0) {
            return -1;
        }
    }

    // 取到 root page
    Page *page = pager_get_page(pager, wal, root_page);
    if (!page) return -1;

    // 这里先简单处理：把 root 初始化成 slotted page
    // Day10 Step2 以后我们会加 node type / leaf header
    slotted_page_init(page);

    // 记录 WAL
    if (wal_append_page(wal, root_page, page->data) != 0) {
        return -1;
    }

    // 标记 dirty
    if (pager_mark_dirty(page) != 0) {
        return -1;
    }

    return 0;
}

// 实现插入接口，优化为 key ordered insert，保持 leaf node 内的 record 是有序的 + 页满了就分裂
// 再优化：如果 root page 是 leaf node 就分裂成两个 leaf node + 一个 internal node 作为新的 root；
// 如果 root page 已经是 internal node 就先找到应该往哪个子页走了，再在对应的 leaf page 中插入
// ✨day13优化成如果 leaf page 满了就向右分裂
// internal node 满了先不处理，后续我们会实现递归分裂
int btree_insert(BTree *tree, const char *key, const char *value) {
    if (!tree || !key || !value) return -1;

    Page *root = pager_get_page(tree->pager, tree->wal, tree->root_page);
    if (!root) return -1;

    PageHeader *header = (PageHeader *)root->data;

    // 情况1：root 还是 leaf
    if (header->node_type == NODE_LEAF) {
        int rc = leaf_insert_page(tree, root, tree->root_page, key, value);
        if (rc == 0) return 0;

        // leaf full -> split root leaf 分裂成两个 leaf node + 一个 internal node 作为新的 root
        return btree_split_root_leaf(tree, key, value);
    }

    // 情况2：root 已经是 internal 先按有序key找到该往哪个子页走了，再在对应的 leaf page 中插入；
    // 如果 leaf page 满了就分裂成两个 leaf node，并把 separator key 插入 root internal node 中
    if (header->node_type == NODE_INTERNAL) {
        uint32_t child_pgno = 0;
        if (choose_child_from_root(root, key, &child_pgno) != 0) {
            return -1;
        }

        Page *child = pager_get_page(tree->pager, tree->wal, child_pgno);
        if (!child) return -1;

        // 插入叶子节点中 child leaf insert
        int rc = leaf_insert_page(tree, child, child_pgno, key, value);
        if (rc == 0) return 0;

        // ✅ child leaf 满了，用 split_child_leaf 分裂并插回 root
        return btree_split_child_leaf(tree, child_pgno, key, value);

    return -1;}
}

// 实现搜索接口，优化成在 leaf page 中搜索 key，找到就把 value 写入 out_value，设置 out_len，并返回 0；找不到返回 -1
// 不在root中搜了 在leaf page中搜 因为现在只有两层 
//后续我们会有更多层的internal node 那时候我们就需要在internal node中根据key的值选择往哪个子页走了
int btree_search(BTree *tree, const char *key, char *out_value, uint16_t *out_len) {
    if (!tree || !key || !out_value || !out_len) return -1;
    // 先从 root page 出发，看看是往左子页走还是右子页走
    Page *root = pager_get_page(tree->pager, tree->wal, tree->root_page);
    if (!root) return -1;
    // 这里我们先简单处理，假设只有两层：一个 root internal node + 两个 leaf node；
    // 后续我们会有更多层的 internal node，那时候我们就需要在 internal node 中根据 key 的值选择往哪个子页走了
    PageHeader *header = (PageHeader *)root->data;
    
    if (header->node_type == NODE_LEAF) {
        return leaf_search_page(root, key, out_value, out_len);
    }

    if (header->node_type == NODE_INTERNAL) {
        uint32_t child_pgno = 0;
        if (choose_child_from_root(root, key, &child_pgno) != 0) {
            return -1;
        }
        // 根据 key 的值选择往哪个子页走了，拿到 child pgno 后在对应的 leaf page 中搜索 key
        Page *child = pager_get_page(tree->pager, tree->wal, child_pgno);
        if (!child) return -1;
        // 在 leaf page 中搜索 key，找到就把 value 写入 out_value，设置 out_len，并返回 0；找不到返回 -1
        return leaf_search_page(child, key, out_value, out_len);
    }

    return -1;
}

int btree_range_scan(BTree *tree, const char *start_key, const char *end_key) {
    if (!tree || !start_key || !end_key) return -1;
    // 先找到应该从哪个 leaf page 开始扫描
    Page *leaf = btree_find_start_leaf(tree, start_key);
    if (!leaf) return -1;
    // 顺序扫描 leaf page 中的 record，直到遇到 key 大于 start_key 的 record 就打印出来
    // 如果遇到 leaf page 的 next_leaf 字段不为 0 就继续往下一个 leaf page 扫描，直到 next_leaf 为 0
    while (leaf) {
        PageHeader *header = (PageHeader *)leaf->data;
        int done = 0;

        for (uint16_t i = 0; i < header->slot_count; i++) {
            unsigned char buf[512];
            uint16_t len = 0;

            if (slotted_page_read(leaf, i, buf, &len) != 0) {
                continue;
            }

            char key[256];
            char value[256];
            decode_record(buf, key, value);

            if (strcmp(key, end_key) > 0) {
                done = 1;
                break;
            }

            if (strcmp(key, start_key) >= 0) {
                printf("%s -> %s\n", key, value);
            }
        }
        // 如果遇到 leaf page 的 next_leaf 字段不为 0 就继续往下一个 leaf page 扫描，直到 next_leaf 为 0
        if (done || header->next_leaf == 0) {
            break;
        }
        // 更新leaf为下一个leaf继续扫描
        leaf = pager_get_page(tree->pager, tree->wal, header->next_leaf);
    }

    return 0;
}