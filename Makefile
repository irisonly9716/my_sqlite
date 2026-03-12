CC = clang
CFLAGS = -Wall -Wextra -g

SRC = src/main.c src/pager.c src/wal.c src/page_layout.c src/btree.c
BENCH_SRC = src/benchmark.c src/pager.c src/wal.c src/page_layout.c src/btree.c
OUT = mini_db
BENCH_OUT = mini_bench

all:
	$(CC) $(CFLAGS) $(SRC) -o $(OUT)

bench:
	$(CC) $(CFLAGS) $(BENCH_SRC) -o $(BENCH_OUT)
	rm -f bench.db bench.wal
	./$(BENCH_OUT)

clean:
	rm -f $(OUT) $(BENCH_OUT) bench.db bench.wal