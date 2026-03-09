CC = clang
CFLAGS = -Wall -Wextra -g

SRC = src/main.c src/pager.c src/wal.c src/page_layout.c
OUT = mini_db

all:
	$(CC) $(CFLAGS) $(SRC) -o $(OUT)

clean:
	rm -f $(OUT)
