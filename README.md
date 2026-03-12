Mini SQLite (C)


A lightweight embedded database engine implemented in C, built from the storage layer upward.


Overview

This project implements a simplified SQLite-style storage engine with:

Page-based file storage

Pager abstraction over a database file

Write-ahead logging (WAL) for crash recovery

Checkpointing to reconcile WAL and the data file

LRU page cache for repeated-read optimization

Slotted-page layout for variable-length records

A simplified B+Tree supporting insert, point lookup, split, and range scan
