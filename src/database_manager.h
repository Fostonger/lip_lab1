#pragma once

#include <stdio.h>
#include <stdbool.h>

typedef struct database_header database_header;
typedef struct page_header page_header;
typedef struct page page;
typedef struct database database;

#include "table.h"
#include "util.h"

#define PAGE_SIZE 8192

struct database_header {
    uint16_t page_size;
    uint16_t first_free_page;
};

struct page_header {
    uint16_t data_offset;
    uint16_t page_number;
    uint16_t next_page_number;
    uint16_t rows_count;
    table_header table_header;
};

struct page {
    page_header *pgheader;
    page *next_page;
    void *data;
    table_header *tbheader;
};

struct database {
    FILE *file;
    database_header *header;
    page *first_loaded_page;
};

OPTIONAL(database)
OPTIONAL(page)

maybe_database initdb(FILE *file);
maybe_database read_db(FILE *file);
result write_db_header(database_header header, FILE *file);
void close_db(database *db);

maybe_page create_page(table_header *tb_header);
maybe_page read_page(database *db, uint16_t page_ordinal);
result ensure_enough_space_string(table *tb, size_t data_size);
result ensure_enough_space_table(table *tb, size_t data_size);
result write_page(database *db, page *pg);
void release_page(page *pg);
