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
#define MIN_VALUABLE_SIZE 31

struct database_header {
    uint16_t page_size;
    uint16_t first_free_page;
    uint16_t last_page_in_file_number;
};

struct page_header {
    uint64_t page_number;
    uint64_t next_page_number;
    uint16_t data_offset;
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
    page **all_loaded_pages;
    size_t loaded_pages_count;
    size_t loaded_pages_capacity;
};

OPTIONAL(database)
OPTIONAL(page)

maybe_database initdb(FILE *file, bool overwrite);
void release_db(database *db);

maybe_page create_page(table *tb);
maybe_page read_page(database *db, uint16_t page_ordinal);
maybe_page get_page_by_number(page *first_page, uint64_t page_ordinal);
maybe_page get_suitable_page(table *tb, size_t data_size);
result ensure_enough_space_table(table *tb, size_t data_size);
result write_page(database *db, page *pg);
void release_page(page *pg);
