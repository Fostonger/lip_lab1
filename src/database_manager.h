#pragma once

#include <stdio.h>
#include <stdbool.h>

typedef struct database_header database_header;
typedef struct page_header page_header;
typedef struct page page;
typedef struct database database;
typedef enum page_type page_type;

#include "table.h"
#include "util.h"

#define PAGE_SIZE 8192
#define MIN_VALUABLE_SIZE 31

struct database_header {
    uint16_t page_size;
    uint16_t first_free_page;
    uint16_t next_page_to_save_number;
};

enum page_type {
    STRING_PAGE,
    TABLE_DATA
};

struct page_header {
    uint64_t page_number;
    uint64_t next_page_number;
    uint16_t data_offset;
    uint16_t rows_count;
    page_type type;
    table_header table_header;
};

struct page {
    page_header *pgheader;
    page *next_page;
    void *data;
    table *tb;
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

maybe_page create_page(table *tb, page_type type);
maybe_page read_page_header(database *db, table *tb, uint16_t page_ordinal);
page *rearrange_page_order(page *pg_to_save);
maybe_page get_page_by_number(database *db, uint64_t page_ordinal);
maybe_page get_suitable_page(table *tb, size_t data_size, page_type type);
result ensure_enough_space_table(table *tb, size_t data_size, page_type type);
result write_page(page *pg);
void release_page(page *pg);
