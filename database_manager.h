#pragma once

#include <stdio.h>
#include <stdbool.h>

#include "table.h"
#include "util.h"

typedef struct {
    uint16_t page_size;
    uint16_t first_free_page;
} database_header;

typedef struct {
    bool is_free;
    uint16_t data_offset;
    uint16_t page_number;
    uint16_t next_page_number;
    table_header table_header;
} page_header;

typedef struct {
    page_header *pgheader;
    page *next_page;
    void *data;
    table_header *tbheader;
} page;

typedef struct {
    FILE *file;
    database_header *header;
    page *first_loaded_page;
} database;

OPTIONAL(database)
OPTIONAL(page)

maybe_database initdb(FILE *file);
maybe_database read_db(FILE *file);
result write_db_header(database_header header, FILE *file);
void close_db(database *db);

maybe_page create_page(database *db);
maybe_page read_page(database *db, uint16_t page_ordinal);
result write_page(database *db, page *pg);
void close_page(page *pg);
