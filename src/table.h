#pragma once

#include <inttypes.h>

typedef enum column_type column_type;
typedef struct column_header column_header;
typedef struct table_header table_header;
typedef struct table table;

#include "util.h"

#define MAX_NAME_LENGTH 31

enum column_type {
    INT_32,
    FLOAT,
    STRING,
    BOOL
};

struct column_header {
    column_type type;
    char name[MAX_NAME_LENGTH];
};

struct table_header {
    uint8_t column_amount;
    uint16_t row_size;
    char name[MAX_NAME_LENGTH];
    column_header columns[];
};

#include "database_manager.h"

struct table {
    page *first_page;
    page *first_string_page;
    page *first_page_to_write;
    page *first_string_page_to_write;
    table_header* header;
};

OPTIONAL(table)

table *unwrap_table( maybe_table optional );

extern inline uint8_t type_to_size(column_type type);
maybe_table read_table(const char *tablename, database *db);
maybe_table create_table(const char *tablename);
size_t offset_to_column(table_header *tb, const char *column_name, column_type type);
void release_table(table *tb);
result add_column(table *tb, const char *column_name, column_type type);
result save_table(database *db, table *tb);
