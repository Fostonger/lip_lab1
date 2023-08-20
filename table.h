#pragma once

#include <inttypes.h>

#include "util.h"
#include "database_manager.h"

#define MAX_NAME_LENGTH 31

typedef enum {
    INT_32,
    FLOAT,
    STRING,
    BOOL
} column_type;

typedef struct {
    column_type type;
    char name[MAX_NAME_LENGTH];
} column_header;

typedef struct {
    uint8_t column_amount;
    uint16_t row_size;
    char name[MAX_NAME_LENGTH];
    column_header columns[];
} table_header;

typedef struct {
    page *first_page;
    page *first_page_to_write;
    table_header* header;
} table;

OPTIONAL(table)

extern inline uint8_t type_to_size(column_type type);
maybe_table read_table(const char *tablename, database *db);
maybe_table create_table(const char *tablename);
void release_table(table *tb);
result add_column(table *tb, const char *column_name, column_type type);
result save_table(database *db, table *tb);
