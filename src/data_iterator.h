#pragma once

#include "table.h"
#include "data.h"
#include "functional.h"

typedef struct {
    table* tb;
    page* cur_page;
    uint16_t ptr;
    uint16_t rows_read_on_page;
    data* cur_data;
} data_iterator;

OPTIONAL(data_iterator)

typedef struct {
    result error;
    int16_t count;
} result_with_count;

maybe_data_iterator init_iterator(table* tb);
void reset_iterator(data_iterator *iter, table *tb);
void release_iterator(data_iterator *iter);
bool seek_next_where(data_iterator* iter, column_type type, const char *column_name, closure predicate_closure);
result_with_count delete_where(table*tb, column_type type, const char *column_name, closure predicate_closure);
result_with_count update_where(table* tb, column_type type, const char *column_name, closure predicate_closure, data *updateVal);
maybe_table join_table(table* tb1, table* tb2, const char* column_name, column_type type, char *new_table_name);
void print_table(table *tb);
