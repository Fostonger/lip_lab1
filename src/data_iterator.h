#pragma once

#include "table.h"
#include "data.h"

typedef struct {
    table* tb;
    page* cur_page;
    uint16_t ptr;
    uint16_t rows_read_on_page;
    void **cur_data;
} data_iterator;

OPTIONAL(data_iterator)

// функция, по которой будет решаться, искомый ли это элемент
typedef bool (*predicate_func)(const void **);
typedef struct {
    result error;
    int16_t count;
} result_with_count;

maybe_data_iterator init_iterator(table* tb);
void release_iterator(data_iterator *iter);
bool has_next(data_iterator *iter);
bool seek_next(data_iterator* iter);
bool seek_next_where(data_iterator* iter, column_type type, const char *column_name, predicate_func predicate_function);
result_with_count delete_where(table*tb, column_type type, const char *column_name, predicate_func predicate_function);
result_with_count update_where(table* tb, column_type type, const char *column_name, predicate_func find_function, const void* updateVal);
maybe_data get_data(data_iterator* iter);
maybe_table join_table(table* tb1, table* tb2, const char* column_name_1, const char* column_name_2, column_type type);
