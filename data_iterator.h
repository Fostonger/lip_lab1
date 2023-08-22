#pragma once

#include "table.h"
#include "database_manager.h"
#include "data.h"

typedef struct {
    table* tb;
    page* cur_page;
    uint16_t ptr;
    uint16_t rows_read_on_page;
} data_iterator;

// функция, по которой будет решаться, искомый ли это элемент. 0 - нет, все остальное - да
typedef int (*predicate_func)(column_type, const char *, const void *);

data_iterator* init_iterator(table* tb);
bool has_next(data_iterator *iter);
bool seek_next(data_iterator* iter);
bool seek_next_where(data_iterator* iter, predicate_func predicate_function);
uint16_t delete_where(table*tb, predicate_func predicate_function);
uint16_t update_where(table* tb, predicate_func find_function,
                    const char* updateColName, const void* updateVal);
data *get_data(data_iterator* iter);
bool get_integer(data_iterator* iter, const char* columnName, int32_t* dest);
bool get_string(data_iterator* iter, const char* columnName, char** dest);
bool get_bool(data_iterator* iter, const char* columnName, bool* dest);
bool get_float(data_iterator* iter, const char* columnName, float* dest);
maybe_table join_table(table* tb1, table* tb2, const char* column_name_1, const char* column_name_2, column_type type);
