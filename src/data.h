#pragma once

#include <stdint.h>

#include "database_manager.h"
#include "table.h"
#include "util.h"

typedef struct {
    table *table;
    char *bytes;
    uint16_t size;
} data;

typedef struct {
    union{
        int32_t int_value;
        float float_value;
        bool bool_value;
        char *string_value;
    };
} any_value;

OPTIONAL(data)

maybe_data init_data(table *tb);
maybe_data init_empty_data(table *tb);
void release_data(data *dt);
void clear_data(data *dt);
result data_init_integer(data *dt, int32_t val);
result data_init_string(data *dt, const char* val);
result data_init_boolean(data *dt, bool val);
result data_init_float(data *dt, float val);
result set_data(data *dt);
result delete_saved_row(data *dt);
void get_integer_from_data(data *dt, int32_t *dest, size_t offset);
void get_string_from_data(data *dt, char **dest, size_t data_offset);
void get_bool_from_data(data *dt, bool *dest, size_t offset);
void get_float_from_data(data *dt, float *dest, size_t offset);
void get_any_from_data(data *dt, any_value *dest, size_t offset, column_type type);
void update_string_data_for_row(data *dst, data *src);
bool has_next_data_on_page(page *cur_page, char *cur_data);
void print_data(data *dt);
result join_data(data *dst, data *dt1, data *dt2, const char *column_name, column_type type);
