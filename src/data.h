#pragma once

#include <stdint.h>

#include "database_manager.h"
#include "table.h"
#include "util.h"

typedef struct {
    table *table;
    void **bytes;
    uint16_t size;
} data;

typedef struct {
    uint64_t string_page_number;
    uint16_t offset;
} string_in_table_data;

OPTIONAL(data)

maybe_data init_data(table *tb);
void release_data(data *dt);
result data_init_integer(data *dt, int32_t val);
result data_init_string(data *dt, const char* val);
result data_init_boolean(data *dt, bool val);
result data_init_float(data *dt, float val);
result set_data(data *dt);
result delete_saved_row(data *dt);
void update_string_data_for_row(data *dt, void **row_start_in_table);
bool has_next_data_on_page(page *cur_page, char *cur_data);
