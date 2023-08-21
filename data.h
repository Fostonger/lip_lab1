#pragma once

#include <stdint.h>

#include "database_manager.h"
#include "table.h"
#include "util.h"

typedef struct {
    bool valid;
    table *table;
    void **bytes;
    uint16_t size;
} data;

OPTIONAL(data)

maybe_data init_data(table *tb);
void release_data(data *dt);
result data_init_integer(data *dt, int32_t val);
result data_init_string(data *dt, const char* val);
result data_init_boolean(data *dt, bool val);
result data_init_float(data *dt, float val);
result set_data(data *dt);
