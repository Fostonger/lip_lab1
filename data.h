#pragma once

#include <stdint.h>

#include "database_manager.h"
#include "table.h"
#include "util.h"

typedef struct __attribute__((packed)) {
    bool valid;
    int32_t nextInvalidRowPage;
    int32_t nextInvalidRowOrdinal;
} dataHeader;

typedef struct {
    dataHeader dataHeader;
    table *table;
    void **bytes;
    uint16_t ptr;
} data;

data *init_data(table *tb);
void data_init_integer(data *dt, int32_t val);
void data_init_string(data *dt, char* val);
void data_init_boolean(data *dt, bool val);
void data_init_float(data *dt, double val);
result set_data(data *dt);
