#pragma once

typedef enum {
    OK = 0
} result;

#define OPTIONAL(STRUCT_NAME)   \
    typedef struct {            \
        result error;       \
        STRUCT_NAME value;      \
    } maybe_##STRUCT_NAME;

void print_result(result result);
