#pragma once

typedef enum {
    OK = 0
} result;

#define OPTIONAL(STRUCT_NAME)       \
    typedef struct {                \
        result error;               \
        STRUCT_NAME value;          \
    } maybe_##STRUCT_NAME;          \
    UNWRAP_OR_ABORT(STRUCT_NAME)

#define UNWRAP_OR_ABORT(STRUCT_NAME)            \
    STRUCT_NAME unwrap_##STRUCT_NAME(           \
                maybe_##STRUCT_NAME optional    \
            ) {                                 \
        if (!optional.error)                    \
            return optional.value;              \
        error(optional.error);                  \
        return (STRUCT_NAME) {};                \
    }

int8_t error( result result );
