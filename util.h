#pragma once

typedef enum {
    OK = 0,
    MUST_BE_EMPTY,
    MALLOC_ERROR
} result;

#define OPTIONAL(STRUCT_NAME)       \
    typedef struct {                \
        result error;               \
        STRUCT_NAME *value;         \
    } maybe_##STRUCT_NAME;          \
    UNWRAP_OR_ABORT(STRUCT_NAME)

#define UNWRAP_OR_ABORT(STRUCT_NAME)            \
    STRUCT_NAME *unwrap_##STRUCT_NAME(          \
                maybe_##STRUCT_NAME optional    \
            ) {                                 \
        if (!optional.error)                    \
            return optional.value;              \
        error(optional.error);                  \
        return NULL;                            \
    }

int8_t error( result result );
