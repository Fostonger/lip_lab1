#pragma once

typedef enum {
    OK = 0,
    MUST_BE_EMPTY,
    MALLOC_ERROR,
    NOT_ENOUGH_SPACE,
    DONT_EXIST,
    CROSS_ON_JOIN,
    WRITE_ERROR,
    READ_ERROR,
    DIFFERENT_DB,
    INVALID_PAGE_NUMBER
} result;

#define OPTIONAL(STRUCT_NAME)       \
    typedef struct {                \
        result error;               \
        STRUCT_NAME *value;         \
    } maybe_##STRUCT_NAME;

#define UNWRAP_OR_ABORT(STRUCT_NAME)            \
    STRUCT_NAME *unwrap_##STRUCT_NAME(          \
                maybe_##STRUCT_NAME optional    \
            ) {                                 \
        if (!optional.error)                    \
            return optional.value;              \
        print_if_failure(optional.error);                  \
        return NULL;                            \
    }

int8_t print_if_failure( result result );
