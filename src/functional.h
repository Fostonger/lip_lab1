#pragma once

typedef struct {
    bool (*func)(void *value1, void *value2);
    void **value1;
} closure;