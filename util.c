#include <stdio.h>

#include "util.h"

static const char *const result_descriptions[] = {
    [OK] = "INFO: Program executed successful!"
};

int8_t error( result result ) {
    printf("-->%s\n", result_descriptions[result]);
    if (!result) return 1;
    return 0;
}