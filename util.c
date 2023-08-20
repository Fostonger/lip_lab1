#include <stdio.h>

#include "util.h"

static const char *const result_descriptions[] = {
    [OK] = "INFO: Program executed successful!",
    [MUST_BE_EMPTY] = "ERROR: The action you perform can be done only with empty entities!",
    [MALLOC_ERROR] = "ERROR: Couldn't allocate memory!"
};

int8_t error( result result ) {
    printf("-->%s\n", result_descriptions[result]);
    if (!result) return 1;
    return 0;
}
