#include <stdio.h>

#include "util.h"

static const char *const result_descriptions[] = {
    [OK] = "INFO: Program executed successful!",
    [MUST_BE_EMPTY] = "ERROR: The action you perform can be done only with empty entities!",
    [MALLOC_ERROR] = "ERROR: Couldn't allocate memory!",
    [NOT_ENOUGH_SPACE] = "ERROR: Row in table is not big enough to hold the value!"
};

int8_t is_success( result result ) {
    if (!result) return 1;
    printf("-->%s\n", result_descriptions[result]);
    return 0;
}
