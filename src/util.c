#include <stdio.h>

#include "util.h"

static const char *const result_descriptions[] = {
    [OK] = "INFO: Program executed successful!",
    [MUST_BE_EMPTY] = "ERROR: The action you perform can be done only with empty entities!",
    [MALLOC_ERROR] = "ERROR: Couldn't allocate memory!",
    [NOT_ENOUGH_SPACE] = "ERROR: Row in table is not big enough to hold the value!",
    [DONT_EXIST] = "ERROR: The element you searching for does not exist",
    [CROSS_ON_JOIN] = "ERROR: You trying to join same table, that's inappropriate",
    [WRITE_ERROR] = "ERROR: Error occured while trying to write to database file",
    [READ_ERROR] = "ERROR: Error occured while trying to read database file",
    [DIFFERENT_DB] = "ERROR: the action you trying to perform meant to be inside single database, not with several of them!"
};

int8_t print_if_failure( result result ) {
    if (!result) return 1;
    printf("-->%s\n", result_descriptions[result]);
    return 0;
}
