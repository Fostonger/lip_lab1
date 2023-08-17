#include <stdio.h>

#include "util.h"

static const char *const result_descriptions[] = {
        [OK] = "INFO: Program executed successful!"
};

void print_result( enum result result ) {
  printf("-->%s\n", result_descriptions[result]);
}