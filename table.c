#include <string.h>
#include <stdlib.h>

#include "table.h"

inline uint8_t type_to_size(column_type type) {
    switch (type) {
    case INT_32:
        return sizeof(int32_t);
    case BOOL:
        return sizeof(int8_t);
    case FLOAT:
        return sizeof(float);
    case STRING:
        return sizeof(size_t) * 2; // 1 - номер страницы; 2 - отступ от начала страницы до строки
    default:
        return 0;
    }
}

maybe_table create_table(const char *tablename) {
    table_header *header = malloc(sizeof(table_header));
    if (header == NULL) return (maybe_table) { .error=MALLOC_ERROR };

    header->column_amount = 0;
    header->row_size = 0;
    strcpy(header->name, tablename);

    table *tb = malloc(sizeof(table));
    if (tb == NULL) return (maybe_table) { .error=MALLOC_ERROR };

    tb->header = header;
    return (maybe_table) { .error=OK, .value=tb };
}

void release_table(table *tb) {
    free(tb->header);
    free(tb);
}

result add_column(table* tb, const char *column_name, column_type type) {
    if (tb->first_page != NULL) return MUST_BE_EMPTY;

    tb->header->column_amount += 1;
    tb->header->row_size += type_to_size(type);

    table_header *new_header = malloc( sizeof(table_header) + sizeof(column_header) * tb->header->column_amount );
    if (new_header == NULL) return MALLOC_ERROR;

    *new_header = *tb->header;
    for (size_t i = 0; i < tb->header->column_amount-1; i++) new_header->columns[i] = tb->header->columns[i];

    column_header *column = &new_header->columns[tb->header->column_amount-1];
    strcpy(column->name, column_name);
    column->type = type;

    free(tb->header);
    tb->header = new_header;
}
