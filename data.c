#include <string.h>

#include "data.h"
#include "database_manager.h"

maybe_data init_data(table *tb) {
    data *dt = malloc(sizeof(data));
    if (dt == NULL) return (maybe_data) { .error=MALLOC_ERROR, .value=NULL };
    dt->valid = true;
    dt->bytes = malloc(tb->header->row_size);
    if (dt->bytes == NULL) return (maybe_data) { .error=MALLOC_ERROR, .value=NULL };
    dt->table = tb;
    dt->size = 0;
    return (maybe_data) { .error=OK, .value=dt };
}

void release_data(data *dt) {
    free(dt->bytes);
    free(dt);
}

result data_init_integer(data *dt, int32_t val) {
    if (dt->size + type_to_size(INT_32) > dt->table->header->row_size) return NOT_ENOUGH_SPACE;
    char *ptr = dt->bytes + dt->size;
    *( (int32_t*) ptr ) = val;
    dt->size += type_to_size(INT_32);
}

result data_init_string(data *dt, const char* val) {
    if (dt->size + type_to_size(STRING) > dt->table->header->row_size) return NOT_ENOUGH_SPACE;
    char *ptr = dt->bytes + dt->size;
    dt->size += type_to_size(STRING);

    size_t string_len = strlen(val);

    result is_enough_space = ensure_enough_space_string(dt->table, string_len);
    if (!is_enough_space) return is_enough_space;

    page *writable_page = dt->table->first_string_page_to_write;

    // Данные о строке заносим в таблицу: ссылка на страницу, в которой хранится строка, и отступ от начала страницы
    *( (page **) ptr ) = writable_page;
    *( (size_t*) ptr + 1 ) = writable_page->pgheader->data_offset;

    char *data = writable_page->data + writable_page->pgheader->data_offset;
    strcpy(data, val);

    writable_page->pgheader->data_offset += string_len;
}

result data_init_boolean(data *dt, bool val) {
    if (dt->size + type_to_size(BOOL) > dt->table->header->row_size) return NOT_ENOUGH_SPACE;
    char *ptr = dt->bytes + dt->size;
    *( (bool*) ptr ) = val;
    dt->size += type_to_size(BOOL);
}

result data_init_float(data *dt, float val) {
    if (dt->size + type_to_size(FLOAT) > dt->table->header->row_size) return NOT_ENOUGH_SPACE;
    char *ptr = dt->bytes + dt->size;
    *( (float*) ptr ) = val;
    dt->size += type_to_size(FLOAT);
}

result set_data(data *dt) {
    result is_enough_space = ensure_enough_space_table(dt->table, dt->size);
    if (!is_enough_space) return is_enough_space;

    void **data_ptr = dt->bytes;
    void *table_ptr = dt->table->first_string_page_to_write->data
                            + dt->table->header->column_amount * dt->table->header->row_size;
    memcpy(table_ptr, data_ptr, dt->size);

    return OK;
}
