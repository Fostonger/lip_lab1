#include <string.h>
#include <stdlib.h>

#include "data.h"
#include "database_manager.h"

maybe_data init_data(table *tb) {
    data *dt = malloc(sizeof(data));
    if (dt == NULL) return (maybe_data) { .error=MALLOC_ERROR, .value=NULL };
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
    char *ptr = (char *)dt->bytes + dt->size;
    *( (int32_t*) ptr ) = val;
    dt->size += type_to_size(INT_32);
    return OK;
}

result data_init_string(data *dt, const char* val) {
    if (dt->size + type_to_size(STRING) > dt->table->header->row_size) return NOT_ENOUGH_SPACE;
    char *ptr = (char *)dt->bytes + dt->size;
    dt->size += type_to_size(STRING);

    size_t string_len = strlen(val) + 1;

    maybe_page suitable_page = get_suitable_page(dt->table, string_len);
    if (suitable_page.error) return suitable_page.error;

    page *writable_page = suitable_page.value;

    // Данные о строке заносим в таблицу: ссылка на страницу, в которой хранится строка, и отступ от начала страницы
    string_in_table_data *string_in_table = ptr;
    string_in_table->offset = writable_page->pgheader->data_offset;
    string_in_table->string_page_number = writable_page->pgheader->page_number;

    char *string_ptr = writable_page->data + writable_page->pgheader->data_offset;

    char *data = string_ptr;
    strcpy(data, val);

    writable_page->pgheader->data_offset += string_len;
    return OK;
}

result data_init_boolean(data *dt, bool val) {
    if (dt->size + type_to_size(BOOL) > dt->table->header->row_size) return NOT_ENOUGH_SPACE;
    char *ptr = (char *)dt->bytes + dt->size;
    *( (bool*) ptr ) = val;
    dt->size += type_to_size(BOOL);
    return OK;
}

result data_init_float(data *dt, float val) {
    if (dt->size + type_to_size(FLOAT) > dt->table->header->row_size) return NOT_ENOUGH_SPACE;
    char *ptr = (char *)dt->bytes + dt->size;
    *( (float*) ptr ) = val;
    dt->size += type_to_size(FLOAT);
    return OK;
}

result set_data(data *dt) {
    result is_enough_space = ensure_enough_space_table(dt->table, dt->size);
    if (is_enough_space) return is_enough_space;

    void **data_ptr = dt->bytes;
    void **table_ptr = dt->table->first_page_to_write->data
                            + dt->table->first_page_to_write->pgheader->rows_count * dt->table->header->row_size;
    memcpy(table_ptr, data_ptr, dt->size);

    dt->table->first_page_to_write->pgheader->rows_count += 1;

    return OK;
}

size_t delete_saved_string(page *page_string, uint16_t offset) {
    char *string_to_delete = (char *)page_string->data + offset;
    size_t delete_string_len = strlen(string_to_delete);
    char *next_strings = string_to_delete + delete_string_len;
    size_t bytes_to_end = PAGE_SIZE - offset;

    memmove(string_to_delete, next_strings, bytes_to_end);

    return next_strings - string_to_delete;
}

bool has_next_data_on_page(page *cur_page, char *cur_data) {
    return cur_data - (char *)cur_page->data < PAGE_SIZE 
            && ( cur_data - (char *)cur_page->data ) / cur_page->tbheader->row_size < cur_page->pgheader->rows_count;
}

void correct_string_references_on_page(table *tb, page *pg_with_string_ref, char *cur_data, size_t bytes_moved, size_t string_offset ) {

    while (has_next_data_on_page(pg_with_string_ref, cur_data)) {
        string_in_table_data *string_data = cur_data + string_offset;

        string_data->offset -= bytes_moved;
    }
    pg_with_string_ref->pgheader->data_offset -= bytes_moved;

    if (PAGE_SIZE - pg_with_string_ref->pgheader->data_offset < MIN_VALUABLE_SIZE 
        && tb->first_string_page->pgheader->page_number > pg_with_string_ref->pgheader->page_number) {
            tb->first_string_page = pg_with_string_ref;
    }
}

result delete_strings_from_row(data *dt) {
    for (size_t column_index = 0; column_index < dt->table->header->column_amount; column_index++) {
        column_header header = dt->table->header->columns[column_index];
        if (header.type != STRING) continue;
        
        size_t string_offset = offset_to_column(dt->table->header, header.name, STRING);
        string_in_table_data *string_data = dt->bytes + string_offset;

        maybe_page page_with_string = get_page_by_number(dt->table->first_string_page, string_data->string_page_number);
        if (page_with_string.error) return page_with_string.error;
        size_t bytes_moved = delete_saved_string(page_with_string.value, string_data->offset);
        correct_string_references_on_page(dt->table, page_with_string.value, dt->bytes, bytes_moved, string_offset);
    }
    return OK;
}

result delete_saved_row(data *dt) {
    result delete_string_result = delete_strings_from_row(dt);
    if (delete_string_result) return delete_string_result;
    
    void **data_ptr = dt->bytes;
    void **table_ptr = dt->table->first_page_to_write->data
                            + dt->table->first_page_to_write->pgheader->rows_count * dt->table->header->row_size;
    memcpy(data_ptr, table_ptr, dt->size);

    dt->table->first_page_to_write->pgheader->rows_count -= 1;

    return OK;
}
