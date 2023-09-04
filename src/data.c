#include <string.h>
#include <stdlib.h>

#include "data.h"
#include "database_manager.h"
#include "functional.h"

typedef struct {
    uint64_t    page;
    uint64_t    offset_to_ref;
    uint64_t    str_len;
    char        string[];
} string_in_storage;

typedef struct {
    uint64_t string_page_number;
    uint16_t offset;
} string_in_table_data;

maybe_data init_empty_data(table *tb) {
    data *dt = malloc(sizeof(data));
    if (dt == NULL) return (maybe_data) { .error=MALLOC_ERROR, .value=NULL };
    dt->table = tb;
    dt->size = 0;
    return (maybe_data) { .error=OK, .value=dt };
}

maybe_data init_data(table *tb) {
    maybe_data dt = init_empty_data(tb);
    if (dt.error) return dt;
    dt.value->bytes = malloc(tb->header->row_size);
    if (dt.value->bytes == NULL) return (maybe_data) { .error=MALLOC_ERROR, .value=NULL };
    return dt;
}

void release_data(data *dt) {
    free(dt->bytes);
    free(dt);
}

void clear_data(data *dt) {
    dt->size = 0;
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
    char *ptr = dt->bytes + dt->size;
    dt->size += type_to_size(STRING);

    size_t string_len = strlen(val);
    size_t string_data_len = sizeof(string_in_storage) + string_len + 1;

    maybe_page suitable_page = get_suitable_page(dt->table, string_data_len);
    if (suitable_page.error) return suitable_page.error;

    page *writable_page = suitable_page.value;

    // Данные о строке заносим в таблицу: ссылка на страницу, в которой хранится строка, и отступ от начала страницы
    string_in_table_data *string_in_table = (string_in_table_data *)ptr;
    string_in_table->offset = writable_page->pgheader->data_offset;
    string_in_table->string_page_number = writable_page->pgheader->page_number;

    string_in_storage *string_data_ptr = (string_in_storage *)((char *)writable_page->data + writable_page->pgheader->data_offset);
    string_data_ptr->str_len = string_len;

    char *data = string_data_ptr->string;
    strcpy(data, val);

    writable_page->pgheader->data_offset += string_data_len;
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

result data_init_any(data *dt, const any_value val, column_type type) {
    switch (type) {
    case INT_32:
        return data_init_integer(dt, val.int_value);
    case FLOAT:
        return data_init_float(dt, val.float_value);
    case BOOL:
        return data_init_boolean(dt, val.bool_value);
    case STRING:
        return data_init_string(dt, val.string_value);
    default:
        break;
    }
}

string_in_storage *get_next_string_data_on_page(string_in_storage *prev_string, page *pg_with_string_data) {
    if (!prev_string || !pg_with_string_data) return NULL;
    if (prev_string->page != pg_with_string_data->pgheader->page_number) return NULL;

    string_in_storage *next_string = (string_in_storage *)((char *)prev_string + sizeof(string_in_storage) + prev_string->str_len + 1);
    // Проверка, не является ли prev_string последней строкой на странице
    if ( (char *)next_string >= (char *)pg_with_string_data->data + pg_with_string_data->pgheader->data_offset ) return NULL;
    return (string_in_storage *)next_string;
}

void set_page_info_in_strings(data *dt, page *pg_with_string_ref, uint64_t offset_to_row) {
    page *pg_with_string_data = dt->table->first_string_page;
    for (size_t column_index = 0; column_index < dt->table->header->column_amount; column_index++) {
        column_header header = dt->table->header->columns[column_index];
        if (header.type != STRING) continue;

        size_t string_offset = offset_to_column(dt->table->header, header.name, STRING);
        string_in_table_data *string_ref = (string_in_table_data *)((char *)dt->bytes + string_offset);

        if (pg_with_string_data->pgheader->page_number < string_ref->string_page_number) {
            maybe_page page_with_string = get_page_by_number(dt->table->first_string_page, string_ref->string_page_number);
            if (page_with_string.error) return;
            pg_with_string_data = page_with_string.value;
        }

        string_in_storage *string_in_storage_ptr = (string_in_storage *)((char *)pg_with_string_data->data + string_ref->offset);
        string_in_storage_ptr->page = pg_with_string_ref->pgheader->page_number;
        string_in_storage_ptr->offset_to_ref = offset_to_row + string_offset;
    }
}

result set_data(data *dt) {
    result is_enough_space = ensure_enough_space_table(dt->table, dt->size);
    if (is_enough_space) return is_enough_space;

    char *data_ptr = dt->bytes;

    page *pg_to_write = dt->table->first_page_to_write;
    uint64_t offset_to_row = pg_to_write->pgheader->rows_count * dt->table->header->row_size;

    set_page_info_in_strings(dt, pg_to_write, offset_to_row);

    void **table_ptr = pg_to_write->data + offset_to_row;
    memcpy(table_ptr, data_ptr, dt->size);

    dt->table->first_page_to_write->pgheader->rows_count += 1;

    return OK;
}

result copy_data(data *dst, data *src) {
    for (size_t column_index = 0; column_index < src->table->header->column_amount; column_index++) {
        column_header header = src->table->header->columns[column_index];
        size_t offset = offset_to_column(src->table->header, header.name, header.type);
        any_value val;
        get_any_from_data(src, &val, offset, header.type);
        result copy_error = data_init_any(dst, val, header.type);
        if (copy_error) return copy_error;
    }
    return OK;
}

result join_data(data *dst, data *dt1, data *dt2, const char *column_name, column_type type) {
    if (dst == NULL || dt1 == NULL || dt2 == NULL || column_name == NULL) return DONT_EXIST;
    if (dst->bytes == dt1->bytes || dst->bytes == dt2->bytes || dt1->bytes == dt2->bytes) return CROSS_ON_JOIN;
    if (dst->table->header->column_amount != dt1->table->header->column_amount + dt2->table->header->column_amount - 1)
        return NOT_ENOUGH_SPACE;
    
    if (offset_to_column(dt1->table->header, column_name, type) == -1 || 
        offset_to_column(dt2->table->header, column_name, type) == -1) {    
        return DONT_EXIST;
    }

    result copy_error = copy_data(dst, dt1);
    if (copy_error) return copy_error;

    for (size_t column_index = 0; column_index < dt2->table->header->column_amount; column_index++) {
        column_header header = dt2->table->header->columns[column_index];
        if (header.type == type && !strcmp(column_name, header.name)) continue;
        size_t offset = offset_to_column(dt2->table->header, header.name, header.type);
        any_value val;
        get_any_from_data(dt2, &val, offset, header.type);
        copy_error = data_init_any(dst, val, header.type);
        if (copy_error) return copy_error;
    }

    return OK;
}

size_t delete_saved_string(page *page_string, uint16_t offset) {
    string_in_storage *string_to_delete = (string_in_storage *)((char *)page_string->data + offset);
    size_t delete_string_len = string_to_delete->str_len + sizeof(string_in_storage) + 1;
    string_in_storage *next_strings = (string_in_storage *)((char *)string_to_delete + delete_string_len);
    size_t bytes_to_end = PAGE_SIZE - offset;

    memmove(string_to_delete, next_strings, bytes_to_end);

    return (char *)next_strings - (char *)string_to_delete;
}

bool has_next_data_on_page(page *cur_page, char *cur_data) {
    return cur_data - (char *)cur_page->data < PAGE_SIZE 
            && ( cur_data - (char *)cur_page->data ) / cur_page->tbheader->row_size < cur_page->pgheader->rows_count;
}

void correct_string_references_on_page(table *tb, page *pg_with_string_data, char *cur_data, size_t bytes_moved, size_t string_offset ) {
    string_in_table_data *cur_string_ref = (string_in_table_data *) (cur_data + string_offset);
    if (cur_string_ref->string_page_number != pg_with_string_data->pgheader->page_number) return;

    string_in_storage *cur_string_data = (string_in_storage *)pg_with_string_data->data + cur_string_ref->offset;
    maybe_page pg_with_string_ref = get_page_by_number(tb->first_page, cur_string_data->page);
    if (pg_with_string_ref.error) return;
    while(cur_string_data != NULL) {
        if (cur_string_data->page != pg_with_string_ref.value->pgheader->page_number) {
            pg_with_string_ref = get_page_by_number(tb->first_string_page, cur_string_data->page);
            if (pg_with_string_ref.error) return;
        }

        cur_string_ref = (string_in_table_data *) ((char *)pg_with_string_ref.value->data + cur_string_data->offset_to_ref);

        cur_string_ref->offset -= bytes_moved;

        cur_string_data = get_next_string_data_on_page(cur_string_data, pg_with_string_data);
    }

    if (PAGE_SIZE - pg_with_string_data->pgheader->data_offset < MIN_VALUABLE_SIZE 
        && tb->first_string_page_to_write->pgheader->page_number > pg_with_string_data->pgheader->page_number) {
            tb->first_string_page_to_write = pg_with_string_data;
    }
}

result delete_strings_from_row(data *dt) {
    for (size_t column_index = 0; column_index < dt->table->header->column_amount; column_index++) {
        column_header header = dt->table->header->columns[column_index];
        if (header.type != STRING) continue;
        
        size_t string_offset = offset_to_column(dt->table->header, header.name, STRING);
        string_in_table_data *string_data = (string_in_table_data *)((char *)dt->bytes + string_offset);

        maybe_page page_with_string = get_page_by_number(dt->table->first_string_page, string_data->string_page_number);
        if (page_with_string.error) return page_with_string.error;
        size_t bytes_moved = delete_saved_string(page_with_string.value, string_data->offset);
        page_with_string.value->pgheader->data_offset -= bytes_moved;
        correct_string_references_on_page(dt->table, page_with_string.value, dt->bytes, bytes_moved, string_offset);
    }
    return OK;
}

result delete_saved_row(data *dt) {
    // result delete_string_result = delete_strings_from_row(dt);
    // if (delete_string_result) return delete_string_result;
    dt->table->first_page_to_write->pgheader->rows_count -= 1;

    void *data_ptr = dt->bytes;
    void *table_ptr = dt->table->first_page_to_write->data
                            + dt->table->first_page_to_write->pgheader->rows_count * dt->table->header->row_size;
    if (data_ptr != table_ptr) memcpy(data_ptr, table_ptr, dt->size);

    return OK;
}

void update_string_data_for_row(data *dst, data *src) {
    delete_saved_row(dst);
    set_data(src);
}

void get_integer_from_data(data *dt, int32_t *dest, size_t offset) {
    *dest = *((int32_t *) ((char *)dt->bytes + offset));
}

void get_string_from_data(data *dt, char **dest, size_t data_offset) {
    string_in_table_data *string_data_ptr = (string_in_table_data *)((char *)dt->bytes + data_offset);
    string_in_table_data *string_in_table = string_data_ptr;
    maybe_page page_with_string = get_page_by_number(dt->table->first_string_page, string_in_table->string_page_number);
    if (page_with_string.error) return;
    string_in_storage *target_string = (string_in_storage *)((char *)page_with_string.value->data + string_in_table->offset);
    *dest = target_string->string;
}

void get_bool_from_data(data *dt, bool *dest, size_t offset) {
    *dest = *((const bool *) dt->bytes + offset);
}

void get_float_from_data(data *dt, float *dest, size_t offset) {
    *dest = *((const float *) ((char *)dt->bytes + offset));
}

void get_any_from_data(data *dt, any_value *dest, size_t offset, column_type type) {
    switch (type) {
    case INT_32:
        get_integer_from_data(dt, &(dest->int_value), offset);
        break;
    case FLOAT:
        get_float_from_data(dt, &(dest->float_value), offset);
        break;
    case BOOL:
        get_bool_from_data(dt, &(dest->bool_value), offset);
        break;
    case STRING:
        get_string_from_data(dt, &(dest->string_value), offset);
        break;
    default:
        break;
    }
}

void print_data(data *dt) {
    size_t offset = 0;
    for (size_t column_index = 0; column_index < dt->table->header->column_amount; column_index++) {
        column_header header = dt->table->header->columns[column_index];
        
        any_value value;
        get_any_from_data(dt, &value, offset, header.type);
        switch (header.type) {
        case INT_32:
            printf("\t%d\t|", value.int_value);
            break;
        case FLOAT:
            printf("\t%f\t|", value.float_value);
            break;
        case BOOL:
            printf("\t%s\t|", value.bool_value ? "TRUE" : "FALSE");
            break;
        case STRING:
            printf("\t%s\t|", value.string_value);
            break;
        default:
            break;
        }
        offset += type_to_size(header.type);
    }
    printf("\n");
}
