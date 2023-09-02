#include <stdlib.h>

#include "data_iterator.h"

void get_integer(data_iterator *iter, int32_t *dest, size_t offset) {
    get_integer_from_data(iter->cur_data, dest, offset);
}

void get_string(data_iterator *iter, char **dest, size_t data_offset) {
    get_string_from_data(iter->cur_data, iter->tb->first_string_page, dest, data_offset);
}

void get_bool(data_iterator *iter, bool *dest, size_t offset) {
    get_bool_from_data(iter->cur_data, dest, offset);
}

void get_float(data_iterator *iter, float *dest, size_t offset) {
    get_float_from_data(iter->cur_data, dest, offset);
}

bool has_next(data_iterator *iter) {
    page *cur_page = iter->cur_page;
    void **cur_data = iter->cur_data->bytes;

    return has_next_data_on_page(cur_page, cur_data) || cur_page->next_page != NULL;
}

void get_next(data_iterator *iter) {
    iter->cur_data->bytes = (char *)iter->cur_data->bytes + iter->tb->header->row_size;
}

bool seek_next_where(data_iterator *iter, column_type type, const char *column_name, predicate_func predicate_function) {
    if (!has_next(iter)) return false;

    iter->cur_data->bytes = (char *)iter->cur_page->data;
    size_t offset = offset_to_column(iter->tb->header, column_name, type);
    
    int32_t int_value;
    float float_value;
    int8_t bool_value;
    char *string_value = NULL;
    while (has_next(iter)) {
        if ((char *)iter->cur_data->bytes - (char *)iter->cur_page->data > PAGE_SIZE) {
            iter->cur_page = iter->cur_page->next_page;
            iter->cur_data->bytes = iter->cur_page->data;
        }

        switch (type) {
        case INT_32:
            get_integer(iter, &int_value, offset);
            if ( predicate_function(&int_value) ) return true;
            break;
        case FLOAT:
            get_float(iter, &float_value, offset);
            if ( predicate_function(&float_value) ) return true;
            break;
        case BOOL:
            get_bool(iter, &bool_value, offset);
            if ( predicate_function(&bool_value) ) return true;
            break;
        case STRING:
            get_string(iter, &string_value, offset);
            if ( predicate_function(string_value) ) return true;
            break;
        default:
            break;
        }

        get_next(iter);
    }
    return false;
}

void print_table(table *tb) {
    maybe_data_iterator iterator = init_iterator(tb);
    if (iterator.error) return;
    printf("\n");
    print_columns(iterator.value->tb->header);
    while (has_next(iterator.value)) {
        print_data(iterator.value->cur_data);
        get_next(iterator.value);
    }
    release_iterator(iterator.value);
    printf("\n");
}

result_with_count delete_where(table *tb, column_type type, const char *column_name, predicate_func predicate_function) {
    maybe_data_iterator iterator = init_iterator(tb);
    if (iterator.error) return (result_with_count) { .error=iterator.error, .count=0 };
    int16_t delete_count = 0;
    while (seek_next_where(iterator.value, type, column_name, predicate_function)) {
        data *data_to_delete = iterator.value->cur_data;
        delete_saved_row(data_to_delete);
        delete_count++;
    }

    release_iterator(iterator.value);
    return (result_with_count) { .error=OK, .count=delete_count };
}

maybe_data_iterator init_iterator(table* tb) {
    data_iterator* iter = malloc(sizeof (data_iterator));
    if (iter == NULL) return (maybe_data_iterator) { .error=MALLOC_ERROR, .value=NULL };
    iter->tb = tb;
    iter->cur_page = tb->first_page;
    maybe_data cur_data = init_empty_data(tb);
    if (cur_data.error) return (maybe_data_iterator) { .error=cur_data.error, .value=NULL};
    cur_data.value->bytes = iter->cur_page->data;
    cur_data.value->size = iter->tb->header->row_size;
    iter->cur_data = cur_data.value;
    iter->ptr = 0;
    iter->rows_read_on_page = 0;
    return (maybe_data_iterator) { .error=OK, .value=iter };
}

void release_iterator(data_iterator *iter) {
    free(iter->cur_data);
    free(iter);
}
