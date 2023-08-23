#include <stdlib.h>

#include "data_iterator.h"

void get_integer(data_iterator *iter, int32_t *dest) {
    *dest = *((int32_t *)iter->cur_data);
}

void get_string(data_iterator *iter, char **dest) {
    page *page_with_string = (page *)iter->cur_data;
    size_t offset = (page *)iter->cur_data + 1;
    *dest = *((const char *)page_with_string->data + offset);
}

void get_bool(data_iterator *iter, bool *dest) {
    *dest = *((const bool *) iter->cur_data);
}

void get_float(data_iterator *iter, float *dest) {
    *dest = *((const float *)iter->cur_data);
}

bool has_next(data_iterator *iter) {
    page *cur_page = iter->cur_page;
    void **cur_data = iter->cur_data;

    return ( (char *)cur_data - (char *)cur_page->data < PAGE_SIZE 
            && ( (char *)cur_data - (char *)cur_page->data ) / iter->tb->header->row_size < cur_page->pgheader->rows_count ) 
        || cur_page->next_page != NULL;
}

bool seek_next_where(data_iterator *iter, column_type type, const char *column_name, predicate_func predicate_function) {
    if (!has_next(iter)) return false;

    iter->cur_data = (char *)iter->cur_page->data + offset_to_column(iter->tb, column_name, type);
    
    int32_t int_value;
    float float_value;
    int8_t bool_value;
    char **string_value = NULL;
    while (has_next(iter)) {
        if ((char *)iter->cur_data - (char *)iter->cur_page->data > PAGE_SIZE) {
            iter->cur_page = iter->cur_page->next_page;
            iter->cur_data = iter->cur_page->data;
        }

        switch (type) {
        case INT_32:
            get_integer(iter, &int_value);
            if ( predicate_function(&int_value) ) return true;
            break;
        case FLOAT:
            get_float(iter, &float_value);
            if ( predicate_function(&float_value) ) return true;
            break;
        case BOOL:
            get_bool(iter, &bool_value);
            if ( predicate_function(&bool_value) ) return true;
            break;
        case STRING:
            get_string(iter, string_value);
            if ( predicate_function(string_value) ) return true;
            break;
        default:
            break;
        }

        iter->cur_data = (char *)iter->cur_data + iter->tb->header->row_size;
    }
    return false;
}

maybe_data_iterator init_iterator(table* tb) {
    data_iterator* iter = malloc(sizeof (data_iterator));
    if (iter == NULL) return (maybe_data_iterator) { .error=MALLOC_ERROR, .value=NULL };
    iter->tb = tb;
    iter->cur_page = tb->first_page;
    iter->cur_data = iter->cur_page->data;
    iter->ptr = 0;
    iter->rows_read_on_page = 0;
    return (maybe_data_iterator) { .error=OK, .value=iter };
}
