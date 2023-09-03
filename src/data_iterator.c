#include <stdlib.h>
#include <string.h>

#include "data_iterator.h"
#include "functional.h"

void get_integer(data_iterator *iter, int32_t *dest, size_t offset) {
    get_integer_from_data(iter->cur_data, dest, offset);
}

void get_string(data_iterator *iter, char **dest, size_t data_offset) {
    get_string_from_data(iter->cur_data, dest, data_offset);
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

bool seek_next_where(data_iterator *iter, column_type type, const char *column_name, closure clr) {
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
            if ( clr.func(clr.value1, &int_value) ) return true;
            break;
        case FLOAT:
            get_float(iter, &float_value, offset);
            if ( clr.func(clr.value1, &float_value) ) return true;
            break;
        case BOOL:
            get_bool(iter, &bool_value, offset);
            if ( clr.func(clr.value1, &bool_value) ) return true;
            break;
        case STRING:
            get_string(iter, &string_value, offset);
            if ( clr.func(clr.value1, string_value) ) return true;
            break;
        default:
            break;
        }

        get_next(iter);
    }
    return false;
}

result_with_count update_where(table* tb, column_type type, const char *column_name, closure clr, data *update_val) {
    maybe_data_iterator iterator = init_iterator(tb);
    if (iterator.error) return (result_with_count) { .error=iterator.error, .count=0 };
    int16_t update_count = 0;
    while (seek_next_where(iterator.value, type, column_name, clr)) {
        data *data_to_update = iterator.value->cur_data;
        update_string_data_for_row(data_to_update, update_val);
        update_count++;
    }

    release_iterator(iterator.value);
    return (result_with_count) { .error=OK, .count=update_count };
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

result_with_count delete_where(table *tb, column_type type, const char *column_name, closure clr) {
    maybe_data_iterator iterator = init_iterator(tb);
    if (iterator.error) return (result_with_count) { .error=iterator.error, .count=0 };
    int16_t delete_count = 0;
    while (seek_next_where(iterator.value, type, column_name, clr)) {
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

void reset_iterator(data_iterator *iter, table *tb) {
    iter->tb = tb;
    iter->cur_page = tb->first_page;
    iter->cur_data->bytes = iter->cur_page->data;
    iter->cur_data->size = iter->tb->header->row_size;
    iter->ptr = 0;
    iter->rows_read_on_page = 0;
}

void release_iterator(data_iterator *iter) {
    free(iter->cur_data);
    free(iter);
}

bool int_join_func(int32_t *value1, int32_t *value2) {
    return *value1 == *value2;
}

bool float_join_func(float *value1, float *value2) {
    return *value1 == *value2;
}

bool bool_join_func(bool *value1, bool *value2) {
    return *value1 == *value2;
}

bool string_join_func(char *value1, char *value2) {
    return !strcmp(value1, value2);
}

closure join_func(const void *value, column_type type) {
    closure clr;
    switch (type) {
    case INT_32:
        clr.value1 = (int32_t *)&value;
        clr.func = int_join_func;
        return clr;
    case FLOAT:
        clr.value1 = (float *)&value;
        clr.func = float_join_func;
        return clr;
    case STRING:
        clr.value1 = (char *)&value;
        clr.func = string_join_func;
        return clr;
    case BOOL:
        clr.value1 = (bool *)&value;
        clr.func = bool_join_func;
        return clr;
    default:
        break;
    }
}

maybe_table join_table(table *tb1, table *tb2, const char *column_name, column_type type, char *new_table_name) {
    maybe_table new_tb;
    maybe_data_iterator iterator1 = init_iterator(tb1);
    if (iterator1.error) {
        new_tb = (maybe_table) { .error=iterator1.error, .value=NULL };
        goto iterator1_release;
    }
    maybe_data_iterator iterator2 = init_iterator(tb2);
    if (iterator2.error) {
        new_tb = (maybe_table) { .error=iterator2.error, .value=NULL };
        goto iterator2_release;
    }

    new_tb = create_table(new_table_name);
    if (new_tb.error)
        goto iterator2_release;

    result join_columns_error = join_columns(new_tb.value, tb1, tb2, column_name, type);
    if (join_columns_error) {
        new_tb = (maybe_table) { .error=join_columns_error, .value=NULL };
        release_table(new_tb.value);
        goto joined_data_release;
    }

    size_t offset_to_column1 = offset_to_column(tb1->header, column_name, type);

    maybe_data joined_data = init_data(new_tb.value);
    if (joined_data.error) {
        new_tb = (maybe_table) { .error=joined_data.error, .value=new_tb.value };
        goto joined_data_release;
    }

    while (has_next(iterator1.value)) {
        void **value = (char *)iterator1.value->cur_data->bytes + offset_to_column1;
        closure join_closure = join_func(*value, type);

        while (seek_next_where(iterator2.value, type, column_name, join_closure)) {
            result join_data_error = join_data(
                joined_data.value, iterator1.value->cur_data,
                iterator2.value->cur_data, column_name, type);
            if (join_data_error) {
                new_tb = (maybe_table) { .error=join_data_error, .value=new_tb.value };
                goto joined_data_release;
            }
            result set_data_error = set_data(joined_data.value);
            if (join_data_error) {
                new_tb = (maybe_table) { .error=set_data_error, .value=new_tb.value };
                goto joined_data_release;
            }
        }
        get_next(iterator1.value);
    }

joined_data_release:
    release_data(joined_data.value);
iterator2_release:
    release_iterator(iterator2.value);
iterator1_release:
    release_iterator(iterator1.value);
    
    return new_tb;
}
