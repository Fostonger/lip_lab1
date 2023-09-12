#include <stdlib.h>
#include <string.h>

#include "data_iterator.h"
#include "functional.h"

void get_any(data_iterator *iter, any_value *dest, size_t offset, column_type type) {
    get_any_from_data(iter->cur_data, dest, offset, type);
}

bool has_next(data_iterator *iter) {
    page *cur_page = iter->cur_page;
    char *cur_data = iter->cur_data->bytes;

    return has_next_data_on_page(cur_page, cur_data) || cur_page->pgheader->next_page_number != 0;
}

void get_next(data_iterator *iter) {
    iter->cur_data->bytes += iter->tb->header->row_size;
}

bool seek_next_where(data_iterator *iter, column_type type, const char *column_name, closure clr) {
    if (!has_next(iter)) return false;

    // iter->cur_data->bytes = iter->cur_page->data;
    size_t offset = offset_to_column(iter->tb->header, column_name, type);
    
    while (has_next(iter)) {
        while (iter->cur_data->bytes - (char *)iter->cur_page->data >= iter->cur_page->pgheader->data_offset) {
            if (iter->cur_page->pgheader->next_page_number == 0) return false;
            
            maybe_page next_page = get_page_by_number(iter->tb->db, iter->tb, iter->cur_page->pgheader->next_page_number);
            iter->cur_page->next_page = next_page.value;
            iter->cur_page = next_page.value;
            iter->cur_data->bytes = iter->cur_page->data;
        }

        any_value value;
        get_any(iter, &value, offset, type);

        if ( clr.func(&clr.value1, &value) ) {
            if (type == STRING) free(value.string_value);
            return true;
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
        update_string_data_for_row(data_to_update, iterator.value->cur_page, update_val);
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
        result delete_error = delete_saved_row(data_to_delete, iterator.value->cur_page);
        if (delete_error) {
            release_iterator(iterator.value);
            return (result_with_count) { .error=delete_error, .count=0 };
        }
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

bool int_join_func(any_value *value1, any_value *value2) {
    return value1->int_value == value2->int_value;
}

bool float_join_func(any_value *value1, any_value *value2) {
    return value1->float_value == value2->float_value;
}

bool bool_join_func(any_value *value1, any_value *value2) {
    return value1->bool_value ^ value2->bool_value;
}

bool string_join_func(any_value *value1, any_value *value2) {
    return !strcmp(value1->string_value, value2->string_value);
}

closure join_func(any_value value, column_type type) {
    closure clr;
    clr.value1 = value;
    switch (type) {
    case INT_32:
        clr.func = int_join_func;
        return clr;
    case FLOAT:
        clr.func = float_join_func;
        return clr;
    case STRING:
        clr.func = string_join_func;
        return clr;
    case BOOL:
        clr.func = bool_join_func;
        return clr;
    default:
        break;
    }
}

maybe_table join_table(table *tb1, table *tb2, const char *column_name, column_type type, char *new_table_name) {
    if (tb1->db != tb2->db) return (maybe_table) { .error=DIFFERENT_DB, .value=NULL };
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

    new_tb = create_table(new_table_name, tb1->db);
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
        any_value val1;
        get_any(iterator1.value, &val1, offset_to_column1, type);
        closure join_closure = join_func(val1, type);

        while (seek_next_where(iterator2.value, type, column_name, join_closure)) {
            result join_data_error = join_data(
                joined_data.value, iterator1.value->cur_data,
                iterator2.value->cur_data, column_name, type);
            if (join_data_error) {
                new_tb = (maybe_table) { .error=join_data_error, .value=new_tb.value };
                if (type == STRING) free(val1.string_value);
                goto joined_data_release;
            }
            result set_data_error = set_data(joined_data.value);
            if (join_data_error) {
                new_tb = (maybe_table) { .error=set_data_error, .value=new_tb.value };
                if (type == STRING) free(val1.string_value);
                goto joined_data_release;
            }
            clear_data(joined_data.value);
            get_next(iterator2.value);
        }
        if (type == STRING) free(val1.string_value);
        reset_iterator(iterator2.value, tb2);
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

maybe_table filter_table(table*tb, column_type type, const char *column_name, closure clr) {
    maybe_table new_tb;
    maybe_data_iterator iterator = init_iterator(tb);
    if (iterator.error) {
        new_tb = (maybe_table) { .error=iterator.error, .value=NULL };
        goto iterator_release;
    }

    new_tb = create_table("filtered table", tb->db);
    if (new_tb.error)
        goto iterator_release;

    copy_columns(new_tb.value, tb);

    maybe_data filtered_data = init_data(new_tb.value);
    if (filtered_data.error) {
        release_table(new_tb.value);
        new_tb = (maybe_table) { .error=filtered_data.error, .value=NULL };
        goto filtered_data_release;
    }

    while (seek_next_where(iterator.value, type, column_name, clr)) {
        result copy_error = copy_data(filtered_data.value, iterator.value->cur_data);
        if (copy_error) {
            release_table(new_tb.value);
            new_tb = (maybe_table) { .error=filtered_data.error, .value=NULL };
            break;
        }
        set_data(filtered_data.value);
        clear_data(filtered_data.value);
        get_next(iterator.value);
    }

filtered_data_release:
    release_data(filtered_data.value);
iterator_release:
    release_iterator(iterator.value);
    
    return new_tb;
}
