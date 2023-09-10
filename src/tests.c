#include "tests.h"
#include <string.h>
#include <time.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

typedef struct {
    any_value   value;
    column_type type;
} any_typed_value;

typedef struct {
    any_typed_value *value;
    result *error;
} packed_any_value_with_error;

result test_table_creation(database *db) {
    maybe_table tb = create_table("test table", db);
    release_table(tb.value);
    return tb.error;
}

void make_image_from_csv(const char *path_to_csv) {
    char *init_str = "python3 writing_script.py -i ";
    char *str_end = " -o chart_images/";
    char *final_str = (char *)malloc(strlen(path_to_csv) + strlen(init_str) + strlen(str_end));
    strcpy(final_str, init_str);
    strcpy(final_str + strlen(init_str), path_to_csv);
    strcpy(final_str + strlen(init_str) + strlen(path_to_csv), str_end);

    system(final_str);
}

maybe_table create_test_table(database *db, const char *name, column_header *headers, size_t headers_count) {
    maybe_table t1 = create_table(name, db);
    if (t1.error) goto end;

    for (size_t i = 0; i < headers_count; i++) {
        t1.error = add_column(t1.value, headers[i].name, headers[i].type);
        if (t1.error) goto end;
    }
end:
    return t1;
}

result fill_table_with_data(database *db, table *tb, size_t columns_count, size_t rows_count, any_typed_value values[rows_count][columns_count]) {
    result filling_error = OK;
    maybe_data dt = init_data(tb);
    if (dt.error) {
        filling_error = dt.error;
        goto release_dt;
    }
    for (size_t rows = 0; rows < rows_count; rows++) {
        for (size_t columns = 0; columns < columns_count; columns ++) {
            filling_error = data_init_any(dt.value, values[rows][columns].value, values[rows][columns].type);
            if (filling_error) goto release_dt;
        }
        filling_error = set_data(dt.value);
        if (filling_error) goto release_dt;
        clear_data(dt.value);
    }

release_dt:
    release_data(dt.value);
    return filling_error;
}

result test_adding_columns(database *db) {
    column_header headers[4] = {
        (column_header) { .type=INT_32, .name="ints"},
        (column_header) { .type=BOOL, .name="bools"},
        (column_header) { .type=FLOAT, .name="floats"},
        (column_header) { .type=STRING, .name="strings"}
    };

    maybe_table tb = create_test_table(db, "table 2", headers, 4);
    if (tb.error) goto release_tb;

    for (size_t i = 0; i < tb.value->header->column_amount; i++) {
        if (tb.value->header->columns[i].type != headers[i].type || strcmp(tb.value->header->columns[i].name, headers[i].name)) {
            tb.error = NOT_EQUAL;
            goto release_tb;
        }
    }

release_tb:
    release_table(tb.value);
    return tb.error;
}

bool values_iterator(any_value *value1, any_value *value2) {
    packed_any_value_with_error *real_value = (packed_any_value_with_error *)value1->string_value;
    switch (real_value->value->type) {
        case STRING:
            *(real_value->error) = strcmp(real_value->value->value.string_value, value2->string_value) ? NOT_EQUAL : OK;
            break;
        case INT_32:
            *(real_value->error) = real_value->value->value.int_value == value2->int_value ? OK : NOT_EQUAL;
            break;
        case FLOAT:
            *(real_value->error) = real_value->value->value.float_value == value2->float_value ? OK : NOT_EQUAL;
            break;
        case BOOL:
            *(real_value->error) = real_value->value->value.bool_value == value2->bool_value ? OK : NOT_EQUAL;
            break;
    }
    return true;
}

bool simple_ints_search_predicate(any_value *value1, any_value *value2) {
    return value1->int_value == value2->int_value;
}

result test_adding_values(database *db) {
    column_header headers[4] = {
        (column_header) { .type=INT_32, .name="ints"},
        (column_header) { .type=BOOL, .name="bools"},
        (column_header) { .type=FLOAT, .name="floats"},
        (column_header) { .type=STRING, .name="strings"}
    };

    any_typed_value data_values[5][4] = {
        {   (any_typed_value) { .type=INT_32, .value=(any_value) { .int_value=10 } }, 
            (any_typed_value) { .type=BOOL, .value=(any_value) { .bool_value=false } },
            (any_typed_value) { .type=FLOAT, .value=(any_value) { .float_value=3.1415 } },
            (any_typed_value) { .type=STRING, .value=(any_value) {.string_value="string test"} } },

        {   (any_typed_value) { .type=INT_32, .value=(any_value) { .int_value=20 } },
            (any_typed_value) { .type=BOOL, .value=(any_value) { .bool_value=false } },
            (any_typed_value) { .type=FLOAT, .value=(any_value) { .float_value=2.01 } },
            (any_typed_value) { .type=STRING, .value=(any_value) {.string_value="testing tests" } } },

        {   (any_typed_value) { .type=INT_32, .value=(any_value) { .int_value=30 } },
            (any_typed_value) { .type=BOOL, .value=(any_value) { .bool_value=true } },
            (any_typed_value) { .type=FLOAT, .value=(any_value) { .float_value=13.115 } },
            (any_typed_value) { .type=STRING, .value=(any_value) {.string_value="don't know what to write" } } },

        {   (any_typed_value) { .type=INT_32, .value=(any_value) { .int_value=40 } },
            (any_typed_value) { .type=BOOL, .value=(any_value) { .bool_value=false } },
            (any_typed_value) { .type=FLOAT, .value=(any_value) { .float_value=1231.134 } },
            (any_typed_value) { .type=STRING, .value=(any_value) {.string_value="bababababa" } } },

        {   (any_typed_value) { .type=INT_32, .value=(any_value) { .int_value=50 } },
            (any_typed_value) { .type=BOOL, .value=(any_value) { .bool_value=true } },
            (any_typed_value) { .type=FLOAT, .value=(any_value) { .float_value=14.15141 } },
            (any_typed_value) { .type=STRING, .value=(any_value) {.string_value="last one" } } }
        
    };

    result test_error = OK;

    maybe_table tb = create_test_table(db, "table 3", headers, 4);
    if (tb.error) { 
        test_error = tb.error;
        goto release_tb;
    }

    test_error = fill_table_with_data(db, tb.value, 4, 5, data_values);

    packed_any_value_with_error val_with_err = (packed_any_value_with_error) { .error=&test_error };
    maybe_data_iterator iterator = init_iterator(tb.value);
    if (iterator.error) {
        test_error = iterator.error;
        goto release_iter;
    }
    for (size_t header_index = 0; header_index < 4; header_index++ ) {
        size_t row_index = 0;
        val_with_err.value = &(data_values[row_index++][header_index]);
        closure print_all = (closure) { .func=values_iterator, .value1=(any_value) { .string_value=(char*)&val_with_err }};

        while (seek_next_where(iterator.value, headers[header_index].type, headers[header_index].name, print_all)) {
            val_with_err.value = &(data_values[row_index++][header_index]);
            if (test_error) goto release_iter;
            get_next(iterator.value);
        }
        reset_iterator(iterator.value, tb.value);
    }

release_iter:
    release_iterator(iterator.value);
release_tb:
    release_table(tb.value);
    return test_error;
}

result test_adding_values_speed_with_writing_result(database *db) {
    column_header headers[4] = {
        (column_header) { .type=INT_32, .name="ints"},
        (column_header) { .type=BOOL, .name="bools"},
        (column_header) { .type=FLOAT, .name="floats"},
        (column_header) { .type=STRING, .name="strings"}
    };

    any_typed_value data_values[1][4] = {
        {   (any_typed_value) { .type=INT_32, .value=(any_value) { .int_value=10 } }, 
            (any_typed_value) { .type=BOOL, .value=(any_value) { .bool_value=false } },
            (any_typed_value) { .type=FLOAT, .value=(any_value) { .float_value=3.1415 } },
            (any_typed_value) { .type=STRING, .value=(any_value) {.string_value="string test"} } }
        
    };

    result test_error = OK;

    maybe_table tb = create_test_table(db, "table 4", headers, 4);
    if (tb.error) { 
        test_error = tb.error;
        goto release_tb;
    }

    char *charts_data_file = "charts_data/writing_values_statistics.csv";

    struct stat st = {0};

    if (stat("charts_datay", &st) == -1)
        mkdir("charts_data", 0777);

    FILE *timestamp_file = fopen(charts_data_file, "wb");

    for (int it = 0; it < 1000; it++) {
        clock_t t = clock();
        test_error = fill_table_with_data(db, tb.value, 4, 1, data_values);
        if (test_error) {
            fclose(timestamp_file);
            goto release_tb;
        }
        t = clock() - t;
        double time_taken = (double)t;

        if (timestamp_file != NULL) fprintf(timestamp_file, "%d, %f\n", it, time_taken);
    }
    fclose(timestamp_file);
    make_image_from_csv(charts_data_file);

release_tb:
    release_table(tb.value);
    return test_error;
}

result test_getting_values_speed_with_writing_result(database *db) {
    column_header headers[4] = {
        (column_header) { .type=INT_32, .name="ints"},
        (column_header) { .type=BOOL, .name="bools"},
        (column_header) { .type=FLOAT, .name="floats"},
        (column_header) { .type=STRING, .name="strings"}
    };

    result test_error = OK;

    maybe_table tb = create_test_table(db, "table 5", headers, 4);
    if (tb.error) { 
        test_error = tb.error;
        goto release_tb;
    }


    for (int it = 0; it < 10000; it++) {
        any_typed_value data_values[1][4] = {
            {   (any_typed_value) { .type=INT_32, .value=(any_value) { .int_value=it } }, 
                (any_typed_value) { .type=BOOL, .value=(any_value) { .bool_value=false } },
                (any_typed_value) { .type=FLOAT, .value=(any_value) { .float_value=3.1415 } },
                (any_typed_value) { .type=STRING, .value=(any_value) {.string_value="string test"} } }
        
        };
        test_error = fill_table_with_data(db, tb.value, 4, 1, data_values);
        if (test_error) goto release_tb;
    }

    char *charts_data_file = "charts_data/getting_values_statistics.csv";

    struct stat st = {0};

    if (stat("charts_datay", &st) == -1)
        mkdir("charts_data", 0777);

    FILE *timestamp_file = fopen(charts_data_file, "wb");

    maybe_data_iterator iterator = init_iterator(tb.value);
    if (iterator.error) {
        test_error = iterator.error;
        goto release_iter;
    }

    for (int it = 0; it < 10000; it++) {

        closure find_int = (closure) { .func=simple_ints_search_predicate, .value1=(any_value) { .int_value=it }};

        clock_t t = clock();
        bool found = seek_next_where(iterator.value, INT_32, "ints", find_int);
        if (!found) {
            test_error = NOT_FOUND;
            goto close_file;
        }
        t = clock() - t;
        double time_taken = (double)t;

        if (timestamp_file != NULL) fprintf(timestamp_file, "%d, %f\n", it, time_taken);

        reset_iterator(iterator.value, tb.value);
    }

close_file:
    fclose(timestamp_file);

    if (!test_error) make_image_from_csv(charts_data_file);

release_iter:
    release_iterator(iterator.value);
release_tb:
    release_table(tb.value);
    return test_error;
}

result test_deleting_value(database *db);
result test_updating_value(database *db);
