#include <stdio.h>
#include <string.h>

#include <errno.h>
#include <limits.h>
#include <unistd.h>

#include "tests.h"

bool int_iterator_func(any_value *value1, any_value *value2) {
    printf("catched int value: %d\n", value2->int_value);
    return 0;
}

bool float_iterator_func(any_value *value1, any_value *value2) {
    printf("catched float value: %f\n", value2->float_value);
    return 0;
}

bool bool_iterator_func(any_value *value1, any_value *value2) {
    printf("catched bool value: %s\n", value2->bool_value ? "TRUE" : "FALSE");
    return 0;
}

bool sting_iterator_func(any_value *value1, any_value *value2) {
    printf("catched string value: %s\n", value2->string_value);
    return 0;
}

bool string_modify_func(any_value *value1, any_value *value2) {
    return !strcmp(value1->string_value, value2->string_value);
}

bool filter_func(any_value *value1, any_value *value2) {
    return value1->int_value < value2->int_value;
}

int work_with_second_table(table *t2) {
    add_column(t2, "ints", INT_32);
    add_column(t2, "bools on second", BOOL);
    add_column(t2, "floats on second", FLOAT);
    add_column(t2, "strings on second", STRING);
    maybe_data data2 = init_data(t2);
    if (data2.error) {
        print_if_failure(data2.error);
        release_table(t2);
        return 1;
    }
    if  (   !print_if_failure( data_init_integer(data2.value, 102) ) 
        ||  !print_if_failure( data_init_boolean(data2.value, 0) )
        ||  !print_if_failure( data_init_float(data2.value, 100.1415) )
        ||  !print_if_failure( data_init_string(data2.value, "string test") ) ) {

            release_table(t2);
            release_data(data2.value);
            return 1;
    }
    if (set_data(data2.value)) {
        printf("error occured while setting data, maybe target table has different stucture\n");
    } else {
        printf("successfully set data!\n");
    }
    clear_data(data2.value);
    
    if  (   !print_if_failure( data_init_integer(data2.value, 20) ) 
        ||  !print_if_failure( data_init_boolean(data2.value, 1) )
        ||  !print_if_failure( data_init_float(data2.value, 7.08) )
        ||  !print_if_failure( data_init_string(data2.value, "alina string on second") ) ) {

            release_table(t2);
            release_data(data2.value);
            return 1;
    }
    if (set_data(data2.value)) {
        printf("error occured while setting data, maybe target table has different stucture\n");
    } else {
        printf("successfully set data!\n");
    }
    clear_data(data2.value);
    
    if  (   !print_if_failure( data_init_integer(data2.value, 10220) ) 
        ||  !print_if_failure( data_init_boolean(data2.value, 0) )
        ||  !print_if_failure( data_init_float(data2.value, 1522.68) )
        ||  !print_if_failure( data_init_string(data2.value, "zinger string") ) ) {

            release_table(t2);
            release_data(data2.value);
            return 1;
    }
    if (set_data(data2.value)) {
        printf("error occured while setting data, maybe target table has different stucture\n");
    } else {
        printf("successfully set data!\n");
    }
    clear_data(data2.value);

    if  (   !print_if_failure( data_init_integer(data2.value, 300) ) 
        ||  !print_if_failure( data_init_boolean(data2.value, 2) )
        ||  !print_if_failure( data_init_float(data2.value, 0.00001) )
        ||  !print_if_failure( data_init_string(data2.value, "contraversary row string") ) ) {

            release_table(t2);
            release_data(data2.value);
            return 1;
    }
    if (set_data(data2.value)) {
        printf("error occured while setting data, maybe target table has different stucture\n");
    } else {
        printf("successfully set data!\n");
    }
    clear_data(data2.value);

    print_table(t2);

    closure filter_closure = (closure) { .func=filter_func, .value1=(any_value){ .int_value=200 } };
    maybe_table filtered_tb = filter_table(t2, INT_32, "ints", filter_closure);
    if ( !print_if_failure(filtered_tb.error) ) return 1;
    printf("filtered table2:\n");
    print_table(filtered_tb.value);

    closure delete_closure = (closure) { .func=string_modify_func, .value1=(any_value){ .string_value="string test" } };

    printf("deleted %d rows\n", delete_where(t2, STRING, "strings on second", delete_closure).count);

    print_table(t2);

    if  (   !print_if_failure( data_init_integer(data2.value, 20) ) 
        ||  !print_if_failure( data_init_boolean(data2.value, 0) )
        ||  !print_if_failure( data_init_float(data2.value, 1115.68) )
        ||  !print_if_failure( data_init_string(data2.value, "zinger string updated") ) ) {

            release_table(t2);
            release_data(data2.value);
            return 1;
    }

    closure update_closure = (closure) { .func=string_modify_func, .value1=(any_value){ .string_value="zinger string" } };

    printf("updated %d rows\n", update_where(t2, STRING, "strings on second", update_closure, data2.value).count);

    release_data(data2.value);

    print_table(t2);
    return 0;
}

int main(void) {
    char *filename = "database.fost.data";
    FILE *dbfile = fopen(filename, "r+");

    maybe_database db = initdb(dbfile, true);
    if (db.error) {
        print_if_failure(db.error);
        return 1;
    }
    // table *t0 = read_table("merged table", db.value).value;
    // print_table(t0);

    // maybe_data data_joined = init_data(t0);
    // if (data_joined.error) {
    //     print_if_failure(data_joined.error);
    //     return 1;
    // }
    // maybe_data_iterator iterator_joined = init_iterator(t0);
    // if (iterator_joined.error) return 1;

    // // не подсасывает

    // copy_data(data_joined.value, iterator_joined.value->cur_data);
    // set_data(data_joined.value);

    // print_table(t0);
    // print_if_failure(save_table(db.value, t0));

    printf("\n\tStarting Test 1: table creation test\n");
    result test_result = test_table_creation(db.value);
    printf("\t\t\tTEST 1 RESULT\n%s\n", result_to_string(test_result));

    printf("\n\tStarting Test 2: table columns adding test\n");
    test_result = test_adding_columns(db.value);
    printf("\t\t\tTEST 2 RESULT\n%s\n", result_to_string(test_result));

    printf("\n\tStarting Test 3: table values adding test\n");
    test_result = test_adding_values(db.value);
    printf("\t\t\tTEST 3 RESULT\n%s\n", result_to_string(test_result));

    printf("\n\tStarting Test 4: table values adding speed test with charts printed\n");
    test_result = test_adding_values_speed_with_writing_result(db.value);
    printf("\t\t\tTEST 4 RESULT\n%s\n", result_to_string(test_result));

    printf("\n\tStarting Test 5: getting table values speed test with charts printed\n");
    test_result = test_getting_values_speed_with_writing_result(db.value);
    printf("\t\t\tTEST 5 RESULT\n%s\n", result_to_string(test_result));

    printf("\n\tStarting Test 6: deleting table values test\n");
    test_result = test_deleting_value(db.value);
    printf("\t\t\tTEST 6 RESULT\n%s\n", result_to_string(test_result));

    printf("\n\tStarting Test 7: updating table values test\n");
    test_result = test_updating_value(db.value);
    printf("\t\t\tTEST 7 RESULT\n%s\n", result_to_string(test_result));

    printf("\n\tStarting Test 8: tables joining test\n");
    test_result = test_tables_merging(db.value);
    printf("\t\t\tTEST 8 RESULT\n%s\n", result_to_string(test_result));

    printf("\n\tStarting Test 9: table filteringing test\n");
    test_result = test_table_filtering(db.value);
    printf("\t\t\tTEST 9 RESULT\n%s\n", result_to_string(test_result));

    printf("\n\tStarting Test 10: table saving test\n");
    test_result = test_table_saving(db.value);
    printf("\t\t\tTEST 10 RESULT\n%s\n", result_to_string(test_result));

    printf("\n\tStarting Test 11: table reading test\n");
    test_result = test_table_reading(db.value);
    printf("\t\t\tTEST 11 RESULT\n%s\n", result_to_string(test_result));

    printf("\n\tStarting Test 12: mulipaged string adding and reading test\n");
    test_result = test_multipaged_strings_adding(db.value);
    printf("\t\t\tTEST 12 RESULT\n%s\n", result_to_string(test_result));

    printf("\n\tStarting Test 13: mulipaged string saving\n");
    test_result = test_multipaged_strings_saving(db.value);
    printf("\t\t\tTEST 13 RESULT\n%s\n", result_to_string(test_result));

    printf("\n\tStarting Test 14: mulipaged string reading\n");
    test_result = test_multipaged_strings_reading(db.value);
    printf("\t\t\tTEST 14 RESULT\n%s\n", result_to_string(test_result));

    // printf("second table returned %d\n", work_with_second_table(t2));

    // maybe_table joined_tb = join_table(t1, t2, "ints", INT_32, "merged table");
    // if ( !print_if_failure(joined_tb.error) ) return 1;
    // print_table(joined_tb.value);

    // maybe_data data_joined = init_data(t0);
    // if (data_joined.error) {
    //     print_if_failure(data_joined.error);
    //     return 1;
    // }
    // maybe_data_iterator iterator_joined = init_iterator(joined_tb.value);
    // if (iterator_joined.error) return 1;

    // copy_data(data_joined.value, iterator_joined.value->cur_data);
    // set_data(data_joined.value);
    // print_table(t0);
    // print_if_failure(save_table(db.value, t0));

    // print_table(t1);

    // print_if_failure(save_table(db.value, joined_tb.value));
    // print_table(joined_tb.value);

    // print_table(t1);

    // if  (   !print_if_failure( data_init_integer(data1.value, 300) ) 
    //     ||  !print_if_failure( data_init_boolean(data1.value, 0) )
    //     ||  !print_if_failure( data_init_float(data1.value, 15.68) )
    //     ||  !print_if_failure( data_init_string(data1.value, "zinger string") ) ) {

    //         release_table(t1);
    //         release_data(data1.value);
    //         return 1;
    // }

    // update_closure = (closure) { .func=string_modify_func, .value1=(any_value){ .string_value="zinger string updated" } };
    // printf("updated %d rows\n", update_where(t1, STRING, "strings", update_closure, data1.value).count);
    
    // release_data(data1.value);

    // print_table(t1);

    // release_table(t1);

    fclose(dbfile);
}