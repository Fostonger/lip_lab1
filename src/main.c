#include <stdio.h>
#include <string.h>

#include "table.h"
#include "database_manager.h"
#include "util.h"
#include "data.h"
#include "data_iterator.h"

bool int_iterator_func(int32_t *value1, int32_t *value2) {
    printf("catched int value: %d\n", *value2);
    return 0;
}

bool float_iterator_func(float *value1, float *value2) {
    printf("catched float value: %f\n", *value2);
    return 0;
}

bool bool_iterator_func(bool *value1, bool *value2) {
    printf("catched bool value: %s\n", *value2 ? "TRUE" : "FALSE");
    return 0;
}

bool sting_iterator_func(char *value1, char *value2) {
    printf("catched string value: %s\n", value2);
    return 0;
}

bool string_modify_func(char *value1, char *value2) {
    return !strcmp(value1, value2);
}

int work_with_second_table() {
    table *t2 = unwrap_table( create_table("table2") );
    add_column(t2, "ints on second", INT_32);
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

    if  (   !print_if_failure( data_init_integer(data2.value, 9999) ) 
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

    closure delete_closure = (closure) { .func=string_modify_func, .value1="string test" };

    printf("deleted %d rows\n", delete_where(t2, STRING, "strings on second", delete_closure).count);

    print_table(t2);

    if  (   !print_if_failure( data_init_integer(data2.value, 31100) ) 
        ||  !print_if_failure( data_init_boolean(data2.value, 0) )
        ||  !print_if_failure( data_init_float(data2.value, 1115.68) )
        ||  !print_if_failure( data_init_string(data2.value, "zinger string updated") ) ) {

            release_table(t2);
            release_data(data2.value);
            return 1;
    }

    closure update_closure = (closure) { .func=string_modify_func, .value1="zinger string" };

    printf("updated %d rows\n", update_where(t2, STRING, "strings on second", update_closure, data2.value).count);

    release_data(data2.value);

    print_table(t2);
    return 0;
}

int main(void) {
    // char *filename = "database.fost.data";
    // FILE *dbfile = fopen(filename, "r+");
    // database db = unwrap_database(initdb(dbfile));
    table *t1 = unwrap_table( create_table("table1") );
    add_column(t1, "ints", INT_32);
    add_column(t1, "bools", BOOL);
    add_column(t1, "floats", FLOAT);
    add_column(t1, "strings", STRING);
    maybe_data data1 = init_data(t1);
    if (data1.error) {
        print_if_failure(data1.error);
        release_table(t1);
        return 1;
    }
    
    if  (   !print_if_failure( data_init_integer(data1.value, 10) ) 
        ||  !print_if_failure( data_init_boolean(data1.value, 0) )
        ||  !print_if_failure( data_init_float(data1.value, 3.1415) )
        ||  !print_if_failure( data_init_string(data1.value, "string test") ) ) {

            release_table(t1);
            release_data(data1.value);
            return 1;
    }
    if (set_data(data1.value)) {
        printf("error occured while setting data, maybe target table has different stucture\n");
    } else {
        printf("successfully set data!\n");
    }
    clear_data(data1.value);
    
    if  (   !print_if_failure( data_init_integer(data1.value, 20) ) 
        ||  !print_if_failure( data_init_boolean(data1.value, 1) )
        ||  !print_if_failure( data_init_float(data1.value, 7.08) )
        ||  !print_if_failure( data_init_string(data1.value, "alina string") ) ) {

            release_table(t1);
            release_data(data1.value);
            return 1;
    }
    if (set_data(data1.value)) {
        printf("error occured while setting data, maybe target table has different stucture\n");
    } else {
        printf("successfully set data!\n");
    }
    clear_data(data1.value);
    
    if  (   !print_if_failure( data_init_integer(data1.value, 100) ) 
        ||  !print_if_failure( data_init_boolean(data1.value, 0) )
        ||  !print_if_failure( data_init_float(data1.value, 15.68) )
        ||  !print_if_failure( data_init_string(data1.value, "zinger string") ) ) {

            release_table(t1);
            release_data(data1.value);
            return 1;
    }
    if (set_data(data1.value)) {
        printf("error occured while setting data, maybe target table has different stucture\n");
    } else {
        printf("successfully set data!\n");
    }
    clear_data(data1.value);

    maybe_data_iterator iterator = init_iterator(t1);
    if (iterator.error) return 1;
    closure print_all = (closure) { .func=int_iterator_func, .value1=0};
    seek_next_where(iterator.value, INT_32, "ints", print_all);

    reset_iterator(iterator.value, t1);
    print_all = (closure) { .func=float_iterator_func, .value1=0};
    seek_next_where(iterator.value, FLOAT, "floats", print_all);

    reset_iterator(iterator.value, t1);
    print_all = (closure) { .func=bool_iterator_func, .value1=0};
    seek_next_where(iterator.value, BOOL, "bools", print_all);

    reset_iterator(iterator.value, t1);
    print_all = (closure) { .func=sting_iterator_func, .value1=""};
    seek_next_where(iterator.value, STRING, "strings", print_all);

    release_iterator(iterator.value);

    print_table(t1);

    closure delete_closure = (closure) { .func=string_modify_func, .value1="string test" };
    printf("deleted %d rows\n", delete_where(t1, STRING, "strings", delete_closure).count);

    print_table(t1);

    if  (   !print_if_failure( data_init_integer(data1.value, 300) ) 
        ||  !print_if_failure( data_init_boolean(data1.value, 0) )
        ||  !print_if_failure( data_init_float(data1.value, 15.68) )
        ||  !print_if_failure( data_init_string(data1.value, "zinger string updated") ) ) {

            release_table(t1);
            release_data(data1.value);
            return 1;
    }

    closure update_closure = (closure) { .func=string_modify_func, .value1="zinger string" };
    printf("updated %d rows\n", update_where(t1, STRING, "strings", update_closure, data1.value).count);

    release_data(data1.value);
    print_table(t1);

    printf("second table returned %d\n", work_with_second_table());

    release_table(t1);
}