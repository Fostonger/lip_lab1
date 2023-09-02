#include <stdio.h>
#include <string.h>

#include "table.h"
#include "database_manager.h"
#include "util.h"
#include "data.h"
#include "data_iterator.h"

bool int_iterator_func(int32_t *value) {
    printf("catched int value: %d\n", *value);
    return 0;
}

bool float_iterator_func(float *value) {
    printf("catched float value: %f\n", *value);
    return 0;
}

bool bool_iterator_func(bool *value) {
    printf("catched bool value: %s\n", *value ? "TRUE" : "FALSE");
    return 0;
}

bool sting_iterator_func(char *value) {
    printf("catched string value: %s\n", value);
    return 0;
}

bool string_delete_func(char *value) {
    return !strcmp(value, "string test");
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

    release_data(data1.value);

    maybe_data_iterator iterator = init_iterator(t1);
    if (iterator.error) return 1;
    seek_next_where(iterator.value, INT_32, "ints", int_iterator_func);

    iterator = init_iterator(t1);
    if (iterator.error) return 1;
    seek_next_where(iterator.value, FLOAT, "floats", float_iterator_func);

    iterator = init_iterator(t1);
    if (iterator.error) return 1;
    seek_next_where(iterator.value, BOOL, "bools", bool_iterator_func);

    iterator = init_iterator(t1);
    if (iterator.error) return 1;
    seek_next_where(iterator.value, STRING, "strings", sting_iterator_func);

    print_table(t1);

    printf("deleted %d rows\n", delete_where(t1, STRING, "strings", string_delete_func).count);

    print_table(t1);

    release_table(t1);
}