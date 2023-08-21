#include <stdio.h>

#include "table.h"
#include "database_manager.h"
#include "util.h"
#include "data.h"

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
    if (!data1.error) {
        is_success(data1.error);
        release_table(t1);
        return 1;
    }
    
    if  (   !is_success( data_init_integer(data1.value, 10) ) 
        ||  !is_success( data_init_boolean(data1.value, 0) )
        ||  !is_success( data_init_float(data1.value, 3.1415) )
        ||  !is_success( data_init_string(data1.value, "string test") ) ) {

            release_table(t1);
            release_data(data1.value);
            return 1;
    }
    if (set_data(data1.value)) {
        printf("error occured while setting data, maybe target table has different stucture\n");
    } else {
        printf("successfully set data!\n");
    }

    release_table(t1);
}