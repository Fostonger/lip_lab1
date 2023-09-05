#include <stdlib.h>
#include <string.h>

#include "database_manager.h"

result write_db_header(database *db) {
    fseek(db->file, 0, SEEK_SET);
    size_t written = fwrite(db->header, sizeof(database_header), 1, db->file);
    if (written != 1) return WRITE_ERROR;
    return OK;
}

result read_db_header(FILE* file, database_header* header) {
    fseek(file, 0, SEEK_SET);
    size_t read = fread(header, sizeof(database_header), 1, file);
    if (read != 1) return READ_ERROR;
    return OK;
}

maybe_database allocate_db(FILE *file) {
    if (file == NULL) return (maybe_database){ .error=DONT_EXIST, .value=NULL };

    database *db_val = (database *)malloc(sizeof(database));
    if (db_val == NULL) return (maybe_database){ .error=MALLOC_ERROR, .value=NULL };

    maybe_database db = (maybe_database) {.error=OK, .value=db_val};
    db_val->file = file;

    database_header *header = (database_header *)malloc(sizeof(database_header));
    if (header == NULL) {
        release_db(db_val);
        return (maybe_database){ .error=MALLOC_ERROR, .value=NULL };
    }

    header->page_size = PAGE_SIZE;
    header->first_free_page = 1;
    header->last_page_in_file_number = 1;
    db_val->header = header;

    db_val->all_loaded_pages = (page **)malloc(sizeof(page *) * 100);
    db_val->loaded_pages_capacity = 100;
    db_val->loaded_pages_count = 0;
    return db;
}

maybe_database create_new_db(FILE *file) {
    maybe_database db = allocate_db(file);
    if (db.error) return db;

    result write_error = write_db_header(db.value);
    if (write_error) {
        release_db(db.value);
        return (maybe_database){ .error=write_error, .value=NULL };
    }
    return db;
}

maybe_database read_db(FILE *file) {
    maybe_database db = allocate_db(file);
    if (db.error) return db;
    
    result read_error = read_db_header(file, db.value->header);
    if (read_error) {
        release_db(db.value);
        return (maybe_database){ .error=read_error, .value=NULL };
    }
    return db;
}

maybe_database initdb(FILE *file, bool overwrite) {
    if (overwrite) return create_new_db(file);
    return read_db(file);
}

uint16_t get_page_number(database *db, page *pg) {
    uint16_t page_num = db->header->first_free_page++;
    if (db->loaded_pages_count == db->loaded_pages_capacity) {
        page **new_pages_storage = (page **)malloc(sizeof(page *) * (db->loaded_pages_capacity + 100));
        db->loaded_pages_capacity += 100;
        memcpy(new_pages_storage, db->all_loaded_pages, sizeof(page *) * db->loaded_pages_count);
        free(db->all_loaded_pages);
        db->all_loaded_pages = new_pages_storage;
    }
    db->all_loaded_pages[db->loaded_pages_count] = pg;
    return page_num;
}

result write_page(database *db, page *pg) {

}

maybe_page create_page(table *tb) {
    page_header *header = malloc(sizeof(page_header));
    if (header == NULL) return (maybe_page) { .error=MALLOC_ERROR, .value = NULL };

    header->data_offset = 0;
    header->table_header = *tb->header;
    header->rows_count = 0;
    header->next_page_number = -1;

    page *pg = malloc(sizeof(page));
    if (pg == NULL) return (maybe_page) { .error=MALLOC_ERROR, .value = NULL };

    header->page_number = get_page_number(tb->db, pg);

    pg->pgheader = header;
    pg->tbheader = tb->header;
    pg->data = malloc (PAGE_SIZE);
    if (pg->data == NULL) return (maybe_page) { .error=MALLOC_ERROR, .value = NULL };

    return (maybe_page) { .error=OK, .value=pg };
}

page *get_first_writable_page(page *pg) {
    page *cur_pg = pg;
    while (cur_pg->next_page != NULL && PAGE_SIZE - cur_pg->pgheader->data_offset < MIN_VALUABLE_SIZE)
        cur_pg = cur_pg->next_page;
    return cur_pg;
}

// Перебираем все страницы с доступным местом пока не найдем ту, в которую можем пихнуть строку
maybe_page get_suitable_page(table *tb, size_t data_size) {
    maybe_page pg = (maybe_page) {};
    if (tb->first_string_page_to_write == NULL && ( pg = create_page(tb) ).error )
        return (maybe_page) { .error=pg.error, .value=NULL };
    if (pg.value != NULL) {
        tb->first_string_page_to_write = pg.value;
        tb->first_string_page = pg.value;
        pg.value->pgheader->data_offset = 0;
    }

    page *writable_page = tb->first_string_page_to_write;

    while (writable_page->pgheader->data_offset + data_size > PAGE_SIZE && writable_page->next_page != NULL)
        writable_page = writable_page->next_page;

    // не нашли страницу подходящего размера, но это не значит, что можно забраковать все страницы!
    // проверку на MIN_VALUABLE_SIZE все равно сделать нужно
    if (writable_page->pgheader->data_offset + data_size > PAGE_SIZE) {
        maybe_page new_pg;
        if ( !( new_pg = create_page(tb) ).error )
            return (maybe_page) { .error=new_pg.error, .value=NULL };
        writable_page = new_pg.value;

        page *last_page = tb->first_string_page;
        while (last_page->next_page != NULL) last_page = last_page->next_page;
        last_page->next_page = new_pg.value;
    }

    // проверка на MIN_VALUABLE_SIZE
    page *first_writable_page = get_first_writable_page(tb->first_string_page_to_write);
    if (first_writable_page != NULL) tb->first_string_page_to_write = first_writable_page;

    return (maybe_page) { .error=OK, .value=writable_page };
}

// TODO: load from disk if page is in storage
maybe_page get_next_page_or_load(page *pg) {
    if (pg->next_page != NULL) return (maybe_page) { .error=OK, .value=pg->next_page };
    return (maybe_page) { .error=DONT_EXIST, .value=NULL };
}

maybe_page get_page_by_number(page *first_page, uint64_t page_ordinal) {
    maybe_page pg = (maybe_page) { .error=OK, .value=first_page };
    while ( pg.value->pgheader->page_number < page_ordinal && !(pg = get_next_page_or_load(pg.value)).error );
    if (!pg.error && pg.value->pgheader->page_number == page_ordinal) return pg;
    return (maybe_page) { .error=DONT_EXIST, .value=NULL };
}

result ensure_enough_space_table(table *tb, size_t data_size) {
    maybe_page pg = (maybe_page) {};
    if (tb->first_page_to_write == NULL && ( pg = create_page(tb) ).error )
        return pg.error;
    if (pg.value != NULL) {
        tb->first_page_to_write = pg.value;
        tb->first_page = pg.value;
        pg.value->pgheader->data_offset = 0;
    }

    page_header *writable_page_header = tb->first_page_to_write->pgheader;

    if (writable_page_header->data_offset + data_size > PAGE_SIZE) {
        maybe_page new_pg;
        if ( !( new_pg = create_page(tb) ).error )
            return new_pg.error;
        if (new_pg.value != NULL) {
            tb->first_page_to_write->next_page = new_pg.value;
            tb->first_page_to_write = new_pg.value;
            new_pg.value->pgheader->data_offset = 0;
        }
    }

    return OK;
}

void release_page(page *pg) {
    page *cur_page = pg;
    page *prev_page = cur_page;
    while (cur_page != NULL) {
        free(cur_page->data);
        free(cur_page->pgheader);
        prev_page = cur_page;
        cur_page = cur_page->next_page;
        free(prev_page);
    }
}

void release_db(database *db) {
    free(db->header);
    free(db);
}
