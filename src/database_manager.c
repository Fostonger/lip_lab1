#include <stdlib.h>

#include "database_manager.h"

maybe_page create_page(table_header *tb_header) {
    page_header *header = malloc(sizeof(page_header));
    if (header == NULL) return (maybe_page) { .error=MALLOC_ERROR, .value = NULL };

    header->data_offset = 0;
    header->table_header = *tb_header;
    header->rows_count = 0;
    header->page_number = -1;
    header->next_page_number = -1;

    page *pg = malloc(sizeof(page));
    if (pg == NULL) return (maybe_page) { .error=MALLOC_ERROR, .value = NULL };

    pg->pgheader = header;
    pg->tbheader = tb_header;
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
    if (tb->first_string_page_to_write == NULL && ( pg = create_page(tb->header) ).error )
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
        if ( !( new_pg = create_page(tb->header) ).error )
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
    if (tb->first_page_to_write == NULL && ( pg = create_page(tb->header) ).error )
        return pg.error;
    if (pg.value != NULL) {
        tb->first_page_to_write = pg.value;
        tb->first_page = pg.value;
        pg.value->pgheader->data_offset = 0;
    }

    page_header *writable_page_header = tb->first_page_to_write->pgheader;

    if (writable_page_header->data_offset + data_size > PAGE_SIZE) {
        maybe_page new_pg;
        if ( !( new_pg = create_page(tb->header) ).error )
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
