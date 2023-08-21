#include <stdlib.h>

#include "database_manager.h"

maybe_page create_page(table_header *tb_header) {
    page_header *header = malloc(sizeof(page_header));
    if (header == NULL) return (maybe_page) { .error=MALLOC_ERROR, .value = NULL };

    header->data_offset = 0;
    header->table_header = *tb_header;
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

result ensure_enough_space_string(table *tb, size_t data_size) {
    maybe_page pg;
    if (tb->first_string_page_to_write == NULL || !( pg = create_page(tb->header) ).error )
        return pg.error;
    if (pg.value != NULL) {
        tb->first_string_page_to_write = pg.value;
        tb->first_string_page = pg.value;
        pg.value->pgheader->data_offset = 0;
    }

    page_header *writable_page_header = tb->first_string_page_to_write->pgheader;

    if (writable_page_header->data_offset + data_size > PAGE_SIZE) {
        maybe_page new_pg;
        if ( !( new_pg = create_page(tb->header) ).error )
            return new_pg.error;
        if (new_pg.value != NULL) {
            tb->first_string_page_to_write->next_page = new_pg.value;
            tb->first_string_page_to_write = new_pg.value;
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
