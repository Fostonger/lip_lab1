#include <stdio.h>
#include <string.h>

#include <errno.h>
#include <limits.h>
#include <unistd.h>

#include "table.h"
#include "database_manager.h"
#include "util.h"
#include "data.h"
#include "data_iterator.h"

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

    char *lord_of_the_rings_str = "When Mr. Bilbo Baggins of Bag End announced that he would shortly be celebrating his eleventy-first birthday with a party of special magnificence, there was much talk and excitement in Hobbiton.\n   Bilbo was very rich and very peculiar, and had been the wonder of the Shire for sixty years, ever since his remarkable disappearance and unexpected return. The riches he had brought back from his travels had now become a local legend, and it was popularly believed, whatever the old folk might say, that the Hill at Bag End was full of tunnels stuffed with treasure. And if that was not enough for fame, there was also his prolonged vigour to marvel at. Time wore on, but it seemed to have little effect on Mr. Baggins. At ninety he was much the same as at fifty. At ninety-nine they began to call him well-preserved; but unchanged would have been nearer the mark. There were some that shook their heads and thought this was too much of a good thing; it seemed unfair that anyone should possess (apparently) perpetual youth as well as (reputedly) inexhaustible wealth.\n   'It will have to be paid for,' they said. 'It isn't natural, and trouble will come of it!'\n   But so far trouble had not come; and as Mr. Baggins was generous with his money, most people were willing to forgive him his oddities and his good fortune. He remained on visiting terms with his relatives (except, of course, the Sackville-Bagginses), and he had many devoted admirers among the hobbits of poor and unimportant families. But he had no close friends, until some of his younger cousins began to grow up.\n   The eldest of these, and Bilbo's favourite, was young Frodo Baggins. When Bilbo was ninety-nine he adopted Frodo as his heir, and brought him to live at Bag End; and the hopes of the Sackville-Bagginses were finally dashed. Bilbo and Frodo happened to have the same birthday, September 22nd. 'You had better come and live here, Frodo my lad,' said Bilbo one day; 'and then we can celebrate our birthday-parties comfortably together.' At that time Frodo was still in his tweens, as the hobbits called the irresponsible twenties between childhood and coming of age at thirty-three.\n   Twelve more years passed. Each year the Bagginses had given very lively combined birthday-parties at Bag End; but now it was understood that something quite exceptional was being planned for that autumn. Bilbo was going to be eleventy-one, 111, a rather curious number, and a very respectable age for a hobbit (the Old Took himself had only reached 130); and Frodo was going to be thirty-three, 33, an important number: the date of his 'coming of age'.\n   Tongues began to wag in Hobbiton and Bywater; and rumour of the coming event travelled all over the Shire. The history and character of Mr. Bilbo Baggins became once again the chief topic of conversation; and the older folk suddenly found their reminiscences in welcome demand.\n   No one had a more attentive audience than old Ham Gamgee, commonly known as the Gaffer. He held forth at The Ivy Bush, a small inn on the Bywater road; and he spoke with some authority, for he had tended the garden at Bag End for forty years, and had helped old Holman in the same job before that. Now that he was himself growing old and stiff in the joints, the job was mainly carried on by his youngest son, Sam Gamgee. Both father and son were on very friendly terms with Bilbo and Frodo. They lived on the Hill itself, in Number 3 Bagshot Row just below Bag End.\n   'A very nice well-spoken gentlehobbit is Mr. Bilbo, as I've always said,' the Gaffer declared. With perfect truth: for Bilbo was very polite to him, calling him 'Master Hamfast', and consulting him constantly upon the growing of vegetables — in the matter of 'roots', especially potatoes, the Gaffer was recognized as the leading authority by all in the neighbourhood (including himself).\n   'But what about this Frodo that lives with him?' asked Old Noakes of Bywater. 'Baggins is his name, but he's more than half a Brandybuck, they say. It beats me why any Baggins of Hobbiton should go looking for a wife away there in Buckland, where folks are so queer.'\n   'And no wonder they're queer,' put in Daddy Twofoot (the Gaffer's next-door neighbour), 'if they live on the wrong side of the Brandywine River, and right agin the Old Forest. That's a dark bad place, if half the tales be true.'\n   'You're right, Dad!' said the Gaffer. 'Not that the Brandybucks of Buckland live in the Old Forest; but they're a queer breed, seemingly. They fool about with boats on that big river — and that isn't natural. Small wonder that trouble came of it, I say. But be that as it may, Mr. Frodo is as nice a young hobbit as you could wish to meet. Very much like Mr. Bilbo, and in more than looks. After all his father was a Baggins. A decent respectable hobbit was Mr. Drogo Baggins; there was never much to tell of him, till he was drownded.'\n   'Drownded?' said several voices. They had heard this and other darker rumours before, of course; but hobbits have a passion for family history, and they were ready to hear it again.\n   'Well, so they say,' said the Gaffer. 'You see: Mr. Drogo, he married poor Miss Primula Brandybuck. She was our Mr. Bilbo's first cousin on the mother's side (her mother being the youngest of the Old Took's daughters); and Mr. Drogo was his second cousin. So Mr. Frodo is his first and second cousin, once removed either way, as the saying is, if you follow me. And Mr. Drogo was staying at Brandy Hall with his father-in-law, old Master Gorbadoc, as he often did after his marriage (him being partial to his vittles, and old Gorbadoc keeping a mighty generous table); and he went out boating on the Brandywine River; and he and his wife were drownded, and poor Mr. Frodo only a child and all.'\n   'I've heard they went on the water after dinner in the moonlight,' said Old Noakes; 'and it was Drogo's weight as sunk the boat.'\n   'And I heard she pushed him in, and he pulled her in after him,' said Sandyman, the Hobbiton miller.\n   'You shouldn't listen to all you hear, Sandyman,' said the Gaffer, who did not much like the miller. 'There isn't no call to go talking of pushing and pulling. Boats are quite tricky enough for those that sit still without looking further for the cause of trouble. Anyway: there was this Mr. Frodo left an orphan and stranded, as you might say, among those queer Bucklanders, being brought up anyhow in Brandy Hall. A regular warren, by all accounts. Old Master Gorbadoc never had fewer than a couple of hundred relations in the place. Mr. Bilbo never did a kinder deed than when he brought the lad back to live among decent folk.\n   'But I reckon it was a nasty knock for those Sackville-Bagginses. They thought they were going to get Bag End, that time when he went off and was thought to be dead. And then he comes back and orders them off; and he goes on living and living, and never looking a day older, bless him! And suddenly he produces an heir, and has all the papers made out proper. The Sackville-Bagginses won't never see the inside of Bag End now, or it is to be hoped not.'\n   'There's a tidy bit of money tucked away up there, I hear tell,' said a stranger, a visitor on business from Michel Delving in the Westfarthing. 'All the top of your hill is full of tunnels packed with chests of gold and silver, and jools, by what I've heard.'\n   'Then you've heard more than I can speak to,' answered the Gaffer. 'I know nothing about jools. Mr. Bilbo is free with his money, and there seems no lack of it; but I know of no tunnel-making. I saw Mr. Bilbo when he came back, a matter of sixty years ago, when I was a lad. I'd not long come prentice to old Holman (him being my dad's cousin), but he had me up at Bag End helping him to keep folks from trampling and trapessing all over the garden while the sale was on. And in the middle of it all Mr. Bilbo comes up the Hill with a pony and some mighty big bags and a couple of chests. I don't doubt they were mostly full of treasure he had picked up in foreign parts, where there be mountains of gold, they say; but there wasn't enough to fill tunnels. But my lad Sam";

    maybe_database db = initdb(dbfile, true);
    if (db.error) {
        print_if_failure(db.error);
        return 1;
    }
    // table *t0 = read_table("merged table", db.value).value;
    // print_table(t0);

    table *t1 = create_table("table1", db.value).value;
    table *t2 = create_table("table2", db.value).value;
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
        ||  !print_if_failure( data_init_string(data1.value, lord_of_the_rings_str) ) ) {

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
    print_all = (closure) { .func=sting_iterator_func, .value1=(any_value){ .string_value="" } };
    seek_next_where(iterator.value, STRING, "strings", print_all);

    release_iterator(iterator.value);

    print_table(t1);

    closure delete_closure = (closure) { .func=string_modify_func, .value1=(any_value){ .string_value="string test" } };
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

    closure update_closure = (closure) { .func=string_modify_func, .value1=(any_value){ .string_value="zinger string" } };
    printf("updated %d rows\n", update_where(t1, STRING, "strings", update_closure, data1.value).count);

    clear_data(data1.value);
    print_table(t1);

    printf("second table returned %d\n", work_with_second_table(t2));

    maybe_table joined_tb = join_table(t1, t2, "ints", INT_32, "merged table");
    if ( !print_if_failure(joined_tb.error) ) return 1;
    print_table(joined_tb.value);

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

    print_if_failure(save_table(db.value, joined_tb.value));

    print_table(t1);

    if  (   !print_if_failure( data_init_integer(data1.value, 300) ) 
        ||  !print_if_failure( data_init_boolean(data1.value, 0) )
        ||  !print_if_failure( data_init_float(data1.value, 15.68) )
        ||  !print_if_failure( data_init_string(data1.value, "zinger string") ) ) {

            release_table(t1);
            release_data(data1.value);
            return 1;
    }

    update_closure = (closure) { .func=string_modify_func, .value1=(any_value){ .string_value="zinger string updated" } };
    printf("updated %d rows\n", update_where(t1, STRING, "strings", update_closure, data1.value).count);
    
    release_data(data1.value);

    print_table(t1);

    release_table(t1);

    fclose(dbfile);
}