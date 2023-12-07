#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <event.h>
#include <pthread.h>



#include "benchmark_internal.h"
#include "memcached.h"

#define ITEM_SIZE (sizeof(item) + BENCHMARK_ITEM_VALUE_SIZE + BENCHMARK_ITEM_KEY_SIZE + 34)

void start_dynrep_protocol(void) {

    register int rdi __asm__ ("rdi") = 2;
    register int rsi __asm__ ("rsi") = 11;

    register int rdx __asm__ ("rdx") = 1;
    register int r10 __asm__ ("r10") = 99;

    __asm__ __volatile__ (
        "syscall"
        :
        : "r" (rdi), "r" (rsi), "r" (rdx), "r" (r10)
        : "rcx", "r11", "memory"
    );
}
// ./configure --disable-extstore --enable-static

void internal_benchmark_config(struct settings* settings)
{
    fprintf(stderr, "=====================================\n");
    fprintf(stderr, "INTERNAL BENCHMARK CONFIGURE\n");
    fprintf(stderr, "=====================================\n");

    // calculate the number of items
    size_t num_items = settings->x_benchmark_mem / ITEM_SIZE;
    if (num_items < 100) {
        fprintf(stderr, "WARNING: too little elements\n");
        num_items = 100;
    }

    // calculate the maximum number of bytes
    settings->maxbytes = settings->x_benchmark_mem;

    settings->use_cas = true;
    settings->lru_maintainer_thread = false;

    size_t hash_power = HASHPOWER_DEFAULT;
    while((num_items >> (hash_power - 1)) != 0 && hash_power < HASHPOWER_MAX) {
        hash_power++;
    }

    settings->hashpower_init = hash_power;

    settings->slab_reassign = false;
    settings->idle_timeout = false;
    settings->item_size_max = 1024;
    settings->slab_page_size = BENCHMARK_USED_SLAB_PAGE_SIZE;
    fprintf(stderr,"------------------------------------------\n");
    fprintf(stderr, " - x_benchmark_mem = %zu MB\n", settings->x_benchmark_mem >> 20);
    fprintf(stderr, " - x_benchmark_queries = %zu\n", settings->x_benchmark_queries);
    fprintf(stderr, " - num_threads = %u\n", omp_get_num_procs());
    fprintf(stderr, " - maxbytes = %zu MB\n", settings->maxbytes >> 20);
    fprintf(stderr, " - slab_page_size = %u kB\n", settings->slab_page_size >> 10);
    fprintf(stderr, " - hashpower_init = %u\n", settings->hashpower_init);
    fprintf(stderr,"------------------------------------------\n");
}

#ifndef __linux__
static void *send_packets_thread(void * arg) {
    fprintf(stderr, "networking hack thread started\n");

    struct sockaddr_in si_me, si_other;
    int s, blen, slen = sizeof(si_other);
    char buf[65];
    snprintf(buf, 64, "HELLO");
    blen = strlen(buf);


    s = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
    if (s == -1) {
        perror("SEND PACKETS: FAILED TO SETUP SOCKET!\n");
        return NULL;
    }


    memset((char *) &si_me, 0, sizeof(si_me));
    si_me.sin_family = AF_INET;
    si_me.sin_port = htons(11000);
    si_me.sin_addr.s_addr = htonl(INADDR_ANY);
    if (bind(s, (struct sockaddr*) &si_me, sizeof(si_me))== -1) {
        perror("SEND PACKETS: COULD NOT BIND!\n");
        return NULL;
    }

    memset((char *) &si_other, 0, sizeof(si_other));
    si_other.sin_family = AF_INET;
    si_other.sin_port = htons(1234);
    si_other.sin_addr.s_addr = htonl(0xac1f0014); // 172.31.0.20

    while (1) {
        sleep(1);

        //send answer back to the client
        int r = sendto(s, buf, blen, 0, (struct sockaddr*) &si_other, slen);
        if (r == -1) {
            perror("SEND PACKETS: SENDING FAILED!\n");
        }
    }
}

static pthread_t network_thread;

#endif

void internal_benchmark_run(struct settings* settings, struct event_base *main_base)
{
    if (settings->x_benchmark_no_run) {
        fprintf(stderr, "=====================================\n");
        fprintf(stderr, "INTERNAL BENCHMARK SKIPPING\n");
        fprintf(stderr, "=====================================\n");
#ifdef __linux__
        fprintf(stderr, "skipping networking thread\n");
#else
        if (pthread_create(&network_thread, NULL, send_packets_thread, NULL) != 0) {
            fprintf(stderr, "COULD NOT CREATE PTHREAD!\n");
        }
#endif

        return;
    }

    fprintf(stderr, "=====================================\n");
    fprintf(stderr, "INTERNAL BENCHMARK STARTING\n");
    fprintf(stderr, "=====================================\n");


    ////////////////////////////////////////////////////////////////////////////////////////////////
    // Establish the connections
    ////////////////////////////////////////////////////////////////////////////////////////////////
    size_t num_threads = omp_get_num_procs();
    conn** my_conns = calloc(num_threads, sizeof(*my_conns));
    for (size_t i = 0; i < num_threads; i++) {
        my_conns[i] = conn_new(i, conn_listening, 0, 0, local_transport, main_base, NULL, 0, ascii_prot);
        my_conns[i]->thread = malloc(sizeof(LIBEVENT_THREAD));
    }

    // set the number of threads to the amount of processors we have
    omp_set_num_threads(num_threads);


    // calculate the amount of items to fit within memory.
    size_t num_items = settings->x_benchmark_mem / (ITEM_SIZE);
    if (num_items < 100) {
        fprintf(stderr, "WARNING: too little elements\n");
        num_items = 100;
    }

    struct timeval start, end, elapsed, iteration;
    uint64_t elapsed_us;

    fprintf(stderr, "number of threads: %zu\n", num_threads);
    fprintf(stderr, "item size: %zu bytes\n", ITEM_SIZE);
    fprintf(stderr, "number of keys: %zu\n", num_items);

    size_t counter = 0;

    fprintf(stderr, "Prefilling slabs\n");
    gettimeofday(&start, NULL);
    slabs_prefill_global();

    gettimeofday(&end, NULL);
    timersub(&end, &start, &elapsed);
    elapsed_us = (elapsed.tv_sec * 1000000) + elapsed.tv_usec;
    fprintf(stderr, "Prefilling slabs took %lu ms\n", elapsed_us / 1000);

    fprintf(stderr, "Populating %zu / %zu key-value pairs.\n", counter, num_items);

    gettimeofday(&start, NULL);

/* prepopulate the thing */
#pragma omp parallel reduction(+ \
                               : counter)
    {
        /* pin threads */
        int thread_id = omp_get_thread_num();
        size_t my_counter=0;

#ifdef __linux__
        // cpu_set_t my_set;
        // CPU_ZERO(&my_set);
        // CPU_SET(thread_id, &my_set);
        // pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &my_set);
#else
        /* BSD/RUMP kernel doesn't do this! */
        // cpuset_t *my_set = cpuset_create();
        // cpuset_zero(my_set);
        // cpuset_set(thread_id, my_set);
        // pthread_setaffinity_np(pthread_self(), cpuset_size(my_set), my_set);
#endif
        conn* myconn = my_conns[thread_id];

        size_t num_added = 0;
        size_t num_not_added = 0;
        size_t num_existed = 0;
        size_t num_other_errors = 0;

#pragma omp for schedule(static, 1024)
        for (size_t i = 0; i < num_items; i++) {

            my_counter++;

            char key[BENCHMARK_ITEM_KEY_SIZE + 1];
            snprintf(key, BENCHMARK_ITEM_KEY_SIZE + 1, "%08x", (unsigned int)i);

            char value[BENCHMARK_ITEM_VALUE_SIZE + 1];
            snprintf(value, BENCHMARK_ITEM_VALUE_SIZE, "value-%016lx", i);

            item* it = item_alloc(key, BENCHMARK_ITEM_KEY_SIZE, 0, 0, BENCHMARK_ITEM_VALUE_SIZE);
            if (!it) {
                printf("Item was NULL! %zu\n", i);
                continue;
            }

            memcpy(ITEM_data(it), value, BENCHMARK_ITEM_VALUE_SIZE);

            uint64_t cas = 0;
            switch (store_item(it, NREAD_SET, myconn->thread, NULL, &cas, CAS_NO_STALE)) {
                case STORED:
                    num_added++;
                    myconn->cas = cas;
                    break;
                case EXISTS:
                    num_existed++;
                    num_not_added++;
                    break;
                case NOT_STORED:
                    num_not_added++;
                    break;
                default:
                    num_other_errors++;
                    break;
            }

            if ((my_counter % 1000000) == 0) {
                fprintf(stderr, "populate: thread %d added %zu elements. \n", thread_id, counter);
            }
        }
        counter = my_counter;
        fprintf(stderr, "populate: thread %d done. added %zu elements, %zu not added of which %zu already existed\n", thread_id, num_added, num_not_added, num_existed);
    }

    gettimeofday(&end, NULL);
    timersub(&end, &start, &elapsed);
    elapsed_us = (elapsed.tv_sec * 1000000) + elapsed.tv_usec;

    fprintf(stderr, "Populated %zu / %zu key-value pairs in %lu ms:\n", counter, num_items, elapsed_us / 1000);
    fprintf(stderr, "=====================================\n");

    fprintf(stderr, "Executing %zu queries with %zu threads.\n", num_threads * settings->x_benchmark_queries, num_threads);

    gettimeofday(&start, NULL);

    size_t num_queries = 0;
// for (size_t i = 0; i < 2; i++) {
#pragma omp parallel reduction(+ \
                               : num_queries)
    {

        /* pin threads */
        int thread_id = omp_get_thread_num();
        size_t unknown = 0;
        size_t found = 0;
        size_t thread_queries = 0;
        uint64_t values = 0;

        conn* myconn = my_conns[thread_id];

        fprintf(stderr,"thread:%i uses connection %p\n", thread_id, (void *)myconn);

        size_t g_seed = (214013UL * thread_id + 2531011UL);

        struct timeval thread_start, thread_current, thread_elapsed;
        gettimeofday(&thread_start, NULL);

        size_t query_counter = 0;
	size_t second_counter = 0;

#pragma omp for schedule(static)
        for (size_t i = 0; i < (num_threads * settings->x_benchmark_queries); i++) {

            query_counter++;
            size_t idx = (i + (g_seed >> 16)) % (num_items);
            g_seed = (214013UL * g_seed + 2531011UL);

            char key[BENCHMARK_ITEM_KEY_SIZE + 1];
            snprintf(key, BENCHMARK_ITEM_KEY_SIZE + 1, "%08x", (unsigned int)idx);

            item* it = item_get(key, BENCHMARK_ITEM_KEY_SIZE, myconn->thread, DONT_UPDATE);
            if (!it) {
                unknown++;
            } else {
                found++;
                // access the item
                values ^= *((uint64_t *)ITEM_data(it));
            }

            if ((query_counter % 100) == 0) {
                gettimeofday(&thread_current, NULL);
                timersub(&thread_current, &thread_start, &thread_elapsed);
                if (thread_elapsed.tv_sec == PERIODIC_PRINT_INTERVAL) {

                    uint64_t thread_elapsed_us = (thread_elapsed.tv_sec * 1000000) + thread_elapsed.tv_usec;
                    fprintf(stderr, "thread.%d executed %lu queries in %lu us\n", thread_id,
                        (query_counter) - thread_queries, thread_elapsed_us);

		    if (thread_id == 0 && second_counter == 20) {
		    	start_dynrep_protocol();
		    }

                    // reset the thread start time
                    thread_start = thread_current;
                    thread_queries = (query_counter);
		    second_counter++;
		}
            }
        }

        num_queries += unknown + found;
        fprintf(stderr,"thread:%i done. executed %zu found %zu, missed %zu  (checksum: %lx)\n", thread_id, num_queries, found, unknown, values);
    }
    gettimeofday(&end, NULL);

    timersub(&end, &start, &elapsed);
    elapsed_us = (elapsed.tv_sec * 1000000) + elapsed.tv_usec;

    fprintf(stderr, "benchmark took %lu ms\n", elapsed_us / 1000);
    fprintf(stderr, "benchmark took %lu queries / second\n", (num_queries / elapsed_us) * 1000000);
    fprintf(stderr, "benchmark executed %lu / %lu queries\n", num_queries, (num_threads * settings->x_benchmark_queries));

    fprintf(stderr, "terminating.\n");
    fprintf(stderr, "===============================================================================\n");
    fprintf(stderr, "===============================================================================\n");

    exit(0);
}
