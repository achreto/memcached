#ifdef __linux__
#define _GNU_SOURCE
#endif
#include <sched.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <event.h>
#include <pthread.h>



#include "benchmark_internal.h"
#include "memcached.h"

#define ITEM_SIZE (sizeof(item) + BENCHMARK_ITEM_VALUE_SIZE + BENCHMARK_ITEM_KEY_SIZE + 34)

// ./configure --disable-extstore --enable-static

size_t thread_barrier;
size_t num_elements;
size_t num_queries;
pthread_mutex_t lock;


struct xor_shift {
    uint64_t state;
};

static inline void xor_shift_init(struct xor_shift *st, uint64_t tid)
{
    st->state = 0xdeadbeefdeadbeef ^ tid;
}

static inline uint64_t xor_shift_next(struct xor_shift *st, uint64_t num_elements) {
    // https://en.wikipedia.org/wiki/Xorshift
    uint64_t x = st->state;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    st->state = x;
    return x % num_elements;
}


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

pthread_barrier_t barrier;
pthread_barrier_t barrier1;
pthread_barrier_t barrier2;
pthread_barrier_t barrier3;

struct thread_data {
    size_t tid;
    pthread_t thread;
    conn* connection;
    struct settings *settings;
    size_t num_items;
    size_t num_items_total;
    size_t num_threads;

    size_t num_elements;
    size_t num_queries_hit;
    size_t num_queries_missed;
};


static void set_affinity( struct thread_data *td) {
    #if defined(__FreeBSD__) && defined(HAVE_CPUSET_SETAFFINITY)

    cpuset_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(td->tid, &cpuset);
    (void)cpuset_setaffinity(CPU_LEVEL_WHICH, CPU_WHICH_TID, -1, sizeof(cpuset), &cpuset);

    #elif defined(__DragonFly__) && defined(HAVE_PTHREAD_SETAFFINITY_NP)

    cpuset_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(td->tid, &cpuset);
    (void)pthread_setaffinity_np(pthread_self(), sizeof(cpuset), &cpuset);

    #elif defined(__NetBSD__) && defined(HAVE_PTHREAD_SETAFFINITY_NP)

    cpuset_t *cpuset = cpuset_create();
    cpuset_set(td->tid, cpuset);
    (void)pthread_setaffinity_np(pthread_self(), cpuset_size(cpuset), cpuset);
    cpuset_destroy(cpuset);

    #elif defined(HAVE_SCHED_SETAFFINITY)

    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(td->tid, &cpuset);
    (void)sched_setaffinity(0, sizeof(cpuset), &cpuset);

    #elif defined(HAVE_CPUSET_SETAFFINITY)

    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(td->tid, &cpuset);
    (void)cpuset_setaffinity(CPU_LEVEL_WHICH, CPU_WHICH_TID, -1, sizeof(cpuset), &cpuset);

    #elif defined(HAVE_PTHREAD_SETAFFINITY_NP) || defined(__linux__)

    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(td->tid, &cpuset);
    (void)pthread_setaffinity_np(pthread_self(), sizeof(cpuset), &cpuset);
    #else
    cpuset_t *cpuset = cpuset_create();
    cpuset_set(td->tid, cpuset);
    (void)pthread_setaffinity_np(pthread_self(), cpuset_size(cpuset), cpuset);
    cpuset_destroy(cpuset);
    #endif
}

static void *do_populate(void *arg) {
    struct thread_data *td = arg;

    size_t my_counter = 0;
    conn* myconn = td->connection;

    size_t num_added = 0;
    size_t num_not_added = 0;
    size_t num_existed = 0;
    size_t num_other_errors = 0;

    for (size_t i = 0; i < td->num_items; i++) {
        // calculate the key id
        size_t keyval = td->tid * td->num_items + i;

        my_counter++;

        char key[BENCHMARK_ITEM_KEY_SIZE + 1];
        snprintf(key, BENCHMARK_ITEM_KEY_SIZE + 1, "%08x", (unsigned int)keyval);

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

        if ((my_counter % 100000) == 0) {
            fprintf(stderr, "populate: thread.%zu added %zu elements. \n",  td->tid, my_counter);
        }
    }
    fprintf(stderr, "populate: thread.%zu done. added %zu elements, %zu not added of which %zu already existed\n", td->tid, num_added, num_not_added, num_existed);

    // store the number of elements
    td->num_elements = my_counter;
    return NULL;
}


static void *do_benchmark(void *arg) {
    struct thread_data *td = arg;

    conn* myconn = td->connection;

    size_t unknown = 0;
    size_t found = 0;
    size_t thread_queries = 0;
    uint64_t values = 0xabcdabcd;

    fprintf(stderr,"thread:%zu uses connection %p\n", td->tid, (void *)myconn);

    struct xor_shift rand;
    xor_shift_init(&rand, td->tid);

    struct timeval thread_start, thread_current, thread_elapsed;
    gettimeofday(&thread_start, NULL);

    size_t query_counter = 0;

    for (size_t i = 0; i < td->settings->x_benchmark_queries; i++) {

        query_counter++;
        uint64_t idx = xor_shift_next(&rand, td->num_items_total);

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
                fprintf(stderr, "thread.%zu executed %lu queries in %lu us\n", td->tid,
                    (query_counter) - thread_queries, thread_elapsed_us);

                // reset the thread start time
                thread_start = thread_current;
                thread_queries = (query_counter);
            }
        }
    }

    fprintf(stderr,"thread:%zu done. executed %zu found %zu, missed %zu  (checksum: %lx)\n", td->tid, query_counter, found, unknown, values);

    td->num_queries_hit = found;
    td->num_queries_missed = unknown;
    return NULL;
}


static void *do_run(void *arg) {
    struct thread_data *td = arg;

    fprintf(stderr, "thread.%zu start running\n", td->tid);

    set_affinity(td);
    pthread_barrier_wait(&barrier);

    do_populate(arg);

    pthread_barrier_wait(&barrier);
    sleep(1);
    pthread_barrier_wait(&barrier);

    do_benchmark(arg);

    pthread_barrier_wait(&barrier);
    // fprintf(stderr, "thread.%zu done. hanging around\n", td->tid);
    pthread_barrier_wait(&barrier);

    return NULL;
}


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

    // yeah we still use openmp to figure out the number of threads!
    size_t num_threads = omp_get_num_procs();

    // initialize barrier
    if (pthread_barrier_init(&barrier, NULL, num_threads + 1) != 0) {
        fprintf(stderr, "ERROR: failed to create thread!\n");
        exit(1);
    }

    pthread_mutex_init(&lock, NULL);

    // calculate the amount of items to fit within memory.
    size_t num_items = settings->x_benchmark_mem / (ITEM_SIZE);
    if (num_items < 100) {
        fprintf(stderr, "WARNING: too little elements\n");
        num_items = 100;
    }

    size_t num_items_per_thread = (num_items + num_threads - 1) / num_threads;
    num_items = num_items_per_thread * num_threads;

    ////////////////////////////////////////////////////////////////////////////////////////////////
    // Establish the connections
    ////////////////////////////////////////////////////////////////////////////////////////////////

    struct thread_data *threads = calloc(num_threads, sizeof(*threads));
    for (size_t i = 0; i < num_threads; i++) {
        threads[i].tid = i;
        threads[i].connection = conn_new(i, conn_listening, 0, 0, local_transport, main_base, NULL, 0, ascii_prot);
        threads[i].connection->thread = malloc(sizeof(LIBEVENT_THREAD));
        threads[i].settings = settings;
        threads[i].num_items = num_items_per_thread;
        threads[i].num_items_total = num_items;
        threads[i].num_threads = num_threads;
    }

    struct timeval start, end, elapsed;
    uint64_t elapsed_us;

    fprintf(stderr, "number of threads: %zu\n", num_threads);
    fprintf(stderr, "item size: %zu bytes\n", ITEM_SIZE);
    fprintf(stderr, "number of keys: %zu\n", num_items);

    fprintf(stderr, "Prefilling slabs\n");
    gettimeofday(&start, NULL);
    slabs_prefill_global();

    gettimeofday(&end, NULL);
    timersub(&end, &start, &elapsed);
    elapsed_us = (elapsed.tv_sec * 1000000) + elapsed.tv_usec;
    fprintf(stderr, "Prefilling slabs took %lu ms\n", elapsed_us / 1000);


    thread_barrier = 0;
    num_elements = 0;

    for (size_t i = 0; i < num_threads; i++) {
        if (pthread_create(&threads[i].thread, NULL, do_run, (void*)&threads[i]) != 0) {
            fprintf(stderr, "ERROR: failed to create thread!\n");
            exit(1);
        }
    }

    // wait until threads have finished setting affinity
    pthread_barrier_wait(&barrier);
    gettimeofday(&start, NULL);
    fprintf(stderr, "Start populating...\n");

    // wait until threads have finished populating
    pthread_barrier_wait(&barrier);
    gettimeofday(&end, NULL);

    // threads now transition to the benchmarking phase
    timersub(&end, &start, &elapsed);
    elapsed_us = (elapsed.tv_sec * 1000000) + elapsed.tv_usec;

    size_t counter = 0;
    for (size_t i = 0; i < num_threads; i++) {
        counter += threads[i].num_elements;
    }

    fprintf(stderr, "Populated %zu / %zu key-value pairs in %lu ms:\n", counter, num_items, elapsed_us / 1000);
    fprintf(stderr, "=====================================\n");

    pthread_barrier_wait(&barrier);
    gettimeofday(&start, NULL);
    fprintf(stderr, "Executing %zu queries with %zu threads.\n", num_threads * settings->x_benchmark_queries, num_threads);

    pthread_barrier_wait(&barrier);
    gettimeofday(&end, NULL);

    size_t num_queries = 0;
    size_t missed_queries = 0;
    for (size_t i = 0; i < num_threads; i++) {
        num_queries  += (threads[i].num_queries_hit + threads[i].num_queries_missed);
        missed_queries += threads[i].num_queries_missed;
    }

    timersub(&end, &start, &elapsed);
    elapsed_us = (elapsed.tv_sec * 1000000) + elapsed.tv_usec;

    fprintf(stderr, "===============================================================================\n");
    fprintf(stderr, "benchmark took %lu ms\n", elapsed_us / 1000);
    // converting num_queries per microsecond to num qeuries per second.
    fprintf(stderr, "benchmark took %lu queries / second\n", (num_queries * 1000000 / elapsed_us));

    fprintf(stderr, "benchmark executed %zu / %zu queries   (%zu missed) \n", num_queries, (num_threads * settings->x_benchmark_queries), missed_queries);

    fprintf(stderr, "terminating.\n");
    fprintf(stderr, "===============================================================================\n");
    fprintf(stderr, "===============================================================================\n");

    exit(0);
}
