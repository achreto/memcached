#include <event.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "benchmark_internal.h"
#include "memcached.h"

// whether to use the homebrew barrier instead of the pthread barrier
#define USE_HOMEBREW_BARRIER 1

// the duration of the sleep at the start of the execution
#define THREADS_INITIAL_SLEEP 5

// maximum number of threads supported
#define THREADS_MAX 1024

// whether or not to prefill the slabs
// #define PREFILL_SLABS 1

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

void memory_prealloc(uint64_t mem_in_mbytes) {

    register int rdi __asm__ ("rdi") = 2;
    register int rsi __asm__ ("rsi") = 12;

    register int rdx __asm__ ("rdx") = 1;
    register int r10 __asm__ ("r10") = mem_in_mbytes;

    __asm__ __volatile__ (
        "syscall"
        :
        : "r" (rdi), "r" (rsi), "r" (rdx), "r" (r10)
        : "rcx", "r11", "memory"
    );
}

size_t num_elements;
size_t num_queries;

#ifndef USE_HOMEBREW_BARRIER
pthread_barrier_t barrier;
#endif

////////////////////////////////////////////////////////////////////////////////////////////////////
// HOMEBREW BARRIER
////////////////////////////////////////////////////////////////////////////////////////////////////

#ifdef USE_HOMEBREW_BARRIER

#define BARRIER_POLL_BEFORE_YIELD 5000

enum benchmark_phase {
    THREAD_READY = 0,
    POPULATE,
    POPULATED,
    RUN,
    DONE
};

static volatile enum benchmark_phase PHASE = THREAD_READY;
static volatile int BARRIER[THREADS_MAX] = {0};

pthread_mutex_t lock;

static inline void barrier_phase_complete_wait(size_t tid)
{
    // get the current phase value
    enum benchmark_phase current = __atomic_load_n(&PHASE, __ATOMIC_SEQ_CST);

    // enter the barrier by setting the thread's barrier flag to 1
    __atomic_store_n(&BARRIER[tid], 1, __ATOMIC_SEQ_CST);

    // wait until we have moved to the next phase, no sleep here.
    size_t counter = 0;
    while (__atomic_load_n(&PHASE, __ATOMIC_SEQ_CST) == current) {
        counter++;
        if (counter > BARRIER_POLL_BEFORE_YIELD) {
            counter = 0;
            sched_yield();
        }
    }
}

static void barrier_wait_trigger_next(size_t num_threads)
{
    // enter the barrier by incrementing the barrier counter
    //__atomic_fetch_add(&BARRIER, 1, __ATOMIC_SEQ_CST);
    BARRIER[0] = 1;

    // go through the threads and wait until they have all entered the barrier
    size_t counter = 0;
    for (size_t tid = 1; tid < num_threads; tid++) {
        // wait until the thread signals it has entered the barrier
        while(__atomic_load_n(&BARRIER[tid], __ATOMIC_SEQ_CST) == 0) {
            counter++;
            if (counter > BARRIER_POLL_BEFORE_YIELD) {
                counter = 0;
                sched_yield();
            }
        }
        // we've seen this thread, so we can reset the the thread's barrier flag again.
        BARRIER[tid] = 0;
    }

    // trigger next phase
    switch (PHASE) {
    case THREAD_READY:
        __atomic_store_n(&PHASE, POPULATE, __ATOMIC_SEQ_CST);
        break;
    case POPULATE:
        __atomic_store_n(&PHASE, POPULATED, __ATOMIC_SEQ_CST);
        break;
    case POPULATED:
        __atomic_store_n(&PHASE, RUN, __ATOMIC_SEQ_CST);
        break;
    case RUN:
        __atomic_store_n(&PHASE, DONE, __ATOMIC_SEQ_CST);
        break;
    case DONE:
        break;
    }
}

#endif

////////////////////////////////////////////////////////////////////////////////////////////////////
// XORSHIFT Random Number Generator
////////////////////////////////////////////////////////////////////////////////////////////////////

struct xor_shift {
    uint64_t state;
    uint64_t num_elements;
};

static inline void xor_shift_init(struct xor_shift* st, uint64_t tid, uint64_t num_elements)
{
    st->state = 0xdeadbeefdeadbeef ^ tid;
    st->num_elements = num_elements;
}

static inline uint64_t xor_shift_next(struct xor_shift* st)
{
    // https://en.wikipedia.org/wiki/Xorshift
    uint64_t x = st->state;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    st->state = x;
    return x % st->num_elements;
}

////////////////////////////////////////////////////////////////////////////////////////////////////
// Benchmark Configuration
////////////////////////////////////////////////////////////////////////////////////////////////////

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
    settings->maxbytes = settings->x_benchmark_mem + (settings->x_benchmark_mem / 16);

    settings->use_cas = true;
    settings->lru_maintainer_thread = false;

    size_t hash_power = HASHPOWER_DEFAULT;
    while ((num_items >> (hash_power - 1)) != 0 && hash_power < HASHPOWER_MAX) {
        hash_power++;
    }

#ifdef PROXY
    settings->proxy_enabled = false;
#endif
    settings->hashpower_init = hash_power;

    settings->slab_reassign = false;
    settings->idle_timeout = false;
    settings->item_size_max = 1024;
    settings->slab_page_size = BENCHMARK_USED_SLAB_PAGE_SIZE;

    fprintf(stderr, "------------------------------------------\n");
    fprintf(stderr, " - x_benchmark_mem = %zu MB\n", settings->x_benchmark_mem >> 20);

    if (settings->prealloc_mem) {
        memory_prealloc(settings->num_threads * 75);
    }

    fprintf(stderr, " - x_benchmark_num_queries = %zu\n", settings->x_benchmark_num_queries);
    fprintf(stderr, " - x_benchmark_query_time = %zu s\n", settings->x_benchmark_query_duration);
    fprintf(stderr, " - num_threads = %u\n", omp_get_num_procs());
    fprintf(stderr, " - maxbytes = %zu MB\n", settings->maxbytes >> 20);
    fprintf(stderr, " - slab_page_size = %u kB\n", settings->slab_page_size >> 10);
    fprintf(stderr, " - hashpower_init = %u\n", settings->hashpower_init);
    fprintf(stderr, "------------------------------------------\n");

}

#ifndef __linux__
static void* send_packets_thread(void* arg)
{
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

    memset((char*)&si_me, 0, sizeof(si_me));
    si_me.sin_family = AF_INET;
    si_me.sin_port = htons(11000);
    si_me.sin_addr.s_addr = htonl(INADDR_ANY);
    if (bind(s, (struct sockaddr*)&si_me, sizeof(si_me)) == -1) {
        perror("SEND PACKETS: COULD NOT BIND!\n");
        return NULL;
    }

    memset((char*)&si_other, 0, sizeof(si_other));
    si_other.sin_family = AF_INET;
    si_other.sin_port = htons(1234);
    si_other.sin_addr.s_addr = htonl(0xac1f0014); // 172.31.0.20

    while (1) {
        sleep(1);

        // send answer back to the client
        int r = sendto(s, buf, blen, 0, (struct sockaddr*)&si_other, slen);
        if (r == -1) {
            perror("SEND PACKETS: SENDING FAILED!\n");
        }
    }
}

static pthread_t network_thread;
#endif

struct thread_data {
    size_t tid;
    pthread_t thread;
    conn* connection;
    struct settings* settings;
    size_t num_items;
    size_t num_items_total;
    size_t num_threads;

    size_t num_elements;
    size_t num_queries_hit;
    size_t num_queries_missed;
};

static void* do_populate(void* arg)
{
    struct thread_data* td = arg;

    size_t my_counter = 0;
    conn* myconn = td->connection;

    size_t num_added = 0;
    size_t num_not_added = 0;
    size_t num_existed = 0;
    size_t num_other_errors = 0;

    if (td->settings->prealloc_mem) {
        fprintf(stderr, "prealloc: thread:%03zu prealloc'ing %03zu memslices. \n", td->tid, td->num_items * (BENCHMARK_ITEM_KEY_SIZE + BENCHMARK_ITEM_VALUE_SIZE + 2) >> 20);
        memory_prealloc(td->num_items * (BENCHMARK_ITEM_KEY_SIZE + BENCHMARK_ITEM_VALUE_SIZE + 2) >> 20);
    }
    for (size_t i = 0; i < td->num_items; i++) {
        // calculate the key id
        size_t keyval = td->tid * td->num_items + i;

        my_counter++;

        char key[BENCHMARK_ITEM_KEY_SIZE + 1];
        snprintf(key, BENCHMARK_ITEM_KEY_SIZE + 1, "%08x", (unsigned int)keyval);

        char value[BENCHMARK_ITEM_VALUE_SIZE + 1];
        snprintf(value, BENCHMARK_ITEM_VALUE_SIZE, "it-%lu-%016lx", i, i);

        item* it = item_alloc(key, BENCHMARK_ITEM_KEY_SIZE, 0, 0, BENCHMARK_ITEM_VALUE_SIZE);
        if (!it) {
            printf("FATAL: Item was NULL! this means we could not allocate storage for the item.\n");
            exit(1);
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
            fprintf(stderr, "populate: thread:%03zu added %zu/%zu elements. \n", td->tid, my_counter, td->num_items);
        }
    }
    fprintf(stderr, "populate: thread:%03zu done. added %zu elements, %zu not added of which %zu already existed\n", td->tid, num_added, num_not_added, num_existed);

    // store the number of elements
    td->num_elements = my_counter;

    return NULL;
}

static void* do_run(void* arg)
{
    struct thread_data* td = arg;

    conn* myconn = td->connection;

    size_t unknown = 0;
    size_t found = 0;
    size_t thread_queries = 0;
    size_t query_counter = 0;
    uint64_t values = 0xabcdabcd;

    // fprintf(stderr, "thread:%03zu uses connection %p\n", td->tid, (void*)myconn);

    struct xor_shift rand;
    xor_shift_init(&rand, td->tid, td->num_items_total);

    struct timeval thread_start, thread_current, thread_elapsed, thread_stop;
    thread_current.tv_usec = 0;
    thread_current.tv_sec = td->settings->x_benchmark_query_duration;
    if (td->settings->x_benchmark_query_duration == 0) {
        thread_current.tv_sec = 3600 * 24; // let's set a timeout to 24 hours...
    }

    size_t max_queries = td->settings->x_benchmark_num_queries;
    if (max_queries == 0) {
        max_queries = 0xffffffffffffffff; // set it to a large number
    }

    // record the start time and calculate the end time
    gettimeofday(&thread_start, NULL);
    timeradd(&thread_start, &thread_current, &thread_stop);

    bool started_dynrep = !(td->settings->dyn_rep_test);
    do {
        if (query_counter == max_queries) {
            break;
        }
        // only check the time so often...
        if ((query_counter % 128) == 0) {
            gettimeofday(&thread_current, NULL);

            timersub(&thread_current, &thread_start, &thread_elapsed);
            if (thread_elapsed.tv_sec == PERIODIC_PRINT_INTERVAL) {

                uint64_t thread_elapsed_us = (thread_elapsed.tv_sec * 1000000) + thread_elapsed.tv_usec;
                fprintf(stderr, "thread:%03zu executed %lu queries in %lu ms\n", td->tid,
                    (query_counter)-thread_queries, thread_elapsed_us / 1000);

                if (!started_dynrep) {
                    start_dynrep_protocol();
                    started_dynrep = true;
                }

                // reset the thread start time
                thread_start = thread_current;
                thread_queries = query_counter;
            }
        }

        query_counter++;
        uint64_t idx = xor_shift_next(&rand);

        char key[BENCHMARK_ITEM_KEY_SIZE + 1];
        snprintf(key, BENCHMARK_ITEM_KEY_SIZE + 1, "%08x", (unsigned int)idx);

        item* it = item_get(key, BENCHMARK_ITEM_KEY_SIZE, myconn->thread, DONT_UPDATE);
        if (!it) {
            unknown++;
        } else {
            found++;
            // access the item
            values ^= *((uint64_t*)ITEM_data(it));
        }
    } while (timercmp(&thread_current, &thread_stop, <));

    fprintf(stderr, "thread:%03zu done. executed %zu found %zu, missed %zu  (checksum: %lx)\n", td->tid, query_counter, found, unknown, values);

    td->num_queries_hit = found;
    td->num_queries_missed = unknown;

    return (void*)query_counter;
}

static void* do_benchmark(void* arg)
{
    struct thread_data* td = arg;
    fprintf(stderr, "thread:%03zu started\n", td->tid);

    // Give some time to the other threads to start up.
    sleep(THREADS_INITIAL_SLEEP);

    if (td->tid == 0) {
        struct timeval start, end, elapsed;
        uint64_t elapsed_us;
        // wait until everyone has reached the ready barrier, THREAD_READY -> POPULATE
        barrier_wait_trigger_next(td->num_threads);
        gettimeofday(&start, NULL);
        fprintf(stderr, "thread:%03zu populating\n", td->tid);
        do_populate(td);
        fprintf(stderr, "thread:%03zu ready\n", td->tid);

        barrier_wait_trigger_next(td->num_threads);
        gettimeofday(&end, NULL);
        timersub(&end, &start, &elapsed);
        elapsed_us = (elapsed.tv_sec * 1000000) + elapsed.tv_usec;

        size_t counter = 0;
        for (size_t i = 0; i < td->num_threads; i++) {
            // XXX: assumes the thread is there
            counter += td[i].num_elements;
        }

        fprintf(stderr, "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n");
        fprintf(stderr, "Populated %zu / %zu key-value pairs in %lu ms:\n", counter, td->num_items, elapsed_us / 1000);
        fprintf(stderr, "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n");

        fprintf(stderr, "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n");
        fprintf(stderr, "Executing %zu queries with %zu threads for %zu seconds.\n", td->num_threads * td->settings->x_benchmark_num_queries, td->num_threads, td->settings->x_benchmark_query_duration);
        fprintf(stderr, "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n");

        // trigger next phase, POPULATED->RUN
        barrier_wait_trigger_next(td->num_threads);
        gettimeofday(&start, NULL);
        do_run(arg);
        // trigger next phase, RUN->DONE
        barrier_wait_trigger_next(td->num_threads);
        gettimeofday(&end, NULL);

        timersub(&end, &start, &elapsed);
        elapsed_us = (elapsed.tv_sec * 1000000) + elapsed.tv_usec;

        size_t num_queries = 0;
        size_t missed_queries = 0;
        for (size_t i = 0; i < td->num_threads; i++) {
            num_queries += (td[i].num_queries_hit + td[i].num_queries_missed);
            missed_queries += td[i].num_queries_missed;
        }

        fprintf(stderr, "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n");
        fprintf(stderr, "Benchmark Done.\n");
        fprintf(stderr, "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n");

        fprintf(stderr, "benchmark took %lu ms (of %lu ms)\n", elapsed_us / 1000, td->settings->x_benchmark_query_duration * 1000);
        fprintf(stderr, "benchmark executed %zu / %zu queries   (%zu missed) \n", num_queries, (td->num_threads * td->settings->x_benchmark_num_queries), missed_queries);
        // converting num_queries per microsecond to num qeuries per second.
        fprintf(stderr, "benchmark throughput %lu queries / second\n", (num_queries * 1000000 / elapsed_us));
        if (missed_queries > 0) {
            fprintf(stderr, "benchmark missed %zu queries!\n", missed_queries);
        }
        fprintf(stderr, "terminating.\n");
        fprintf(stderr, "===============================================================================\n");
        fprintf(stderr, "===============================================================================\n");
        exit(0);
    } else {
        barrier_phase_complete_wait(td->tid); // THREAD_READY -> POPULATE
        fprintf(stderr, "thread:%03zu populating\n", td->tid);
        do_populate(arg);
        // fprintf(stderr, "thread:%03zu ready\n", td->tid);
        barrier_phase_complete_wait(td->tid); // POPULATE -> POPULATED
        // give time to print stats...
        barrier_phase_complete_wait(td->tid); // POPULATED -> RUN
        // fprintf(stderr, "thread:%03zu running\n", td->tid);
        do_run(arg);
        barrier_phase_complete_wait(td->tid); // RUN -> DONE
        // fprintf(stderr, "thread:%03zu done\n", td->tid);
    }
    return NULL;
}

void internal_benchmark_run(struct settings* settings, struct event_base* main_base)
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

    printf("HOMEBREW BARRIER VERSION\n");

    // yeah we still use openmp to figure out the number of threads!
    size_t num_threads = omp_get_num_procs();

#ifndef USE_HOMEBREW_BARRIER
    // initialize barrier
    if (pthread_barrier_init(&barrier, NULL, num_threads + 1) != 0) {
        fprintf(stderr, "ERROR: failed to create thread!\n");
        exit(1);
    }
#else
    printf("HOMEBREW BARRIER VERSION\n");
#endif

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

    struct thread_data* threads = calloc(num_threads, sizeof(*threads));
    for (size_t i = 0; i < num_threads; i++) {
        threads[i].tid = i;
        threads[i].connection = conn_new(i, conn_listening, 0, 0, local_transport, main_base, NULL, 0, ascii_prot);
        threads[i].connection->thread = malloc(sizeof(LIBEVENT_THREAD));
        threads[i].settings = settings;
        threads[i].num_items = num_items_per_thread;
        threads[i].num_items_total = num_items;
        threads[i].num_threads = num_threads;
    }

    fprintf(stderr, "number of threads: %zu\n", num_threads);
    fprintf(stderr, "item size: %zu bytes\n", ITEM_SIZE);
    fprintf(stderr, "number of keys: %zu\n", num_items);

#ifdef PREFILL_SLABS
    fprintf(stderr, "Prefilling slabs...\n");
    gettimeofday(&start, NULL);
    slabs_prefill_global();

    gettimeofday(&end, NULL);
    timersub(&end, &start, &elapsed);
    elapsed_us = (elapsed.tv_sec * 1000000) + elapsed.tv_usec;
    fprintf(stderr, "Prefilling slabs took %lu ms\n", elapsed_us / 1000);
#endif

    fprintf(stderr, "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n");
    fprintf(stderr, "Populating %zu key-value pairs....\n", num_items);
    fprintf(stderr, "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n");

    fprintf(stderr, "Starting threads...\n");
    for (size_t i = 0; i < num_threads; i++) {
        if (pthread_create(&threads[i].thread, NULL, do_benchmark, (void*)&threads[i]) != 0) {
            fprintf(stderr, "ERROR: failed to create thread!\n");
            exit(1);
        }
    }

    // here we just sleep to get out of the way...
    // we run only on freshly started threads...
    printf("main thread sleeping...\n");
    sleep(3600 * 24);

    // join the threads in the end
    for (size_t i = 0; i < num_threads; i++) {
        void* retval = NULL;
        pthread_join(threads[i].thread, &retval);
    }

    exit(0);
}
