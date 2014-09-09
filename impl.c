/* 
    Copyright (C) 2011 Wei Dong <wdong.pku@gmail.com>. All Rights Reserved.

    DISTRIBUTION OF THIS PROGRAM IN EITHER BINARY OR SOURCE CODE FORM MUST BE
    PERMITTED BY THE AUTHOR.  NO COMMERCIAL USE ALLOWED.
*/
#include <time.h>
#include <errno.h>
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <stdarg.h>
#include <unistd.h>
#include <pthread.h>
#include <dirent.h>
#include <sys/timeb.h>
#include <sys/types.h> 
#include <sys/stat.h>
#include <sys/mman.h> 
#include <sys/socket.h>
#include <sys/epoll.h>
#include <sys/times.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netdb.h>
#include <zlib.h>
#include <lzo/lzo1x.h>
#include "nxn.h"
#include "nxn-impl.h"
#include "config.h"

// GLOBAL/LOCAL DATA, COMMUNICATION INFRASTRUCTURE
//
// Data in this section are initialized in
// nxn_startup and cleaned-up in nxn_shutdown.

static long tick_per_sec = 0;
static int page = 0;                    // page size

static int standalone = 0;              // client runs in standalone mode (for testing)
static int size = -1, rank = -1;        // like in MPI
static int peers[MAX_NODE];             // peer sockets, peers[rank] == -1
static int master = -1;                 // master socket
static pthread_mutex_t master_lock;     // master is protected.
                // all threads can report to master, so master need to be synchronized
                // peers is only read by reader, and written by writer
                // do not need to be protected.
                //
static nxn_stat_t perf;                 // performance stats

static short n_roots = -1;              // # roots, each one corresponds to a disk
static char roots[MAX_ROOT][STR_SIZE];

int load_config () {                    // load root configuration
    char *home;
    char tmp[STR_SIZE];
    int ret;
    home = getenv("HOME");
    verify(home);
    ret = snprintf(tmp, STR_SIZE, "%s/.nxnrc", home);
    verify(ret < STR_SIZE);

    FILE *fin = fopen(tmp, "r");
    if (fin == NULL) {
        LOG(ERROR, "please setup the config file at %s.\n", tmp);
    }
    verify(fin);

    n_roots = 0;
    for (;;) {
        if (n_roots >= MAX_ROOT) break;
        if (fscanf(fin, "%s", roots[n_roots]) != 1) break;
        ++n_roots;
    }

    fclose(fin);

    return 0;
}

// COMMUNICATION
int nxn_startup () {

    int ret;

    ret = lzo_init();
    verify(ret == LZO_E_OK);

    consistency_check();

    load_config();


    tick_per_sec = sysconf(_SC_CLK_TCK);
    page = sysconf(_SC_PAGESIZE);

    bzero(&perf, sizeof(perf));

    const char *master_host;
    int master_port;

    struct sockaddr_in my_addr, master_addr, peer_addr;
    struct hostent *server;

    socklen_t socklen;
    int i, s;

    int my_fd;   // listening socket fd

    {
        master_host = getenv("NXN_MASTER");
        if (master_host == NULL) {
            standalone = 1;
            size = 1;
            rank = 0;
            peers[0] = -1;
            return 0;
        }
        const char *master_port_s = getenv("NXN_PORT");
        const char *size_s = getenv("NXN_SIZE");
        const char *rank_s = getenv("NXN_RANK");
        verify(master_host);
        verify(master_port_s);
        verify(size_s);
        verify(rank_s);
        master_port = atoi(master_port_s);
        size = atoi(size_s);
        rank = atoi(rank_s);
        verify(size > 0);
        verify(size <= MAX_NODE);
        verify(rank >= 0);
        verify(rank < size);
    }

    // 1. set up listening

    my_fd = socket(AF_INET, SOCK_STREAM, 0);
    verify(my_fd >= 0);

    bzero(&my_addr, sizeof(my_addr));
    my_addr.sin_family = AF_INET;
    my_addr.sin_addr.s_addr = INADDR_ANY;
    my_addr.sin_port = 0;   // bind to any available port

    ret = bind(my_fd, (struct sockaddr *)&my_addr, sizeof(my_addr));
    verify(ret == 0);

    socklen = sizeof(my_addr);
    ret = getsockname(my_fd, (struct sockaddr *)&my_addr, &socklen);
    verify(ret == 0);
    verify(socklen == sizeof(my_addr));

    listen(my_fd, BACKLOG);

    // 2. connect to master
    master = socket(AF_INET, SOCK_STREAM, 0);
    server = gethostbyname(master_host);
    verify(server);

    bzero(&master_addr, sizeof(master_addr));
    master_addr.sin_family = AF_INET;
    bcopy(server->h_addr, &master_addr.sin_addr.s_addr, server->h_length);
    master_addr.sin_port = htons(master_port);

    ret = connect(master, (struct sockaddr *)&master_addr, sizeof(master_addr));
    verify(ret == 0);

    // 3. send up message
    msg_t up;
    
    up.tag = MSG_UP;
    up.rank = rank;
    up.u.u16 = my_addr.sin_port;

    ret = sendx(master, &up, sizeof(up));
    verify(ret == sizeof(up));

    // 4. get cluster information
    
    struct {
        msg_t header;
        addr_t peers[MAX_NODE];
    } __attribute__((__packed__)) all_up;

    ret = recvx(master, &all_up, sizeof(msg_t) + size * sizeof(addr_t));
    verify(ret == sizeof(msg_t) + size * sizeof(addr_t));

    verify(all_up.header.tag == MSG_ALL_UP);

    // 5. accept connections from lower ranks
    msg_t hello;

    for (i = 0; i < rank; i++) {
        socklen = sizeof(peer_addr);
        s = accept(my_fd, (struct sockaddr *)&peer_addr, &socklen);
        verify(s >= 0);
        verify(socklen == sizeof(peer_addr));

        ret = recvx(s, &hello, sizeof(hello));
        verify(ret == sizeof(hello));
        verify(hello.tag == MSG_HELLO);
        peers[hello.rank] = s;
        // LOG(DEBUG, "%d <- %d\n", rank, hello.rank);
        /*
        ret = fcntl(s, F_SETFL, O_NONBLOCK);
        verify(ret == 0);
        */

    }

    peers[rank] = -1;
    // 6. send connections to higher ranks
    for (i = rank + 1; i < size; i++) {
        s = socket(AF_INET, SOCK_STREAM, 0);
        bzero(&peer_addr, sizeof(master_addr));
        peer_addr.sin_family = AF_INET;
        peer_addr.sin_addr.s_addr = all_up.peers[i].addr;
        peer_addr.sin_port = all_up.peers[i].port;
        ret = connect(s, (struct sockaddr *)&peer_addr, sizeof(peer_addr));
        verify(ret == 0);
        hello.tag = MSG_HELLO;
        hello.rank = rank;
        ret = sendx(s, &hello, sizeof(hello));
        verify(ret == sizeof(hello));

        peers[i] = s;

        int bs = 256 * 1024 * 2;
        socklen_t len = sizeof(int);
        ret = setsockopt(s, SOL_SOCKET, SO_SNDBUF, &bs, len);
        verify(ret == 0);
        ret = setsockopt(s, SOL_SOCKET, SO_RCVBUF, &bs, len);
        verify(ret == 0);

        /*
        ret = fcntl(s, F_SETFL, O_NONBLOCK);
        verify(ret == 0);
        */
        // LOG(DEBUG, "%d -> %d\n", rank, i);
    }

    close(my_fd);   // we don't need to wait for more connections

    ret = pthread_mutex_init(&master_lock, NULL);
    verify(ret == 0);

    return 0;
}

int nxn_shutdown () {
    int ret, i;

    if (standalone) return 0;

    ret = pthread_mutex_destroy(&master_lock);
    verify(ret == 0);
    
    // 1. report to master
    msg_t done;

    done.tag = MSG_QUIT;
    done.rank = rank;

    ret = sendx(master, &done, sizeof(done));
    verify(ret == sizeof(done));

    close(master);
    
    for (i = 0; i < size; i++) {
        if (i != rank) {
            close(peers[i]);
        }
    }

    return 0;
}

int nxn_size () {
    return size;
}

int nxn_rank () {
    return rank;
}

int nxn_scatter (const void *out, size_t len, void *in) {
    struct {
        msg_t header;
        char data[MAX_SCATTER * MAX_NODE];
    } __attribute__((__packed__)) scatter;

    int ret;

    verify(len <= MAX_SCATTER);
    scatter.header.tag = MSG_SCATTER;
    scatter.header.rank = rank;
    scatter.header.u.i32 = len;
    if (len) {
        memcpy(scatter.data, out, len);
    }
    ret = pthread_mutex_lock(&master_lock);
    verify(ret == 0);
    ret = sendx(master, &scatter, sizeof(msg_t) + len);
    verify(ret == sizeof(msg_t) + len);
    atomic_add(&perf.net_out, ret);
    ret = pthread_mutex_unlock(&master_lock);
    verify(ret == 0);

    ret = recvx(master, &scatter, sizeof(msg_t) + len * size);
    verify(scatter.header.tag == MSG_SCATTER);
    verify(ret == sizeof(msg_t) + len * size);
    atomic_add(&perf.net_in, ret);
    if (len) {
        memcpy(in, scatter.data, len * size);
    }
    return 0;
}

int nxn_getline (char *buf) {
    struct {
        msg_t header;
        char data[STR_SIZE];
    } __attribute__((__packed__)) input;

    int ret;

    input.header.tag = MSG_INPUT;
    input.header.rank = rank;
    input.header.u.i32 = 0;
    ret = pthread_mutex_lock(&master_lock);
    verify(ret == 0);
    ret = sendx(master, &input.header, sizeof(msg_t));
    verify(ret == sizeof(msg_t));
    atomic_add(&perf.net_out, ret);
    ret = pthread_mutex_unlock(&master_lock);
    verify(ret == 0);

    ret = recvx(master, &input, sizeof(input));
    verify(input.header.tag == MSG_INPUT);
    verify(ret == sizeof(input));
    atomic_add(&perf.net_in, ret);
    strncpy(buf, input.data, STR_SIZE);
    return 0;
}

int nxn_report (const char *fmt, ...) {

    int ret;

    struct {
        msg_t header;
        char message[STR_SIZE];
    } __attribute__((__packed__)) report;
   
    va_list args;
    va_start(args, fmt);
    ret = vsnprintf(report.message, STR_SIZE, fmt, args);
    va_end(args);
    verify(ret < STR_SIZE);

    report.header.tag = MSG_REPORT;
    report.header.rank = rank;
    report.header.u.i32 = strlen(report.message);

    size_t len = sizeof(msg_t) + report.header.u.i32;

    ret = pthread_mutex_lock(&master_lock);
    verify(ret == 0);
    ret = sendx(master, &report, len);
    verify(ret == len);
    atomic_add(&perf.net_out, ret);
    ret = pthread_mutex_unlock(&master_lock);
    verify(ret == 0);
    return 0;
}

int nxn_report_0 (const char *fmt, ...) {

    if (rank != 0) return 0;
    int ret;

    struct {
        msg_t header;
        char message[STR_SIZE];
    } __attribute__((__packed__)) report;
   
    va_list args;
    va_start(args, fmt);
    ret = vsnprintf(report.message, STR_SIZE, fmt, args);
    va_end(args);
    verify(ret < STR_SIZE);

    report.header.tag = MSG_REPORT;
    report.header.rank = rank;
    report.header.u.i32 = strlen(report.message);

    size_t len = sizeof(msg_t) + report.header.u.i32;

    ret = pthread_mutex_lock(&master_lock);
    verify(ret == 0);
    ret = sendx(master, &report, len);
    verify(ret == len);
    atomic_add(&perf.net_out, ret);
    ret = pthread_mutex_unlock(&master_lock);
    verify(ret == 0);
    return 0;
}


long long nxn_time () {
    struct timeb tmb;
    ftime(&tmb);
    return (tmb.time - MY_EPOCH) * 1000 + tmb.millitm;
}

long long nxn_thread_time () {
    struct timespec tp; 
    clock_gettime(CLOCK_THREAD_CPUTIME_ID, &tp);
    return tp.tv_sec * 1000 + tp.tv_nsec / 1000000;
}

// JOB DATA

typedef struct {    // a block of pre-allocated memory
    link_t link;
    char *data;
    int len;        // if len < 0, then the file descriptor is to be closed
    union {
        struct {
            int peer;
        } send;
        struct {
            int part;
            int file;
        } write;
    } u;
} block_t;


// Data in this section is initialized in nxn_init and cleaned-up in nxn_cleanup.
// These data are job specific.
//
//

static nxn_job_t const * job = NULL; // current running job

// pipe lines and threading
static int /*n_readers = 0,*/ n_zippers = 0, n_workers = 0;

// signal indicating whether threads should initialize the shutdown process
static int writer_stop = 0, sender_stop = 0, receiver_stop = 0, monitor_stop = 0; 
static int unzipper_stop = 0, zipper_stop = 0, worker_stop = 0;

static pthread_t //preaders[MAX_READER],
                 pwriters[MAX_ROOT],
                 psenders[MAX_NODE],
                 preceivers[MAX_NODE],
                 pmonitor,
                 punzipper,                 
                 pzippers[MAX_ZIPPER],
                 pworkers[MAX_WORKER];

//static queue_t worker_tasks;
static queue_t sender_tasks[MAX_NODE];
static queue_t writer_tasks[MAX_ROOT];
static queue_t unzipper_tasks;
static queue_t zipper_tasks;

//static void *reader (void *param);
static void *worker (void *param);
static void *writer (void *param);
static void *sender (void *param);
static void *receiver (void *param);
static void *monitor (void *param);
static void *unzipper (void *param);
static void *zipper (void *param);

static void nxn_flush_remote ();
static void nxn_flush_write ();

// global data structures.

// input/output settings
static int n_global_parts = 0;                  // total parts across cluster

static char const *dir = NULL;
static short n_locals = -1, n_remotes = -1;
static nxn_local_t const *locals = NULL;
static nxn_remote_t const *remotes = NULL;     // output information
static size_t total_local_size = 0;
static size_t max_part_size = 0;

static int local_parts[MAX_PART]; //[MAX_ROOT][MAX_PART];     // end with -1
static int local_roots[MAX_PART]; //[MAX_ROOT][MAX_PART];     // end with -1
static size_t local_sizes[MAX_PART][MAX_DATASET]; // [MAX_ROOT][MAX_PART][MAX_OUTPUT];
static int next_local = -1;
static pthread_mutex_t next_local_lock;

static __thread int cur_worker;

int nxn_parts (int id) {
    return id % n_global_parts;
}

static inline void part_split (int part, short *node, short *l_part) {
    *node = part % size;
    *l_part = part / size;
}

static inline int part_merge (short node, short l_part) {
    return l_part * size + node;
}


//
    static void mkidir (char *path, int root, short input) {
        nxn_sprintf(path, "%s/%s/%s", roots[root], dir, locals[input].name);
    }

    static void mkodir (char *path, int root, short output) {
        nxn_sprintf(path, "%s/%s/%s", roots[root], dir, remotes[output].name);
    }

    static void mkipath (char *path, int root, int part, short input) {
        nxn_sprintf(path, "%s/%s/%s/%d", roots[root], dir, locals[input].name, part);
    }

    static void mkopath (char *path, int root, short l_part, short output) {
        nxn_sprintf(path, "%s/%s/%s/%d", roots[root], dir, remotes[output].name, part_merge(rank, l_part));
    }

    static void setup_dir (int root) {
        char path[STR_SIZE];
        nxn_sprintf(path, "%s/%s/", roots[root], dir);
        char *p = path + strlen(roots[root]);
        verify(*p == '/');
        while (*p == '/') ++p;
        while (*p) {
            p = strchr(p, '/');
            verify(p);
            *p = 0;
            int ret = mkdir(path, 0777);
            verify((ret == 0) || (errno = EEXIST));
            *p = '/';
            while (*p == '/') ++p;
        }
    }

    static int set_insert (int *set, int len, int item) {
        int pos = 0;
        int lw = 0, up = len - 1, mid;

        if (len == 0) pos = 0;
        else if (set[0] == item) return len;
        else if (set[0] > item) pos = 0;
        else if (set[up] == item) return len;
        else if (set[up] < item) pos = len;
        else {
            // set[lw] < item < set[up]
            while (lw + 1 < up) {
                mid = (lw + up) / 2;
                if (set[mid] == item) return len;
                else if (set[mid] < item) lw = mid;
                else up = mid;
            }
            pos = up;
        }

        verify(len + 1 <= MAX_PART);

        for (up = len; up > pos; --up) {
            set[up] = set[up-1];
        }

        set[up] = item;
        return len + 1;
    }




// STORAGE MANAGEMENT
int nxn_init_storage () {
    char path[STR_SIZE];
    static char const *empty = "";
    // 
    int i, j, k, ret;
    // bzero(&avail, sizeof(avail));
    n_global_parts = job->npart;
    dir = job->dir;
    if (dir == NULL) dir = empty;
    locals = job->localv;
    remotes = job->remotev;

    n_locals = n_remotes = 0;

    if (locals) for (; locals[n_locals].name; n_locals++);
    verify(n_locals <= MAX_DATASET);

    if (remotes) for (; remotes[n_remotes].name; n_remotes++);
    verify(n_remotes <= MAX_DATASET);

    max_part_size = 0;
    total_local_size = 0;

    next_local = 0;

    perf.local_parts = 0;

    int l_parts[MAX_ROOT][MAX_PART];     // end with -1
    size_t l_sizes[MAX_ROOT][MAX_PART][MAX_DATASET];
    // check insert and make output dir
    for (i = 0; i < n_roots; i++) {
        // setup dir dirs
        setup_dir(i);

        int n_p = 0;

        for (j = 0; j < n_locals; j++) {
            mkidir(path, i, j);
            DIR *dir = opendir(path);
            if (dir == NULL) continue;  // input doesn't exist, skip
            struct dirent *ent;
            while((ent  = readdir(dir))) {
                char *name = ent->d_name;
                char *endptr;
                if (*name == 0) continue;
                int pt = strtol(name, &endptr, 10);
                if (*endptr != 0) continue;
                n_p = set_insert(l_parts[i], n_p, pt);
                verify(n_p < MAX_PART);
            }
            ret = closedir(dir);
            verify(ret == 0);
        }

        l_parts[i][n_p] = -1;
        // get input size
        j = 0;
        while (j < n_p) {
            LOG(INFO, "INPUT %s:%d\n", roots[i], l_parts[i][j]);
            int keep = 0;
            for (k = 0; k < n_locals; k++) {
                mkipath(path, i, l_parts[i][j], k);
                struct stat st;
                ret = stat(path, &st);
                l_sizes[i][j][k] = 0;
                if (ret == 0) {
                    l_sizes[i][j][k] = st.st_size;
                }
                if (l_sizes[i][j][k] && !(locals[k].flags & NXN_LOCAL_AUX)) {
                    keep = 1;
                }
            }
            if (keep) {    // zero input, remove current part
                size_t sz = 0;
                for (k = 0; k < n_locals; k++) {
                    total_local_size += l_sizes[i][j][k];
                    sz += (l_sizes[i][j][k] + page - 1) / page * page;
                }
                if (max_part_size < sz) max_part_size = sz;
                ++j;
            }
            else {
                for (k = j; k < n_p; ++k) {
                    l_parts[i][k] = l_parts[i][k+1];
                }
                --n_p;
            }
        }
        perf.local_parts += n_p;

        // create output dir
        for (j = 0; j < n_remotes; j++) {
            mkodir(path, i, j);
            if (!(remotes[j].flags & NXN_REMOTE_APPEND)) {
                char cmd[STR_SIZE];
                ret = snprintf(cmd, STR_SIZE, "rm -rf %s", path);
                verify(ret < STR_SIZE);
                system(cmd);
            }
            ret = mkdir(path, 0777);
            verify((ret == 0) || (errno = EEXIST));
        }
    }

    // merge l_parts/l_sizes to local_parts/local_roots
    int n_idx[MAX_ROOT];
    bzero(n_idx, sizeof(n_idx));
    int quit = 0;
    int nn = 0;
    while (!quit) {
        quit = 1;
        for (i = 0; i < n_roots; i++) {
            if (l_parts[i][n_idx[i]] >= 0) {
                quit = 0;
                local_parts[nn] = l_parts[i][n_idx[i]];
                local_roots[nn] = i;
                for (j = 0; j < n_locals; j++) {
                    local_sizes[nn][j] = l_sizes[i][n_idx[i]][j];
                }
                n_idx[i]++;
                nn++;
            }
        }
    }
    local_parts[nn] = -1;
    verify(nn == perf.local_parts);

    ret = pthread_mutex_init(&next_local_lock, NULL);
    verify(ret == 0);

    return 0;
}

int nxn_cleanup_storage () {
    int ret = pthread_mutex_init(&next_local_lock, NULL);
    verify(ret == 0);
    return 0;
}


// memory
static size_t total_size = 0;       // total allocated size
static int block_size = 0;          // size of each block
static int max_data_block_size = 0; // considering compress cost
static char *start = NULL;          // start of mmap
static size_t length = 0;           // length of mmap -- total_size limited to pageboundary
static size_t N = 0;                // total blocks

static char *part_buffers[MAX_WORKER];

static queue_t free_queue;          // free blocks

int nxn_init_mem () {

    int i;
    // determine total size

    n_workers = job->worker;
    if (n_workers <= 0) n_workers = 1;
    if (job->routine == NULL) n_workers = 1;
    verify(n_workers >= 0);
    verify(n_workers <= MAX_WORKER);

    total_size = job->total_size;

    block_size = job->block_size;
    verify(block_size < MAX_BLOCK_SIZE);

    
    if (job->zipper > 0) {
#if USE_ZLIB
        max_data_block_size = (block_size - sizeof(msg_t) - 13) / 1.001 + sizeof(msg_t);
#else
        max_data_block_size = (block_size - sizeof(msg_t) - 67) / 1.08 + sizeof(msg_t);
        verify(block_size >= 500 * 1024);   // 448K for LZO
#endif
    }
    else {
        max_data_block_size = block_size;
    }

    verify(max_part_size % page == 0);

    verify(total_size > max_part_size * n_workers);

    N = (total_size - n_workers * max_part_size) / (block_size + sizeof(block_t));
    total_size = N * (block_size + sizeof(block_t)) + max_part_size * n_workers;

    length = (total_size + page - 1) / page * page;

    start = mmap(NULL, length, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    verify(start != MAP_FAILED);
    
    /*
    int ret = mlock(start, length);
    verify(ret == 0);
    */

    for (i = 0; i < n_workers; ++i) {
        part_buffers[i] = start + length - (n_workers - i) * max_part_size;
    }

    queue_init(&free_queue, N);

    // populate the queue with preallocated blocks
    block_t *blocks = (block_t *)(start + N * block_size);
    char *cur = start;
    for (i = 0; i < N; ++i) {
        blocks[i].data = cur;
        blocks[i].link.next = &blocks[i+1].link;
        cur += block_size;
    }
    blocks[N-1].link.next = NULL;

    free_queue.next = &blocks[0].link;
    free_queue.tail = &blocks[N-1].link;
    free_queue.cnt = N;

    return 0;
}

int nxn_cleanup_mem () {
    verify(queue_size(&free_queue) == N);
    free_queue.cnt = 0;
    free_queue.next = free_queue.tail = NULL;
    queue_cleanup(&free_queue);
    /*
    int ret = munlock(start, length);
    verify(ret == 0);
    */
    int ret = munmap(start, length);
    verify(ret == 0);

    total_size = 0;
    block_size = 0;
    start = NULL;
    bzero(part_buffers, sizeof(part_buffers));
    length = N = 0;
    return 0;
}

block_t *nxn_alloc () {
    int stop = 0;
    block_t *r;
    dequeue(&free_queue, &r, &stop);
    return r;
}

void nxn_free (block_t *b) {
    enqueue_front(&free_queue, b);
}

int nxn_mem_avail () {
    return queue_size(&free_queue);
}

// static size_t avail[MAX_ROOT];  // available space on each disk
//static char roots[MAX_ROOT][STR_SIZE];

/////////////////// !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
// data accumulators
static short n_parts = -1;
static block_t *out[MAX_WORKER][MAX_NODE];  // those to be sent

static struct in_accu {                 // those to be written to disks
    block_t *block;
    int fd;                     // lazily opened
    int root;
} in[MAX_PART][MAX_DATASET];

static pthread_mutex_t in_locks[IN_LOCKS]; // master is protected.

// UTILITY FUNCTIONS
//
// MEMORY MANAGEMENT
// DATA 

static inline void init_file_data (block_t *blk, int part, int file) {
    blk->len = 0;
    blk->u.write.part = part;
    blk->u.write.file = file;
}

static inline void init_data_msg (block_t *blk, int peer) {
    blk->len = sizeof(msg_t);
    blk->u.send.peer = peer;
}

static inline int data_msg_size (block_t *blk) {
    return blk->len - sizeof(msg_t);
}

static inline void finalize_data_msg (block_t *blk) {
    msg_t msg;
    msg.tag = MSG_DATA;
    msg.rank = rank;
    msg.u.i32 = data_msg_size(blk);
    *(msg_t *)(blk->data) = msg;
}


int nxn_init_accu (const nxn_job_t *job) {
    int i, j, r, ret;
    for (i = 0; i < n_workers; i++) {
        for (j = 0; j < size; j++) {
            if (j == rank) {
                out[i][j] = NULL;
            }
            else {
                out[i][j] = nxn_alloc();
                init_data_msg(out[i][j], j);
            }
        }
    }

    int n_parts_i = (job->npart + size - 1) / size;
    verify(n_parts_i <= MAX_PART);
    n_parts = n_parts_i;

    r = 0;
    for (i = 0; i < n_parts; i++) {
        for (j = 0; j < n_remotes; j++) {
            in[i][j].block = nxn_alloc();
            init_file_data(in[i][j].block, i, j);
            in[i][j].fd = -1;
//!!! TO BE IMPROVED
            in[i][j].root = r;
        }
        r = (r + 1) % n_roots;
    }
    for (i = 0; i < IN_LOCKS; ++i) {
        ret = pthread_mutex_init(&in_locks[i], NULL);
        verify(ret == 0);
    }
    return 0;
}

int nxn_cleanup_accu () {
    int i, j, ret;
    for (i = 0; i < IN_LOCKS; i++) {
        ret = pthread_mutex_destroy(&in_locks[i]);
        verify(ret == 0);
    }
    for (i = 0; i < n_workers; i++) {
        for (j = 0; j < size; j++) {
            if (j == rank) continue;
            verify(out[i][j] == NULL);
        }
    }
    for (i = 0; i < n_parts; i++) {
        for (j = 0; j < n_remotes; j++) {
            if (in[i][j].fd >= 0) {
                close(in[i][j].fd);
            }
            verify(in[i][j].block == NULL);
        }
    }
    return 0;
}

static void nxn_flush_remote () {
    // send out all unfilled buffers in 'out'
    int i, j;
    for (i = 0; i < n_workers; i++) {
        for (j = 0; j < size; j++) {
            if (j == rank) continue;
            if (data_msg_size(out[i][j]) == 0) {
                nxn_free(out[i][j]);
            }
            else {
                finalize_data_msg(out[i][j]);
                if (n_zippers > 0) {
                    enqueue(&zipper_tasks, out[i][j]);
                }
                else {
                    enqueue(&sender_tasks[j], out[i][j]);
                }
            }
            out[i][j] = NULL;
        }
    }
}

static inline int append_send (int16_t output, int16_t peer, int16_t l_part, const char *inbuf) {
    int record_size = remotes[output].record_size;
    block_t *blk = out[cur_worker][peer];
    if (blk->len + record_size + 2 * sizeof(int16_t) > max_data_block_size) {
        finalize_data_msg(blk);
        if (n_zippers > 0) {
            enqueue(&zipper_tasks, blk);
        }
        else {
            enqueue(&sender_tasks[peer], blk);
        }
        blk = out[cur_worker][peer] = nxn_alloc();
        init_data_msg(blk, peer);
    }
    int new_l = blk->len + record_size + 2 * sizeof(int16_t);
    verify(new_l <= block_size);
    char *buf = blk->data + blk->len;
    *(int16_t *)buf = l_part;
    buf += sizeof(int16_t);
    *(int16_t *)buf = output;
    buf += sizeof(int16_t);
    memcpy(buf, inbuf, record_size);
    blk->len = new_l;
    return record_size;
}

static void nxn_flush_write () {
    int i, j;
    for (i = 0; i < n_parts; i++) {
        for (j = 0; j < n_remotes; j++) {
            if (in[i][j].block->len == 0) {
                nxn_free(in[i][j].block);
            }
            else {
                enqueue(&writer_tasks[in[i][j].root], in[i][j].block);
            }
            in[i][j].block = NULL;
        }
    }
}

static inline int append_write (int16_t l_part, int16_t output, const char *buf) {
    int lock = (l_part * n_remotes + output) % IN_LOCKS;
    int ret = pthread_mutex_lock(&in_locks[lock]);
    verify(ret == 0);
    struct in_accu *accu = &in[l_part][output];
    block_t *block = accu->block;
    int record_size = remotes[output].record_size;
    if (block->len + record_size > block_size) {
        enqueue(&writer_tasks[accu->root], block);  // send for writing
        block = accu->block = nxn_alloc();         // allocate an empty block
        init_file_data(block, l_part, output);
    }
    verify(block->len + record_size <= block_size);
    memcpy(block->data + block->len, buf, record_size);
    block->len += record_size;
    ret = pthread_mutex_unlock(&in_locks[lock]);
    verify(ret == 0);
    return record_size;
}

static inline void dispatch (const char *buf, int len) {
    const char *endp = buf + len;
    while (buf < endp) {
        int16_t l_part = *(int16_t *)buf;
        buf += sizeof(int16_t);
        int16_t out = *(int16_t *)buf;
        buf += sizeof(int16_t);
        verify((l_part >= 0) && (l_part < n_parts));
        verify((out >= 0) && (out < n_remotes));
        buf += append_write(l_part, out, buf);
    }
}


/*
static void *reader (void *param) {
    return NULL;
}
*/

static void *writer (void *param) {
    int id;
    int fd, ret;
    queue_t *tasks = (queue_t *)param;

    id = tasks - writer_tasks;

    for (;;) {
        block_t *task = NULL;
        dequeue(tasks, &task, &writer_stop);
        if (task == NULL) break;
        struct in_accu *accu = &in[task->u.write.part][task->u.write.file];
        fd = accu->fd;
        verify(id == accu->root);
        if (fd < 0) {
            char path[STR_SIZE];
            mkopath(path, accu->root, task->u.write.part, task->u.write.file);
            accu->fd = fd = open(path, O_CREAT | O_WRONLY, 0666);
            if (fd < 0) {
                LOG(WARN, "(%d) writing to %s\n", rank, path);
                verify(fd >= 0);
            }
            lseek(fd, 0, SEEK_END);
        }
        ret = writex(fd, task->data, task->len);
        verify(ret == task->len);
        atomic_add(&perf.disk_out, ret);
        nxn_free(task);
    }

    return NULL;
}

static void *unzipper (void *param) {
    queue_t *tasks = (queue_t *)param;
    if (n_zippers > 0) {
        block_t *workmem = nxn_alloc();
        block_t *tmp = nxn_alloc();
        int ret;
        for (;;) {
            block_t *task = NULL;
            dequeue(tasks, &task, &unzipper_stop);
            if (task == NULL) break;
            unsigned long len = block_size;
#if USE_ZLIB
            ret = uncompress((unsigned char *)tmp->data, &len, (unsigned char*)task->data, task->len);
            verify(ret == Z_OK);
#else
            ret = lzo1x_decompress((unsigned char*)task->data, task->len, (unsigned char *)tmp->data, &len, workmem->data);
            verify(ret == LZO_E_OK);
#endif
            tmp->len = len;
            nxn_free(task);
            dispatch(tmp->data, tmp->len);
        }
        nxn_free(tmp);
        nxn_free(workmem);
    }
    else {
        for (;;) {
            block_t *task = NULL;
            dequeue(tasks, &task, &unzipper_stop);
            if (task == NULL) break;
            dispatch(task->data, task->len);
            nxn_free(task);
        }
    }
    return NULL;
}

static void *receiver (void *param) {

    int peer = (int)(intptr_t)param;
    verify(peer >= 0);
    verify(peer < size);
    verify(peer != rank);

    block_t *buf = nxn_alloc();
    buf->len = 0;
    int left = 0;

    int ret;

    for (;;) {
        if (left == 0) {
            msg_t msg;
            ret = recvx(peers[peer], &msg, sizeof(msg));
            if (ret != sizeof(msg)) {
                LOG(ERROR, "node %d, expect %d, get %d", rank,
                        sizeof(msg), ret);
            }
            verify(ret == sizeof(msg));
            atomic_add(&perf.net_in, ret);

            if (msg.tag == MSG_DONE) {
                break;
            }
            else if (msg.tag == MSG_DATA) {
                verify(msg.u.i32 <= block_size);
                buf->len = 0;
                left = msg.u.i32;
            }
            else verify(0);
        }
        else { // left over data
            ret = recv(peers[peer], buf->data + buf->len, left, 0);
            verify(ret >= 0);
            atomic_add(&perf.net_in, ret);
            buf->len += ret;
            left -= ret;
            if (left == 0) {
                enqueue(&unzipper_tasks, buf);
                buf = nxn_alloc();
                buf->len = 0;
            }
        }
    }

    nxn_free(buf);

    return NULL;
}


static void *sender (void *param) {
    int peer = (int)(intptr_t)param;
    verify(peer >= 0);
    verify(peer < size);
    verify(peer != rank);
    int ret;
    queue_t *tasks = &sender_tasks[peer];
    for (;;) {
        block_t *task = NULL;
        dequeue(tasks, &task, &sender_stop);
        if (task == NULL) break;
        verify(task->u.send.peer == peer);
        ret = sendx(peers[task->u.send.peer], task->data, task->len);
        verify(ret == task->len);
        atomic_add(&perf.net_out, ret);
        nxn_free(task);
    }
    msg_t bye;
    bye.tag = MSG_DONE;
    bye.rank = rank;
    ret = sendx(peers[peer], &bye, sizeof(bye));
    verify(ret == sizeof(bye));
    atomic_add(&perf.net_out, ret);
    return NULL;
}

static void *zipper (void *param) {
    queue_t *tasks = (queue_t *)param;

    block_t *workmem = nxn_alloc();
    block_t *tmp = nxn_alloc();
    int ret;
    for (;;) {
        block_t *task = NULL;
        dequeue(tasks, &task, &zipper_stop);
        if (task == NULL) break;

        msg_t *from = (msg_t *)task->data;
        msg_t *to = (msg_t *)tmp->data;
        *to = *from;
        unsigned long len = block_size - sizeof(msg_t);
#if USE_ZLIB
        verify(compressBound(from->u.i32) <= len);
        ret = compress((unsigned char *)(to + 1), &len, (unsigned char *)(from + 1), from->u.i32);
        verify(ret == Z_OK);
#else
        ret = lzo1x_1_compress((unsigned char *)(from + 1), from->u.i32, (unsigned char *)(to + 1), &len, workmem->data);
        verify(ret == Z_OK);
#endif
        atomic_add(&perf.zip, from->u.i32);
        to->u.i32 = len;
        tmp->u = task->u;
        tmp->len = len + sizeof(msg_t);
        enqueue(&sender_tasks[tmp->u.send.peer], tmp);
       
        tmp = task;
    }
    nxn_free(tmp);
    nxn_free(workmem);
    return NULL;
}

static void *worker (void *param) {

    if (job->routine == NULL) return NULL;

    cur_worker = (int)(intptr_t)param;

    int ret;

    int cur;

    char *input_mem = part_buffers[cur_worker];

    long long oldtime = nxn_thread_time();

    for (;;) {

        nxn_update_app_time();
        long long newtime = nxn_thread_time();
        atomic_add(&perf.atime, newtime - oldtime);
        oldtime = newtime;

        ret = pthread_mutex_lock(&next_local_lock);
        verify(ret == 0);

        cur = next_local;
        if (local_parts[next_local] >= 0) {
            next_local++;
        }

        ret = pthread_mutex_unlock(&next_local_lock);
        verify(ret == 0);

        if (local_parts[cur] < 0) break;


        int i;
        char *next = input_mem;

        nxn_segment_t segs[MAX_DATASET];

        for (i = 0; i < n_locals; i++) {
            size_t sz = local_sizes[cur][i];
            if (sz == 0) {
                segs[i].data = next;
                segs[i].len = 0;
            }
            else { // read file
                segs[i].data = next;
                segs[i].len = sz;
                next += (sz + page - 1) / page * page;
                char path[STR_SIZE];
                mkipath(path, local_roots[cur], local_parts[cur], i);
                int fd = open(path, O_RDONLY);
                verify(fd >= 0);
                ssize_t rd = readx(fd, segs[i].data, sz);
                verify(rd == sz);
                atomic_add(&perf.disk_in, rd);
                close(fd);
            }
        }
        job->routine(job->u_ptr, cur, segs);
        atomic_add(&perf.done_parts, 1);
    }

    return NULL;
}


static void *monitor (void *param) {
    if (standalone) return NULL;

    struct {
        msg_t header;
        nxn_stat_t perf;
    } __attribute__((__packed__)) msg;

    msg.header.tag = MSG_PERF;
    msg.header.rank = rank;
    perf.local_total = total_local_size;
    perf.mem_total = total_size;

    int i, ret;

    while (!monitor_stop) {
        usleep(REPORT_PERIOD);
        struct tms tms;
        times(&tms);
        perf.time = nxn_time();
        perf.mem_free = (size_t)nxn_mem_avail() * block_size;
        perf.utime = (long long)tms.tms_utime * 1000 / tick_per_sec;
        perf.stime = (long long)tms.tms_stime * 1000 / tick_per_sec;

        int busy;
        for (busy = 0; busy < MAX_BUSY_PEER; busy++) {
            perf.busy_peer[busy] = -1;
        }

        busy = 0;
        perf.net_queue = 0;
        for (i = 0; i < size; i++) {
            if (i == rank) continue;
            if (queue_full(&sender_tasks[i])) {
                if (busy < MAX_BUSY_PEER) {
                    perf.busy_peer[busy] = i;
                    ++busy;
                }
            }
            perf.net_queue += queue_size(&sender_tasks[i]); 
        }
        perf.net_queue = perf.net_queue * 1000 / (SEND_DEPTH * (size -1));

        for (busy = 0; busy < MAX_BUSY_DISK; busy++) {
            perf.busy_disk[busy] = - 1;
        }

        busy = 0;
        perf.disk_queue = 0;
        for (i = 0; i < n_roots; i++) {
            perf.disk_queue += queue_size(&writer_tasks[i]) ;
            if (queue_full(&writer_tasks[i])) {
                if (busy < MAX_BUSY_DISK) {
                    perf.busy_disk[busy] = i;
                    ++busy;
                }
            }
        }

        perf.disk_queue = perf.disk_queue * 1000 / (WRITE_DEPTH * n_roots);

        if (n_zippers > 0) {
            perf.zip_queue = queue_size(&zipper_tasks) * 1000 / ZIP_DEPTH;
        }

        msg.perf = perf;
        ret = pthread_mutex_lock(&master_lock);
        verify(ret == 0);
        ret = sendx(master, &msg, sizeof(msg));
        verify(ret == sizeof(msg));
        atomic_add(&perf.net_out, ret);
        ret = pthread_mutex_unlock(&master_lock);
        verify(ret == 0);
    }

    return NULL;
}

int nxn_init_threads () {
    int i, ret;

    n_zippers = job->zipper;
    if (n_zippers > MAX_ZIPPER) n_zippers = MAX_ZIPPER;

    writer_stop = sender_stop = receiver_stop = 0;
    monitor_stop = unzipper_stop = zipper_stop = worker_stop = 0; 

    ret = pthread_create(&pmonitor, NULL, monitor, NULL);
    verify(ret == 0);

    // writer

    for (i = 0; i < n_roots; i++) {
        queue_init(&writer_tasks[i], WRITE_DEPTH);
        ret = pthread_create(&pwriters[i], NULL, writer, &writer_tasks[i]);
        verify(ret == 0);
    }

    // unzipper
    queue_init(&unzipper_tasks, ZIP_DEPTH);
    ret = pthread_create(&punzipper, NULL, unzipper, &unzipper_tasks);
    verify(ret == 0);

    // receiver
    for (i = 0; i < size; i++) {
        if (i == rank) continue;
        ret = pthread_create(&preceivers[i], NULL, receiver, (void *)(intptr_t)i);
        verify(ret == 0);
    }

    // sender
    for (i = 0; i < size; i++) {
        if (i == rank) continue;
        queue_init(&sender_tasks[i], SEND_DEPTH);
        ret = pthread_create(&psenders[i], NULL, sender, (void *)(intptr_t)i);
        verify(ret == 0);
    }

    // zipper
    if (n_zippers > 0) {
        queue_init(&zipper_tasks, ZIP_DEPTH);
        for (i = 0; i < n_zippers; i++) {
            ret = pthread_create(&pzippers[i], NULL, zipper, &zipper_tasks);
            verify(ret == 0);
        }
    }

    if (job->prelude) {
        cur_worker = 0;
        job->prelude(job->u_ptr);
    }

    // worker
    //queue_init(&worker_tasks, WORKER_DEPTH);
    for (i = 0; i < n_workers; i++) {
        ret = pthread_create(&pworkers[i], NULL, worker, (void *)(intptr_t)i);
        verify(ret == 0);
    }

    // reader
    /*
    n_readers = 0; // no readers, workers directly load
    for (i = 0; i < n_readers; i++) {
        ret = pthread_create(&preaders[i], NULL, reader, NULL);
        verify(ret == 0);
    }
    */

    return 0;
}

int nxn_cleanup_threads () {
    int i, ret;
    void *value;

    // reader
    /*
    for (i = 0; i < n_readers; i++) {
        ret = pthread_join(preaders[i], &value);
        verify(ret == 0);
    }*/

    // worker
    worker_stop = 1;
    for (i = 0; i < n_workers; i++) {
        ret = pthread_join(pworkers[i], &value);
        verify(ret == 0);
    }
    // queue_cleanup(&worker_tasks);

    if (job->postlude) {
        job->postlude(job->u_ptr);
    }

    nxn_flush_remote();

    // zipper
    if (n_zippers > 0) {
        zipper_stop = 1;
        queue_wakeall(&zipper_tasks);
        for (i = 0; i < n_zippers; i++) {
            ret = pthread_join(pzippers[i], &value);
            verify(ret == 0);
        }
        queue_cleanup(&zipper_tasks);
    }

    // sender
    sender_stop = 1;
    for (i = 0; i < size; i++) {
        if (i == rank) continue;
        queue_wakeall(&sender_tasks[i]);
        ret = pthread_join(psenders[i], &value);
        verify(ret == 0);
        queue_cleanup(&sender_tasks[i]);
    }

    // receiver
    receiver_stop = 1;
    for (i = 0; i < size; i++) {
        if (i == rank) continue;
        ret = pthread_join(preceivers[i], &value);
        verify(ret == 0);
    }

    // unzipper
    unzipper_stop = 1;
    queue_wakeall(&unzipper_tasks);
    ret = pthread_join(punzipper, &value);
    verify(ret == 0);
    queue_cleanup(&unzipper_tasks);

    // writers
    nxn_flush_write();
    writer_stop = 1;
    for (i = 0; i < n_roots; i++) {
        queue_wakeall(&writer_tasks[i]);
        ret = pthread_join(pwriters[i], &value);
        verify(ret == 0);
        queue_cleanup(&writer_tasks[i]);
    }

    // monitor
    monitor_stop = 1;
    ret = pthread_join(pmonitor, &value);
    verify(ret == 0);
    return 0;
}




int nxn_run (const nxn_job_t *job_) {
    //bzero(&perf, sizeof(perf));
    verify(job == NULL);
    job = job_;
    perf.done_parts = 0;
    nxn_init_storage(job);
    nxn_init_mem(job);
    nxn_init_accu(job);
    nxn_init_threads(job);
    nxn_cleanup_threads();
    nxn_cleanup_accu();
    nxn_cleanup_mem();
    nxn_cleanup_storage();
    job = NULL;
    return 0;
}

// routines used in the computation routines, all thread-safe
int nxn_write (short output, int id, const void *buf) {
    int part = id % n_global_parts;
    verify(part >= 0);
    verify(part < MAX_PART);
    short peer; //= part / n_parts;
    short l_part;// = part % n_parts;
    part_split(part, &peer, &l_part);
    if (peer == rank) {   // local data
        append_write(l_part, output, buf);
    }
    else {
        append_send(output, peer, l_part, buf);
    }
    return 0;
}

int nxn_update_app_time()
{
    /*
    struct timespec tp; 
    clock_gettime(CLOCK_THREAD_CPUTIME_ID, &tp);
    perf.atime = tp.tv_sec * 1000 + tp.tv_nsec / 1000000;
    */
    return 0;
}

int nxn_write_local (int cur, short input, nxn_segment_t const *buf) {
    char path[STR_SIZE];
    mkidir(path, local_roots[cur], input);
    int ret = mkdir(path, 0777);
    verify((ret == 0) || (errno == EEXIST));
    mkipath(path, local_roots[cur], local_parts[cur], input);
    int fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0666);
    verify(fd >= 0);
    ssize_t rd = writex(fd, buf->data, buf->len);
    verify(rd == buf->len);
    atomic_add(&perf.disk_out, rd);
    close(fd);
    return 0;
}

int nxn_local_fd (int cur, short input) {
    char path[STR_SIZE];
    mkidir(path, local_roots[cur], input);
    int ret = mkdir(path, 0777);
    verify((ret == 0) || (errno == EEXIST));
    mkipath(path, local_roots[cur], local_parts[cur], input);
    int fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0666);
    verify(fd >= 0);
    return fd;
}

// monitoring routines
//
long long nxn_local_size (short l) {
    long long r = 0;
    int i;
    for (i = 0; local_parts[i] >= 0; ++i) {
        r += local_sizes[i][l];
    }
    return r;
}

long long nxn_max_input_size () {
    return max_part_size;
}

