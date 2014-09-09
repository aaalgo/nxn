/* 
    Copyright (C) 2011 Wei Dong <wdong.pku@gmail.com>. All Rights Reserved.

    DISTRIBUTION OF THIS PROGRAM IN EITHER BINARY OR SOURCE CODE FORM MUST BE
    PERMITTED BY THE AUTHOR.  NO COMMERCIAL USE ALLOWED.
*/
#ifndef WDONG_NXN_IMPL
#define WDONG_NXN_IMPL

#include <stdint.h>
#include <pthread.h>
#include <unistd.h>
#include <sys/socket.h>

//////////// Protocol
enum { MSG_UP = 1, MSG_ALL_UP, MSG_HELLO, MSG_QUIT, MSG_REPORT, MSG_DONE, MSG_DATA, MSG_PERF, MSG_SCATTER, MSG_INPUT};


typedef struct {
    int16_t tag;
    int16_t rank;       // master's rank is -1
    union {
        int32_t i32;
        uint32_t u32;
        int16_t i16;
        uint16_t u16;
    } u;
} __attribute__((__packed__)) msg_t;

typedef struct {
    uint32_t addr;
    uint16_t port;
    uint16_t p0;
} __attribute__((__packed__)) addr_t;


/*
#define MSG_UP      1
    // i16: my_port

#define MSG_ALL_UP  2
    // followed by
    // num_nodes * addr_t

#define MSG_HELLO   3

#define MSG_DONE  4

#define MSG_BYE 6

#define MSG_DATA 7
    16-bit-local-part 16-bit-output data
    ...
#define MSG_PERF 
    // followed by perf

#define MSG_SCATTER
    // i32: size
    //
#define MSG_SCATTER_ACK
    // i32: current round of scatter

*/

/* log levels */
enum { DEBUG = 0, INFO, WARN, ERROR, FATAL};

/* log level to use */
extern int loglevel; 

void nxn_sprintf(char *buf, const char *fmt, ...);

void LOG(int level, const char *fmt, ...);

#define verify(_x) \
    do { \
        if (!(_x)) { \
            LOG(FATAL, "Assertion failure in %s line %d of %s: %s\n", \
                    __FUNCTION__, __LINE__, __FILE__, #_x); \
            exit(-1);\
        } \
    } while (0)

void consistency_check ();

typedef struct __link {
    struct __link *next;
} link_t;

typedef struct {
    link_t *next;
    link_t *tail;          // when next == NULL, tail is undefined
    pthread_mutex_t lock;
    pthread_cond_t avail;
    pthread_cond_t notfull;
    int depth;
    size_t cnt;
} queue_t;

static inline int queue_full (queue_t *queue) {
    return queue->cnt == queue->depth;
}

void queue_init (queue_t *queue, int depth);

void queue_cleanup (queue_t *queue);

int queue_empty (queue_t *queue);

int queue_size (queue_t *queue);

static inline int queue_depth (queue_t *queue) {
    return queue->depth;
}

void queue_wakeall (queue_t *queue);

// add cnt to the counter
#define enqueue(queue,link) do {__enqueue(queue, (link_t *)(link)); link = NULL; } while (0)
void __enqueue (queue_t *queue, link_t *link);

// memory allocator should use a stack instead of queue to maintain locality
// and enqueue_front should be used to release a block
#define enqueue_front(queue,link) do {__enqueue(queue, (link_t *)(link)); link = NULL; } while (0)
void __enqueue_front (queue_t *queue, link_t *link);

#define dequeue(queue,link,stop) __dequeue(queue, (link_t **)(link), stop)
void __dequeue (queue_t *queue, link_t **link, int *stop);


// performance monitoring
typedef struct {
    long long running;
    long long size;
    long long start_time;

    long long p3;
    long long p4, p5, p6, p7;
    long long p8, p9, p10, p11;
    long long p12, p13, p14, p15;
} nxn_mon_t;

#define MAX_BUSY_PEER             8
#define MAX_BUSY_DISK             4
typedef struct {
    long long time;
    long long atime;                // in milisecs
    long long utime;                // in milisecs
    long long stime;                // in milisecs
    long long disk_in;              // bytes read from disk
    long long disk_out;             // bytes written to disk
    long long disk_queue;           // queue %

    long long net_in;
    long long net_out;
    long long net_queue;            // queue %

    long long mem_total;
    long long mem_free;

    long long local_parts;
    long long done_parts;
    long long local_total;

    long long zip;
    long long zip_queue;
    long long p1, p2, p3, p4, p5, p6, p7;

    int busy_peer[MAX_BUSY_PEER];
    int busy_disk[MAX_BUSY_DISK];
} nxn_stat_t;

int nxn_monitor_startup (char const *pfile, nxn_mon_t **mon, nxn_stat_t **stat);
int nxn_monitor_shutdown ();

// auxiliary routines

// I/O in full, do not return only all data are done
static inline ssize_t writex (int s, const void *buf_, size_t len) {
    const char *buf = buf_;
    ssize_t ret;
    ssize_t sent = 0;
    while(len) {
        ret = write(s, buf, len);
        if (ret <= 0) return ret;
        verify(ret <= len);
        buf += ret;
        len -= ret;
        sent += ret;
    }
    return sent;
}

static inline ssize_t sendx (int s, const void *buf_, size_t len) {
    const char *buf = buf_;
    ssize_t ret;
    ssize_t sent = 0;
    while(len) {
        ret = send(s, buf, len, 0);
        if (ret <= 0) return ret;
        verify(ret <= len);
        buf += ret;
        len -= ret;
        sent += ret;
    }
    return sent;
}

static inline ssize_t readx (int s, void *buf_, size_t len) {
    char *buf = buf_;
    ssize_t ret;
    ssize_t recvd = 0;
    while(len) {
        ret = read(s, buf, len);
        if (ret <= 0) return ret;
        verify(ret <= len);
        buf += ret;
        len -= ret;
        recvd += ret;
    }
    return recvd;
}

static inline ssize_t recvx (int s, void *buf_, size_t len) {
    char *buf = buf_;
    ssize_t ret;
    ssize_t recvd = 0;
    while(len) {
        ret = recv(s, buf, len, 0);
        if (ret <= 0) return ret;
        verify(ret <= len);
        buf += ret;
        len -= ret;
        recvd += ret;
    }
    return recvd;
}


#endif
