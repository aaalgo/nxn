/* 
    Copyright (C) 2011 Wei Dong <wdong.pku@gmail.com>. All Rights Reserved.

    DISTRIBUTION OF THIS PROGRAM IN EITHER BINARY OR SOURCE CODE FORM MUST BE
    PERMITTED BY THE AUTHOR.  NO COMMERCIAL USE ALLOWED.
*/
#ifndef WDONG_NXN
#define WDONG_NXN

#include <stdlib.h>

#ifdef __cplusplus
extern "C" {
#endif

// startup/shutdown nxn.
// These functions are not thread-safe.
// The structure pointed to by the pointer job should remain valid until nxn_cleanup is called.
int nxn_startup();                  // must be invoked before everything
int nxn_shutdown();                 // must be invoked after everything is done

int nxn_size ();
int nxn_rank ();

long long nxn_time ();
long long nxn_thread_time ();
// report to master node, the message will be displayed on screen
int nxn_report (const char *fmt, ...);
// report the message only once at rank 0
int nxn_report_0 (const char *fmt, ...);

int nxn_scatter (const void *send, size_t len, void *receive);

static inline int nxn_barrier () {
    return nxn_scatter(NULL, 0, NULL);
}

int nxn_getline (char *line);

// nxn jobs

// local dataset

enum {
    NXN_LOCAL_AUX = 0x10,            // if a part has only AUX files, this part is skipped
};

typedef struct {
    char const *name;
    int flags;
} nxn_local_t;

// remote dataset

enum {
    NXN_REMOTE_APPEND = 0x01        // by default, all output are truncated at the beginning of a job
                                    // this flag keeps the old file
};

typedef struct {
    char const *name;               // name of the dataset
    int flags;                     // 0: remove first, 1: append
    int record_size;
} nxn_remote_t;

typedef struct {
    char *data;
    size_t len;
} nxn_segment_t;

typedef int (*nxn_prelude_t) (void *u_ptr);
typedef int (*nxn_postlude_t) (void *u_ptr);
typedef int (*nxn_routine_t) (void *u_ptr, int part, nxn_segment_t segs[]);

typedef struct {
    char const *dir;
    nxn_local_t const *localv;      // local dataset
    nxn_remote_t const *remotev;    // remote dataset
    nxn_prelude_t prelude;
    nxn_prelude_t postlude;
    nxn_routine_t routine;
    void *u_ptr;
    int npart;
    int zipper;
    int worker;

    size_t total_size;              // total size to allocate
    size_t block_size;              // divide memory to blocks of this size
} nxn_job_t;

// run a nxn job
int nxn_run (nxn_job_t const *job);

// the following functions can be used in nxn_routine_t, these are all thread-safe

long long nxn_local_size (short l);

int nxn_write (short output, int id, const void *buf);   // to remote
int nxn_write_local (int cur, short input, nxn_segment_t const *buf);  // to local
int nxn_local_fd (int cur, short input);

// performance monitoring
int nxn_update_app_time();

int nxn_part (int id);

// report the message to master

long long nxn_max_input_size();

static inline int nxn_max_part_size (int total, int part) {
    return (total + part - 1)/part;
}

static inline int nxn_in_part_rank (int id, int part) {
    return id / part;
}

static inline void atomic_add (long long *v, long long a) {
    __sync_fetch_and_add(v, a);
}

#ifdef __cplusplus
}
#endif

#endif

