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
#include <sys/timeb.h>
#include <sys/types.h> 
#include <sys/stat.h>
#include <sys/mman.h> 
#include <sys/times.h>
#include <fcntl.h>
#include "nxn.h"
#include "nxn-impl.h"
#include "config.h"

static int shm_fd = -1;
static void *shm_addr = NULL;
static size_t shm_len = 0;

int nxn_monitor_startup (char const *pfile, nxn_mon_t **mon, nxn_stat_t **stat) {
    verify(shm_fd < 0);
    FILE *fin = fopen(pfile ? pfile : "/tmp/nxn.pid", "r");
    if (fin == NULL) return -1;
    int pid, ret;
    ret = fscanf(fin, "%d", &pid);
    fclose(fin);
    if (ret != 1) return -1;
    
    char name[STR_SIZE];
    ret = snprintf(name, STR_SIZE, "/nxn.%d", pid);
    verify(ret < STR_SIZE);

    shm_fd = shm_open(name, O_RDONLY, 0666);
    if (shm_fd < 0) return -1;

    struct stat st;
    ret = fstat(shm_fd, &st);
    verify(ret == 0);
    shm_len = st.st_size;

    size_t page = sysconf(_SC_PAGESIZE);
    shm_len = (shm_len + page - 1) / page * page;

    shm_addr = mmap(NULL, shm_len, PROT_READ, MAP_SHARED, shm_fd, 0);
    verify(shm_addr);
    verify(mon);
    verify(stat);
    *mon = (nxn_mon_t *)shm_addr;
    *stat = (nxn_stat_t *)(shm_addr + sizeof(nxn_mon_t));
    return 0;
}

int nxn_monitor_shutdown () {
    verify(shm_fd >= 0);
    int ret = munmap(shm_addr, shm_len);
    verify(ret == 0);
    ret = close(shm_fd);
    verify(ret == 0);
    return 0;
}

