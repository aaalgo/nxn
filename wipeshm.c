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


int main () {
    int i, ret;
    char buf[STR_SIZE];
    for (i = 0; i < 32768; i++) {
        sprintf(buf, "/nxn.%d", i);
        ret = shm_unlink(buf);
        if (ret == 0) {
            printf("removed: %d\n", i);
        }
    }
    return 0;
}
