/* 
    Copyright (C) 2011 Wei Dong <wdong.pku@gmail.com>. All Rights Reserved.

    DISTRIBUTION OF THIS PROGRAM IN EITHER BINARY OR SOURCE CODE FORM MUST BE
    PERMITTED BY THE AUTHOR.  NO COMMERCIAL USE ALLOWED.
*/
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/time.h>
#include <sys/resource.h>
#include "nxn.h"
#include "nxn-impl.h"
#include "config.h"

void setup_intermediate_dir (int root);

int main (int argc, char *argv[]) {
    loglevel = DEBUG;
    nxn_setup_t job;
    int r;

    nxn_local_t *locals[] = {{NULL, 0}};
    nxn_remote_t remotes[] = {{NULL, 0, 0}};

    nxn_suggest_setup(&job);
    job.dir = "test";
    job.localv = locals;
    job.remotev = remotes;
    job.total_size = 1024L * 1024 * 512;
    job.block_size = 2 * 1024 * 1024;
    job.npart = 10;
    job.zipper = 0;
    nxn_startup();
    nxn_init(&job);
    for (r = 0; r < 5; r++) {
        sleep(1);
    }
    nxn_cleanup();
    nxn_shutdown();
    return 0;
}
