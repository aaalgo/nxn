/* 
    Copyright (C) 2011 Wei Dong <wdong.pku@gmail.com>. All Rights Reserved.

    DISTRIBUTION OF THIS PROGRAM IN EITHER BINARY OR SOURCE CODE FORM MUST BE
    PERMITTED BY THE AUTHOR.  NO COMMERCIAL USE ALLOWED.
*/
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "nxn.h"
#include "nxn-impl.h"
#include "config.h"

void setup_intermediate_dir (int root);

int main (int argc, char *argv[]) {
    loglevel = DEBUG;

    char buf[STR_SIZE];
    for (;;) {
        nxn_getline(buf);
        if (strcmp(buf, "exit") == 0) break;
        nxn_report("%s", buf);
    }

    //nxn_cleanup();
    nxn_shutdown();
    return 0;
}
