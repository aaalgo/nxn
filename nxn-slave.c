/* 
    Copyright (C) 2011 Wei Dong <wdong.pku@gmail.com>. All Rights Reserved.

    DISTRIBUTION OF THIS PROGRAM IN EITHER BINARY OR SOURCE CODE FORM MUST BE
    PERMITTED BY THE AUTHOR.  NO COMMERCIAL USE ALLOWED.
*/
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include "nxn.h"
#include "nxn-impl.h"
#include "config.h"

int main (int argc, char **argv) {
    verify(argc >= 3);
    int ret;
    char *nxn_params = argv[1];

    puts(nxn_params);

    char sep[2] = {nxn_params[0], 0};
    nxn_params++;
    char *str = strtok(nxn_params, sep);
    verify(str);
    setenv("NXN_MASTER", str, 1);

    str = strtok(NULL, sep);
    verify(str);
    setenv("NXN_PORT", str, 1);

    str = strtok(NULL, sep);
    verify(str);
    setenv("NXN_SIZE", str, 1);

    str = strtok(NULL, sep);
    verify(str);
    setenv("NXN_RANK", str, 1);

    str = strtok(NULL, sep);
    verify(str);
    ret = chdir(str);
    verify(ret == 0);

    argv += 2;
    execvp(argv[0], argv);
    return 0;
}

