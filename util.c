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
#include "nxn.h"
#include "nxn-impl.h"
#include "config.h"

void consistency_check () {
    verify(sizeof(msg_t) == 8);
}

// logging
/*
*/
int loglevel = WARN;            // LOG LEVEL

void LOG(int level, const char *fmt, ...)
{
    static const char *level_str[] = {
        "DEBUG", "INFO", "WARN", "ERROR", "FATAL"
    };
    assert(loglevel >= DEBUG);
    assert(loglevel <= FATAL);
    if (level >= loglevel) {
        va_list args;
        fprintf(stderr, "[%s] ", level_str[level]);
        if (level >= ERROR) {
            fprintf(stderr, "(%d: %s) ", errno, strerror(errno));
        }
        va_start(args, fmt);
        vfprintf(stderr, fmt, args);
        va_end(args);
    }
}

void nxn_sprintf(char *str, const char *fmt, ...) {
    va_list args;
    va_start(args, fmt);
    int ret = vsnprintf(str, STR_SIZE, fmt, args);
    va_end(args);
    verify(ret < STR_SIZE);
}

size_t get_disk_avail (const char *path) {
    char cmd[STR_SIZE];
    int ret = snprintf(cmd, STR_SIZE, "df %s | tail -n 1 | awk '{print $4;}'", path);
    verify(ret < STR_SIZE);
    FILE *fin = popen(cmd, "r");
    verify(fin);
    size_t v;
    ret = fscanf(fin, "%ld", &v);
    verify(ret);
    pclose(fin);
    return v * 1024;
}
