#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <ncurses.h>
#include "nxn.h"
#include "nxn-impl.h"
#include "config.h"


int main (int argc, char *argv[]) {
    int use_name = 0;
    char names[MAX_NODE][STR_SIZE];

    bzero(names, sizeof(names));
    if (argc > 1) {
        FILE *fin = fopen(argv[1], "r");
        int i = 0;
        while (fscanf(fin, "%s", names[i]) == 1) {
            i++;
        }
        fclose(fin);
        use_name = 1;
    }

    int ret;

    nxn_mon_t *mon;
    nxn_stat_t *stat;

    ret = nxn_monitor_startup(NULL, &mon, &stat);
    if (ret < 0) return -1;

    printf("BUSY PEER\n");

    int i, j;
    for (i = 0; i < mon->size; i++) {
        if (stat->busy_peer[0] < 0) continue;
        if (use_name) {
            printf("%s:", names[i]);
        }
        else {
            printf("%d:", i);
        }
        for (j = 0; j < MAX_BUSY_PEER; j++) {
            if (stat->busy_peer[j] >= 0) {
                if (use_name) {
                    printf(" %s", names[stat->busy_peer[j]]);
                }
                else {
                    printf(" %d", stat->busy_peer[j]);
                }
            }
        }
        printf("\n");
    }

    printf("BUSY DISK\n");

    for (i = 0; i < mon->size; i++) {
        if (stat->busy_disk[0] < 0) continue;
        if (use_name) {
            printf("%s:", names[i]);
        }
        else {
            printf("%d:", i);
        }
        for (j = 0; j < MAX_BUSY_DISK; j++) {
            if (stat->busy_disk[j] >= 0) {
                printf(" %d", stat->busy_disk[j]);
            }
        }
        printf("\n");
    }

    nxn_monitor_shutdown();

    return 0;
}

