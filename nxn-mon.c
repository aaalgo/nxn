#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <ncurses.h>
#include "nxn.h"
#include "nxn-impl.h"
#include "config.h"

void diff (int size, nxn_stat_t *avg, nxn_stat_t *perf, nxn_stat_t *cur, nxn_stat_t *prev) {
    int i, j;
    int D = sizeof(nxn_stat_t) / sizeof(long long);
    bzero(avg, sizeof(nxn_stat_t));
    for (i = 0; i < size; i++) {
        nxn_stat_t *p = perf + i;
        nxn_stat_t *cu = cur + i;
        nxn_stat_t *pr = prev + i;
        *p = *cu;
        long long tm = cu->time - pr->time; 
        p->time = 0;
        if (tm > 0) {
            p->time = (cu->utime + cu->stime - pr->utime - pr->stime) * 1000 / tm;
            p->p6 = (cu->atime - pr->atime) * 1000 / tm;
            p->p1 = (cu->disk_in - pr->disk_in) * 1000 / tm;
            p->p2 = (cu->disk_out - pr->disk_out) * 1000 / tm;
            p->p3 = (cu->net_in - pr->net_in) * 1000 / tm;
            p->p4 = (cu->net_out - pr->net_out) * 1000 / tm;
            p->p5 = (cu->zip - pr->zip) * 1000 / tm;
        }
        for (j = 0; j < D; j++) {
            ((long long *)avg)[j] += ((long long *)p)[j];
        }
    }
    for (j = 0; j < D; j++) {
        ((long long *)avg)[j] /= size;
    }

}

char *format_num (long long w) {
    static char buf[STR_SIZE];
    sprintf(buf, "%lld", w);
    return buf;
}

char *format_throughput (long long w) {
    static char buf[STR_SIZE];
    double v = w;

    v /= 1024.0 * 1024.0;
    sprintf(buf, "%3.1f", v);
    return buf;
}

char *format_size (long long w) {
    static char buf[STR_SIZE];
    char suf = 'B';
    double v = w;

    suf = 'M';
    v /= 1024.0 * 1024.0;
    /*
    if (v > 1000) {
        suf = 'K';
        v /= 1024.0;
    }
    if (v > 1000) {
        suf = 'M';
        v /= 1024.0;
    }
    */
    if (v > 1000) {
        suf = 'G';
        v /= 1024.0;
    }
    if (v > 1000) {
        suf = 'T';
        v /= 1024.0;
    }
    sprintf(buf, "%3.1f%c", v, suf);
    return buf;
}

char *format_time (long long t) {
    static char buf[STR_SIZE];
    char suf = 's';
    double v = t / 1000.0;
    if (v > 100) {
        v /= 60;
        suf = 'm';
    }
    if (v > 100) {
        v /= 60;
        suf = 'h';
    }
    sprintf(buf, "%02.2f%c", v, suf);
    return buf;
}

char *format_percent (long long t) {
    static char buf[STR_SIZE];
    double v = t / 10.0;
    sprintf(buf, "%03.1f", v);
    return buf;
}

void print_head (int prefix) {
    verify(prefix < STR_SIZE);
    printw("              CPU TIME  READ WRITE  RECV  SEND   ZIP           QUEUE %%    MEM     TASKS\n");
   printw(" NO  TOTAL  ALL%%  APP%%  MB/s  MB/s  MB/s  MB/s  MB/s   ZIP  DISK   NET   FREE  ALL DONE\n");
    printw("---------------------------------------------------------------------------------------\n"); // MEM FREE INPUT DONE\n", buf);
}

void print_perf (nxn_stat_t *stat) {
    printw(" %6s", format_time(stat->stime + stat->utime));
    printw(" %5s", format_percent(stat->time));
    printw(" %5s", format_percent(stat->p6));
    //printw(" %6s", format_size(stat->disk_in));
    printw(" %5s", format_throughput(stat->p1));
    //printw(" %6s", format_size(stat->disk_out));
    printw(" %5s", format_throughput(stat->p2));
    printw(" %5s", format_throughput(stat->p3));
    printw(" %5s", format_throughput(stat->p4));
    printw(" %5s", format_throughput(stat->p5));
    printw(" %5s", format_percent(stat->zip_queue));
    printw(" %5s", format_percent(stat->disk_queue));
    printw(" %5s", format_percent(stat->net_queue));
    printw(" %6s", format_size(stat->mem_free));
    printw(" %4s", format_num(stat->local_parts));
    printw(" %4s", format_num(stat->done_parts));
    /*
    printw(" %s", format_size(stat->done_parts));
    */
    printw("\n");
}

nxn_stat_t *perf = NULL;

int compare (const void *p1, const void *p2) {
    int s1 = *(int const *)p1;
    int s2 = *(int const *)p2;
    return perf[s1].done_parts - perf[s2].done_parts;
}

int main (int argc, char *argv[]) {
    nxn_mon_t *mon = NULL;
    nxn_stat_t *stat = NULL;

    int delay = 5;
    int period = 10;
    int Q = 10 / delay * period;

    nxn_stat_t avg;
    nxn_stat_t **stats = NULL;
    int ref[MAX_NODE];

    int i, ret;
    int x, y;
    int cur = 0;

    ret = nxn_monitor_startup(NULL, &mon, &stat);
    if (ret < 0) return -1;

    stats = calloc(Q, sizeof(nxn_stat_t *));
    verify(stats);
    perf = calloc(mon->size, sizeof(nxn_stat_t));
    verify(perf);
    for (i = 0; i < Q; i++) {
        stats[i] = calloc(mon->size, sizeof(nxn_stat_t));
        verify(stats[i]);
    }

    initscr();
    raw();
    halfdelay(2);
    noecho();

    //int freeze = 0;

    //for (;;) {
    while (mon->running) {
        int ch = getch();
        if (ch == 'q') break;
        getmaxyx(stdscr, y, x);
        y -= 6;

        /*
        if (!mon->running) {
            if (!freeze) {
                move(0,0);
                attron(A_BLINK);
                printw("FIN");
                attroff(A_BLINK);
                refresh();
                freeze = 1;
            }
            continue;
        }
        */
        int next = (cur + 1) % Q;

        long long elapsed = nxn_time() - mon->start_time;

        // copy stats to cur
        memcpy(stats[cur], stat, mon->size * sizeof(nxn_stat_t));
        diff(mon->size, &avg, perf, stats[cur], stats[next]);

        move(0, 0);
        /*
        attron(A_BLINK);
        printw("RUN"); 
        attroff(A_BLINK);
        */
        printw("CLUSTER AVERAGE");
        printw(" MEM: %s", format_size(stat->mem_total));
        printw(" READ: %s", format_size(stat->disk_in));
        printw(" WRITE: %s", format_size(stat->disk_out));
        printw(" TIME: %s (%.3fs)", format_time(elapsed), elapsed/1000.0);
        printw("\n");
        print_head(2);
        attron(A_BOLD);
        printw("AVG");
        print_perf(&avg);
        attroff(A_BOLD);

        for (i = 0; i < mon->size; i++) {
            ref[i] = i;
        }
        qsort(ref, mon->size, sizeof(ref[0]), compare);
        for (i = 0; i < mon->size; i++) {
            if (i >= y) break;
            printw("%03d", ref[i]);
            print_perf(&perf[ref[i]]);
        }
        refresh();
        cur = next;
    }

    endwin();
    nxn_monitor_shutdown();

    for (i = 0; i < Q; i++) {
        free(stats[i]);
    }
    free(perf);
    free(stats);
    return 0;
}

