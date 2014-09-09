/* 
    Copyright (C) 2011 Wei Dong <wdong.pku@gmail.com>. All Rights Reserved.

    DISTRIBUTION OF THIS PROGRAM IN EITHER BINARY OR SOURCE CODE FORM MUST BE
    PERMITTED BY THE AUTHOR.  NO COMMERCIAL USE ALLOWED.
*/
static const char *COPYRIGHT =
    "NxN out-of-core cluster computation infrastructure.\n"
    "Copyright (C) 2011 Wei Dong <wdong.pku@gmail.com>. All Rights Reserved.\n"
    "Do not redistribute the program in either binary or source code form.\n"
    "\n";

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h> 
#include <sys/socket.h>
#include <sys/wait.h>
#include <sys/mman.h>
#include <netinet/in.h>
#include <netdb.h>
#include <sys/epoll.h>
#include "nxn.h"
#include "nxn-impl.h"
#include "config.h"

static const char SEP = ':';

void usage (FILE *f) {
    fprintf(f, "Usage:\n"
           "    nxn [options] <command>\n\n"
           "Options:\n"
           "    -c              don't change to the same dir\n"
           "    -p <file>       pid file (default /tmp/nxn.pid)\n"
           "    -h              print help message\n"
           "    -s <machine>    add a node\n"
           "    -m <file>       machine file\n"
           "\n"
          );
}

int main (int argc, char *argv[]) {

    consistency_check();

    char pfile[STR_SIZE] = "/tmp/nxn.pid";
    char shm_name[STR_SIZE];
    int do_ch = 1;
    int size = 0;
    char machines[MAX_NODE][MAX_HOST_NAME];
    int c, i, ret, s;
    pid_t pid = getpid();

    fprintf(stderr, "%s", COPYRIGHT);

    // process commandline
    while ((c = getopt(argc, argv, "+chm:s:p:qv")) != -1) {
        switch (c) {
            case 'c': do_ch = 0;
                      break;
            case 'h': usage(stdout);
                      return 0;
            case 'm': {   // read host file
                          FILE *fin = fopen(optarg, "r");
                          verify(fin);
                          for (;;) {
                            if (size >= MAX_NODE) {
                                LOG(WARN, "only %d nodes are used.\n", MAX_NODE);
                                break;
                            }
                            if (fscanf(fin, "%s", machines[size]) < 1) break;
                            size++;
                          }
                          fclose(fin);
                      }
                      break;
            case 'p': verify(strlen(optarg) < STR_SIZE);
                      strcpy(pfile, optarg);
                      break;
            case 'q': if (loglevel < FATAL) loglevel++;
                      break;
            case 's': {
                          if (size >= MAX_NODE) {
                            LOG(WARN, "only %d nodes are used.\n", MAX_NODE);
                          }
                          else {
                            strcpy(machines[size], optarg);
                            size++;
                          }
                      }
                      break;
            case 'v': if (loglevel > DEBUG) loglevel--;
                      break;
            default:
                      usage(stderr);
                      return -1;

        }
    }

    if ((optind >= argc)) {
        usage(stderr);
        return -1;
    }


    {   // write pid file
        FILE *fout = fopen(pfile, "w");
        verify(fout);
        fprintf(fout, "%d\n", pid);
        fclose(fout);
    }

    int slaves[MAX_NODE];

    int shm = -1;
    nxn_mon_t *mon = NULL;
    nxn_stat_t *stats = NULL;//[MAX_NODE];
    size_t map_length = 0;

    // populate the empty file
    {
        ret = snprintf(shm_name, STR_SIZE, "/nxn.%d", pid);
        verify(ret < STR_SIZE);
        shm = shm_open(shm_name, O_RDWR | O_CREAT | O_EXCL, 0777);
        verify(shm >= 0);
        size_t len = sizeof(nxn_mon_t) + size * sizeof(nxn_stat_t);
        ret = ftruncate(shm, len);
        verify(ret == 0);
        int page = sysconf(_SC_PAGESIZE);
        map_length = (len + page - 1)/page * page;
        mon = mmap(NULL, map_length, PROT_READ | PROT_WRITE, MAP_SHARED, shm, 0);
        verify(mon);
        stats = (nxn_stat_t *)(mon + 1);
    }

    mon->running = 1;
    mon->size = size;
    mon->start_time = nxn_time();

    struct sockaddr_in my_addr, slave_addr;

    socklen_t socklen;

    int my_fd;   // listening socket fd

    // 1. set up listening

    my_fd = socket(AF_INET, SOCK_STREAM, 0);
    verify(my_fd >= 0);

    bzero(&my_addr, sizeof(my_addr));
    my_addr.sin_family = AF_INET;
    my_addr.sin_addr.s_addr = INADDR_ANY;
    my_addr.sin_port = 0;   // bind to any available port

    ret = bind(my_fd, (struct sockaddr *)&my_addr, sizeof(my_addr));
    verify(ret == 0);

    socklen = sizeof(my_addr);
    ret = getsockname(my_fd, (struct sockaddr *)&my_addr, &socklen);
    verify(ret == 0);
    verify(socklen == sizeof(my_addr));

    listen(my_fd, size);

    // 2. spawn slaves
    //
    //
    char *ssh_args[MAX_ARGC];
    char nxn_params[BUFSIZE];
    char hostname[BUFSIZE];
    char pwd[BUFSIZE];

    ret = gethostname(hostname, BUFSIZE);
    verify(ret == 0);

    if (do_ch) {
        char *p = getcwd(pwd, BUFSIZE);
        verify(p == pwd);
    }
    else {
        strcpy(pwd, ".");
    }

    verify(argc - optind + 4 <= MAX_ARGC);
    ssh_args[0] = "ssh";
    c = 2;
    ssh_args[c++] = "nxn-slave";
    ssh_args[c++] = nxn_params;
    for (i = optind; i < argc; ++i) {
        ssh_args[c++] = argv[i];
    }
    ssh_args[c] = NULL;

    for (i = 0; i < size; i++) {
        pid_t p = fork();
        verify(p >= 0);
        if (p == 0) {
            close(0);
            sprintf(nxn_params, "%c%s%c%d%c%d%c%d%c%s",
                SEP,hostname,
                SEP,(int)ntohs(my_addr.sin_port),
                SEP,size,
                SEP,i,
                SEP,pwd);
            ssh_args[1] = machines[i];
            execvp("ssh", ssh_args);
            return 0;
        }
    }

    LOG(INFO, "processes spawned, waiting for connection...\n");
    // 3. wait for connections
    msg_t up;

    struct {
        msg_t header;
        addr_t peers[MAX_NODE];
    } __attribute__((__packed__)) all_up;

    for (i = 0; i < size; i++) {
        socklen = sizeof(slave_addr);
        s = accept(my_fd, (struct sockaddr *)&slave_addr, &socklen);
        verify(s >= 0);
        verify(socklen == sizeof(slave_addr));

        ret = recvx(s, &up, sizeof(up));
        verify(ret == sizeof(up));
        verify(up.tag == MSG_UP);
        verify((up.rank >= 0) && (up.rank < size));
        slaves[up.rank] = s;
        all_up.peers[up.rank].addr = slave_addr.sin_addr.s_addr;
        all_up.peers[up.rank].port = up.u.u16;
        LOG(DEBUG, "%d up.\n", up.rank);
    }

    close(my_fd);   // we don't need to wait for more connections

    // 4. send all up message
    all_up.header.tag = MSG_ALL_UP;
    all_up.header.rank = -1;
    all_up.header.u.i32 = size;

    int len = sizeof(msg_t) + sizeof(addr_t) * size;
    for (i = 0; i < size; i++) {
        ret = sendx(slaves[i], &all_up, len);
        verify(ret == len);
    }

    LOG(INFO, "everyone is connected.\n");

    // 5. message loop
    //
    int pfd = epoll_create(size);
    verify(pfd >= 0);

    for (i = 0; i < size; i++) {
        struct epoll_event ev;
        ev.events = EPOLLIN | EPOLLPRI;
        ev.data.fd = slaves[i];
        ret = epoll_ctl(pfd, EPOLL_CTL_ADD, slaves[i], &ev);
        verify(ret == 0);
    }

    int left = size;

    struct {
        msg_t header;
        char data[MAX_SCATTER * MAX_NODE];
    } __attribute__((__packed__)) scatter;

    struct {
        msg_t header;
        char data[STR_SIZE];
    } __attribute__((__packed__)) input;

    scatter.header.tag = MSG_SCATTER;
    scatter.header.rank = -1;
    scatter.header.u.i32 = 0;
    int scatter_size = 0;
    int scatter_left = size;

    input.header.tag = MSG_INPUT;
    input.header.rank = -1;
    input.header.u.i32 = 0;
    int input_left = size;

    for (;;) {
        struct epoll_event events[MAX_EVENT];
        int n = epoll_wait(pfd, events, MAX_EVENT, -1);
        for (i = 0; i < n; ++i) {
            int fd = events[i].data.fd;

            msg_t msg;

            ret = recvx(fd, &msg, sizeof(msg));
            verify(ret == sizeof(msg));

            if (msg.tag == MSG_QUIT) {
                struct epoll_event ev;
                ev.events = EPOLLIN | EPOLLPRI;
                ev.data.fd = fd;
                ret = epoll_ctl(pfd, EPOLL_CTL_DEL, fd, &ev);
                verify(ret == 0);
                close(fd);
                left--;
                if (left == 0) goto done;
            }
            else if (msg.tag == MSG_REPORT) {
                char txt[STR_SIZE];
                ret = recvx(fd, txt, msg.u.i32);
                verify(ret == msg.u.i32);
                txt[msg.u.i32] = 0;
                puts(txt);
            }
            else if (msg.tag == MSG_PERF) {
                ret = recvx(fd, &stats[msg.rank], sizeof(nxn_stat_t));
                verify(ret == sizeof(nxn_stat_t));
            }
            else if (msg.tag == MSG_SCATTER) {
                if (scatter_left == size) {
                    scatter_size = msg.u.i32;
                }
                else {
                    verify(scatter_size == msg.u.i32);
                }
                if (msg.u.i32) {
                    ret = recvx(fd, scatter.data + msg.rank * msg.u.i32, msg.u.i32);
                    verify(ret == msg.u.i32);
                }
                scatter_left--;
                if (scatter_left == 0) {
                    int len = sizeof(msg_t) + scatter_size * size;
                    for (i = 0; i < size; i++) {
                        ret = sendx(slaves[i], &scatter, len);
                        verify(ret == len);
                    }
                    scatter_left = size;
                    scatter.header.u.i32++;
                }
            }
            else if (msg.tag == MSG_INPUT) {
                input_left--;
                if (input_left == 0) {
                    printf("? ");
                    fgets(input.data, STR_SIZE, stdin);
                    int l = strlen(input.data);
                    if (input.data[l-1] == '\n') input.data[l-1] = 0;
                    for (i = 0; i < size; i++) {
                        ret = sendx(slaves[i], &input, sizeof(input));
                        verify(ret == sizeof(input));
                    }
                    input_left = size;
                    input.header.u.i32++;
                }
            }
            else verify(0);
        }
    }

done:
    close(pfd);

    {
        mon->running = 0;
        ret = munmap(mon, map_length);
        verify(ret == 0);
        ret = close(shm);
        verify(ret == 0);
        ret = shm_unlink(shm_name);
        verify(ret == 0);
    }

    for (i = 0; i < size; i++) {
        wait(&ret);
    }

    unlink(pfile);

    return 0;
}

