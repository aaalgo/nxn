
static void *sender (void *param)
{
    int i, ret;
    queue_t *tasks = (queue_t *)param;

    int polled[MAX_NODE];   // whether a peer is currently polled
    int off[MAX_NODE];
    block_t *bufs[MAX_NODE];

    bzero(&polled, sizeof(polled));
    bzero(&bufs, sizeof(bufs));
    bzero(&off, sizeof(bufs));

    int pfd = epoll_create(size);
    verify(pfd >= 0);

    int empty = 0;
    while ((tasks->next) || (!empty) || !sender_stop) {

        if (tasks->next) { // steal jobs from queue
            ret = pthread_mutex_lock(&tasks->lock);
            verify(ret == 0);
            // (queue is empty, all data are sent and asked to stop)
            // enumerate queue
            link_t *cur = tasks->next;
            link_t *pre = NULL;
            while (cur != NULL) {
                int p = ((block_t *)cur)->u.send.peer;
                if (bufs[p] == NULL) {  // steal job
                    tasks->cnt--;
                    bufs[p] = (block_t *)cur;
                    off[p] = 0;
                    if (pre == NULL) {
                        tasks->next = cur->next;
                    }
                    else {
                        pre->next = cur->next;
                    }
                    cur = cur->next;
                    bufs[p]->link.next = NULL;
                }
                else {
                    pre = cur;
                    cur = cur->next;
                }
            }
            tasks->tail = pre;

            ret = pthread_mutex_unlock(&tasks->lock);
            verify(ret == 0);
            if (tasks->cnt < tasks->depth) {
                ret = pthread_cond_signal(&tasks->notfull);
                verify(ret == 0);
            }
        }

        empty = 1;
        for (i = 0; i < size; i++) {
            if (i == rank) continue;
            struct epoll_event ev;
            ev.events = EPOLLOUT;
            ev.data.u32 = i;
            if (bufs[i]) empty = 0;
            if (polled[i] && (bufs[i] == NULL)) {
                // remove
                ret = epoll_ctl(pfd, EPOLL_CTL_DEL, peers[i], &ev);
                verify(ret == 0);
                polled[i] = 0;
            }
            else if ((!polled[i]) && bufs[i]) {
                // add
                ret = epoll_ctl(pfd, EPOLL_CTL_ADD, peers[i], &ev);
                verify(ret == 0);
                polled[i] = 1;
            }
        }

        if (empty) {
            usleep(SLEEP_TIME);
            continue;
        }

        struct epoll_event events[MAX_EVENT];
        int n = epoll_wait(pfd, events, MAX_EVENT, -1);
        for (i = 0; i < n; ++i) {
            int p = events[i].data.u32;
            verify(p != rank);
            block_t *task = bufs[p];
            verify(task != NULL);
            int fd = peers[p];

            ret = send(fd, task->data + off[p], task->len - off[p], 0);
            verify(ret >= 0);
            atomic_add(&perf.net_out, ret);

            off[p] += ret;
            if (off[p] >= task->len) {
                bufs[p] = NULL;
                off[p] = 0;
                nxn_free(task);
            }
        }
    }

    ret = close(pfd);
    // done, send "done" messages
    msg_t bye;
    bye.tag = MSG_DONE;
    bye.rank = rank;
    for (i = 0; i < size; i++) {
        if (i == rank) continue;
        ret = sendx(peers[i], &bye, sizeof(bye));
        verify(ret == sizeof(bye));
        atomic_add(&perf.net_out, ret);
    }
    return NULL;
}

#endif
