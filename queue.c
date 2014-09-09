/* 
    Copyright (C) 2011 Wei Dong <wdong.pku@gmail.com>. All Rights Reserved.

    DISTRIBUTION OF THIS PROGRAM IN EITHER BINARY OR SOURCE CODE FORM MUST BE
    PERMITTED BY THE AUTHOR.  NO COMMERCIAL USE ALLOWED.
*/
#include <pthread.h>
#include "nxn.h"
#include "nxn-impl.h"

void queue_init (queue_t *queue, int depth) {
    int r;
    queue->cnt = 0;
    r = pthread_mutex_init(&queue->lock, NULL);
    verify(r == 0);
    r = pthread_cond_init(&queue->avail, NULL);
    verify(r == 0);
    r = pthread_cond_init(&queue->notfull, NULL);
    verify(r == 0);
    queue->depth = depth;
    queue->next = queue->tail = NULL;
}

void queue_cleanup (queue_t *queue) {
    int r;
    verify(queue_empty(queue));
    r = pthread_mutex_destroy(&queue->lock);
    verify(r == 0);
    r = pthread_cond_destroy(&queue->avail);
    verify(r == 0);
}

int queue_empty (queue_t *queue) {
    return queue->next == NULL;
}

int queue_size (queue_t *queue) {
    return queue->cnt;
}

void queue_wakeall (queue_t *queue) {
    pthread_cond_broadcast(&queue->avail);
}

// add cnt to the counter
void __enqueue (queue_t *queue, link_t *block) {
    int r;
    verify(block->next == NULL);
    r = pthread_mutex_lock(&queue->lock);
    verify(r == 0);
    while (queue->cnt >= queue->depth) {
        pthread_cond_wait(&queue->notfull, &queue->lock);
        verify(r == 0);
    }
    if (queue->next == NULL) {
        verify(queue->tail == NULL);
        queue->next = queue->tail = block;
    }
    else {
        queue->tail->next = block;
        queue->tail = block;
    }
    queue->cnt++;
    r = pthread_cond_signal(&queue->avail);
    verify(r == 0);
    r = pthread_mutex_unlock(&queue->lock);
    verify(r == 0);
}

void __enqueue_front (queue_t *queue, link_t *block) {
    int r;
    verify(block->next == NULL);
    r = pthread_mutex_lock(&queue->lock);
    verify(r == 0);
    while (queue->cnt >= queue->depth) {
        pthread_cond_wait(&queue->notfull, &queue->lock);
        verify(r == 0);
    }
    if (queue->next == NULL) {
        verify(queue->tail == NULL);
        queue->next = queue->tail = block;
    }
    else {
        block->next = queue->next;
        queue->next = block;
    }
    queue->cnt++;
    r = pthread_cond_signal(&queue->avail);
    verify(r == 0);
    r = pthread_mutex_unlock(&queue->lock);
    verify(r == 0);
}

// if flag != NULL, then check *flag when queue is empty
// if *flag == 0, will return NULL when queue is empty
void __dequeue (queue_t *queue, link_t **link, int *stop) {
    verify(link);
    verify(stop);

    link_t *b;
    int r;
    r = pthread_mutex_lock(&queue->lock);
    verify(r == 0);
    while ((queue->next == NULL) && (!*stop)) {
        pthread_cond_wait(&queue->avail, &queue->lock);
        verify(r == 0);
    }
    b = queue->next;
    if (b != NULL) {
        queue->next = b->next;
        b->next = NULL;
        if (queue->next ==NULL) {
            queue->tail = NULL;
        }
        queue->cnt--;
    }
    r = pthread_cond_signal(&queue->notfull);
    verify(r == 0);
    r = pthread_mutex_unlock(&queue->lock);
    verify(r == 0);
    *link = b;
}

