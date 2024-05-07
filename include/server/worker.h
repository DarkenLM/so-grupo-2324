/******************************************************************************
 *                               WORKER PROCESS                               *
 *                                                                            *
 *   The worker process is responsible for executing the different tasks that *
 * are assigned to it by the operator process. On creation, it shall connect  *
 * to the event system of the operator process and transmit a ready signal,   *
 * upon which the operator process shall distribute tasks as they arrive.     *
 *   The amount of worker processes to be created depends on the server input *
 * parallel_tasks.                                                            *
 ******************************************************************************/

#ifndef SERVER_WORKER_H
#define SERVER_WORKER_H

#include <fcntl.h>

typedef struct worker {
    pid_t pid;
    int pipe_write;
} WORKER, *Worker;

/**
 * @brief Starts a new worker process.
 */
Worker start_worker(int operator_pd, int worker_id, char* output_dir, char* history_file_path);

#endif