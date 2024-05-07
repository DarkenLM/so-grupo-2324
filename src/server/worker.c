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
 
#define _XOPEN_SOURCE 500

#include "server/worker.h"
#include "server/worker_datagrams.h"
#include "server/process_mark.h"
#include "common/datagram/status.h"
#include "common/util/alloc.h"
#include "common/util/string.h"
#include "common/util/parser.h"
#include "common/util/mysystem.h"
#include "common/error.h"
#include "common/io/fifo.h"
#include "common/io/io.h"
#include "common/util/string.h"
#include <stdio.h>
#include <fcntl.h>
#include <unistd.h>
#include <signal.h>
#include <sys/wait.h>
#include <string.h>

#define LOG_HEADER "[WORKER] "
#define LOG_HEADER_PID "[WORKER@%d] "
#define READ 0
#define WRITE 1

void worker_signal_sigsegv(int signum) {
    if (signum != SIGSEGV) return;

    int pid = getpid();

    MAIN_LOG(LOG_HEADER_PID "Segmentation fault.\n", pid);
    _exit(1);
}

int run_task(char* input, char* output_file) {
    int pid = getpid();
    DEBUG_PRINT(LOG_HEADER_PID "Running command '%s'\n                 - Outputting to %s\n", pid, input, output_file);

    Tokens cmds = tokenize_char_delim(input, 16, "|");
    pid_t pids[cmds->len];

    // Backup STDOUT and STDERR
    int old_stdout = dup(STDOUT_FILENO);
    int old_stderr = dup(STDERR_FILENO);

    // Create and open the output file
    int task_fd = SAFE_OPEN(output_file, O_WRONLY | O_CREAT, 0644);

    // Redirect STDOUT and STDERR to output file.
    dup2(task_fd, STDOUT_FILENO);
    dup2(task_fd, STDERR_FILENO);

    int pfds[cmds->len][2];
    for (int i = 0; i < cmds->len; i++) {
        char* tcmd = trim(cmds->data[i]);

        int pfd[2];
        if (i != cmds->len - 1) {
            if (pipe(pfd) != 0) {
                perror("pipe");
                return 1;
            }

            pfds[i][0] = pfd[0];
            pfds[i][1] = pfd[1];
        }

        int pid = fork();
        if (pid == 0) {
            if (i == 0) {
                close(pfd[0]);

                dup2(pfd[1], STDOUT_FILENO);
                close(pfd[1]);
            } else if (i == cmds->len - 1) {
                dup2(pfds[i - 1][0], STDIN_FILENO);
                close(pfds[i - 1][0]);
            } else {
                close(pfds[i][0]);

                dup2(pfds[i - 1][0], STDIN_FILENO);
                dup2(pfds[i][1], STDOUT_FILENO);
                close(pfds[i - 1][0]);
                close(pfds[i][1]);
            }

            mysystem(tcmd);
            _exit(0);
        } else {
            pids[i] = pid;

            if (i == 0) {
                close(pfd[1]);
            } else if (i == cmds->len - 1) {
                close(pfds[i - 1][0]);
            } else {
                close(pfds[i - 1][0]);
                close(pfds[i][1]);
            }
        }
    }

    for (int i = 0; i < cmds->len; i++) {
        int status = 0;
        waitpid(pids[i], &status, 0);
    }
    
    // Close output file and restore STDOUT and STDERR
    close(task_fd);
    dup2(old_stdout, STDOUT_FILENO);
    dup2(old_stderr, STDERR_FILENO);
    close(old_stdout);
    close(old_stderr);

    DEBUG_PRINT(LOG_HEADER_PID "Finished running command '%s'\n", pid, input);
    return 0;
}

Worker start_worker(int operator_pd, int worker_id, char* output_dir, char* history_file_path) {
    #define ERR NULL
    ERROR_HEADER
    int _err_pid = 0;

    // Return value
    Worker ret = SAFE_ALLOC(Worker, sizeof(WORKER));

    // int old_stdout = dup(STDOUT_FILENO);

    int pfd[2];
    if (pipe(pfd) != 0) ERROR("Unable to open worker pipe");

    int pid = fork();
    if (pid == 0) {
        pid = getpid();
        _err_pid = pid;

        volatile sig_atomic_t shutdown_requested = 0;

        MAIN_LOG(LOG_HEADER_PID "Ready.\n", pid);

        while (!shutdown_requested) {
            // dup2(old_stdout, STDOUT_FILENO);
            MAIN_LOG(LOG_HEADER_PID "1m.\n", pid);
            DEBUG_PRINT(LOG_HEADER_PID "1d.\n", pid);

            WORKER_DATAGRAM_HEADER dh = read_worker_datagram_header(pfd[READ]);

            DEBUG_PRINT(LOG_HEADER_PID "Received datagram header: MODE: %d, TYPE: %d, TID: %d\n", pid, dh.mode, dh.type, dh.task_id);

            switch (dh.mode) {
                case WORKER_DATAGRAM_MODE_STATUS_REQUEST: {
                    DEBUG_PRINT(LOG_HEADER_PID "1\n", pid);
                    WorkerStatusRequestDatagram req = read_partial_worker_status_request_datagram(pfd[READ], dh);
                    DEBUG_PRINT(LOG_HEADER_PID "2\n", pid);
                    DEBUG_PRINT(LOG_HEADER_PID "Received status request.\n", pid);

                    int history_fd = SAFE_OPEN(history_file_path, O_RDONLY, 0644);

                    char* res = NULL;
                    res = strdup("Scheduled tasks:\n");

                    DEBUG_PRINT(LOG_HEADER_PID "RES 1: '%s'.\n", pid, res);

                    for(int i = 0 ; i < req->num_tasks_queued ; i++) {
                        char* _res = res;
                        res = isnprintf("%s%d %s\n", res, req->tasks[i]->task_id, req->tasks[i]->data);
                        free(_res);
                        DEBUG_PRINT(LOG_HEADER_PID "RES 2: '%s'.\n", pid, res);
                    }

                    char* _back = res;
                    res = isnprintf("%s\nExecuting tasks:\n", res);
                    free(_back);
                    for(int i = req->num_tasks_queued ; i < req->num_tasks ; i++) {
                        char* _res = res;
                        res = isnprintf("%s%d %s\n", res, req->tasks[i]->task_id, req->tasks[i]->data);
                        free(_res);
                        DEBUG_PRINT(LOG_HEADER_PID "RES 3: '%s'.\n", pid, res);
                    }

                    _back = res;
                    res = isnprintf("%s\nCompleted tasks:\n", res);
                    free(_back);
                    DEBUG_PRINT(LOG_HEADER_PID "RES 3: '%s'.\n", pid, res);

                    SAFE_SEEK(history_fd, 0, SEEK_END);
                    int hist_len = lseek(history_fd, 0, SEEK_CUR);
                    SAFE_SEEK(history_fd, 0, SEEK_SET);

                    DEBUG_PRINT(LOG_HEADER_PID "History len: %d\n", pid, hist_len);

                    char* hist = SAFE_ALLOC(char*, hist_len * sizeof(char));
                    SAFE_READ(history_fd, hist, hist_len * sizeof(char));
                    
                    DEBUG_PRINT(LOG_HEADER_PID "History:\n==================\n%s\n==================\n", pid, hist);

                    _back = res;
                    res = isnprintf("%s%s", res, hist);
                    free(_back);

                    DEBUG_PRINT(LOG_HEADER_PID "RES:\n==================\n%s\n==================\n", pid, res);
                    DEBUG_PRINT(LOG_HEADER_PID "Clients: %d\n", pid, req->num_clients);

                    for (int i = 0; i < req->num_clients; i++) {
                        DEBUG_PRINT(LOG_HEADER_PID "CLIENT: %d\n", pid, req->clients[i]);
                        // Send to client
                        char* client_fifo_name = isnprintf(CLIENT_FIFO "%d", req->clients[i]);
                        char* client_fifo_path = join_paths(2, "build/", client_fifo_name);
                        int client_fifo_fd = SAFE_OPEN(client_fifo_path, O_WRONLY, 0600);

                        StatusResponseDatagram client_res = create_status_response_datagram((char*)res, strlen(res));
                        DEBUG_PRINT(LOG_HEADER_PID "RES:\n------------n%s\n------------n", pid, res);
                        SAFE_WRITE(client_fifo_fd, client_res, sizeof(STATUS_RESPONSE_DATAGRAM) + strlen(res) * sizeof(char));
                        close(client_fifo_fd);
                    }

                    {
                        WorkerCompletionResponseDatagram res = create_worker_completion_response_datagram();
                        res->header.task_id = req->header.task_id;
                        res->worker_id = worker_id;

                        WRITE_PROCESS_MARK(operator_pd, WORKER_PROCESS_MARK);
                        SAFE_WRITE(operator_pd, res, sizeof(WORKER_COMPLETION_RESPONSE_DATAGRAM));
                    }
                    break;
                }
                case WORKER_DATAGRAM_MODE_EXECUTE_REQUEST: {
                    WorkerExecuteRequestDatagram req = read_partial_worker_execute_request_datagram(pfd[READ], dh);
                    MAIN_LOG(LOG_HEADER_PID "Received execute request.\n", pid);
                    DEBUG_PRINT(
                        LOG_HEADER_PID "Mode: %d, Type: %d, Id: %d, Data: '%s'\n", 
                        pid,
                        req->header.mode, 
                        req->header.type, 
                        req->header.task_id, 
                        (char*)(req->data)
                    );

                    // Execute Task
                    // char* task_name = isnprintf(TASK "%d", req->header.task_id);
                    // char* task_path = join_paths(2, output_dir, task_name);
                    // int task_fd = SAFE_OPEN(task_path, O_WRONLY | O_CREAT, 0644);
                    // dup2(task_fd, STDOUT_FILENO);

                    char* task_name = isnprintf(TASK "%d", req->header.task_id);
                    char* task_path = join_paths(2, output_dir, task_name);

                    MAIN_LOG(LOG_HEADER_PID "Executing task #%d.\n", pid, req->header.task_id);
                    run_task(req->data, task_path);

                    free(task_path);
                    free(task_name);

                    WorkerCompletionResponseDatagram res = create_worker_completion_response_datagram();
                    res->header.task_id = req->header.task_id;
                    res->worker_id = worker_id;

                    WRITE_PROCESS_MARK(operator_pd, WORKER_PROCESS_MARK);
                    SAFE_WRITE(operator_pd, res, sizeof(WORKER_COMPLETION_RESPONSE_DATAGRAM));
                    break;
                }
                case WORKER_DATAGRAM_MODE_SHUTDOWN_REQUEST: {
                    WorkerShutdownRequestDatagram req = read_partial_worker_shutdown_request_datagram(pfd[READ], dh);
                    UNUSED(req);

                    MAIN_LOG(LOG_HEADER_PID "Received shutdown request.\n", pid);
                    shutdown_requested = 1;
                    break;
                }
            }
        }

        // Shutdown worker
        MAIN_LOG(LOG_HEADER_PID "Shutting down...\n", pid);

        close(pfd[0]);
        close(pfd[1]);

        MAIN_LOG(LOG_HEADER_PID "Successfully shutdown.\n", pid);
        _exit(0);
    }

    close(pfd[0]);
    ret->pid = pid;
    ret->pipe_write = pfd[1];
    
    return ret;

    err: {
        if (_err_pid) MAIN_LOG(LOG_HEADER "[%s:%d] %s", __FILE__, __line, __error);
        else MAIN_LOG(LOG_HEADER_PID "[%s:%d] %s", _err_pid, __FILE__, __line, __error);
        return ERR;
    }
}
