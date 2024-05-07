/******************************************************************************
 *                             OPERATOR PROCESS                               *
 *                                                                            *
 *   The operator process is responsible for controlling and managing the     *
 * various worker processes used to parallelise the workload of the server.   *
 *   It implements an event system through the usage of anonymous pipes used  *
 * to establish a communication channel between the various worker processes  *
 * and the operator process.                                                  *
 *   Only one operator process is supposed to be used per server instance.    *
 ******************************************************************************/

#define _POSIX_C_SOURCE 199309L
#define _DEFAULT_SOURCE
#define _CRITICAL

#include <sys/wait.h>
#include <stdint.h>
#include "server/operator.h"
#include "server/worker.h"
#include "server/worker_datagrams.h"
#include "server/process_mark.h"

#define LOG_HEADER "[OPERATOR] "
#define SHUTDOWN_TIMEOUT 1000
#define SHUTDOWN_TIMEOUT_INTERVAL 10
#define TASK_SPECULATE_TIME 500

/**
 * @brief Represents the current status of a Worker.
 */
typedef enum {
    WORKER_STATUS_UNKNOWN,
    WORKER_STATUS_IDLE,
    WORKER_STATUS_BUSY
} OperatorStatus;

/**
 * @brief Represents the internal state of a Worker for the Operator.
 */
typedef struct operator_worker_entry {
    Worker worker;
    OperatorStatus status;
} OPERATOR_WORKER_ENTRY, *OperatorWorkerEntry;

#pragma region ============== SIGNAL HANDLING ==============
// Yes, this uses a global variable, but the alternative is having the workers be orphaned after the operator dies.
GArray* _children;
static void operator_signal_sigsegv(int signum) {
    if (signum != SIGSEGV) return;

    MAIN_LOG(LOG_HEADER "Segmentation fault. Terminating workers.\n");

    for (guint i = 0; i < _children->len; ++i) {
        kill(g_array_index(_children, OperatorWorkerEntry, i)->worker->pid, SIGKILL);
    }

    _exit(EXIT_FAILURE);
}
#pragma endregion

#pragma region ============== WORKER ARRAY ==============
typedef GArray* WorkerArray;

OperatorWorkerEntry create_operator_worker_entry(Worker worker) {
    #define ERR NULL

    OperatorWorkerEntry we = SAFE_ALLOC(OperatorWorkerEntry, sizeof(OPERATOR_WORKER_ENTRY));
    we->worker = worker;
    we->status = WORKER_STATUS_IDLE;

    return we;

    #undef ERR
}

static inline WorkerArray create_workers_array() { // len == num_parallel_tasks + 1 (1 for status)
    return g_array_new(FALSE, FALSE, sizeof(OperatorWorkerEntry));
}

OperatorWorkerEntry get_idle_worker(WorkerArray workers) {
    for (guint i = 1; i < workers->len; i++) {
        OperatorWorkerEntry entry = g_array_index(workers, OperatorWorkerEntry, i);
        if (entry->status == WORKER_STATUS_IDLE) return entry;
    }

    return NULL;
}

static inline OperatorWorkerEntry get_worker_by_id(WorkerArray workers, int worker_id) {
    return g_array_index(workers, OperatorWorkerEntry, worker_id);
}
#pragma endregion

#pragma region ============== WORKER REQUEST QUEUE ==============

typedef struct operator_task {
    int id_task;
    short int speculate_time;
    WorkerDatagram datagram; // Any Worker Datagram
    int datagram_size;
    struct timeval* start;
} OPERATOR_TASK, *OperatorTask;

typedef GQueue* RequestQueue;

#pragma region ======= FUNCTION PREDICATES =======
static inline gint request_queue_compare_fifo(gconstpointer a, gconstpointer b, gpointer user_data) {
    UNUSED(user_data);
    return ((OperatorTask)a)->start - ((OperatorTask)b)->start;
}

static inline gint request_queue_compare_sjb(gconstpointer a, gconstpointer b, gpointer user_data) {
    UNUSED(user_data);

    OperatorTask task_a = (OperatorTask)a;
    OperatorTask task_b = (OperatorTask)b;

    if(task_a->speculate_time < task_b->speculate_time) return -1;
    if(task_a->speculate_time > task_b->speculate_time) return  1;

    return 0;
}

static inline gint request_queue_compare_ljb(gconstpointer a, gconstpointer b, gpointer user_data) {
    UNUSED(user_data);

    OperatorTask task_a = (OperatorTask)a;
    OperatorTask task_b = (OperatorTask)b;

    if(task_a->speculate_time < task_b->speculate_time) return  1;
    if(task_a->speculate_time > task_b->speculate_time) return -1;

    return 0;
}

static inline gint request_queue_compare_certain(gconstpointer a, gconstpointer b, gpointer user_data) {
    UNUSED(user_data);

    OperatorTask task_a = (OperatorTask)a;
    OperatorTask task_b = (OperatorTask)b;

    if(abs(TASK_SPECULATE_TIME - task_a->speculate_time) < abs(TASK_SPECULATE_TIME - task_b->speculate_time)) return  -1;
    if(abs(TASK_SPECULATE_TIME - task_a->speculate_time) > abs(TASK_SPECULATE_TIME - task_b->speculate_time)) return   1;

    return 0;
}

static inline gint find_queue_task_by_id(gconstpointer src, gconstpointer ctrl) {
    return ((OperatorTask)src)->id_task == *((int*)ctrl);
}
#pragma endregion

static inline RequestQueue create_request_queue() {
    return g_queue_new();
}

OperatorTask create_task(int id_task, int speculate_time, WorkerDatagram datagram, int datagram_size) {
    OperatorTask execute_task = malloc(sizeof(OPERATOR_TASK));
    execute_task->id_task = id_task;
    execute_task->speculate_time = speculate_time;
    execute_task->datagram = datagram;
    execute_task->datagram_size = datagram_size;
    execute_task->start = malloc(sizeof(struct timeval));
    gettimeofday(execute_task->start, NULL);    // TODO: Check for error

    return execute_task;
}

static inline OperatorTask get_next_task(RequestQueue request_waiting_queue) {
    return (OperatorTask)g_queue_pop_head(request_waiting_queue);
}

static inline void add_task_to_backlog(RequestQueue request_waiting_queue, GCompareDataFunc comparator, OperatorTask task) {
    g_queue_insert_sorted(request_waiting_queue, task, comparator, NULL);
}

OperatorTask prepare_task_from_queue(RequestQueue request_waiting_queue, RequestQueue active_request_queue) {
    OperatorTask next_task = get_next_task(request_waiting_queue);
    g_queue_push_tail(active_request_queue, next_task);
    
    return next_task;
}

void complete_task_from_queue(RequestQueue active_request_queue, int task_id) {
    GList* task = g_queue_find_custom(active_request_queue, &task_id, find_queue_task_by_id);

    if (task != NULL) {
        int ind = g_queue_index(active_request_queue, task->data);
        OperatorTask task = g_queue_peek_nth(active_request_queue, ind);
        struct timeval end; 
        gettimeofday(&end, NULL);
        time_t time_took = (end.tv_sec*1000 + end.tv_usec/1000) - (task->start->tv_sec*1000 + task->start->tv_usec/1000);
        DEBUG_PRINT(LOG_HEADER "CTFQ Index: %d\n", ind);
        DEBUG_PRINT(LOG_HEADER "Time elapsed: %ld\n", time_took);

        g_queue_pop_nth(active_request_queue, ind);
    }
}

void execute_task(OperatorWorkerEntry worker, OperatorTask task) {
    #define ERR
    INIT_CRITICAL_MARK
    SET_CRITICAL_MARK(1);

    MAIN_LOG(LOG_HEADER "WP=%d, TD=%p, TDS=%d\n", worker->worker->pipe_write, task->datagram, task->datagram_size);

    SAFE_WRITE(worker->worker->pipe_write, task->datagram, task->datagram_size);
    #undef ERR
}
#pragma endregion

void printer(RequestQueue queue) {
    for(guint i = 0 ; i < queue->length ; i++) {
        OperatorTask task = g_queue_peek_nth(queue, i);
        DEBUG_PRINT("[Task %d] Time: %d\n", i, task->speculate_time);
    }
}

OPERATOR start_operator(int num_parallel_tasks, char* output_dir, char* history_file_path, char* escalation_policy) {
    #define ERR (OPERATOR){ 0 }

    int pd[2];
    pipe(pd); 

    void* escalation_policy_comparator;
    if(!strcmp(escalation_policy, "fifo")) escalation_policy_comparator = request_queue_compare_fifo;
    else if(!strcmp(escalation_policy, "sjb")) escalation_policy_comparator = request_queue_compare_sjb;
    else if(!strcmp(escalation_policy, "ljb")) escalation_policy_comparator = request_queue_compare_ljb;
    else escalation_policy_comparator = request_queue_compare_certain;

    INIT_CRITICAL_MARK
    #define CRITICAL_START SET_CRITICAL_MARK(1);
    #define CRITICAL_END SET_CRITICAL_MARK(0);

    CRITICAL_START
        // AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA↓↓↓↓↓↓↓↓AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAFUCK
        int write_to_history_fd = SAFE_OPEN(history_file_path, O_APPEND | O_WRONLY | O_CREAT, 0644);
        int read_from_history_fd = SAFE_OPEN(history_file_path, O_RDONLY, 0644);
    CRITICAL_END

    MAIN_LOG(LOG_HEADER "Starting operator.\n");

    pid_t pid = fork();
    if(pid == 0) {
        MAIN_LOG(LOG_HEADER "Operator started.\n");
        // close(pd[1]);
        
        #pragma region ======= WORKER INITIALIZATION =======
        MAIN_LOG(LOG_HEADER "Stating %d Worker Processes.\n", num_parallel_tasks + 1);
        
        WorkerArray worker_array = create_workers_array();
        int workers_busy = 0;

        for(int i = 0 ; i < num_parallel_tasks + 1 ; i++) {
            Worker worker = start_worker(pd[1], i, output_dir, history_file_path);

            OperatorWorkerEntry entry = create_operator_worker_entry(worker);
            g_array_insert_val(worker_array, i, entry);

            MAIN_LOG(LOG_HEADER "Started worker #%d with PID %d.\n", i, worker->pid);
        }

        // Set global variable for SIGSEGV handling.
        _children = worker_array;
        #pragma endregion

        #pragma region ======= WORKER REQUEST QUEUE INITIALIZATION =======
        RequestQueue active_request_queue = create_request_queue();
        RequestQueue request_waiting_queue = create_request_queue();
        RequestQueue status_request_queue = create_request_queue();
        #pragma endregion


        // Handle Segmentation Faults "gracefully"
        signal(SIGSEGV, operator_signal_sigsegv);
        volatile sig_atomic_t shutdown_requested = 0;

        // Read data from both the main server and worker
        while(!shutdown_requested) {
            DEBUG_PRINT(LOG_HEADER "New cycle.\n");

            // Read and discriminate process mark.
            char* mark = read_process_mark(pd[0]);
            if (shutdown_requested) break;

            DEBUG_PRINT(LOG_HEADER "Mark: %d %d\n", mark[0], mark[1]);

            switch (PROCESS_MARK_SOLVER(mark)) {
                case 0: {
                    // TODO: Possibly simply ignore the request?
                    shutdown_requested = 1;
                    break;
                }
                case MAIN_SERVER_PROCESS_MARK_DISCRIMINATOR: {
                    MAIN_LOG(LOG_HEADER "Received message from Main Server Process.\n");
                    CRITICAL_START
                        DATAGRAM_HEADER header = read_datagram_header(pd[0]);
                    CRITICAL_END

                    if (header.version == 0) {
                        // Read fuck all. Ignore it.
                        continue;
                    } else if (header.version != DATAGRAM_VERSION) {
                        // Datagram not supported.
                        MAIN_LOG(LOG_HEADER "Received unsupported datagram with version %d:\n", header.version);

                        // TODO: Read until next Process Mark.
                        drain_fifo(pd[0]);
                        continue;
                    }

                    switch (header.mode) {
                        case DATAGRAM_MODE_EXECUTE_REQUEST: {
                            ExecuteRequestDatagram request = read_partial_execute_request_datagram(pd[0], header);

                            int id = 0;
                            SAFE_READ(pd[0], &id, sizeof(int));

                            char* req_str = execute_request_datagram_to_string(request, 1, 1);
                            MAIN_LOG(LOG_HEADER "Received execute request: %s\n", req_str);
                            MAIN_LOG(LOG_HEADER "Task id: %d\n", id);
                            free(req_str);
                            
                            WorkerExecuteRequestDatagram dg = create_worker_execute_request_datagram();
                            dg->header.task_id = id;
                            memcpy(dg->data, request->data, EXECUTE_REQUEST_DATAGRAM_PAYLOAD_LEN);

                            OperatorTask task = create_task(
                                id, 
                                request->time, 
                                (WorkerDatagram)dg, 
                                sizeof(WORKER_EXECUTE_REQUEST_DATAGRAM)
                            );

                            add_task_to_backlog(
                                request_waiting_queue, 
                                escalation_policy_comparator,
                                task
                            );
                            break;
                        }
                        case DATAGRAM_MODE_STATUS_REQUEST: {
                            StatusRequestDatagram request = read_partial_status_request_datagram(pd[0], header);

                            char* req_str = status_request_datagram_to_string(request, 1);
                            MAIN_LOG(LOG_HEADER "Received status request: %s\n", req_str);
                            free(req_str);

                            // FUCK OFF GCC, I'M NOT FUCKING TRYING TO CAST A VOID* TO INT, YOU USELESS FUCK
                            // pid_t* req_pid = (pid_t*)calloc(sizeof(pid_t), 1);
                            // *req_pid = request->header.pid;

                            OperatorTask task = create_task(
                                0,
                                0,
                                // (void*)(&req_pid),
                                (void*)(request->header.pid),
                                sizeof(WORKER_STATUS_REQUEST_DATAGRAM)
                            );

                            add_task_to_backlog(
                                status_request_queue,
                                request_queue_compare_fifo,
                                task
                            );
                            break;
                        }
                        case DATAGRAM_MODE_CLOSE_REQUEST: {
                            MAIN_LOG(LOG_HEADER "Received shutdown request.\n");
                            shutdown_requested = 1;
                            break;
                        }
                        default: {
                            // Tf is this datagram? Idk and idc.
                            // Do nothing.
                        }
                    }
                    break;
                }
                case WORKER_PROCESS_MARK_DISCRIMINATOR: {
                    MAIN_LOG(LOG_HEADER "Received message from Worker Process.\n");

                    CRITICAL_START
                        WORKER_DATAGRAM_HEADER header = read_worker_datagram_header(pd[0]);
                    CRITICAL_END

                    if (header.mode == WORKER_DATAGRAM_MODE_NONE) {
                        // Read fuck all. Ignore it.
                        continue;
                    }

                    switch (header.mode) {
                        case WORKER_DATAGRAM_MODE_COMPLETION_RESPONSE: {
                            WorkerCompletionResponseDatagram res = read_partial_worker_completion_response_datagram(
                                pd[0], 
                                header
                            );

                            MAIN_LOG(LOG_HEADER "Received Completion Response from Worker #%d.\n", res->worker_id);

                            if(res->worker_id != 0) {
                                DEBUG_PRINT(LOG_HEADER "0a\n");
                                OperatorWorkerEntry entry = get_worker_by_id(worker_array, res->worker_id);
                                GList* task = g_queue_find_custom(active_request_queue, &(res->header.task_id), find_queue_task_by_id);
                                if (task != NULL) {
                                    // Get task
                                    int ind = g_queue_index(active_request_queue, task->data);
                                    OperatorTask task = g_queue_peek_nth(active_request_queue, ind);
                                    
                                    // Get time of execution
                                    struct timeval end; 
                                    gettimeofday(&end, NULL);
                                    time_t time_took = (end.tv_sec*1000 + end.tv_usec/1000) - (task->start->tv_sec*1000 + task->start->tv_usec/1000);

                                    // Write to history
                                    WorkerExecuteRequestDatagram execute = (WorkerExecuteRequestDatagram)task->datagram;
                                    char* res = isnprintf("%d %s %dms\n", task->id_task, execute->data, time_took);

                                    CRITICAL_START
                                        SAFE_WRITE(write_to_history_fd, res, strlen(res));
                                    CRITICAL_END
                                    free(res);

                                    DEBUG_PRINT(LOG_HEADER "CTFQ Index: %d\n", ind);
                                    DEBUG_PRINT(LOG_HEADER "Time elapsed: %ld\n", time_took);

                                    g_queue_pop_nth(active_request_queue, ind);
                                }

                                // Reset worker
                                entry->status = WORKER_STATUS_IDLE;
                                workers_busy--;

                                MAIN_LOG(LOG_HEADER "Worker #%d (@%d) finished.\n", res->worker_id, entry->worker->pid);
                            }
                            break;
                        }
                        default: {
                            // We should never recieved any requests from the workers.
                            // If we recieve one, we should ignore them.
                            drain_fifo(pd[0]);
                            continue;
                        }
                    }
                    break;
                }
            }

            // Take cycle opportunity to attempt to execute queued requests.
            MAIN_LOG(LOG_HEADER "Attempting to dispatch queued tasks.\n");

            DEBUG_PRINT(
                LOG_HEADER "Tasks in backlog: %d | Tasks in execution: %d\n", 
                request_waiting_queue->length, 
                active_request_queue->length
            );

            DEBUG_PRINT(LOG_HEADER "Workers available: %d/%d\n", num_parallel_tasks - workers_busy, num_parallel_tasks);
            if (request_waiting_queue->length > 0) {
                // TODO: Dispatch status tasks

                if (workers_busy == num_parallel_tasks) {
                    MAIN_LOG(LOG_HEADER "Unable to dispatch tasks: All workers are busy.\n");
                    continue;
                }

                OperatorWorkerEntry entry = get_idle_worker(worker_array);
                OperatorTask task = prepare_task_from_queue(request_waiting_queue, active_request_queue);

                execute_task(entry, task);
                entry->status = WORKER_STATUS_BUSY;
                workers_busy++;
            } else {
                DEBUG_PRINT(LOG_HEADER "No execute tasks queued.\n");
            }

            if(status_request_queue->length > 0) {
                OperatorWorkerEntry entry = g_array_index(worker_array, OperatorWorkerEntry, 0);
                if(entry->status == WORKER_STATUS_IDLE) {
                    // OperatorTask task = get_next_task(status_request_queue);
                    OperatorTask task = g_queue_peek_head(status_request_queue);  
                    int num_clients = status_request_queue->length;
                    int* clients = SAFE_ALLOC(int*, sizeof(int) * num_clients);

                    UNUSED(task);
                    
                    for(int i = 0 ; i < num_clients ; i++) {
                        OperatorTask task = g_queue_peek_nth(status_request_queue, i);
                        clients[i] = (pid_t)(task->datagram);
                    }

                    int num_tasks = request_waiting_queue->length + active_request_queue->length;
                    WorkerStatusPayload* tasks = SAFE_ALLOC(WorkerStatusPayload*, sizeof(WorkerStatusPayload) * num_tasks);
                    for(int i = 0 ; request_waiting_queue->length ; i++) {
                        OperatorTask task = g_queue_peek_nth(request_waiting_queue, i);
                        tasks[i]->task_id = task->id_task;
                        WorkerExecuteRequestDatagram execute = (WorkerExecuteRequestDatagram)task->datagram;
                        memcpy(tasks[i]->data, execute->data, sizeof(EXECUTE_REQUEST_DATAGRAM_PAYLOAD_LEN + 1));
                    }
                    for(int i = request_waiting_queue->length, j = 0 ; i < num_tasks ; i++, j++) {
                        OperatorTask task = g_queue_peek_nth(active_request_queue, j);
                        tasks[i]->task_id = task->id_task;
                        WorkerExecuteRequestDatagram execute = (WorkerExecuteRequestDatagram)task->datagram;
                        memcpy(tasks[i]->data, execute->data, sizeof(EXECUTE_REQUEST_DATAGRAM_PAYLOAD_LEN + 1));
                    }

                    WorkerStatusRequestDatagram req = create_worker_status_request_datagram(
                        num_clients, 
                        clients, 
                        request_waiting_queue->length,
                        num_tasks,
                        tasks
                    );
                    // printf(LOG_HEADER "%d -- %d -- %d -- %d -- %d\n", num_clients, status_request_queue->length, num_tasks, request_waiting_queue->length, active_request_queue->length);

                    MAIN_LOG(LOG_HEADER "========<<<< REQ >>>>======\n");
                    MAIN_LOG(LOG_HEADER "Num Clients: %d\n", num_clients);
                    for(int i = 0 ; i < num_clients ; i++) {
                        MAIN_LOG(LOG_HEADER "Client %d: %d\n", i, clients[i]);
                    }
                    MAIN_LOG(LOG_HEADER "Num Tasks: %d\n", num_tasks);
                    for(int i = 0 ; i < num_tasks ; i++) {
                        MAIN_LOG(LOG_HEADER "Task %d: %d %s\n", i, tasks[i]->task_id, tasks[i]->data);
                    }
                    MAIN_LOG(LOG_HEADER "========<<<< --- >>>>======\n");
                    fflush(stdout);


                    // Write Worker Status Request Datagram
                    {
                        uint8_t* dg = SAFE_ALLOC(
                            uint8_t*, 
                            sizeof(WORKER_DATAGRAM_HEADER) 
                            + sizeof(int) + sizeof(int) * req->num_clients
                            + sizeof(int) + sizeof(WorkerStatusPayload) * req->num_tasks
                        );

                        // Copy header
                        memcpy(dg, &(req->header), sizeof(WORKER_DATAGRAM_HEADER));

                        // Copy clients
                        memcpy((((void*)(dg)) + sizeof(WORKER_DATAGRAM_HEADER)), &(req->num_clients), sizeof(int));
                        memcpy((((void*)(dg)) + sizeof(WORKER_DATAGRAM_HEADER) + sizeof(int)), req->clients, req->num_clients * sizeof(int));

                        // Copy tasks
                        memcpy(
                            (((void*)(dg)) + sizeof(WORKER_DATAGRAM_HEADER) + sizeof(int) + req->num_clients * sizeof(int)), 
                            &(req->num_tasks_queued), 
                            sizeof(int)
                        );
                        memcpy(
                            (((void*)(dg)) + sizeof(WORKER_DATAGRAM_HEADER) + sizeof(int) + req->num_clients * sizeof(int) + sizeof(int)), 
                            &(req->num_tasks), 
                            sizeof(int)
                        );
                        memcpy(
                            (((void*)(dg)) + sizeof(WORKER_DATAGRAM_HEADER) + sizeof(int) + req->num_clients * sizeof(int) + sizeof(int) + sizeof(int)), 
                            req->tasks, 
                            req->num_tasks * sizeof(WorkerStatusPayload)
                        );

                        SAFE_WRITE(
                            entry->worker->pipe_write, 
                            dg, 
                            sizeof(WORKER_DATAGRAM_HEADER) 
                                + sizeof(int) 
                                + req->num_clients * sizeof(int) 
                                + sizeof(int) 
                                + sizeof(int) 
                                + req->num_clients * sizeof(WorkerStatusPayload)
                        );
                    }

                    g_queue_pop_head(status_request_queue);

                    // TODO: Mandar para o caralho
                }
            } else {
                DEBUG_PRINT(LOG_HEADER "No status tasks queued.\n");
            }
        }

        // Handle graceful shutdown.
        #pragma region ======= GRACEFUL SHUTDOWN =======
        MAIN_LOG(LOG_HEADER "Shutting down operator...\n");

        // TODO: Suicide Workers
        for (guint i = 0; i < worker_array->len; i++) {
            OperatorWorkerEntry entry = g_array_index(worker_array, OperatorWorkerEntry, i);
            MAIN_LOG(
                LOG_HEADER "Shutting down worker #%d @ %d\n", 
                i, 
                entry->worker->pid
            );

            WorkerShutdownRequestDatagram request = create_worker_shutdown_request_datagram();
            SAFE_WRITE(entry->worker->pipe_write, request, sizeof(WORKER_SHUTDOWN_REQUEST_DATAGRAM));

            int status;
            int elapsed = 0;
            int shutdown = 0;
            int wret = 0;
            while ((elapsed += SHUTDOWN_TIMEOUT_INTERVAL) < SHUTDOWN_TIMEOUT / (int)(worker_array->len)) {
                wret = waitpid(entry->worker->pid, &status, WNOHANG);
                if (wret == -1) {
                    _exit(1);
                }

                usleep(SHUTDOWN_TIMEOUT_INTERVAL * 1000);
            
                if (wret && WIFEXITED(status)) {
                    DEBUG_PRINT(
                        LOG_HEADER "Worker #%d @ %d exited with code %d\n", 
                        i, 
                        entry->worker->pid, 
                        WEXITSTATUS(status)
                    );
                    shutdown = 1;
                    break;
                }
            }

            // Worker refuses to shutdown, suicide it.
            if (!shutdown) {
                MAIN_LOG(LOG_HEADER "Worker has not closed within the acceptable timeout. Forcefully killing process.\n");
                kill(entry->worker->pid, SIGKILL);
            }

            MAIN_LOG(
                LOG_HEADER "Successfully closed worker #%d @ %d\n", 
                i, 
                entry->worker->pid
            );
        }
        
        close(read_from_history_fd);
        close(write_to_history_fd);
        close(pd[0]);

        MAIN_LOG(LOG_HEADER "Successfully closed operator.\n");
        _exit(0);
        #pragma endregion
    } else {
        DEBUG_PRINT(LOG_HEADER "Server continue.\n");

        OPERATOR op = (OPERATOR){
            .pid = pid,
            .pd_write = pd[1]
        };

        return op;
    }

    // If flow reaches here, you fucked up.
    DEBUG_PRINT(LOG_HEADER "WTF.\n");
    #undef ERR
}