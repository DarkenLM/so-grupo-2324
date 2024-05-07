/******************************************************************************
 *                              SERVER PROCESS                                *
 *                                                                            *
 *   The server process is the main entry point for any connecting client.    *
 *   It is responsible for listening for incomming requests, process them,    *
 * pass the to the operator process, and respond to the connecting clients.   *
 *   At startup, it should create all required files for execution and spawn  *
 * all processes that are required.                                           *
 *   Only one server process is supposed to exist per instance.               *
 ******************************************************************************/

#define _POSIX_C_SOURCE 199309L
#define _DEFAULT_SOURCE
#define _CRITICAL

#include <fcntl.h>
#include <unistd.h>
#include <sys/types.h>
#include <signal.h>
#include <sys/wait.h>
#include <time.h>

#include "common/io/io.h"
#include "common/io/fifo.h"
#include "common/datagram/datagram.h"
#include "common/datagram/execute.h"
#include "common/datagram/status.h"
#include "common/util/string.h"
#include "server/operator.h"
#include "server/process_mark.h"

#define LOG_HEADER "[MAIN] "
#define SHUTDOWN_TIMEOUT 2000
#define SHUTDOWN_TIMEOUT_INTERVAL 10

volatile sig_atomic_t shutdown_requested = 0;

void signal_handler_shutdown(int sig) {
    if (sig == SIGINT || sig == SIGTERM) {
        shutdown_requested = 1;
    }
}

int main(int argc, char const *argv[]) {
    #define ERR 1

    #pragma region ======= SIGNALS / CRITICAL MARK =======
    struct sigaction critical_sa_apply;
    struct sigaction critical_sa_restore;

    memset(&critical_sa_apply, 0, sizeof(critical_sa_apply));
    sigemptyset(&critical_sa_apply.sa_mask);
    critical_sa_apply.sa_handler = signal_handler_shutdown;
    critical_sa_apply.sa_flags = 0;

    INIT_CRITICAL_MARK

    #define CRITICAL_START SET_CRITICAL_MARK(0);\
        sigaction(SIGINT, &critical_sa_apply, &critical_sa_restore);\
        sigaction(SIGTERM, &critical_sa_apply, &critical_sa_restore);
    #define CRITICAL_END SET_CRITICAL_MARK(1);\
        sigaction(SIGINT, &critical_sa_restore, NULL);\
        sigaction(SIGTERM, &critical_sa_restore, NULL);
    
    #pragma endregion

    pid_t pid = getpid();

    if(argc < 3) {
        printf("Usage: $ server <output_folder> <number_of_parallel_tasks> [escalation_policy]\n");
        exit(EXIT_FAILURE);
    } else {
        char* output_folder = (char*) argv[1];
        CREATE_DIR(output_folder, 0700);

        CRITICAL_START
            char* id_file_path = join_paths(2, output_folder, "id");
            int id_fd = SAFE_OPEN(id_file_path, O_RDWR | O_CREAT, 0600);

            char* history_file_path = join_paths(2, output_folder, "history");

            char* server_fifo_path = join_paths(2, "build/", SERVER_FIFO);
            SAFE_FIFO_SETUP(server_fifo_path, 0600);

            int id = SETUP_ID(id_fd);

            int _main_pid = getpid();

            OPERATOR operator = start_operator(atoi(argv[2]), output_folder, history_file_path, (char*) argv[3]);
            if (operator.pid == 0) {
                if (_main_pid != getpid()) {
                    _exit(0);
                }

                printf(LOG_HEADER "Unable to start operator. Shutting down.\n");
                shutdown_requested = 1;
            }
            int operator_pd = operator.pd_write;
            
            int MAIN_PID = getpid();

            // int operator_pd = NULL;
            DEBUG_PRINT(LOG_HEADER "Operator PID: %d\n", operator.pid);
            DEBUG_PRINT(LOG_HEADER "Operator Pipe: %d\n", operator_pd);
        CRITICAL_END

        while(!shutdown_requested) {
            DEBUG_PRINT(LOG_HEADER "New cycle.\n");

            // Read incoming requests.
            CRITICAL_START
                int server_fifo_fd = SAFE_OPEN(server_fifo_path, O_RDONLY, 0600);
            CRITICAL_END

            // If kill signal recieved, do not read or process requests
            if (shutdown_requested) break;

            DATAGRAM_HEADER header = read_datagram_header(server_fifo_fd);
            if (header.version == 0) {
                // Read 0 bytes, fallback to 0.
                close(server_fifo_fd);
                continue;
            } else if (header.version != DATAGRAM_VERSION) {
                // Datagram not supported.
                printf(LOG_HEADER "Recieved unsupported datagram with version %d:\n", header.version);

                drain_fifo(server_fifo_fd);
                close(server_fifo_fd);
                continue;
            }

            printf(LOG_HEADER "New request recieved.\n");

            #ifdef DEBUG
            char* dh_str = datagram_header_to_string(&header, 1);
            printf(LOG_HEADER "Request Header: %s\n", dh_str);
            free(dh_str);
            #endif

            char* client_fifo_name = isnprintf(CLIENT_FIFO "%d", header.pid);
            char* client_fifo_path = join_paths(2, "build/", client_fifo_name);
            // int client_fifo_fd = SAFE_OPEN(client_fifo_path, O_WRONLY, 0600);
            int client_fifo_fd = NULL;

            if(header.mode == DATAGRAM_MODE_EXECUTE_REQUEST) {
                DEBUG_PRINT(LOG_HEADER "Processing Execute Request.\n");
                client_fifo_fd = SAFE_OPEN(client_fifo_path, O_WRONLY, 0600);

                ExecuteRequestDatagram request_execute = read_partial_execute_request_datagram(server_fifo_fd, header);

                ExecuteResponseDatagram response = create_execute_response_datagram();
                response->taskid = ++id;
                SAFE_WRITE(client_fifo_fd, response, sizeof(EXECUTE_RESPONSE_DATAGRAM));

                WRITE_PROCESS_MARK(operator_pd, MAIN_SERVER_PROCESS_MARK);
                // char mark_buf[] = MAIN_SERVER_PROCESS_MARK;
                // printf(LOG_HEADER "MARK: %d %d\n", mark_buf[0], mark_buf[1]);
                // SAFE_WRITE(operator_pd, &mark_buf, 2 * sizeof(char));

                SAFE_WRITE(operator_pd, request_execute, sizeof(EXECUTE_REQUEST_DATAGRAM));
                SAFE_WRITE(operator_pd, &id, sizeof(int));

                printf(LOG_HEADER "Task with identifier %d queued.\n", id);

                // char* task_name = isnprintf(TASK "%d", id);
                // char* task_path = join_paths(2, output_folder, task_name);
                // int task_fd = SAFE_OPEN(task_path, O_WRONLY | O_CREAT, 0600);

                // close(task_fd);
                // free(task_path);
                // free(task_name);

                DEBUG_PRINT(LOG_HEADER "Execute Request finalized.\n");
            } else if(header.mode == DATAGRAM_MODE_STATUS_REQUEST) {
                DEBUG_PRINT(LOG_HEADER "Processing Status Request.\n");

                StatusRequestDatagram request_status = read_partial_status_request_datagram(server_fifo_fd, header);

                WRITE_PROCESS_MARK(operator_pd, MAIN_SERVER_PROCESS_MARK);
                SAFE_WRITE(operator_pd, request_status, sizeof(STATUS_REQUEST_DATAGRAM));

                printf(LOG_HEADER "Status task queued.\n");

                DEBUG_PRINT(LOG_HEADER "Status Request finalized.\n");
            } else if(header.mode == DATAGRAM_MODE_CLOSE_REQUEST) {
                DEBUG_PRINT(LOG_HEADER "Processing Close request.\n");

                client_fifo_fd = SAFE_OPEN(client_fifo_path, O_WRONLY, 0600);

                CRITICAL_START
                    DATAGRAM_HEADER response = create_datagram_header();
                    response.mode = DATAGRAM_MODE_CLOSE_RESPONSE;

                    WRITE_PROCESS_MARK(operator_pd, MAIN_SERVER_PROCESS_MARK);
                    SAFE_WRITE(client_fifo_fd, &response, sizeof(DATAGRAM_HEADER));
                CRITICAL_END

                // Request shutdown and fallthrough.
                shutdown_requested = 1;
            }

            if (client_fifo_fd != NULL) close(client_fifo_fd);
            close(server_fifo_fd);
            free(client_fifo_path);
            free(client_fifo_name);
        }

        // Handle server shutdown
        {
            // printf(LOG_HEADER "Server shutting down...\n");
            printf(LOG_HEADER "%d Server shutting down...\n", MAIN_PID);

            if (operator.pid != 0) {
                DATAGRAM_HEADER shutdown_request = create_datagram_header();
                shutdown_request.mode = DATAGRAM_MODE_CLOSE_REQUEST;
                shutdown_request.pid = pid;

                #ifdef DEBUG
                char* req_str = datagram_header_to_string(&shutdown_request, 1);
                DEBUG_PRINT(LOG_HEADER "Shutdown Request: %s\n", req_str);
                #endif
                
                // WARN: Do not send a Process Mark when shutting down the Operator process. A SIGINT will interrupt the read.
                // WRITE_PROCESS_MARK(operator_pd, MAIN_SERVER_PROCESS_MARK);
                SAFE_WRITE(operator_pd, &shutdown_request, sizeof(DATAGRAM_HEADER));

                {
                    int status;
                    int elapsed = 0;
                    int shutdown = 0;
                    int wret = 0;
                    while ((elapsed += SHUTDOWN_TIMEOUT_INTERVAL) < SHUTDOWN_TIMEOUT) {
                        wret = waitpid(operator.pid, &status, WNOHANG);
                        if (wret == -1) {
                            return EINVAL;
                        }

                        usleep(SHUTDOWN_TIMEOUT_INTERVAL * 1000);
                    
                        if (wret && WIFEXITED(status)) {
                            DEBUG_PRINT(LOG_HEADER "Operator exited with code %d\n", WEXITSTATUS(status));
                            shutdown = 1;
                            break;
                        }
                    }

                    printf(LOG_HEADER "Shutdown status: %d\n", shutdown);

                    // Operator refuses to shutdown, suicide it.
                    if (!shutdown) {
                        printf(LOG_HEADER "Operator has not closed within the acceptable timeout. Forcefully killing process.\n");
                        kill(operator.pid, SIGKILL);
                    }

                    printf(LOG_HEADER "Successfully closed operator.\n");
                }
            }

            // Save current ID
            lseek(id_fd, 0, SEEK_SET);
            SAFE_WRITE(id_fd, &id, sizeof(int));

            // Close file descriptors
            close(id_fd);

            // Delete server fifo
            unlink(server_fifo_path);

            // Free allocated strings
            free(id_file_path);
            free(history_file_path);
            free(server_fifo_path);

            printf(LOG_HEADER "Successfully closed server.\n");
            exit(EXIT_SUCCESS);
        }
    }

    #undef ERR
}
