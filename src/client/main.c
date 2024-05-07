/******************************************************************************
 *                              CLIENT PROCESS                                *
 *                                                                            *
 *   The client process is an interface that allows a user to send requests   *
 * to a listening server, recieve its response and display it to the user.    *
 *   In case no active server exists, it will wait until a server opens up.   *
 *   Multiple clients are supposed to be able to connect to the same server   *
 * instance without any significant lag or errors.                            *
 ******************************************************************************/

#include <common/io/io.h>
#include <common/io/fifo.h>
#include <common/datagram/datagram.h>
#include <common/datagram/execute.h>
#include <common/datagram/status.h>
#include "common/util/string.h"
#include <stdio.h>
#include <stdlib.h>

int main(int argc, char const *argv[]) {
    #define ERR 1
    printf("Hello world from client!\n\n");

    if(argc == 2) {
        char* mode = (char*) argv[1];

        if(!strcmp("status", mode)) {

            char* client_fifo_name = isnprintf(CLIENT_FIFO "%d", getpid());
            char* client_fifo_path = join_paths(2, "build/", client_fifo_name);
            SAFE_FIFO_SETUP(client_fifo_path, 0600);

            char* server_fifo_path = join_paths(2, "build/", SERVER_FIFO);
            int server_fifo_fd = SAFE_OPEN(server_fifo_path, O_WRONLY, 0600);

            StatusRequestDatagram request = create_status_request_datagram();
            
            #ifdef DEBUG
            char* req_str = status_request_datagram_to_string(request, 1);
            DEBUG_PRINT("[DEBUG] Sending request with %ld bytes:\n%s\n", sizeof(STATUS_REQUEST_DATAGRAM), req_str);
            free(req_str);
            #endif
            
            SAFE_WRITE(server_fifo_fd, request, sizeof(STATUS_REQUEST_DATAGRAM));

            int client_fifo_fd = SAFE_OPEN(client_fifo_path, O_RDONLY, 0600);
            StatusResponseDatagram response = read_status_response_datagram(client_fifo_fd);

            DEBUG_PRINT("[DEBUG] Printing payload with %d bytes.\n", response->payload_len);
            printf("%s", response->payload);

            close(client_fifo_fd);
            unlink(client_fifo_path);
            free(client_fifo_path);
            free(client_fifo_name);
            close(server_fifo_fd);
            free(server_fifo_path);
            
        } else if(!strcmp("close", mode)) {
            
            char* client_fifo_name = isnprintf(CLIENT_FIFO "%d", getpid());
            char* client_fifo_path = join_paths(2, "build/", client_fifo_name);
            SAFE_FIFO_SETUP(client_fifo_path, 0600);

            char* server_fifo_path = join_paths(2, "build/", SERVER_FIFO);
            int server_fifo_fd = SAFE_OPEN(server_fifo_path, O_WRONLY, 0600);

            DATAGRAM_HEADER request = create_datagram_header();
            request.mode = DATAGRAM_MODE_CLOSE_REQUEST;
            SAFE_WRITE(server_fifo_fd, &request, sizeof(DATAGRAM_HEADER));

            int client_fifo_fd = SAFE_OPEN(client_fifo_path, O_RDONLY, 0600);
            DATAGRAM_HEADER response = read_datagram_header(client_fifo_fd);

            if(response.mode == DATAGRAM_MODE_CLOSE_RESPONSE) {
                printf("Server shutdown request sucessfully sent.\n");
            } else {
                printf("Server shutdown request was not received.\n");
            }

            close(client_fifo_fd);
            unlink(client_fifo_path);
            free(client_fifo_path);
            free(client_fifo_name);
            close(server_fifo_fd);
            free(server_fifo_path);

        } else {
            printf("Invalid mode. Try again later.\n");
            exit(EXIT_FAILURE);
        }
    } else if(argc == 5) {
        char* mode = (char*) argv[1];

        if(!strcmp("execute", mode)) {
            
            short int time = (atoi(argv[2]));
            char* type = (char*) argv[3];
            char* data = (char*) argv[4];
            if(strlen(data) > 300) {
                perror("ERROR! Arguments passed to execute mode exceeds 300 bytes.\n");
                exit(EXIT_FAILURE);
            }

            char* client_fifo_name = isnprintf(CLIENT_FIFO "%d", getpid());
            char* client_fifo_path = join_paths(2, "build/", client_fifo_name);
            SAFE_FIFO_SETUP(client_fifo_path, 0600);

            char* server_fifo_path = join_paths(2, "build/", SERVER_FIFO);
            int server_fifo_fd = SAFE_OPEN(server_fifo_path, O_WRONLY, 0600);

            ExecuteRequestDatagram request = create_execute_request_datagram();
            request->header.type = (!strcmp("-u", type)) ? DATAGRAM_TYPE_UNIQUE : DATAGRAM_TYPE_PIPELINE;
            request->time = time;
            memcpy(request->data, data, strlen(data));

            #ifdef DEBUG
            char* req_str = execute_request_datagram_to_string(request, 1, 0);
            DEBUG_PRINT("[DEBUG] Sending request with %ld bytes:\n%s\n", sizeof(EXECUTE_REQUEST_DATAGRAM), req_str);
            free(req_str);
            #endif

            SAFE_WRITE(server_fifo_fd, request, sizeof(EXECUTE_REQUEST_DATAGRAM));

            int client_fifo_fd = SAFE_OPEN(client_fifo_path, O_RDONLY, 0600);
            ExecuteResponseDatagram response = read_execute_response_datagram(client_fifo_fd);

            printf("Task queued with identifier %d.\n", response->taskid);

            close(client_fifo_fd);
            unlink(client_fifo_path);
            free(client_fifo_path);
            free(client_fifo_name);
            close(server_fifo_fd);
            free(server_fifo_path);

        } else {
            printf("Invalid mode. Try again later.\n");
        }
    } else {
        printf("Insufficient arguments.\n"
            "Please provide the following parameters:\n"
            "(execution_mode) [task_time] [task_type] [\"task\"]\n");
        exit(EXIT_FAILURE);
    }

    #undef ERR
    return 0;
}

