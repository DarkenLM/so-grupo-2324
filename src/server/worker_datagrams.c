#include "server/worker_datagrams.h"
#include "common/util/alloc.h"
#include "common/io/io.h"

#pragma region ======= HEADER =======
WORKER_DATAGRAM_HEADER create_worker_datagram_header() {
    WORKER_DATAGRAM_HEADER dh = {
        .mode = WORKER_DATAGRAM_MODE_NONE,
        .type = DATAGRAM_TYPE_NONE,
        .task_id = 0
    };

    return dh;
}

WORKER_DATAGRAM_HEADER read_worker_datagram_header(int fd) {
    #define ERR ((WORKER_DATAGRAM_HEADER){ 0 })
    
    WORKER_DATAGRAM_HEADER header = { 0 };
    SAFE_READ(fd, &header, sizeof(WORKER_DATAGRAM_HEADER));

    return header;
    #undef ERR
}
#pragma endregion


#pragma region ======= STATUS REQUEST =======
WorkerStatusRequestDatagram create_worker_status_request_datagram(int num_clients, int* clients, int num_tasks_queued, int num_tasks, WorkerStatusPayload* tasks) {
    #define ERR NULL

    WorkerStatusRequestDatagram dg = SAFE_ALLOC(WorkerStatusRequestDatagram, sizeof(WORKER_STATUS_REQUEST_DATAGRAM));
    dg->header = create_worker_datagram_header();
    dg->header.mode = WORKER_DATAGRAM_MODE_STATUS_REQUEST;

    dg->num_clients = num_clients;
    dg->clients = SAFE_ALLOC(int*, sizeof(int) * num_clients);
    memcpy(dg->clients, clients, sizeof(int) * num_clients);

    dg->num_tasks_queued = num_tasks_queued;
    dg->num_tasks = num_tasks;
    dg->tasks = SAFE_ALLOC(WorkerStatusPayload*, sizeof(WorkerStatusPayload) * num_tasks);
    memcpy(dg->tasks, tasks, sizeof(WorkerStatusPayload) * num_tasks);

    return dg;

    #undef ERR
}

WorkerStatusRequestDatagram read_partial_worker_status_request_datagram(int fd, WORKER_DATAGRAM_HEADER header) {
    #define ERR NULL
    
    WorkerStatusRequestDatagram dg = SAFE_ALLOC(WorkerStatusRequestDatagram, sizeof(WORKER_STATUS_REQUEST_DATAGRAM));
    dg->header = header;

    // int offset = sizeof(WORKER_DATAGRAM_HEADER);

    // // Read clients
    // int num_clients = STRUCT_MEMBER_SIZE(WORKER_STATUS_REQUEST_DATAGRAM, num_clients);
    // SAFE_READ(fd, (((void*)dg) + offset), num_clients);
    // offset += num_clients;

    // dg->clients = SAFE_ALLOC(int*, sizeof(int) * num_clients);
    // SAFE_READ(fd, (((void*)dg) + offset), dg->clients);
    // offset += sizeof(int) * num_clients;

    // // Read tasks
    // SAFE_READ(fd, (((void*)dg) + offset), sizeof(int));
    // offset += sizeof(int);

    // int num_tasks = STRUCT_MEMBER_SIZE(WORKER_STATUS_REQUEST_DATAGRAM, num_tasks);
    // SAFE_READ(fd, (((void*)dg) + offset), num_tasks);
    // offset += num_tasks;

    // dg->tasks = SAFE_ALLOC(WorkerStatusPayload*, sizeof(WorkerStatusPayload) * num_tasks);
    // SAFE_READ(fd, (((void*)dg) + offset), dg->tasks);

    // Read clients
    SAFE_READ(fd, &(dg->num_clients), STRUCT_MEMBER_SIZE(WORKER_STATUS_REQUEST_DATAGRAM, num_clients));

    dg->clients = SAFE_ALLOC(int*, sizeof(int) * dg->num_clients);
    SAFE_READ(fd, dg->clients, sizeof(int) * dg->num_clients);

    // Read tasks
    SAFE_READ(fd, &(dg->num_tasks_queued), STRUCT_MEMBER_SIZE(WORKER_STATUS_REQUEST_DATAGRAM, num_tasks_queued));
    SAFE_READ(fd, &(dg->num_tasks), STRUCT_MEMBER_SIZE(WORKER_STATUS_REQUEST_DATAGRAM, num_tasks));

    dg->tasks = SAFE_ALLOC(WorkerStatusPayload*, sizeof(WorkerStatusPayload) * dg->num_tasks);
    SAFE_READ(fd, dg->tasks, sizeof(WorkerStatusPayload) * dg->num_tasks);

    return dg;
    #undef ERR
}



#pragma endregion


#pragma region ======= EXECUTE REQUEST =======
WorkerExecuteRequestDatagram create_worker_execute_request_datagram() {
    #define ERR NULL

    WorkerExecuteRequestDatagram dg = SAFE_ALLOC(WorkerExecuteRequestDatagram, sizeof(WORKER_EXECUTE_REQUEST_DATAGRAM));
    dg->header = create_worker_datagram_header();
    dg->header.mode = WORKER_DATAGRAM_MODE_EXECUTE_REQUEST;

    return dg;

    #undef ERR
}


WorkerExecuteRequestDatagram read_partial_worker_execute_request_datagram(int fd, WORKER_DATAGRAM_HEADER header) {
    #define ERR NULL
    
    WorkerExecuteRequestDatagram dg = SAFE_ALLOC(WorkerExecuteRequestDatagram, sizeof(WORKER_EXECUTE_REQUEST_DATAGRAM));
    dg->header = header;

    // SAFE_READ(
    //     fd, 
    //     (((void*)dg) + sizeof(WORKER_DATAGRAM_HEADER)), 
    //     sizeof(WORKER_EXECUTE_REQUEST_DATAGRAM) - sizeof(WORKER_DATAGRAM_HEADER)
    // );

    SAFE_READ(fd, dg->data, sizeof(dg->data) - 1);

    return dg;
    #undef ERR
}
#pragma endregion


#pragma region ======= SHUTDOWN REQUEST =======
WorkerShutdownRequestDatagram create_worker_shutdown_request_datagram() {
    #define ERR NULL

    WorkerShutdownRequestDatagram dg = SAFE_ALLOC(WorkerShutdownRequestDatagram, sizeof(WORKER_SHUTDOWN_REQUEST_DATAGRAM));
    dg->header = create_worker_datagram_header();
    dg->header.mode = WORKER_DATAGRAM_MODE_SHUTDOWN_REQUEST;

    return dg;

    #undef ERR
}


WorkerShutdownRequestDatagram read_partial_worker_shutdown_request_datagram(int fd, WORKER_DATAGRAM_HEADER header) {
    UNUSED(fd);

    #define ERR NULL
    
    WorkerShutdownRequestDatagram dg = SAFE_ALLOC(WorkerShutdownRequestDatagram, sizeof(WORKER_SHUTDOWN_REQUEST_DATAGRAM));
    dg->header = header;

    return dg;
    #undef ERR
}
#pragma endregion

#pragma region ======= COMPLETION RESPONSE =======
WorkerCompletionResponseDatagram create_worker_completion_response_datagram() {
    #define ERR NULL

    WorkerCompletionResponseDatagram dg = SAFE_ALLOC(WorkerCompletionResponseDatagram, sizeof(WORKER_COMPLETION_RESPONSE_DATAGRAM));
    dg->header = create_worker_datagram_header();
    dg->header.mode = WORKER_DATAGRAM_MODE_COMPLETION_RESPONSE;

    return dg;

    #undef ERR
}


WorkerCompletionResponseDatagram read_partial_worker_completion_response_datagram(int fd, WORKER_DATAGRAM_HEADER header) {
    #define ERR NULL
    
    WorkerCompletionResponseDatagram dg = SAFE_ALLOC(WorkerCompletionResponseDatagram, sizeof(WORKER_COMPLETION_RESPONSE_DATAGRAM));
    dg->header = header;

    SAFE_READ(
        fd, 
        (((void*)dg) + sizeof(WORKER_DATAGRAM_HEADER)), 
        sizeof(WORKER_COMPLETION_RESPONSE_DATAGRAM) - sizeof(WORKER_DATAGRAM_HEADER)
    );

    return dg;
    #undef ERR
}
#pragma endregion
