#ifndef SERVER_WORKER_DATAGRAMS_H
#define SERVER_WORKER_DATAGRAMS_H

#include <stdint.h>
#include "common/datagram/execute.h"
#include <glib-2.0/glib.h>

enum WorkerDatagramMode {
    WORKER_DATAGRAM_MODE_NONE,
    WORKER_DATAGRAM_MODE_STATUS_REQUEST,
    WORKER_DATAGRAM_MODE_EXECUTE_REQUEST,
    WORKER_DATAGRAM_MODE_SHUTDOWN_REQUEST,
    WORKER_DATAGRAM_MODE_COMPLETION_RESPONSE
};

typedef struct worker_datagram_header {
    uint8_t mode; // The mode for this datagram. See WorkerDatagramMode.
    uint8_t type; // The type for this datagram. See DatagramType.
    int task_id;  // The id of the current task.
} WORKER_DATAGRAM_HEADER, *WorkerDatagramHeader;

/**
 * @brief Generic datagram pointer type, compatible with every datagram.
 */
typedef struct worker_datagram {
    WORKER_DATAGRAM_HEADER header;
} *WorkerDatagram;

typedef struct worker_status_payload {
    int task_id;
    char data[EXECUTE_REQUEST_DATAGRAM_PAYLOAD_LEN + 1];
} WORKER_STATUS_PAYLOAD, *WorkerStatusPayload;

typedef struct worker_status_request_datagram {
    WORKER_DATAGRAM_HEADER header; // Header for this datagram. Mode must be set to WORKER_DATAGRAM_MODE_STATUS_REQUEST.
    int num_clients;
    int* clients;
    int num_tasks_queued;
    int num_tasks;
    WorkerStatusPayload* tasks;
} WORKER_STATUS_REQUEST_DATAGRAM, *WorkerStatusRequestDatagram;

typedef struct worker_execute_request_datagram {
    WORKER_DATAGRAM_HEADER header; // Header for this datagram. Mode must be set to WORKER_DATAGRAM_MODE_EXECUTE_REQUEST.
    char data[EXECUTE_REQUEST_DATAGRAM_PAYLOAD_LEN + 1]; // The task to be executed.
} WORKER_EXECUTE_REQUEST_DATAGRAM, *WorkerExecuteRequestDatagram;

typedef struct worker_shutdown_request_datagram {
    WORKER_DATAGRAM_HEADER header; // Header for this datagram. Mode must be set to WORKER_DATAGRAM_MODE_SHUTDOWN_REQUEST.
} WORKER_SHUTDOWN_REQUEST_DATAGRAM, *WorkerShutdownRequestDatagram;

typedef struct worker_completion_response_datagram {
    WORKER_DATAGRAM_HEADER header; // Header for this datagram. Mode must be set to WORKER_DATAGRAM_MODE_COMPLETION_RESPONSE.
    uint8_t worker_id;
} WORKER_COMPLETION_RESPONSE_DATAGRAM, *WorkerCompletionResponseDatagram;

/**
 * @brief Creates a new empty Worker Datagram Header.
 */
WORKER_DATAGRAM_HEADER create_worker_datagram_header();

/**
 * @brief Reads a Worker Datagram Header from a file descriptor, or NULL if it fails.
 * @param fd The file descriptor to read.
 */
WORKER_DATAGRAM_HEADER read_worker_datagram_header(int fd);

/**
 * @brief Creates a new empty Worker Status Request Datagram.
 */
WorkerStatusRequestDatagram create_worker_status_request_datagram(int num_clients, int* clients, int num_tasks_queued, int num_tasks, WorkerStatusPayload* tasks);

/**
 * @brief Reads a Worker Status Request Datagram from a file descriptor, or NULL if it fails. This version should be 
 * called after reading the header of a datagram, for distinguishing the datagram mode.
 * 
 * @param fd     The file descriptor to read.
 * @param header The already read header.
 */
WorkerStatusRequestDatagram read_partial_worker_status_request_datagram(int fd, WORKER_DATAGRAM_HEADER header);

/**
 * @brief Creates a new empty Worker Execute Request Datagram.
 */
WorkerExecuteRequestDatagram create_worker_execute_request_datagram();

/**
 * @brief Reads a Worker Execute Request Datagram from a file descriptor, or NULL if it fails. This version should be 
 * called after reading the header of a datagram, for distinguishing the datagram mode.
 * 
 * @param fd     The file descriptor to read.
 * @param header The already read header.
 */
WorkerExecuteRequestDatagram read_partial_worker_execute_request_datagram(int fd, WORKER_DATAGRAM_HEADER header);

/**
 * @brief Creates a new empty Worker Shutdown Request Datagram.
 */
WorkerShutdownRequestDatagram create_worker_shutdown_request_datagram();

/**
 * @brief Reads a Worker Shutdown Request Datagram from a file descriptor, or NULL if it fails. This version should be 
 * called after reading the header of a datagram, for distinguishing the datagram type.
 * 
 * @param fd     The file descriptor to read.
 * @param header The already read header.
 */
WorkerShutdownRequestDatagram read_partial_worker_shutdown_request_datagram(int fd, WORKER_DATAGRAM_HEADER header);

/**
 * @brief Creates a new empty Worker Completion Response Datagram.
 */
WorkerCompletionResponseDatagram create_worker_completion_response_datagram();

/**
 * @brief Reads a Worker Completion Response Datagram from a file descriptor, or NULL if it fails. This version should be 
 * called after reading the header of a datagram, for distinguishing the datagram type.
 * 
 * @param fd     The file descriptor to read.
 * @param header The already read header.
 */
WorkerCompletionResponseDatagram read_partial_worker_completion_response_datagram(int fd, WORKER_DATAGRAM_HEADER header);


#endif