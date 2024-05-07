/******************************************************************************
 *                           STATUS MODE DATAGRAMS                            *
 *                                                                            *
 *   The Status Mode Datagrams is the common name given to the Request and    *
 * Response Datagrams for the Status Mode of the application architecture.    *
 *   The Status Mode is the mode used to query the current state of a server  *
 * instance, returning the queues, running and completed tasks.               *
 *                                                                            *
 *   The create_status_<kind>_datagram functions create a new empty datagram  *
 * of the specified kind, initially valid for transmission, that allows, but  *
 * discourages modfication of the data before being sent.                     *
 *   The read_status_<kind>_datagram functions read a full datagram of the    *
 * specificed kind. They are meant to be used only if it is known beforehand  *
 * that there is a valid datagram present within a file descriptor.           *
 *   The read_partial_status_<kind>_datagram read the payload of a datagram   *
 * of the specified kind. They are meant to be used in conjunction with the   *
 * read_datagram_header function to process the datagram header separatedly   *
 * from the payload.                                                          *
 *   The status_<kind>_datagram_to_string converts a datagram of the          *
 * specified kind to a null-terminated, trimmed string.                       *
 ******************************************************************************/

#include "common/datagram/datagram.h"
#include "common/datagram/status.h"
#include "common/util/string.h"
#include "common/util/alloc.h"
#include "common/io/io.h"

#pragma region ======= REQUEST =======
StatusRequestDatagram create_status_request_datagram() {
    #define ERR NULL

    StatusRequestDatagram dg = SAFE_ALLOC(StatusRequestDatagram, sizeof(STATUS_REQUEST_DATAGRAM));
    dg->header = create_datagram_header();
    dg->header.mode = DATAGRAM_MODE_STATUS_REQUEST;

    return dg;
    #undef ERR
}

StatusRequestDatagram read_status_request_datagram(int fd) {
    #define ERR NULL

    StatusRequestDatagram status = calloc(1, sizeof(STATUS_REQUEST_DATAGRAM));
    SAFE_READ(fd, status, sizeof(STATUS_REQUEST_DATAGRAM));

    return status;
    #undef ERR
}

StatusRequestDatagram read_partial_status_request_datagram(int fd, DATAGRAM_HEADER header) {
    UNUSED(fd);

    #define ERR NULL
    
    StatusRequestDatagram status = calloc(1, sizeof(STATUS_REQUEST_DATAGRAM));
    status->header = header;

    // SAFE_READ(fd, (((void*)status) + sizeof(DATAGRAM_HEADER)), sizeof(STATUS_REQUEST_DATAGRAM) - sizeof(DATAGRAM_HEADER));

    return status;
    #undef ERR
}

char* status_request_datagram_to_string(StatusRequestDatagram dg, int expandEnums) {
    char* dh = datagram_header_to_string(&dg->header, expandEnums);
    // char* bytes = bytes_to_hex_string(dg->data, STATUS_REQUEST_DATAGRAM_PAYLOAD_LEN, ':');

    char* str = isnprintf(
        // "StatusRequestDatagram{ header: %s, data: '%s' }",
        // dh,
        // bytes
        "StatusRequestDatagram{ header: %s }",
        dh
    );

    // free(bytes);
    free(dh);

    return str;
}
#pragma endregion

#pragma region ======= RESPONSE =======
StatusResponseDatagram create_status_response_datagram(uint8_t payload[], int payload_len) {
    #define ERR NULL
    size_t total_size = sizeof(STATUS_RESPONSE_DATAGRAM) + payload_len;

    StatusResponseDatagram dg = SAFE_ALLOC(StatusResponseDatagram, total_size);
    dg->header = create_datagram_header();
    dg->header.mode = DATAGRAM_MODE_STATUS_RESPONSE;

    dg->payload_len = payload_len;
    if (payload_len > 0 && payload != NULL) {
        memcpy(dg->payload, payload, payload_len);
    }


    return dg;
    #undef ERR
}

StatusResponseDatagram read_status_response_datagram(int fd) {
    #define ERR NULL

    StatusResponseDatagram status = calloc(1, sizeof(STATUS_RESPONSE_DATAGRAM));

    // Read datagram header + payload length
    int size = STRUCT_MEMBER_SIZE(STATUS_RESPONSE_DATAGRAM, header) + STRUCT_MEMBER_SIZE(STATUS_RESPONSE_DATAGRAM, payload_len);
    SAFE_READ(fd, status, size);
    
    // Allocate space for payload
    SAFE_REALLOC(status, sizeof(STATUS_RESPONSE_DATAGRAM) + status->payload_len);

    // Read datagram payload
    SAFE_READ(fd, (((void*)status) + size), status->payload_len);

    return status;
    #undef ERR
}

StatusResponseDatagram read_partial_status_response_datagram(int fd, DATAGRAM_HEADER header) {
    #define ERR NULL
    
    StatusResponseDatagram status = calloc(1, sizeof(STATUS_RESPONSE_DATAGRAM));
    status->header = header;

    // SAFE_READ(fd, (((void*)status) + sizeof(DATAGRAM_HEADER)), sizeof(STATUS_RESPONSE_DATAGRAM) - sizeof(DATAGRAM_HEADER));
    
    int offset = sizeof(DATAGRAM_HEADER);

    // Read datagram header + payload length
    int size = STRUCT_MEMBER_SIZE(STATUS_RESPONSE_DATAGRAM, payload_len);
    SAFE_READ(fd, (((void*)status) + offset), size);
    offset += size;

    // Allocate space for payload
    SAFE_REALLOC(status, sizeof(STATUS_RESPONSE_DATAGRAM) + status->payload_len);

    // Read datagram payload
    SAFE_READ(fd, (((void*)status) + offset), status->payload_len);

    return status;
    #undef ERR
}

char* status_response_datagram_to_string(StatusResponseDatagram dg, int expandEnums, int stringPayload) {
    #define ERR NULL

    char* dh = datagram_header_to_string(&dg->header, expandEnums);
    // char* bytes = bytes_to_hex_string(dg->payload, dg->payload_len, ':');
    char* bytes;
    if (stringPayload) {
        bytes = SAFE_ALLOC(char*, dg->payload_len + 1);
        memcpy(bytes, dg->payload, dg->payload_len);
    } else {
        bytes = bytes_to_hex_string((char*)dg->payload, dg->payload_len, ':');
    }

    char* str = isnprintf(
        "StatusResponseDatagram{ header: %s, payload_len: %d, data: '%s' }",
        dh,
        dg->payload_len,
        bytes
    );

    if (!stringPayload) free(bytes);
    free(dh);

    return str;
    #undef ERR
}
#pragma endregion
