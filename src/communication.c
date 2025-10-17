#include "communication.h"

size_t send_all(int sock_fd, const void *buf, size_t len) {
    size_t total = 0;
    const char *p = buf;

    while (total < len) {
        size_t n = write(sock_fd, p + total, len - total);
        if (n <= 0) return -1;
        total += n;
    }

    return total;
}

size_t recv_all(int sock_fd, void *buf, size_t len) {
    size_t total = 0;
    char *p = buf;

    while (total < len) {
        size_t n = read(sock_fd, p + total, len - total);
        if (n <= 0) return -1;
        total += n;
    }

    return total;
}

size_t md_send_command(int sock_fd, DNCommand cmd, void *payload, size_t payload_size) {
    DNHeader header = {cmd, payload_size};
    
    if (send_all(sock_fd, &header, sizeof(header)) != sizeof(header)) 
        return -1;

    if (payload_size > 0 && send_all(sock_fd, payload, payload_size) != payload_size)
        return -1;

    return 0;
}

size_t dn_recv_command(int sock_fd, DNCommand *cmd, void **payload, size_t *payload_size) {
    DNHeader header = {0};
    
    if (recv_all(sock_fd, &header, sizeof(header)) != sizeof(header))
        return -1;
    
    *cmd = header.cmd;
    *payload_size = header.payload_size;
    
    if (*payload_size > 0) {
        *payload = malloc(*payload_size);
        if (!*payload) return -1;
        
        if (recv_all(sock_fd, *payload, *payload_size) != *payload_size) {
            free(*payload);
            return -1;
        }
    } else {
        *payload = NULL;
    }

    return 0;
}

size_t dn_send_response(int sock_fd, DNStatus status, void *payload, size_t payload_size) {
    DNResponseHeader header = {status, payload_size};
    
    if (send_all(sock_fd, &header, sizeof(header)) != sizeof(header))
        return -1;

    if (payload_size > 0 && send_all(sock_fd, payload, payload_size) != payload_size)
        return -1;

    return 0;
}

size_t md_recv_response(int sock_fd, DNStatus *status, void **payload, size_t *payload_size) {
    DNResponseHeader header = {0};
    
    if (recv_all(sock_fd, &header, sizeof(header)) != sizeof(header)) 
        return -1;
    
    *status = header.status;
    *payload_size = header.payload_size;
    
    if (*payload_size > 0) {
        *payload = malloc(*payload_size);
        if (!*payload) return -1;
        
        if (recv_all(sock_fd, *payload, *payload_size) != *payload_size) {
            free(*payload);
            return -1;
        }
    } else {
        *payload = NULL;
    }

    return 0;
}
