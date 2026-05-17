#include <stdint.h>
// #include <sys/queue.h>
#define DEBUG
#include <errno.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/poll.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#define SERVER_PORT 1883
#define PCKT_BUF_SIZE 1024
#define TRUE 1
#define FALSE 0
/*
todo:
- probably create structs for every packet type
- create a data structure for remembering clients - linked list? sys/queue.h
   seems useful for that
*/

// MQTTv3.1.1 section 2.2.1
enum packet_types {
  FORBIDDEN1 = 0,   // 0000
  CONNECT = 1,      // 0001
  CONNACK = 2,      // 0010
  PUBLISH = 3,      // 0011
  PUBACK = 4,       // 0100
  PUBREC = 5,       // 0101
  PUBREL = 6,       // 0110
  PUBCOMP = 7,      // 0111
  SUBSCRIBE = 8,    // 1000
  SUBACK = 9,       // 1001
  UNSUBSCRIBE = 10, // 1010
  UNSUBACK = 11,    // 1011
  PINGREQ = 12,     // 1100
  PINGRESP = 13,    // 1101
  DISCONNECT = 14,  // 1110
  FORBIDDEN2 = 15,  // 1111
};

// packets can be split among multiple reads, and they can probably overlap. i
// hope its correct to assume that one client will keep writing from the same
// address, otherwise i have no way to tell which parts of packets im supposed
// to connect
typedef struct packet {
  // enum packet_types type;
  char data[PCKT_BUF_SIZE];
  int expected_len;
  int current_len;
} packet_t;

typedef struct response {
  char data[PCKT_BUF_SIZE];
  int len;
  int for_fd;
} response_t;

typedef struct client {
  packet_t packet;
  int fd;

} client_t;

int length_bytes(int len) {
  // retroactively figure out how many bytes were needed for the length. bit of
  // a hack...
  if (len <= 127)
    return 1;
  if (len <= 16383)
    return 2;
  if (len <= 2097151)
    return 3;
  return 4;
}
int decode_remaining_length(char *bytes, int len) {
  // MQTTv3.1.1 section 2.2.3
  // note: i wrote this before i noticed the non normative comment in the
  // specification that gives the algorithm for this, but this works so im
  // keeping it for now
  int result = 0;

  result += (int)(bytes[0] & 0x7f);
  if ((bytes[0] & 0x80) == 0) {
    return result;
  }
  if (len < 2)
    return -1;
  result += ((int)(bytes[1] & 0x7f) << 7);
  if ((bytes[1] & 0x80) == 0) {
    return result;
  }

  if (len < 3)
    return -1;
  result += ((int)(bytes[2] & 0x7f) << 14);
  if ((bytes[2] & 0x80) == 0) {
    return result;
  }
  if (len < 4)
    return -1;
  result += ((int)(bytes[3] & 0x7f) << 21);
  if ((bytes[3] & 0x80) == 0) {
    fprintf(stderr, "ERROR: remaining length has incorrect formatting - the "
                    "4th byte has the most significant bit set to 1.");
  }
  return result;
}
void update_expected_len(client_t *client) {
  if (client->packet.current_len < 2) {
    // not enough data to know the packet size
    client->packet.expected_len = 2;
    return;
  }

  int len = decode_remaining_length(&(client->packet.data[1]),
                                    client->packet.current_len - 1);
  if (len < 0) {
    // still not enough data to know the packet size
    client->packet.expected_len = client->packet.current_len + 1;
    return;
  }

  // length decoded successfully
  client->packet.expected_len =
      len + length_bytes(len) +
      1; // remaining length doesnt include the byte for the packet type and
         // flags, and it doesnt include the bytes for the encoded length, so
         // here im adding it back in
  return;
}

int handle_packet(client_t *client) {
  char response_buffer[PCKT_BUF_SIZE];
  int response_length = 0;
  /// todo: handle all packet types
  uint8_t type = (uint8_t)client->packet.data[0] >> 4;

  switch (type) {
  case PINGREQ:
    if ((client->packet.data[0] & 0x0f) != 0) {
      fprintf(stderr, "WARNING: PINGREQ packet has invalid flags.\n");
    }
    response_buffer[0] = (char)(PINGRESP << 4);
    response_buffer[1] = 0;
    response_length = 2;
    break;
  default:
    fprintf(stderr, "ERROR: invalid/unsupported packet type: %d\n", type);
  }

  int result = write(client->fd, response_buffer, response_length);
  if (result < response_length) {
    fprintf(stderr, "ERROR: Could not write full response.\n");
  }

  return 0;
}

int packet_builder(client_t *client, char *message, int message_len) {
#ifdef DEBUG
  for (int i = 0; i < message_len; ++i) {
    fprintf(stderr, "DEBUG: byte %d: %.8b, %c\n", i, (uint8_t)(message[i]),
            message[i]);
  }
#endif
  if (client->packet.expected_len == 0) { // no buffered packet part
    memcpy(client->packet.data, message, message_len);
    client->packet.current_len = message_len;
    update_expected_len(client);

  } else {
    memcpy(client->packet.data + client->packet.current_len, message,
           message_len);
    client->packet.current_len += message_len;
    update_expected_len(client);
  }
  while (client->packet.current_len >= client->packet.expected_len &&
         client->packet.expected_len !=
             0) { // we have at least one finished packet, we are in a while
                  // loop because theoretically we can receive multiple packets
                  // in one read()
    handle_packet(client);

    // the data of the handled packet is not erased, be careful not to touch it
    if (client->packet.current_len == client->packet.expected_len) {
      client->packet.expected_len = 0;
      client->packet.current_len = 0;
    } else {
      // here, there is `expected_len` of a packet that has been handled, and
      // `(current_len - expected_len)` of another packet, so im copying that
      // unhandled part to the start of the buffer and adjusting the lengths
      memcpy(client->packet.data,
             &(client->packet.data[client->packet.expected_len]),
             (client->packet.current_len - client->packet.expected_len));
      client->packet.current_len -= client->packet.expected_len;
      update_expected_len(client);
    }
  }
  return 0;
}

int main(int argc, char *argv[]) {
  int rec_len, rc, on = 1;
  int listen_sd = -1, new_sd = -1;
  int end_server = FALSE, compress_array = FALSE;
  int close_conn;
  char receive_buffer[PCKT_BUF_SIZE];
  char response_buffer[PCKT_BUF_SIZE];
  struct sockaddr_in6 addr;
  int timeout;
  int client_buf_size = 8;
  // struct pollfd fds[200]; // might have to make this expandable, cant be
  // incorporated into the linked list for clients since
  // poll() needs it to be contiguous (i assume)
  struct pollfd *fds =
      (struct pollfd *)malloc(sizeof(struct pollfd) * client_buf_size);
  client_t *clients = (client_t *)malloc(sizeof(client_t) * client_buf_size);
  if (fds == NULL) {
    perror("ERROR: fds malloc failed");
    exit(-1);
  }
  if (clients == NULL) {
    perror("ERROR: clients malloc failed");
    exit(-1);
  }

  int nfds = 1, current_size = 0, i, j;

  listen_sd = socket(AF_INET6, SOCK_STREAM, 0);
  if (listen_sd < 0) {
    perror("ERROR: socket() failed");
    exit(-1);
  }

  // reuse socket descriptors - possibly useless
  rc = setsockopt(listen_sd, SOL_SOCKET, SO_REUSEADDR, (char *)&on, sizeof(on));
  if (rc < 0) {
    perror("ERROR: setsockopt() failed");
    close(listen_sd);
    exit(-1);
  }

  // sets socket to be non-blocking - possibly useless, since it didnt seem to
  // work? had to set MSG_DONTWAIT on recv()
  rc = ioctl(listen_sd, FIONBIO, (char *)&on);
  if (rc < 0) {
    perror("ERROR: ioctl() failed");
    close(listen_sd);
    exit(-1);
  }

  memset(&addr, 0, sizeof(addr));
  addr.sin6_family = AF_INET6;
  memcpy(&addr.sin6_addr, &in6addr_any, sizeof(in6addr_any));
  addr.sin6_port = htons(SERVER_PORT);
  rc = bind(listen_sd, (struct sockaddr *)&addr, sizeof(addr));
  if (rc < 0) {
    perror("ERROR: bind() failed");
    close(listen_sd);
    exit(-1);
  }

  rc = listen(listen_sd, 32);
  if (rc < 0) {
    perror("ERROR: listen() failed");
    close(listen_sd);
    exit(-1);
  }

  memset(fds, 0, sizeof(*fds));
  memset(clients, 0, sizeof(*clients));

  fds[0].fd = listen_sd;
  fds[0].events = POLLIN;
  timeout = (3 * 60 * 1000);

  do {
    printf("Waiting on poll()...\n");
    rc = poll(fds, nfds, timeout);

    if (rc < 0) {
      perror("  ERROR: poll() failed");
      break;
    }

    if (rc == 0) {
      printf("  poll() timed out.  End program.\n");
      break;
    }

    current_size = nfds;
    for (i = 0; i < current_size; i++) {
      if (fds[i].revents == 0)
        continue;

      if (fds[i].revents != POLLIN) {
        printf("  ERROR: revents = %d\n, expected POLLIN", fds[i].revents);
        end_server = TRUE;
        break;
      }
      if (fds[i].fd == listen_sd) {
        printf("  Listening socket is readable\n");

        do {
          new_sd = accept(listen_sd, NULL, NULL);
          if (new_sd < 0) {
            if (errno != EWOULDBLOCK) {
              perror("  ERROR: accept() failed");
              end_server = TRUE;
            }
            break;
          }

          printf("  New incoming connection - %d\n", new_sd);

          if (nfds >= client_buf_size) {
            client_buf_size *= 2;
            fds = realloc(fds, client_buf_size * sizeof(struct pollfd));
            clients = realloc(clients, client_buf_size * sizeof(client_t));
            if (!fds || !clients) {
              perror("ERROR: Reallocation failed");
              exit(-1);
            }
            for (int i = nfds; i < client_buf_size; ++i) {
              memset(&(fds[i]), 0, sizeof(struct pollfd));
              memset(&(clients[i]), 0, sizeof(client_t));
            }
          }
          fds[nfds].fd = new_sd;
          fds[nfds].events = POLLIN;
          clients[nfds].fd = new_sd;
          clients[nfds].packet.expected_len = 0;
          nfds++;

        } while (new_sd != -1);
      } else {
        printf("  Descriptor %d is readable\n", fds[i].fd);
        close_conn = FALSE;

        do {
          // doesnt wait for the client to finish sending, might be incorrect
          rc = recv(fds[i].fd, receive_buffer, sizeof(receive_buffer), 0);
          if (rc < 0) {
            if (errno != EWOULDBLOCK) {
              perror("  ERROR: recv() failed");
              close_conn = TRUE;
            }
            break;
          }

          if (rc == 0) {
            printf("  Connection closed\n");
            close_conn = TRUE;
            break;
          }

          rec_len = rc;
          fprintf(stderr, "  %d bytes received\n", rec_len);

          // rc = send(fds[i].fd, buffer, len, 0);
          // rc = write(0, buffer, len);

          // rc = handle_packet(rec_len, receive_buffer, response_buffer);
          rc = packet_builder(&(clients[i]), receive_buffer, rc);
          if (rc < 0) {
            fprintf(stderr, "  ERROR: handle_packet() failed.\n");
            close_conn = TRUE;
            break;
          }
          // NOTE: moving responsibility for responding to client elsewhere

          // int n = write(fds[i].fd, response_buffer, response_length);
          // if (n < response_length) {
          // fprintf(stderr, "ERROR: Couldnt write the whole response.\n");
          // close_conn = TRUE;
          // break;
          // }

        } while (TRUE);

        if (close_conn) {
          close(fds[i].fd);
          fds[i].fd = -1;
          compress_array = TRUE;
        }
      }
    }

    // maintains size of array, probably optional
    if (compress_array) {
      compress_array = FALSE;
      for (i = 0; i < nfds; i++) {
        if (fds[i].fd == -1) {
          for (j = i; j < nfds - 1; j++) {
            fds[j].fd = fds[j + 1].fd;
            clients[j].fd = clients[j + i].fd;
            clients[j].packet = clients[j + i].packet;
          }
          i--;
          nfds--;
        }
      }
    }

  } while (end_server == FALSE);

  for (i = 0; i < nfds; i++) {
    if (fds[i].fd >= 0)
      close(fds[i].fd);
  }
  free(clients);
  free(fds);
}
