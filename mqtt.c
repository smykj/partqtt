#include <stdint.h>
// #include <sys/types.h>
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

#define SERVER_PORT 1883
#define PCKT_BUF_SIZE 1024
#define TRUE 1
#define FALSE 0

// MQTTv3.1.1 section 2.2.3
uint32_t decode_remaining_length(char *bytes) {
  // note: i wrote this before i noticed the non normative comment in the
  // specification that gives the algorithm for this, but this works so im
  // keeping it for now
  uint32_t result = 0;

  result += (uint32_t)(bytes[0] & 0x7f);
  if ((bytes[0] & 0x80) == 0) {
    return result;
  }

  result += ((uint32_t)(bytes[1] & 0x7f) << 7);
  if ((bytes[1] & 0x80) == 0) {
    return result;
  }

  result += ((uint32_t)(bytes[2] & 0x7f) << 14);
  if ((bytes[2] & 0x80) == 0) {
    return result;
  }
  result += ((uint32_t)(bytes[3] & 0x7f) << 21);
  if ((bytes[3] & 0x80) == 0) {
    perror("ERROR: remaining length has incorrect formatting - the 4th byte "
           "has the most significant bit set to 1.");
  }
  return result;
}

// Writes a response to the response buffer, and the length of the response is
// the return value. Return value of -1 signifies an error
int handle_packet(int len, char receive_buffer[PCKT_BUF_SIZE],
                  char response_buffer[PCKT_BUF_SIZE]) {
  if (len < 2) {
    fprintf(stderr,
            "ERROR: handle_packet received packet of length less than 2.\n");
    return -1;
  }
#ifdef DEBUG
  for (int i = 0; i < len; ++i) {
    printf("DEBUG: byte %d: %.8b, %c\n", i, (uint8_t)(receive_buffer[i]),
           receive_buffer[i]);
  }
#endif

  uint32_t remaining_length = decode_remaining_length(&(receive_buffer[1]));

  /// todo: handle all packet types
  uint8_t type = (uint8_t)receive_buffer[0] >> 4;

  switch (type) {
  case PINGREQ:
    if ((receive_buffer[0] & 0x0f) != 0) {
      fprintf(stderr, "ERROR: PINGREQ packet has invalid flags.\n");
      return -1;
    }
    if (remaining_length != 0) {
      fprintf(stderr, "ERROR: PINGREQ packet has non-zero remaining_length.\n");
      return -1;
    }
    response_buffer[0] = (char)(PINGRESP << 4);
    response_buffer[1] = 0;
    return 2;
    break;
  default:
    fprintf(stderr, "ERROR: invalid/unsupported packet type.\n");
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
  struct pollfd fds[200]; // might have to make this expandable, cant be
                          // incorporated into the linked list for clients since
                          // poll() needs it to be contiguous (i assume)
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

  memset(fds, 0, sizeof(fds));

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
          fds[nfds].fd = new_sd;
          fds[nfds].events = POLLIN;
          nfds++;

        } while (new_sd != -1);
      }

      else {
        printf("  Descriptor %d is readable\n", fds[i].fd);
        close_conn = FALSE;

        do {
          // doesnt wait for the client to finish sending, might be incorrect
          rc = recv(fds[i].fd, receive_buffer, sizeof(receive_buffer),
                    MSG_DONTWAIT);
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
          printf("  %d bytes received\n", rec_len);

          // rc = send(fds[i].fd, buffer, len, 0);
          // rc = write(0, buffer, len);

          int response_length =
              handle_packet(rec_len, receive_buffer, response_buffer);
          if (response_length < 0) {
            fprintf(stderr, "  ERROR: handle_packet() failed.\n");
            close_conn = TRUE;
            break;
          }
          int n = write(fds[i].fd, response_buffer, response_length);
          if (n < response_length) {
            fprintf(stderr, "ERROR: Couldnt write the whole response.\n");
            close_conn = TRUE;
            break;
          }

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
}
