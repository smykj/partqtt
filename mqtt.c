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
#define SERVER_PORT 1883
#define PCKT_BUF_SIZE 1024
#define TRUE 1
#define FALSE 0

int handle_packet(int len, char buffer[PCKT_BUF_SIZE]) {
  // todo
  return 0;
}

int main(int argc, char *argv[]) {
  int len, rc, on = 1;
  int listen_sd = -1, new_sd = -1;
  int end_server = FALSE, compress_array = FALSE;
  int close_conn;
  char buffer[PCKT_BUF_SIZE];
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
          rc = recv(fds[i].fd, buffer, sizeof(buffer), MSG_DONTWAIT);
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

          len = rc;
          printf("  %d bytes received\n", len);

          // rc = send(fds[i].fd, buffer, len, 0);
          // rc = write(0, buffer, len);
          rc = handle_packet(len, buffer);
          if (rc < 0) {
            perror("  ERROR: handle_packet() failed");
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
