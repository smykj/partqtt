#include <stdint.h>
#include <sys/queue.h>
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

typedef struct item {
  char *value;
  SLIST_ENTRY(item) items;
} item_t;

typedef SLIST_HEAD(head_struct, item) head_t;

void insert_val(
    head_t *head, char *topic,
    int topic_len) { // expects to recieve strings which arent null-terminated
  item_t *ptr = malloc(sizeof(item_t));
  if (ptr == NULL) {
    fprintf(stderr, "ERROR: malloc failed\n");
    exit(EXIT_FAILURE);
  }

  char *val = malloc(sizeof(char) * topic_len + 1);
  if (val == NULL) {
    free(ptr);
    fprintf(stderr, "ERROR: malloc failed\n");
    exit(EXIT_FAILURE);
  }

  memcpy(val, topic, topic_len);
  val[topic_len] = 0; // now its null terminated
  ptr->value = val;
  SLIST_INSERT_HEAD(head, ptr, items);
}

void free_slist(head_t *head) {
  item_t *ptr = NULL;
  while (!SLIST_EMPTY(head)) {
    ptr = SLIST_FIRST(head);
    SLIST_REMOVE(head, ptr, item, items);
    free(ptr->value);
    free(ptr);
    ptr = NULL;
  }
}

void print_slist(head_t *head) {
  item_t *ptr = NULL;
  SLIST_FOREACH(ptr, head, items) { printf("%s\n", ptr->value); }
}
int search_slist(head_t *head,
                 char *topic) { // expects topic to be null-terminated
  item_t *ptr = NULL;
  SLIST_FOREACH(ptr, head, items) {
    if (strcmp(topic, ptr->value) == 0) {
      return TRUE;
    }
  }
  return FALSE;
}
/*
todo:
- probably create structs for every packet type
- create a data structure for remembering clients - linked list? sys/queue.h
   seems useful for that
*/

// MQTTv3.1.1 section 2.2.1
enum packet_types {
  FORBIDDEN1 = 0,   // 0000 not handled
  CONNECT = 1,      // 0001 message flags not handled
  CONNACK = 2,      // 0010 the server only sends this
  PUBLISH = 3,      // 0011 message flags not handled
  PUBACK = 4,       // 0100 not handled
  PUBREC = 5,       // 0101 not handled
  PUBREL = 6,       // 0110 not handled
  PUBCOMP = 7,      // 0111 not handled
  SUBSCRIBE = 8,    // 1000
  SUBACK = 9,       // 1001
  UNSUBSCRIBE = 10, // 1010
  UNSUBACK = 11,    // 1011
  PINGREQ = 12,     // 1100
  PINGRESP = 13,    // 1101
  DISCONNECT = 14,  // 1110
  FORBIDDEN2 = 15,  // 1111 not handled
};

// packets can be split among multiple reads, and they can probably overlap. i
// hope its correct to assume that one client will keep writing from the same
// file descriptor, otherwise i have no way to tell which parts of packets im
// supposed to connect
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
  uint32_t keep_alive;
  uint32_t time_since_last_message;
  char client_id[23]; // 23 because thats the maximum allowed length
                      // [MQTT-3.1.3-5]
  head_t topics;
} client_t;

int length_bytes(int len) {
  // retroactively figure out how many bytes were needed for the length. it's a
  // bit of a hack...
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
    fprintf(stderr, "WARNING: remaining length has incorrect formatting - the "
                    "4th byte has the most significant bit set to 1.");
  }
  return result;
}

void encode_remaining_length(int length, char result[4]) {
  int i = 0;
  char encodedByte;
  do {
    encodedByte = length % 128;
    length = length / 128;
    if (length > 0) {
      encodedByte = encodedByte | 128;
    }
    result[i] = encodedByte;
    ++i;
  } while (length > 0);
}
// this is part of the mechanism for assembling packets, it tries to decode the
// remaining length so it can get the final packet size
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

int handle_connect_packet(client_t *client, char *response) {
  int length = 0;
  int position = 0;
  // todo: refuse connections as per 3.1.4-1
  if ((client->packet.data[0] & 0x0f) != 0) {
    fprintf(stderr, "WARNING: CONNECT packet has invalid flags.\n");
  }
  length = decode_remaining_length(&(client->packet.data[1]),
                                   client->packet.expected_len);
  if (length < 0) {
    fprintf(stderr, "WARNING: CONNECT packet length decoding failed.\n");
  }
  position = 1 + length_bytes(length);
  if (client->packet.data[position] != 0x0 ||
      client->packet.data[position + 1] != 0x4 ||
      client->packet.data[position + 2] != 'M' ||
      client->packet.data[position + 3] != 'Q' ||
      client->packet.data[position + 4] != 'T' ||
      client->packet.data[position + 5] != 'T') {

    fprintf(stderr,
            "WARNING: CONNECT packet has a malformed variable header.\n");
  }
  position += 6;
  if (client->packet.data[position] != 0x4) {
    // todo: send CONNACK with return code 0x1 - unacceptable protocol level
    // as per 3.1.2-2
  }

  ++position;

  if (client->packet.data[position] != 0x0) {
    fprintf(stderr, "WARNING: CONNECT packet has connect flags which arent "
                    "supported in this implementation.\n");
  }
  ++position;

  client->keep_alive += client->packet.data[position] << 8;
  client->keep_alive += client->packet.data[position + 1];
  position += 2;
  // todo: refuse client id duplicate as per 3.1.4-2
  // todo: make sure client id is always null terminated
  if (strlen(&(client->packet.data[position])) > 23) {

    response[0] = (char)(CONNACK << 4);
    response[1] = 0x2;
    response[2] = 0;
    response[3] = 0x2;
    return 4;
  } else {
    strcpy(client->client_id, &(client->packet.data[position]));
    response[0] = (char)(CONNACK << 4);
    response[1] = 0x2;
    response[2] = 0;
    response[3] = 0;
    return 4;
  }
  // todo: check if im supposed to set the session present packet,
  // even thou the session content will always be empty
}

int handle_publish_packet(client_t *clients, int cidx, char *response) {
  int length = 0;
  int position = 0;
  if ((clients[cidx].packet.data[0] & 0x0f) != 0) {
    fprintf(stderr, "WARNING: PUBLISH packet has flags which arent supported "
                    "by this implementation.\n");
  }
  length = decode_remaining_length(&(clients[cidx].packet.data[1]),
                                   clients[cidx].packet.expected_len);
  if (length < 0) {
    fprintf(stderr, "WARNING: PUBLISH packet length decoding failed.\n");
  }
  position = 1 + length_bytes(length);

  int topic_name_length = clients[cidx].packet.data[position] << 8;
  topic_name_length += clients[cidx].packet.data[position + 1];
  position += 2;
  char topic_name[topic_name_length + 1];
  memcpy(topic_name, &(clients[cidx].packet.data[position]), topic_name_length);
  topic_name[topic_name_length] =
      0; // null terminating it so i can use it in strcmp() in search_slist()
  position += topic_name_length;
  int message_length = clients[cidx].packet.expected_len - position;
  char message[message_length];
  memcpy(message, &(clients[cidx].packet.data[position]), message_length);

  // writing to every client who subscribed to this topic
  for (int i = 0; clients[i].fd != 0; ++i) { // clients array is null-terminated
    if (search_slist(&clients[i].topics, topic_name) ==
        TRUE) { // we found a client who subscribed to this topic

      // fixed header
      int response_position = 0;
      response[response_position] = (uint8_t)(PUBLISH << 4);
      ++response_position;
      int lbytes = length_bytes(2 + topic_name_length + message_length);
      char len[lbytes];
      encode_remaining_length(2 + topic_name_length + message_length, len);
      for (int j = 0; j < lbytes; ++j) { // remaining length
        response[response_position] = len[j];
        ++response_position;
      }

      // variable header
      response[response_position] = (uint16_t)topic_name_length >> 8;
      ++response_position;
      response[response_position] =
          (uint8_t)((uint16_t)topic_name_length & 0x00ff);
      ++response_position;
      for (int j = 0; j < topic_name_length; ++j) {
        response[response_position] = topic_name[j];
        ++response_position;
      }

      // payload
      for (int j = 0; j < message_length; ++j) {
        response[response_position] = message[j];
        ++response_position;
      }

      // write to subscribed client
      int result = write(clients[i].fd, response, response_position - 1);
      if (result < response_position - 1) {
        fprintf(stderr, "ERROR: Could not write full response.\n");
      }
    }
  }
  return 0; // we're not sending a response to the client that sent the publish
}

int subscribe_packet(client_t *client, char *response) {
  int length = 0;
  int position = 0;
  if ((client->packet.data[0] & 0x0f) != 2) {
    // todo close network connection
    fprintf(stderr, "WARNING: SUBSCRIBE packet has invalid flags.\n");
  }
  length = decode_remaining_length(&(client->packet.data[1]),
                                   client->packet.expected_len);
  if (length < 0) {
    fprintf(stderr, "WARNING: SUBSCRIBE packet length decoding failed.\n");
  }
  position = 1 + length_bytes(length);
  int packet_identifier_position = position;
  position += 2;
  if (position >= client->packet.expected_len) {
    // todo: disconnect, protocol violation, 3.8.3-3
    return -1;
  }
  int topic_counter = 0;
  // todo: finish the response
  while (position < client->packet.expected_len) {
    ++topic_counter;
    int topic_name_length = client->packet.data[position] << 8;
    topic_name_length += client->packet.data[position + 1];
    position += 2;
    char topic_name[topic_name_length];
    memcpy(topic_name, &(client->packet.data[position]), topic_name_length);
    insert_val(&(client->topics), topic_name, topic_name_length);
    position += topic_name_length;
    if (client->packet.data[position] != 0) {
      // todo: close network connection?
      fprintf(stderr, "WARNING: SUBSCRIBE packet has unsupported QoS.\n");
    }
    ++position;
  }
  // assembling response
  // fixed header
  int response_position = 0;
  response[response_position] = (uint8_t)(SUBACK << 4);
  ++response_position;
  int lbytes = length_bytes(2 + topic_counter);
  char len[lbytes];
  encode_remaining_length(2 + topic_counter, len);
  for (int j = 0; j < lbytes; ++j) { // remaining length
    response[response_position] = len[j];
    ++response_position;
  }
  // variable header
  response[response_position] = client->packet.data[packet_identifier_position];
  ++response_position;
  response[response_position] =
      client->packet.data[packet_identifier_position + 1];
  ++response_position;
  // payload - we only support QoS 0 and for now there isnt a way for the
  // subscribtion to fail, so the response is all zeros
  for (int i = 0; i < topic_counter; ++i) {
    response[response_position] = 0;
    ++response_position;
  }
  return response_position - 1;
}

int handle_packet(client_t *clients, int cidx) {
#ifdef DEBUG
  for (int i = 0; i < clients[cidx].packet.expected_len; ++i) {
    fprintf(stderr, "DEBUG: packet byte %d: %.8b, %c\n", i,
            (uint8_t)(clients[cidx].packet.data[i]),
            clients[cidx].packet.data[i]);
  }
#endif
  char response_buffer[PCKT_BUF_SIZE];
  int response_length = 0;
  /// todo: handle all packet types
  uint8_t type = (uint8_t)clients[cidx].packet.data[0] >> 4;
  switch (type) {
  case PINGREQ:
    if ((clients[cidx].packet.data[0] & 0x0f) != 0) {
      fprintf(stderr, "WARNING: PINGREQ packet has invalid flags.\n");
    }
    response_buffer[0] = (char)(PINGRESP << 4);
    response_buffer[1] = 0;
    response_length = 2;
    break;
  case CONNECT:
    response_length = handle_connect_packet(&clients[cidx], response_buffer);
    break;
  case PUBLISH:
    response_length = handle_publish_packet(clients, cidx, response_buffer);
    break;
  case SUBSCRIBE:
    response_length = subscribe_packet(&clients[cidx], response_buffer);
    break;
  default:
    fprintf(stderr, "ERROR: invalid/unsupported packet type: %d\n", type);
  }

#ifdef DEBUG
  for (int i = 0; i < response_length; ++i) {
    fprintf(stderr, "DEBUG: response byte %d: %.8b, %c\n", i,
            (uint8_t)(response_buffer[i]), response_buffer[i]);
  }
#endif
  // todo: check if we made any response
  // todo: make it possible to write multiple responses, possibly to different
  // clients
  int result = write(clients[cidx].fd, response_buffer, response_length);
  if (result < response_length) {
    fprintf(stderr, "ERROR: Could not write full response.\n");
  }

  return 0;
}

int packet_builder(client_t *clients, int cidx, char *message,
                   int message_len) {
#ifdef DEBUG
  for (int i = 0; i < message_len; ++i) {
    fprintf(stderr, "DEBUG: byte %d: %.8b, %c\n", i, (uint8_t)(message[i]),
            message[i]);
  }
#endif
  if (clients[cidx].packet.expected_len == 0) { // no buffered packet part
    memcpy(clients[cidx].packet.data, message, message_len);
    clients[cidx].packet.current_len = message_len;
    update_expected_len(&clients[cidx]);

  } else {
    memcpy(clients[cidx].packet.data + clients[cidx].packet.current_len,
           message, message_len);
    clients[cidx].packet.current_len += message_len;
    update_expected_len(&clients[cidx]);
  }
  while (clients[cidx].packet.current_len >=
             clients[cidx].packet.expected_len &&
         clients[cidx].packet.expected_len !=
             0) { // we have at least one finished packet, we are in a while
                  // loop because theoretically we can receive multiple packets
                  // in one read()
    handle_packet(clients, cidx);

    // the data of the handled packet is not erased, be careful not to touch it
    if (clients[cidx].packet.current_len == clients[cidx].packet.expected_len) {
      clients[cidx].packet.expected_len = 0;
      clients[cidx].packet.current_len = 0;
    } else {
      // here, there is `expected_len` of a packet that has been handled, and
      // `(current_len - expected_len)` of another packet, so im copying that
      // unhandled part to the start of the buffer and adjusting the lengths
      memcpy(clients[cidx].packet.data,
             &(clients[cidx].packet.data[clients[cidx].packet.expected_len]),
             (clients[cidx].packet.current_len -
              clients[cidx].packet.expected_len));
      clients[cidx].packet.current_len -= clients[cidx].packet.expected_len;
      update_expected_len(&clients[cidx]);
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
  // char response_buffer[PCKT_BUF_SIZE];
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

          // + 1 because i always want an empty client at the end, as a
          // null-termination
          if (nfds + 1 >= client_buf_size) {
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
          SLIST_INIT(&(clients[nfds].topics));
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
          rc = packet_builder(clients, i, receive_buffer, rc);
          if (rc < 0) {
            fprintf(stderr, "  ERROR: packet_builder() failed.\n");
            close_conn = TRUE;
            break;
          }
          // NOTE: moving responsibility for responding to client into
          // handle_packet

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
          free_slist(&(clients[i].topics));
          for (j = i; j < nfds - 1; j++) {
            fds[j].fd = fds[j + 1].fd;
            clients[j].fd = clients[j + i].fd;
            // NOTE: these are huge copies - client.packet has the entire packet
            // buffer, currently 1KB. Allegedly speed is not important, but it
            // might be worth fixing this. (another problem is that im
            // allocating it contiguously for no reason, relying on having a
            // massive contiguous block of memory)
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
