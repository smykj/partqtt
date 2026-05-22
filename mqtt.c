#include <stdint.h>
#include <sys/queue.h>
// #define DEBUG
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
#define PCKT_BUF_SIZE 4096
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

int remove_el_slist(head_t *head,
                    char *val) { // expects val to be null-terminated
  int found = FALSE;
  item_t *ptr = SLIST_FIRST(head);
  while (1) {
    if (strcmp(ptr->value, val) == 0) {
      found = TRUE;
      break;
    }
    ptr = SLIST_NEXT(ptr, items);
  }
  if (found) {
    SLIST_REMOVE(head, ptr, item, items);
    free(ptr->value);
    free(ptr);
  }
  return found;
}

// MQTTv3.1.1 section 2.2.1
enum packet_types {
  FORBIDDEN1 = 0,   // 0000 not handled
  CONNECT = 1,      // 0001 message flags not handled
  CONNACK = 2,      // 0010
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
  char client_id[24]; // 24 because 23 is the maximum allowed length, and i want
                      // it to be null-terminated [MQTT-3.1.3-5]
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

  int clientid_length = client->packet.data[position] << 8;
  clientid_length += client->packet.data[position + 1];
  position += 2;
  if (clientid_length > 23) {

    fprintf(stderr, "WARNING: CONNECT packet has client id longer than 23.\n");
    response[0] = (char)(CONNACK << 4);
    response[1] = 0x2;
    response[2] = 0;
    response[3] = 0x2;
    return 4;
  } else {
    memcpy(client->client_id, &(client->packet.data[position]),
           clientid_length);
    client->client_id[clientid_length] = 0; // null-terminating it
    fprintf(stderr, "DEBUG: CONNECT client id: %s.\n", client->client_id);
    response[0] = (char)(CONNACK << 4);
    response[1] = 0x2;
    response[2] = 0;
    response[3] = 0;
    return 4;
  }
  // todo: check if im supposed to set the session present packet,
  // even thou the session content will always be empty
}
int handle_publish_packet(client_t *clients, int cidx, char *response,
                          int client_count) {
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

  fprintf(stderr, "DEBUG: HEREEEEEEEE\n");
  int iterations = 0;
  // writing to every client who subscribed to this topic
  for (int i = 0; i < client_count; ++i) { // clients array is null-terminated
    ++iterations;
    fprintf(stderr, "DEBUG: testing client %d\n", i);
    if (clients[i].fd == 0)
      continue;
    if (search_slist(&clients[i].topics, topic_name) ==
        TRUE) { // we found a client who subscribed to this topic

      fprintf(stderr, "DEBUG: SUCCESS\n");
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
#ifdef DEBUG
      for (int i = 0; i < response_position; ++i) {
        fprintf(stderr, "DEBUG: response byte %d: %.8b, %c\n", i,
                (uint8_t)(response[i]), response[i]);
      }
#endif
      // write to subscribed client
      int result = write(clients[i].fd, response, response_position);
      if (result < response_position) {
        fprintf(stderr, "ERROR: Could not write full response.\n");
      }
    }
  }
  fprintf(stderr, "DEBUG: iterations %d\n", iterations);
  return 0; // we're not sending a response to the client that sent the publish
}

int sub_unsub_packet(client_t *client, char *response) {
  // SUBSCRIBE and UNSUBSCRIBE packets are so similar i'm handling them with one
  // function
  int length = 0;
  int position = 0;
  int sub = (uint8_t)(client->packet.data[0]) >> 4 == SUBSCRIBE;
  if ((client->packet.data[0] & 0x0f) != 2) {
    // todo close network connection
    fprintf(stderr, "WARNING: SUB/UNSUB packet has invalid flags.\n");
  }
  length = decode_remaining_length(&(client->packet.data[1]),
                                   client->packet.expected_len);
  if (length < 0) {
    fprintf(stderr, "WARNING: SUB/UNSUB packet length decoding failed.\n");
  }
  position = 1 + length_bytes(length);
  int packet_identifier_position = position;
  position += 2;
  if (position >= client->packet.expected_len) {
    // todo: disconnect, protocol violation, 3.8.3-3
    return -1;
  }
  int topic_counter = 0;
  while (position < client->packet.expected_len) {
    ++topic_counter;
    int topic_name_length = client->packet.data[position] << 8;
    topic_name_length += client->packet.data[position + 1];
    position += 2;
    char topic_name[topic_name_length + 1];
    memcpy(topic_name, &(client->packet.data[position]), topic_name_length);
    topic_name[topic_name_length] = 0; // null terminating it so i can use it in
                                       // strcmp() in remove_el_slist()
    if (sub == TRUE) {
      insert_val(&(client->topics), topic_name, topic_name_length);
      position += topic_name_length;

      if (client->packet.data[position] != 0) {
        // todo: close network connection?
        fprintf(stderr, "WARNING: SUBSCRIBE packet has unsupported QoS.\n");
      }
      ++position;
    } else {
      fprintf(stderr, "DEBUG: attempting to remove a topic\n");
      remove_el_slist(&(client->topics), topic_name);
      position += topic_name_length;
    }
  }
  // assembling response
  // fixed header
  int response_position = 0;
  if (sub == TRUE) {

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
    response[response_position] =
        client->packet.data[packet_identifier_position];
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
  } else {
    response[response_position] = (uint8_t)(UNSUBACK << 4);
    ++response_position;
    response[response_position] = 2;
    ++response_position;
    response[response_position] =
        client->packet.data[packet_identifier_position];
    ++response_position;
    response[response_position] =
        client->packet.data[packet_identifier_position + 1];
    ++response_position;
  }
  return response_position;
}

int handle_packet(client_t *clients, int cidx, int client_count) {
#ifdef DEBUG
  for (int i = 0; i < clients[cidx].packet.expected_len; ++i) {
    fprintf(stderr, "DEBUG: packet byte %d: %.8b, %c\n", i,
            (uint8_t)(clients[cidx].packet.data[i]),
            clients[cidx].packet.data[i]);
  }
#endif
  char response_buffer[PCKT_BUF_SIZE];
  int response_length = 0;
  uint8_t type = (uint8_t)clients[cidx].packet.data[0] >> 4;
  switch (type) {
  case PINGREQ:
    if ((clients[cidx].packet.data[0] & 0x0f) != 0) {
      fprintf(stderr, "WARNING: PINGREQ packet has invalid flags.\n");
    }
    if ((clients[cidx].packet.data[1]) != 0) {
      fprintf(stderr,
              "WARNING: PINGREQ packet has non-zero remaining length.\n");
    }
    response_buffer[0] = (char)(PINGRESP << 4);
    response_buffer[1] = 0;
    response_length = 2;
    break;
  case CONNECT:
    response_length = handle_connect_packet(&clients[cidx], response_buffer);
    break;
  case PUBLISH:
    response_length =
        handle_publish_packet(clients, cidx, response_buffer, client_count);
    break;
  case SUBSCRIBE:
    response_length = sub_unsub_packet(&clients[cidx], response_buffer);
    break;
  case UNSUBSCRIBE:
    response_length = sub_unsub_packet(&clients[cidx], response_buffer);
    break;
  case DISCONNECT:
    if ((clients[cidx].packet.data[0] & 0x0f) != 0) {
      fprintf(stderr, "WARNING: DISCONNECT packet has invalid flags.\n");
    }
    if ((clients[cidx].packet.data[1]) != 0) {
      fprintf(stderr,
              "WARNING: DISCONNECT packet has non-zero remaining length.\n");
    }
    // there is no response to DISCONNECT
    return -1;
    break;
  default:
    fprintf(stderr, "ERROR: invalid/unsupported packet type: %d\n", type);
  }

  if (response_length < 0)
    return -1;
#ifdef DEBUG
  for (int i = 0; i < response_length; ++i) {
    fprintf(stderr, "DEBUG: response byte %d: %.8b, %c\n", i,
            (uint8_t)(response_buffer[i]), response_buffer[i]);
  }
#endif
  // todo: make it possible to write multiple responses, possibly to different
  // clients
  if (response_length > 0) {
    int result = write(clients[cidx].fd, response_buffer, response_length);
    if (result < response_length) {
      fprintf(stderr, "ERROR: Could not write full response.\n");
    }
  }
  return 0;
}

int packet_builder(client_t *clients, int cidx, char *message, int message_len,
                   int client_count) {
  int result = 0;
  // #ifdef DEBUG
  //   for (int i = 0; i < message_len; ++i) {
  //     fprintf(stderr, "DEBUG: byte %d: %.8b, %c\n", i, (uint8_t)(message[i]),
  //             message[i]);
  //   }
  // #endif
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
    result = handle_packet(clients, cidx, client_count);

    // the data of the handled packet is not erased, be careful not to touch it
    if (clients[cidx].packet.current_len == clients[cidx].packet.expected_len) {
      clients[cidx].packet.expected_len = 0;
      clients[cidx].packet.current_len = 0;
    } else {
      // here, there is `expected_len` bytes of a packet that has been handled,
      // and `(current_len - expected_len)` bytes of another packet, so im
      // copying that unhandled part to the start of the buffer and adjusting
      // the lengths
      memcpy(clients[cidx].packet.data,
             &(clients[cidx].packet.data[clients[cidx].packet.expected_len]),
             (clients[cidx].packet.current_len -
              clients[cidx].packet.expected_len));
      clients[cidx].packet.current_len -= clients[cidx].packet.expected_len;
      update_expected_len(&clients[cidx]);
    }
  }
  return result;
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

  int current_size = 0, i, j;
  int nfds = 1;
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
    fprintf(stderr, "Waiting on poll() on %d descriptors...\n", nfds);
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
        fprintf(stderr, "  ERROR: revents = %d\n, expected POLLIN",
                fds[i].revents);
        end_server = TRUE;
        break;
      }
      if (fds[i].fd == listen_sd) {
        fprintf(stderr, "  Listening socket is readable\n");

        do {
          new_sd = accept(listen_sd, NULL, NULL);
          if (new_sd < 0) {
            if (errno != EWOULDBLOCK) {
              perror("  ERROR: accept() failed");
              end_server = TRUE;
            }
            break;
          }

          fprintf(stderr, "  New incoming connection - %d\n", new_sd);

          // + 1 because i always want an empty client at the end with fd set to
          // -1
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
          clients[nfds].fd = -1;
        } while (new_sd != -1);
      } else {
        fprintf(stderr, "  Descriptor %d is readable\n", fds[i].fd);
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
            fprintf(stderr, "  Connection closed on %d\n", fds[i].fd);
            close_conn = TRUE;
            break;
          }

          rec_len = rc;
          fprintf(stderr, "  %d bytes received from fd %d\n", rec_len,
                  fds[i].fd);

          rc = packet_builder(clients, i, receive_buffer, rc, nfds);
          if (rc < 0) {
            fprintf(stderr, "WARNING: disconnecting client.\n");
            close_conn = TRUE;
            break;
          }
          // NOTE: moving responsibility for responding to client into
          // handle_packet

        } while (TRUE);

        if (close_conn) {
          close(fds[i].fd);
          fds[i].fd = -1;
          compress_array = TRUE;
        }
      }
    }

    // array compression after disconnecting client
    if (compress_array) {
      compress_array = FALSE;
      for (i = 0; i < nfds; i++) {
        if (fds[i].fd == -1) {
          free_slist(&(clients[i].topics));
          for (j = i; j < nfds - 1; j++) {
            fds[j].fd = fds[j + 1].fd;
            clients[j].fd = clients[j + i].fd;
            // NOTE: these are huge copies - client.packet has the entire
            // packet
            // buffer, currently 1KB. Allegedly speed is not important, but
            // it
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
    if (fds[i].fd >= 0) {
      free_slist(&(clients[i].topics));
      close(fds[i].fd);
    }
  }
  free(clients);
  free(fds);
}
