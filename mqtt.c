#include <errno.h>
#include <netinet/in.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/poll.h>
#include <sys/queue.h>
#include <time.h>
#include <unistd.h>

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
  while (ptr != NULL) {
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
  char *data;
  int buffer_size;
  int expected_len;
  int current_len;
} packet_t;

typedef struct client {
  packet_t packet;
  int fd;
  uint32_t keep_alive;
  time_t last_message;
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

int handle_connect_packet(client_t *clients, int cidx, char *response,
                          int client_count) {
  int length = 0;
  int position = 0;
  if ((clients[cidx].packet.data[0] & 0x0f) != 0) {
    fprintf(stderr, "ERROR: CONNECT packet has invalid flags.\n");
    return -1;
  }
  length = decode_remaining_length(&(clients[cidx].packet.data[1]),
                                   clients[cidx].packet.expected_len);
  if (length < 0) {
    fprintf(stderr, "ERROR: CONNECT packet length decoding failed.\n");
    return -1;
  }
  position = 1 + length_bytes(length);
  if (clients[cidx].packet.data[position] != 0x0 ||
      clients[cidx].packet.data[position + 1] != 0x4 ||
      clients[cidx].packet.data[position + 2] != 'M' ||
      clients[cidx].packet.data[position + 3] != 'Q' ||
      clients[cidx].packet.data[position + 4] != 'T' ||
      clients[cidx].packet.data[position + 5] != 'T') {

    fprintf(stderr, "ERROR: CONNECT packet has a malformed variable header.\n");
    return -1;
  }
  position += 6;
  if (clients[cidx].packet.data[position] != 0x4) {
    fprintf(stderr,
            "WARNING: CONNECT packet has unacceptable protocol level.\n");
    response[0] = (char)(CONNACK << 4);
    response[1] = 0x2;
    response[2] = 0;
    response[3] = 0x1;
    return 4;
  }

  ++position;

  if (clients[cidx].packet.data[position] != 0x2) {
    fprintf(stderr,
            "WARNING: CONNECT packet has connect flags which arent "
            "supported in this implementation:%d .\n",
            clients[cidx].packet.data[position]);
    return -1;
  }
  ++position;

  clients[cidx].keep_alive += clients[cidx].packet.data[position] << 8;
  clients[cidx].keep_alive += clients[cidx].packet.data[position + 1];
  position += 2;

  int clientid_length = clients[cidx].packet.data[position] << 8;
  clientid_length += clients[cidx].packet.data[position + 1];
  position += 2;
  if (clientid_length > 23) {

    fprintf(stderr, "WARNING: CONNECT packet has client id longer than 23.\n");
    response[0] = (char)(CONNACK << 4);
    response[1] = 0x2;
    response[2] = 0;
    response[3] = 0x2;
    return 4;
  } else {
    memcpy(clients[cidx].client_id, &(clients[cidx].packet.data[position]),
           clientid_length);
    clients[cidx].client_id[clientid_length] = 0; // null-terminating it

    for (int i = 0; i < client_count; ++i) {
      if (i != cidx &&
          strcmp(clients[i].client_id, clients[cidx].client_id) == 0) {
        fprintf(stderr, "WARNING: duplicate id, disconnecting.\n");
        return -1;
      }
    }

    response[0] = (char)(CONNACK << 4);
    response[1] = 0x2;
    response[2] = 0;
    response[3] = 0;
    return 4;
  }
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

  // writing to every client who subscribed to this topic
  for (int i = 0; i < client_count; ++i) { // clients array is null-terminated
    if (clients[i].fd == 0)
      continue;
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
      int result = write(clients[i].fd, response, response_position);
      if (result < response_position) {
        fprintf(stderr, "ERROR: Could not write full response.\n");
      }
    }
  }
  return 0; // we're not sending a response to the client that sent the publish
}

int sub_unsub_packet(client_t *client, char *response) {
  // SUBSCRIBE and UNSUBSCRIBE packets are so similar i'm handling them with one
  // function
  int length = 0;
  int position = 0;
  int sub = (uint8_t)(client->packet.data[0]) >> 4 == SUBSCRIBE;
  if ((client->packet.data[0] & 0x0f) != 2) {
    fprintf(stderr,
            "WARNING: SUB/UNSUB packet has invalid flags, disconnecting.\n");
    return -1;
  }
  length = decode_remaining_length(&(client->packet.data[1]),
                                   client->packet.expected_len);
  if (length < 0) {
    fprintf(stderr, "ERROR: SUB/UNSUB packet length decoding failed.\n");
    return -1;
  }
  position = 1 + length_bytes(length);
  int packet_identifier_position = position;
  position += 2;
  if (position >= client->packet.expected_len) {
    fprintf(stderr,
            "WARNING: SUB/UNSUB packet has no payload, disconnecting.\n");
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
    topic_name[topic_name_length] = 0; // null terminating it
    if (strchr(topic_name, '#') != NULL || strchr(topic_name, '$') != NULL ||
        strchr(topic_name, '+') != NULL) {

      fprintf(stderr, "WARNING: This implementation does not support "
                      "wildcards, disconnecting.\n");
      return -1;
    }
    if (sub == TRUE) {
      insert_val(&(client->topics), topic_name, topic_name_length);
      position += topic_name_length;

      if (client->packet.data[position] != 0) {
        fprintf(stderr, "WARNING: This implementation does not support QoS > "
                        "0, disconnecting.\n");
        return -1;
      }
      ++position;
    } else {
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

  char response_buffer[PCKT_BUF_SIZE];
  int response_length = 0;
  uint8_t type = (uint8_t)clients[cidx].packet.data[0] >> 4;
  switch (type) {
  case PINGREQ:
    if ((clients[cidx].packet.data[0] & 0x0f) != 0) {
      fprintf(stderr,
              "WARNING: PINGREQ packet has invalid flags, disconnecting.\n");
      return -1;
    }
    if ((clients[cidx].packet.data[1]) != 0) {
      fprintf(stderr, "WARNING: PINGREQ packet has non-zero remaining length, "
                      "disconnecting.\n");
      return -1;
    }
    response_buffer[0] = (char)(PINGRESP << 4);
    response_buffer[1] = 0;
    response_length = 2;
    break;
  case CONNECT:
    response_length =
        handle_connect_packet(clients, cidx, response_buffer, client_count);
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
  while (clients[cidx].packet.current_len + message_len >=
         clients[cidx]
             .packet.buffer_size) { // making sure the buffer is large enough
    clients[cidx].packet.buffer_size *= 2;
    clients[cidx].packet.data =
        realloc(clients[cidx].packet.data,
                clients[cidx].packet.buffer_size * sizeof(char));
    if (!clients[cidx].packet.data) {
      perror("ERROR: Reallocation failed");
      exit(-1);
    }
  }
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

    // the data of the handled packet is not erased, only overwritten
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

int disconnect_inactive_clients(client_t *clients, struct pollfd *fds,
                                int nfds) {
  int compress_array = FALSE;
  for (int i = 0; i < nfds; ++i) {
    if (clients[i].keep_alive > 0 && (time(NULL) - clients[i].last_message) >
                                         (clients[i].keep_alive * 1.5)) {
      fprintf(stderr, "WARNING: disconnecting client for inactivity: %d\n",
              fds[i].fd);
      close(fds[i].fd);
      fds[i].fd = -1;
      compress_array = TRUE;
    }
  }
  return compress_array;
}

static volatile int end_server;
static void handle_shutdown(int sig) {
  (void)sig;
  end_server = TRUE; // this breaks out of the while loop after which it frees
                     // and closes all clients
}
int main(int argc, char *argv[]) {
  int return_code, on = 1;
  int listen_sd = -1, new_sd = -1;
  int compress_array = FALSE;
  end_server = FALSE;
  int close_conn;
  char receive_buffer[PCKT_BUF_SIZE];
  struct sockaddr_in6 addr;
  int timeout =
      1000; // we have to wake up every second to check keep_alive times
  int client_buf_size = 8;
  int server_port = 1883;
  signal(SIGINT, handle_shutdown);
  signal(SIGTERM, handle_shutdown);
  if (argc > 1) {
    if (argc != 3) {
      fprintf(stderr, "ERROR: Incorrect number of arguments.\n");
      exit(-1);
    }
    if (strcmp("-p", argv[1]) != 0) {
      fprintf(stderr, "ERROR: Incorrect arguments.\n");
      exit(-1);
    }
    char *end;
    server_port = strtol(argv[2], &end, 10);
    if (end == argv[2] || *end != '\0' || errno == ERANGE) {
      fprintf(stderr, "ERROR: Could not parse port number.\n");
      exit(-1);
    }
  }
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

  int current_size = 0;
  int nfds = 1;
  listen_sd = socket(AF_INET6, SOCK_STREAM, 0);
  if (listen_sd < 0) {
    perror("ERROR: socket() failed");
    exit(-1);
  }

  // reuse socket descriptors
  return_code =
      setsockopt(listen_sd, SOL_SOCKET, SO_REUSEADDR, (char *)&on, sizeof(on));
  if (return_code < 0) {
    perror("ERROR: setsockopt() failed");
    close(listen_sd);
    exit(-1);
  }

  // sets socket to be non-blocking
  return_code = ioctl(listen_sd, FIONBIO, (char *)&on);
  if (return_code < 0) {
    perror("ERROR: ioctl() failed");
    close(listen_sd);
    exit(-1);
  }

  memset(&addr, 0, sizeof(addr));
  addr.sin6_family = AF_INET6;
  memcpy(&addr.sin6_addr, &in6addr_any, sizeof(in6addr_any));
  addr.sin6_port = htons(server_port);
  return_code = bind(listen_sd, (struct sockaddr *)&addr, sizeof(addr));
  if (return_code < 0) {
    perror("ERROR: bind() failed");
    close(listen_sd);
    exit(-1);
  }

  return_code = listen(listen_sd, 32);
  if (return_code < 0) {
    perror("ERROR: listen() failed");
    close(listen_sd);
    exit(-1);
  }

  memset(fds, 0, sizeof(*fds));
  memset(clients, 0, sizeof(*clients));

  fds[0].fd = listen_sd;
  fds[0].events = POLLIN;

  do {
    return_code = poll(fds, nfds, timeout);

    if (return_code < 0) {
      perror("ERROR: poll() failed");
      break;
    }

    if (return_code == 0) { // poll timed out
      compress_array = disconnect_inactive_clients(clients, fds, nfds);
      continue;
    }

    current_size = nfds;
    for (int i = 0; i < current_size; i++) {
      if (fds[i].revents != POLLIN)
        continue;

      if (fds[i].fd == listen_sd) {

        do {
          new_sd = accept(listen_sd, NULL, NULL);
          if (new_sd < 0) {
            if (errno != EWOULDBLOCK) {
              perror("ERROR: accept() failed");
              end_server = TRUE;
            }
            break;
          }

          if (nfds >=
              client_buf_size) { // making sure client buffers are large enough
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
          clients[nfds].packet.buffer_size = 8;
          clients[nfds].last_message = time(NULL);
          SLIST_INIT(&(clients[nfds].topics));
          clients[nfds].packet.data =
              (char *)malloc(sizeof(char) * clients[nfds].packet.buffer_size);
          if (clients[nfds].packet.data == NULL) {
            perror("ERROR: packet_data malloc failed");
            exit(-1);
          }
          nfds++;
        } while (new_sd != -1);
      } else {
        close_conn = FALSE;

        clients[nfds].last_message = time(NULL);
        do {
          // doesnt wait for the client to finish sending
          return_code = recv(fds[i].fd, receive_buffer, sizeof(receive_buffer),
                             MSG_DONTWAIT);
          if (return_code < 0) {
            if (errno != EWOULDBLOCK) {
              perror("ERROR: recv() failed");
              close_conn = TRUE;
            }
            break;
          }

          if (return_code == 0) {
            close_conn = TRUE;
            break;
          }

          return_code =
              packet_builder(clients, i, receive_buffer, return_code, nfds);
          if (return_code < 0) {
            fprintf(stderr, "WARNING: disconnecting client.\n");
            close_conn = TRUE;
            break;
          }
        } while (end_server == FALSE);

        if (close_conn) {
          close(fds[i].fd);
          fds[i].fd = -1;
          compress_array = TRUE;
        }
      }
    }

    // array compression and deallocation of topics and packet data after
    // disconnecting client
    if (compress_array) {
      compress_array = FALSE;
      for (int i = 0; i < nfds; i++) {
        if (fds[i].fd == -1) {
          free_slist(&(clients[i].topics));
          free(clients[i].packet.data);
          for (int j = i; j < nfds - 1; j++) {
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

  for (int i = 0; i < nfds; i++) {
    if (fds[i].fd >= 0) {
      free_slist(&(clients[i].topics));
      free(clients[i].packet.data);
      close(fds[i].fd);
    }
  }
  free(clients);
  free(fds);
}
