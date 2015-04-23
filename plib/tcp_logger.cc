//
//  tcp_store.cc
//  vm_persistence
//
//  Created by Jinglei Ren on Apr. 23, 2015.
//  Copyright (c) 2015 Jinglei Ren <jinglei@ren.systems>.
//

#include <cassert>
#include <cstdlib>
#include <cstdio>
#include <cstring>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <errno.h>

#define DATA_SIZE (4096)

void Receive(int sock, char *mem, int len) {
  ssize_t status;
  while (true) {
    status = recv(sock, mem, len, 0);
    if (status < 0) {
      fprintf(stderr, "Receive failed: %s\n", strerror(errno));
    } else {
      mem += status;
      len -= status;
      if (!len) return;
    }
  }
}

void Send(int sock, char *mem, int len) {
  ssize_t status;
  while (true) {
    status = send(sock, mem, len, 0);
    if (status < 0) {
      fprintf(stderr, "Send failed: %s\n", strerror(errno));
    } else {
      mem += status;
      len -= status;
      if (!len) return;
    }
  }
}

int main(int argc, const char *argv[]) {
  if (argc < 2) {
    fprintf(stdout, "Usage: %s [PORT]\n", argv[0]);
    exit(EXIT_FAILURE);
  }
  int port = atoi(argv[1]);
  int sock = socket(AF_INET, SOCK_STREAM, 0);
  if (sock < 0) {
    fprintf(stderr, "Failed to create socket!");
    exit(EXIT_FAILURE);
  }

  sockaddr_in server;
  memset(&server, 0, sizeof(server));
  server.sin_family = AF_INET;
  server.sin_addr.s_addr = INADDR_ANY;
  server.sin_port = htons(port);
  if (bind(sock, (sockaddr *)&server, sizeof(server))) {
    fprintf(stderr, "Failed to bind port %d: %s\n", port, strerror(errno));
    exit(EXIT_FAILURE);
  }

  listen(sock, 5);
  sockaddr_in client;
  socklen_t len = sizeof(client);
  int conn = accept(sock, (sockaddr *)&client, &len);
  if (conn < 0) {
    fprintf(stderr, "Failed to accept: %s\n", strerror(errno));
    exit(EXIT_FAILURE);   
  }

  uint32_t size = 0;
  char ack = 1;
  char mem[DATA_SIZE];
  while (true) {
    Receive(conn, (char *)&size, sizeof(size));
    assert(size < DATA_SIZE);
    Receive(conn, mem, size);
    Send(conn, &ack, 1);
    Receive(conn, mem, sizeof(uint64_t) * 2); // TODO
    Send(conn, &ack, 1);
  }
}

