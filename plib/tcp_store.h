//
//  tcp_store.h
//  vm_persistence
//
//  Created by Jinglei Ren on Apr. 21, 2015.
//  Copyright (c) 2015 Jinglei Ren <jinglei@ren.systems>.
//

#ifndef VM_PERSISTENCE_PLIB_TCP_STORE_H_
#define VM_PERSISTENCE_PLIB_TCP_STORE_H_

#include "versioned_persistence.h"

#include <cstdio>
#include <cassert>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include "format.h"

namespace plib {

template <typename DataEntry>
class TcpStore : public VersionedPersistence<DataEntry> {
 public:
  TcpStore(const char *host, int port);
  ~TcpStore();

  void *Submit(DataEntry data[], uint32_t n);
  int Commit(void *handle, uint64_t timestamp,
      uint64_t metadata[], uint32_t n);

  void **CheckoutPages(uint64_t timestamp, uint64_t addr[], int n);
  void DestroyPages(void *pages[], int n) { }
 private:
  int sock_fd_;
  sockaddr_in ResolveHostName(const char *host);
};

template <typename DataEntry>
inline TcpStore<DataEntry>::TcpStore(const char *host, int port) {
  sock_fd_ = socket(AF_INET, SOCK_STREAM, 0);
  if (sock_fd_ < 0) {
    fprintf(stderr, "Failed to create socket!\n");
    exit(EXIT_FAILURE);
  }

  sockaddr_in server = ResolveHostName(host);
  server.sin_family = AF_INET; 
  server.sin_port = htons(port);
  if (connect(sock_fd_, (sockaddr *)&server, sizeof(server))) {
    fprintf(stderr, "Failed to connect to %s: %s\n", host, strerror(errno));
    exit(EXIT_FAILURE);
  }
}

template <typename DataEntry>
inline TcpStore<DataEntry>::~TcpStore() {
  close(sock_fd_);
}

template <typename DataEntry>
inline sockaddr_in TcpStore<DataEntry>::ResolveHostName(const char *host) {
  addrinfo *info;
  sockaddr_in addr;
  int err = getaddrinfo(host, nullptr, nullptr, &info);
  if (err) {
    fprintf(stderr, "Failed to resolve host %s: %s\n", host, strerror(err));
    exit(EXIT_FAILURE);
  }
  memcpy(&addr, info->ai_addr, sizeof(addr));
  freeaddrinfo(info);
  return addr;
}

template <typename DataEntry>
inline void *TcpStore<DataEntry>::Submit(DataEntry data[], uint32_t n) {
  const ssize_t buf_len = sizeof(n) + sizeof(DataEntry) * n;
  char *const buffer = (char *)malloc(buf_len);

  char *cur = buffer;
  uint32_t size = sizeof(DataEntry) * n;
  cur = Serialize(cur, size);
  cur = Serialize(cur, data, size);
  assert(cur - buffer == buf_len);

  cur = buffer;
  size = buf_len;
  ssize_t status;
  for (int i = 0; i < 100; ++i) {
    status = send(sock_fd_, cur, size, MSG_DONTWAIT);
    if (status < 0) {
      fprintf(stderr, "Send failed on submit: %s\n", strerror(errno));
    } else {
      cur += status;
      size -= status;
      if (!size) return buffer;
    }
  }
  return nullptr;
}

template <typename DataEntry>
inline int TcpStore<DataEntry>::Commit(void *handle, uint64_t timestamp,
    uint64_t metadata[], uint32_t n) {
  char ack = 0;
  if (recv(sock_fd_, &ack, 1, 0) != 1) {
    fprintf(stderr, "Submit ack not received: %s\n", strerror(errno));
    return EIO;
  }
  if (ack != 1) {
    fprintf(stderr, "Submit ack not correct: %d\n", (int)ack);
    return EIO;
  }
  free(handle);

  char *buf = (char *)metadata;
  assert(n >= 2); // TODO
  size_t size = sizeof(uint64_t) * 2;
  ssize_t status = 0;
  for (int i = 0; i < 100; ++i) {
    status = send(sock_fd_, buf, size, 0);
    if (status < 0) {
      fprintf(stderr, "Send failed on commit: %s\n", strerror(errno));
    } else {
      buf += status;
      size -= status;
      if (!size) break;
    }
  }
  ack = 0;
  if (recv(sock_fd_, &ack, 1, 0) != 1) {
    fprintf(stderr, "Commit ack not received: %s\n", strerror(errno));
    return EIO;
  }
  if (ack != 1) {
    fprintf(stderr, "Commit ack not correct: %d\n", (int)ack);
    return EIO;
  }
  return 0;
}

template <typename DataEntry>
inline void **TcpStore<DataEntry>::CheckoutPages(uint64_t timestamp,
    uint64_t addr[], int n) {
  return nullptr;
}

} // namespace plib

#endif // VM_PERSISTENCE_PLIB_TCP_STORE_H_

