//
//  group_committer.h
//  vm_persistence
//
//  Created by Jinglei Ren on May 11, 2015.
//  Copyright (c) 2015 Jinglei Ren <jinglei@ren.systems>.
//

#ifndef VM_PERSISTENCE_PLIB_GROUP_COMMITER_H_
#define VM_PERSISTENCE_PLIB_GROUP_COMMITER_H_

#include <cassert>
#include <condition_variable>
#include <cstdint>
#include <atomic>
#include <mutex>
#include <thread>
#include <vector>
#include <error.h>
#include <pthread.h>
#include <sys/ioctl.h>
#include "format.h"
#include "group_buffer.h"
#include "versioned_persistence.h"

namespace plib {

template <typename DataEntry>
class GroupCommitter : public VersionedPersistence<DataEntry> {
 public:
  GroupCommitter(const char *dev, int block_bits, int num_lanes, int size);

  void *Submit(DataEntry data[], uint32_t n) { return data; }
  int Commit(void *handle, uint64_t timestamp,
      uint64_t metadata[], uint32_t n);

  void **CheckoutPages(uint64_t timestamp, uint64_t addr[], int n);
  void DestroyPages(void *pages[], int n) {}

 private:
  const int block_bits_;
  int fildes_;

  GroupBuffer buffer_;
  std::vector<std::thread> flushers_;
  std::condition_variable ready_;
  std::mutex mutex_;

  static void Flush(GroupBuffer *buffer,
    std::condition_variable *ready, std::mutex *mutex);
};

template <typename DataEntry>
inline GroupCommitter<DataEntry>::GroupCommitter(
    const char *dev, int blk_bits, int nlanes, int size) :
    block_bits_(blk_bits), buffer_(nlanes, size) {
  /*fildes_ = open(dev, O_RDWR);
  if (fildes_ < 0) {
    perror(dev);
    return;
  }*/

  for (int i = 0; i < nlanes; ++i) {
    flushers_.push_back(std::thread(Flush, &buffer_, &ready_, &mutex_));
  }
}

template <typename DataEntry>
inline int GroupCommitter<DataEntry>::Commit(void *handle, uint64_t timestamp,
    uint64_t metadata[], uint32_t n) {
  //if (fildes_ < 0) return -EIO;

  size_t data_size = sizeof(DataEntry) * n;
  size_t len = CRC32DataLength(data_size);
  char data_buf[len];
  CRC32DataEncode(data_buf, timestamp, handle, data_size);

  sem_t sem;
  sem_init(&sem, 0, 1);
 
  ready_.notify_one();
  int8_t *dest = buffer_.Lock(len);
  memcpy(dest, data_buf, len);
  buffer_.Release(dest, &sem);

  return sem_wait(&sem);
}

template <typename DataEntry>
void GroupCommitter<DataEntry>::Flush(
    GroupBuffer *buffer, std::condition_variable *ready, std::mutex *mutex) {
  std::unique_lock<std::mutex> lock(*mutex);
  while (true) {
    ready->wait(lock);
    int8_t *mem = buffer->BeginFlush();
    // TODO
    for (int i = 0; i < 100000; ++i) {}
    printf("flushed %p\n", mem);
    buffer->EndFlush(mem);
  }
}

template <typename DataEntry>
inline void **GroupCommitter<DataEntry>::CheckoutPages(uint64_t timestamp,
    uint64_t addr[], int n) {
  return nullptr;
}

} // namespace plib

#endif // VM_PERSISTENCE_PLIB_GROUP_COMMITER_H_

