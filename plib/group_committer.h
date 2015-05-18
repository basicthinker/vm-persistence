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
#include <cstdint>
#include <atomic>
#include <chrono>
#include <thread>
#include <vector>

#include <error.h>
#include <pthread.h>
#include <semaphore.h>
#include <linux/nvme.h>
#include <sys/ioctl.h>

#include "format.h"
#include "group_buffer.h"
#include "versioned_persistence.h"

namespace plib {

template <typename DataEntry>
class GroupCommitter : public VersionedPersistence<DataEntry> {
 public:
  GroupCommitter(const char *dev, int block_bits, int num_lanes, int size);
  ~GroupCommitter();

  void *Submit(DataEntry data[], uint32_t n) { return data; }
  int Commit(void *handle, uint64_t timestamp,
      uint64_t metadata[], uint32_t n);

  int block_bits() const { return block_bits_; }
  int single_buffer_size() const { return buffer_.single_size(); }
  int fildes() const { return fildes_; }
  GroupBuffer &buffer() { return buffer_; }
  bool running() const { return running_; }
  sem_t *flush_sem() { return &flush_sem_; }

  uint64_t slba(int nblocks) {
    return slba_.fetch_add(nblocks, std::memory_order_relaxed);
  }

  void **CheckoutPages(uint64_t timestamp, uint64_t addr[], int n);
  void DestroyPages(void *pages[], int n) {}

 private:
  const int block_bits_;
  int fildes_;
  std::atomic_uint_fast64_t slba_;

  GroupBuffer buffer_;
  std::vector<std::thread> flushers_;
  sem_t flush_sem_;
  bool running_;

  static void Flush(GroupCommitter<DataEntry> *committer);
};

template <typename DataEntry>
inline GroupCommitter<DataEntry>::GroupCommitter(
    const char *dev, int blk_bits, int nlanes, int size) :
    block_bits_(blk_bits), slba_(0), buffer_(nlanes, size) {
  fildes_ = open(dev, O_RDWR);
  if (fildes_ < 0) {
    perror(dev);
    return;
  }

  sem_init(&flush_sem_, 0, 0);
  running_ = true;
  for (int i = 0; i < nlanes; ++i) {
    flushers_.push_back(std::thread(Flush, this));
  }
}

template <typename DataEntry>
inline GroupCommitter<DataEntry>::~GroupCommitter() {
  if (fildes_ < 0) return;
  running_ = false;
  for (size_t i = 0; i < flushers_.size(); ++i) {
    sem_post(&flush_sem_);
  }
  for (std::thread &t : flushers_) {
    t.join();
  }
  sem_destroy(&flush_sem_);
}

template <typename DataEntry>
inline int GroupCommitter<DataEntry>::Commit(void *handle, uint64_t timestamp,
    uint64_t metadata[], uint32_t n) {
  if (fildes_ < 0) return -EIO;

  size_t data_size = sizeof(DataEntry) * n;
  size_t len = CRC32DataLength(data_size);
  char data_buf[len];
  CRC32DataEncode(data_buf, timestamp, handle, data_size);

  sem_t commit_sem;
  int err = sem_init(&commit_sem, 0, 0);
  if (err) {
    perror("[Error] sem_init() in GroupCommitter::Commit():");
    return err;
  }
 
  int8_t *dest = buffer_.Lock(len, &flush_sem_);
  memcpy(dest, data_buf, len);
  buffer_.Release(dest, &commit_sem);

  err = sem_wait(&commit_sem);
  sem_destroy(&commit_sem);
  return err;
}

template <typename DataEntry>
void GroupCommitter<DataEntry>::Flush(GroupCommitter<DataEntry> *committer) {
  while (true) {
    sem_wait(committer->flush_sem());
    if (!committer->running()) break;
    int8_t *mem;
    do {
      mem = committer->buffer().BeginFlush();

      int n = (committer->single_buffer_size() >> committer->block_bits());
      struct nvme_user_io io = {};
      io.opcode = nvme_cmd_write;
      io.addr = (unsigned long)mem;
      io.slba = committer->slba(n);
      io.nblocks = n - 1;
      io.dsmgmt = NVME_RW_DSM_LATENCY_LOW;
      if (ioctl(committer->fildes(), NVME_IOCTL_SUBMIT_IO, &io)) {
        perror("Error: ioctl");
      }

    } while (committer->buffer().EndFlush(mem));
  }
}

template <typename DataEntry>
inline void **GroupCommitter<DataEntry>::CheckoutPages(uint64_t timestamp,
    uint64_t addr[], int n) {
  return nullptr;
}

} // namespace plib

#endif // VM_PERSISTENCE_PLIB_GROUP_COMMITER_H_

