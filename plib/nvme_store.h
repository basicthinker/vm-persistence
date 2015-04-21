//
//  nvme_store.h
//  vm_persistence
//
//  Created by Jinglei Ren on Apr. 11, 2015.
//  Copyright (c) 2015 Jinglei Ren <jinglei@ren.systems>.
//

#ifndef VM_PERSISTENCE_PLIB_NVME_STORE_H_
#define VM_PERSISTENCE_PLIB_NVME_STORE_H_

#include <atomic>
#include <cassert>
#include <error.h>
#include <linux/nvme.h>
#include <sys/ioctl.h>
#include "format.h"
#include "versioned_persistence.h"
#include <iostream>
#include <chrono>

using namespace std::chrono;
using microsec = duration<double, std::ratio<1,1000000>>;

namespace plib {

template <typename DataEntry>
class NVMeStore : public VersionedPersistence<DataEntry> {
 public:
  NVMeStore(int block_bits, const char *devices, uint64_t offset);
  void *Submit(DataEntry data[], uint32_t n) { return data; }
  int Commit(void *handle, uint64_t timestamp,
      uint64_t metadata[], uint32_t n);

  void **CheckoutPages(uint64_t timestamp, uint64_t addr[], int n);
  void DestroyPages(void *pages[], int n) { }
 private:
  const int block_bits_;
  const size_t block_mask_;
  int fildes_;
  std::atomic_uint_fast64_t meta_slba_;
  std::atomic_uint_fast64_t data_slba_;
  FlashStriper striper_;

  uint16_t NumBlocks(size_t size) {
    return (size >> block_bits_) + ((size & block_mask_) > 0);
  }
};

template <typename DataEntry>
inline NVMeStore<DataEntry>::NVMeStore(int blk_bits,
    const char *dev, uint64_t offset) :
    block_bits_(blk_bits), block_mask_((1 << blk_bits) - 1),
    meta_slba_(0), data_slba_(offset), striper_(8, 3) {
  fildes_ = open(dev, O_RDWR);
  if (fildes_ < 0) {
    std::cerr << "Failed to open " << dev << std::endl;
    exit(EXIT_FAILURE);
  }
}

template <typename DataEntry>
inline int NVMeStore<DataEntry>::Commit(void *handle, uint64_t timestamp,
    uint64_t metadata[], uint32_t n) {
  struct nvme_user_io io;
  memset(&io, 0, sizeof(io));
  int err;
#ifdef PERF_TRACE
  high_resolution_clock::time_point t1, t2;
#endif
  uint16_t nblocks = NumBlocks(sizeof(DataEntry) * n);
  uint64_t slba = data_slba_.fetch_add(nblocks, std::memory_order_relaxed);

  for (unsigned long i = 0; i < nblocks; ++i) {
    io.opcode = nvme_cmd_write;
    io.metadata = 0;
    io.addr = (unsigned long)handle + (i << block_bits_);
    io.slba = striper_.Translate(slba + i);
    io.nblocks = 0;
    io.dsmgmt = NVME_RW_DSM_LATENCY_LOW;
#ifdef PERF_TRACE
    high_resolution_clock::time_point t1 = high_resolution_clock::now();
#endif
    err = ioctl(fildes_, NVME_IOCTL_SUBMIT_IO, &io);
    if (err) return err;
#ifdef PERF_TRACE
    high_resolution_clock::time_point t2 = high_resolution_clock::now();
    std::cout << io.slba << '\t' << io.nblocks << '\t';
    std::cout << duration_cast<microsec>(t2 - t1).count() << std::endl;
#endif
  }

  nblocks = NumBlocks(MetaLength(n));
  slba = meta_slba_.fetch_add(nblocks, std::memory_order_relaxed);
  char meta_buf[nblocks << block_bits_];
  for (unsigned long i = 0; i < nblocks; ++i) {
    io.opcode = nvme_cmd_write;
    io.metadata = 0;
    io.addr = (unsigned long)meta_buf + (i << block_bits_);
    io.slba = striper_.Translate(slba + i);
    io.nblocks = 0;
    io.dsmgmt = NVME_RW_DSM_LATENCY_LOW;
#ifdef PERF_TRACE
    t1 = high_resolution_clock::now();
#endif
    err = ioctl(fildes_, NVME_IOCTL_SUBMIT_IO, &io);
    if (err) return err;
#ifdef PERF_TRACE
    t2 = high_resolution_clock::now();
    std::cout << io.slba << '\t' << io.nblocks << '\t';
    std::cout << duration_cast<microsec>(t2 - t1).count() << std::endl;
#endif
  }
  return err;
}

template <typename DataEntry>
inline void **NVMeStore<DataEntry>::CheckoutPages(uint64_t timestamp,
    uint64_t addr[], int n) {
  return nullptr;
}

} // namespace plib

#endif // VM_PERSISTENCE_PLIB_NVME_STORE_H_

