//
//  nvme_store.h
//  vm_persistence
//
//  Created by Jinglei Ren on Apr. 11, 2015.
//  Copyright (c) 2015 Jinglei Ren <jinglei@ren.systems>.
//

#ifndef VM_PERSISTENCE_PLIB_NVME_STORE_H_
#define VM_PERSISTENCE_PLIB_NVME_STORE_H_

#include <vector>
#include <atomic>
#include <cassert>
#include <linux/nvme.h>
#include <sys/ioctl.h>
#include "format.h"
#include "versioned_persistence.h"

namespace plib {

template <typename DataEntry>
class NVMeStore : public VersionedPersistence<DataEntry> {
 public:
  NVMeStore(int block_bits, const char *devices[], int n);
  void *Submit(DataEntry data[], uint32_t n);
  int Commit(void *handle, uint64_t timestamp,
      uint64_t metadata[], uint32_t n);

  void **CheckoutPages(uint64_t timestamp, uint64_t addr[], int n);
  void DestroyPages(void *pages[], int n) { }
 private:
  const int block_bits_;
  const size_t block_mask_;
  std::vector<int> fildes_; // index 0 is for metadata (versions)
  std::vector<std::atomic_uint_fast64_t> slba_;

  uint8_t OutIndex(uint64_t timestamp) {
    return timestamp % (fildes_.size() - 1) + 1;
  }

  uint16_t NumBlocks(size_t size) {
    return (size >> block_bits_) + ((size & block_mask_) > 0);
  }
};

template <typename DataEntry>
inline NVMeStore<DataEntry>::NVMeStore(int blk_bits, const char *dev[], int n) :
    block_bits_(blk_bits), block_mask_((1 << blk_bits) - 1), slba_(n) {
  for (int i = 0; i < n; ++i) {
    int fd = open(dev[i], O_RDWR);
    assert(fd > 0);
    fildes_.push_back(fd);
  }
  // TODO: avoid overwriting existent data
}

template <typename DataEntry>
inline void *NVMeStore<DataEntry>::Submit(DataEntry data[], uint32_t n) {
  uint32_t size = sizeof(DataEntry) * n;

  void *handle = malloc(NumBlocks(size) << block_bits_);
  memcpy(handle, data, size);
  return handle;
}

template <typename DataEntry>
inline int NVMeStore<DataEntry>::Commit(void *handle, uint64_t timestamp,
    uint64_t metadata[], uint32_t n) {
  uint8_t index = OutIndex(timestamp);
  uint16_t nblocks = NumBlocks(sizeof(DataEntry) * n);

  struct nvme_user_io io;
  io.opcode = nvme_cmd_write;
  io.flags = 0;
  io.control = 0;
  io.metadata = (unsigned long)0;
  io.addr = (unsigned long)handle;
  io.nblocks = nblocks - 1;
  io.slba = slba_[index].fetch_add(nblocks, std::memory_order_relaxed);
  io.dsmgmt = NVME_RW_DSM_LATENCY_LOW | NVME_RW_DSM_SEQ_REQ;
  io.reftag = 0;
  io.apptag = 0;
  io.appmask = 0;

  int err = ioctl(fildes_[index], NVME_IOCTL_SUBMIT_IO, &io);
  if (err) return err;
  free(handle);

  nblocks = NumBlocks(MetaLength(n));
  char meta_buf[nblocks << block_bits_];
  io.opcode = nvme_cmd_write;
  io.flags = 0;
  io.control = 0;
  io.metadata = (unsigned long)0;
  io.addr = (unsigned long)meta_buf;
  io.nblocks = nblocks - 1;
  io.slba = slba_[0].fetch_add(nblocks, std::memory_order_relaxed);
  io.dsmgmt = NVME_RW_DSM_LATENCY_LOW | NVME_RW_DSM_SEQ_REQ;
  io.reftag = 0;
  io.apptag = 0;
  io.appmask = 0;

  return ioctl(fildes_[0], NVME_IOCTL_SUBMIT_IO, &io);
}

template <typename DataEntry>
inline void **NVMeStore<DataEntry>::CheckoutPages(uint64_t timestamp,
    uint64_t addr[], int n) {
  return nullptr;
}

} // namespace plib

#endif // VM_PERSISTENCE_PLIB_NVME_STORE_H_

