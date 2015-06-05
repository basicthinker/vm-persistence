//
//  writer.h
//  vm_persistence
//
//  Created by Jinglei Ren on May 24, 2015.
//  Copyright (c) 2015 Jinglei Ren <jinglei@ren.systems>.
//

#ifndef VM_PERSISTENCE_PLIB_WRITER_H_
#define VM_PERSISTENCE_PLIB_WRITER_H_

#include <thread>
#include <chrono>
#include <fcntl.h>
#include <linux/nvme.h>
#include <sys/ioctl.h>

namespace plib {

class Writer {
 public:
  virtual int Write(void *mem, int len, uint64_t addr, int flag) = 0;
  virtual ~Writer() {}
};

class SleepWriter : public Writer {
 public:
  SleepWriter(int latency, int bandwidth); // MB/s
  int Write(void *mem, int len, uint64_t addr, int flag);

 private:
  int latency_; // microsec
  int bandwidth_; // bytes/usec
};

class NVMeWriter : public Writer {
 public:
  NVMeWriter(const char *dev, int block_bits);
  int Write(void *mem, int len, uint64_t addr, int flag);

 private:
  const int block_bits_;
  const size_t block_mask_;
  int fildes_;

  int NumBlocks(int size) {
    size_t n = (size >> block_bits_) + ((size & block_mask_) > 0);
    return n;
  }
};

// Implementation of SleepWriter

inline SleepWriter::SleepWriter(int latency, int bandwidth) :
    latency_(latency), bandwidth_(bandwidth) {
}

inline int SleepWriter::Write(void *mem, int len, uint64_t addr, int flag) {
  int usec = latency_ + len / bandwidth_;
  std::this_thread::sleep_for(std::chrono::microseconds(usec));
  return 0;
}

// Implementation of NVMeWriter

inline NVMeWriter::NVMeWriter(const char *dev, int block_bits) :
    block_bits_(block_bits), block_mask_((1 << block_bits) - 1) {
  fildes_ = open(dev, O_RDWR);
  if (fildes_ < 0) {
    perror("[ERROR] NVMeWriter::NVMeWriter open()");
    exit(EXIT_FAILURE);
  }
}

inline int NVMeWriter::Write(void *mem, int len, uint64_t addr, int flag) {
  struct nvme_user_io io = {};
  io.opcode = nvme_cmd_write;
  io.addr = (unsigned long)mem;
  io.slba = (addr >> block_bits_);
  io.nblocks = NumBlocks(len) - 1;
  io.dsmgmt |= flag;
  if (ioctl(fildes_, NVME_IOCTL_SUBMIT_IO, &io)) {
    perror("[ERROR] NVMeWriter::Write ioctl()");
    return -1;
  }
  return 0;
}

} // namespace plib

#endif // VM_PERSISTENCE_PLIB_WRITER_H_

