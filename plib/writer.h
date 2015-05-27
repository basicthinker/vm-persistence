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

namespace plib {

class Writer {
 public:
  virtual int Write(void *mem, int len, uint64_t addr) = 0;
  virtual ~Writer() {}
};

class SleepWriter : public Writer {
 public:
  SleepWriter(int latency, int bandwidth); // MB/s
  int Write(void *mem, int len, uint64_t addr);
 private:
  int latency_; // microsec
  int bandwidth_; // byte/usec
};

SleepWriter::SleepWriter(int latency, int bandwidth) :
    latency_(latency), bandwidth_(bandwidth) {
}

int SleepWriter::Write(void *mem, int len, uint64_t addr) {
  int usec = latency_ + len / bandwidth_;
  std::this_thread::sleep_for(std::chrono::microseconds(usec));
  return 0;
}

} // namespace plib

#endif // VM_PERSISTENCE_PLIB_WRITER_H_

