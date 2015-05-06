//
//  file_store.h
//  vm_persistence
//
//  Created by Jinglei Ren on Apr. 2, 2015.
//  Copyright (c) 2015 Jinglei Ren <jinglei@ren.systems>.
//

#ifndef VM_PERSISTENCE_PLIB_FILE_STORE_H_
#define VM_PERSISTENCE_PLIB_FILE_STORE_H_

#include <cassert>
#include <string>
#include <vector>
#include <atomic>
#include <mutex>

#include <fcntl.h>
#include <unistd.h>

#include "versioned_persistence.h"

namespace plib {

class File {
 public:
  File(int fd = -1) { set_descriptor(fd); }
  File(const File &other) {
    descriptor_ = other.descriptor_;
    offset_ = other.offset_;
  }

  int descriptor() const { return descriptor_; }
  void set_descriptor(int fd) {
    descriptor_ = fd;
    offset_ = fd >= 0 ? lseek(fd, 0, SEEK_CUR) : -1;
  }

  off_t offset() const { return offset_; }
  void inc_offset(off_t nbytes) { offset_ += nbytes; }

  void lock() { mutex_.lock(); }
  void unlock() { mutex_.unlock(); }

 private:
  int descriptor_;
  off_t offset_;
  std::mutex mutex_;
};

template <typename DataEntry>
class FileStore : public VersionedPersistence<DataEntry> {
 public:
  FileStore(const char *name_prefix, int num_files);
  ~FileStore();

  void **CheckoutPages(uint64_t timestamp, uint64_t addr[], int n);
  void DestroyPages(void *pages[], int n);

  unsigned int sync_freq() const { return sync_freq_; }
  void set_sync_freq(unsigned int freq) { sync_freq_ = freq; }

 protected:
  std::vector<File> out_files_; // index 0 is reserved for metadata (versions)

  unsigned int seq_num() { return seq_num_++; }
  uint8_t OutIndex(unsigned int seq) {
    return seq % (out_files_.size() - 1) + 1;
  }
 private:
  std::atomic_uint seq_num_;
  unsigned int sync_freq_;
};

// Implementation

template <typename DataEntry>
FileStore<DataEntry>::FileStore(const char *prefix, int num_files) :
    seq_num_(0), sync_freq_(-1) {
  assert(num_files < 0xff); // index is 8-bit

  std::string name(prefix);
  mode_t mode = S_IRUSR | S_IWUSR | S_IRGRP;

  std::string meta_name = name + "0";
  int fd = open(meta_name.c_str(), O_RDWR | O_CREAT, mode);
  assert(fd > 0);
  lseek(fd, 0, SEEK_END);
  out_files_.push_back(fd);

  for (int i = 1; i <= num_files; ++i) {
    std::string data_name = name + std::to_string(i);
    int fd = open(data_name.c_str(), O_RDWR | O_CREAT, mode);
    assert(fd > 0);
    lseek(fd, 0, SEEK_END);
    out_files_.push_back(fd);
  }
}

template <typename DataEntry>
FileStore<DataEntry>::~FileStore() {
  for (File &f : out_files_) {
    f.lock();
    close(f.descriptor());
  }
}

template <typename DataEntry>
void **FileStore<DataEntry>::CheckoutPages(uint64_t timestamp, uint64_t addr[], int n) {
  return nullptr; //TODO
}

template <typename DataEntry>
void FileStore<DataEntry>::DestroyPages(void *pages[], int n) {
  for (int i = 0; i < n; ++i) {
    free(pages[i]);
  }
}

} // namespace plib

#endif // VM_PERSISTENCE_PLIB_FILE_STORE_H_

