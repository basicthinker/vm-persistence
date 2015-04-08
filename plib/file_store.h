//
//  file_store.h
//  vm_persistence
//
//  Created by Jinglei Ren on Apr. 2, 2015.
//  Copyright (c) 2015 Jinglei Ren <jinglei@ren.systems>.
//

#ifndef VM_PERSISTENCE_PLIB_FILE_STORE_H_
#define VM_PERSISTENCE_PLIB_FILE_STORE_H_

#include <cstdio>
#include <cassert>
#include <string>
#include <vector>
#include <atomic>
#include <mutex>

#include "versioned_persistence.h"

namespace plib {

class File {
 public:
  File(FILE *fp = nullptr) { set_filptr(fp); }
  File(const File &other) {
    filptr_ = other.filptr_;
    offset_ = other.offset_;
  }

  FILE *filptr() const { return filptr_; }
  void set_filptr(FILE *fp) {
    filptr_ = fp;
    offset_ = fp ? ftell(fp) : -1;
  }

  off_t offset() const { return offset_; }
  void inc_offset(off_t nbytes) { offset_ += nbytes; }

  void lock() { mutex_.lock(); }
  void unlock() { mutex_.unlock(); }

 private:
  FILE *filptr_;
  off_t offset_;
  std::mutex mutex_;
};

class FileStore : public VersionedPersistence {
 public:
  FileStore(size_t ent_size, const char *name_prefix, int num_out, int num_in);
  ~FileStore();

  void **CheckoutPages(uint64_t timestamp, uint64_t addr[], int n);
  void DestroyPages(void *pages[], int n);

 protected:
  std::vector<File> out_files_; // index 0 is for metadata (versions) 
  std::vector<File> in_files_;

  uint8_t OutIndex() {
    return out_index_++ % (out_files_.size() - 1) + 1;
  }
 private:
  std::atomic_int out_index_;
};

} // namespace plib

#endif // VM_PERSISTENCE_PLIB_FILE_STORE_H_

