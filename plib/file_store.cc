//
//  file_store.cc
//  vm_persistence
//
//  Created by Jinglei Ren on Apr. 2, 2015.
//  Copyright (c) 2015 Jinglei Ren <jinglei@ren.systems>.
//

#include "file_store.h"

using namespace plib;

FileStore::FileStore(size_t ent_size, const char *name_prefix,
    int num_out, int num_in) : VersionedPersistence(ent_size) {
  assert(ent_size && num_out < 0xff); // index is 8-bit

  std::string name(name_prefix);
  mode_t mode = S_IRUSR | S_IWUSR | S_IRGRP;

  std::string meta_name = name + "0";
  int fd = open(meta_name.c_str(), O_RDWR | O_CREAT, mode);
  assert(fd > 0);
  lseek(fd, 0, SEEK_END);
  out_files_.push_back(fd);

  for (int i = 1; i <= num_out; ++i) {
    std::string data_name = name + std::to_string(i);
    int fd = open(data_name.c_str(), O_RDWR | O_CREAT, mode);
    assert(fd > 0);
    lseek(fd, 0, SEEK_END);
    out_files_.push_back(fd);
  }

  out_index_ = 0;

  for (int i = 0; i < num_in; ++i) {
    int fd = open(meta_name.c_str(), O_RDONLY);
    assert(fd > 0);
    in_files_.push_back(fd);
  }
}

FileStore::~FileStore() {
  for (File &f : out_files_) {
    f.lock();
    close(f.descriptor());
  }

  for (File &f : in_files_) {
    f.lock();
    close(f.descriptor());
  }
}

void **FileStore::CheckoutPages(uint64_t timestamp, uint64_t addr[], int n) {
  return nullptr; //TODO
}

void FileStore::DestroyPages(void *pages[], int n) {
  for (int i = 0; i < n; ++i) {
    free(pages[i]);
  }
}

