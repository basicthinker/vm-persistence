//
//  file_store.cc
//  vm_persistence
//
//  Created by Jinglei Ren on Apr. 2, 2015.
//  Copyright (c) 2015 Jinglei Ren <jinglei@ren.systems>.
//

#include "file_store.h"

using namespace plib;

FileStore::FileStore(int num_streams, const char *name_prefix) {
  std::string name(name_prefix);
  std::string meta_name = name + "0";
  File file(fopen(meta_name.c_str(), "r+b"));
  if (!file.descriptor) {
    file.descriptor = fopen(meta_name.c_str(), "wb");
    assert(file.descriptor);
  } else {
    int err = fseek(file.descriptor, 0, SEEK_END);
    assert(!err);
  }
  files_.push_back(file);

  for (int i = 1; i <= num_streams; ++i) {
    std::string data_name = name + std::to_string(i);
    file.descriptor = fopen(data_name.c_str(), "r+b");
    if (!file.descriptor) {
      file.descriptor = fopen(data_name.c_str(), "wb");
      assert(file.descriptor);
    } else {
      int err = fseek(file.descriptor, 0, SEEK_END);
      assert(!err);
    }
    files_.push_back(file);
  }
}

FileStore::~FileStore() {
  for (File &f : files_) {
    std::lock_guard<std::mutex> lock(f.mutex);
    fclose(f.descriptor);
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

