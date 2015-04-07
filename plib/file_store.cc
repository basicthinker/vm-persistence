//
//  file_store.cc
//  vm_persistence
//
//  Created by Jinglei Ren on Apr. 2, 2015.
//  Copyright (c) 2015 Jinglei Ren <jinglei@ren.systems>.
//

#include "file_store.h"

using namespace plib;

FileStore::FileStore(const char *name_prefix, int num_out, int num_in) {
  assert(num_out < 0xff); // index is 8-bit
  File file;

  std::string name(name_prefix);
  std::string meta_name = name + "0";
  FILE *fp = fopen(meta_name.c_str(), "r+b");
  if (!fp) {
    file.set_filptr(fopen(meta_name.c_str(), "wb"));
    assert(file.filptr());
  } else {
    int err = fseek(fp, 0, SEEK_END);
    assert(!err);
    file.set_filptr(fp);
  }
  out_files_.push_back(file);

  for (int i = 1; i <= num_out; ++i) {
    std::string data_name = name + std::to_string(i);
    FILE *fp = fopen(data_name.c_str(), "r+b");
    if (!fp) {
      file.set_filptr(fopen(data_name.c_str(), "wb"));
      assert(file.filptr());
    } else {
      int err = fseek(fp, 0, SEEK_END);
      assert(!err);
      file.set_filptr(fp);
    }
    out_files_.push_back(file);
  }

  out_index_ = 0;

  for (int i = 0; i < num_in; ++i) {
    FILE *fp = fopen(meta_name.c_str(), "rb");
    assert(fp);
    in_files_.push_back(fp);
  }
}

FileStore::~FileStore() {
  for (File &f : out_files_) {
    f.lock();
    fclose(f.filptr());
  }

  for (File &f : in_files_) {
    f.lock();
    fclose(f.filptr());
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

