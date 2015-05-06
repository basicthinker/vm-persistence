//
//  bench-size-thread.cc
//  vm_persistence
//
//  Created by Jinglei Ren on Apr. 15, 2015.
//  Copyright (c) 2015 Jinglei Ren <jinglei@ren.systems>.
//

#include <atomic>
#include <cassert>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <thread>

#include "sync_file_store.h"
#include "async_file_store.h"
#include "mem_store.h"
#include "nvme_store.h"
#include "tcp_store.h"

using DataEntry = int64_t;

plib::VersionedPersistence<DataEntry> *persist = nullptr;
std::atomic<int64_t> sum_time(0);

void DoPersist(int num_entries, int num_runs) {
  using namespace std::chrono;

  DataEntry mem[num_entries];
  high_resolution_clock::time_point t1 = high_resolution_clock::now();
  for (int i = 0; i < num_runs; ++i) {
    void *handle = persist->Submit(mem, num_entries);
    int err = persist->Commit(handle, 0, nullptr, 0);
    assert(!err);
  }
  high_resolution_clock::time_point t2 = high_resolution_clock::now();
  sum_time += duration_cast<nanoseconds>(t2 - t1).count() / num_runs;
}

int main(int argc, const char *argv[]) {
  if (argc < 4) {
    printf("Usage: %s METHOD SIZE THREADS\n", argv[0]);
    return 1;
  }

  const char *method = argv[1];
  int size = atoi(argv[2]);
  int num_threads = atoi(argv[3]);

  if (strcmp(method, "sync") == 0) {
    static plib::SyncFileStore<DataEntry> sync("log_sync_", num_threads);
    persist = &sync;
  } else if (strcmp(method, "async") == 0) {
    static plib::AsyncFileStore<DataEntry> async("log_async_", num_threads);
    persist = &async;
  } else if (strcmp(method, "mem") == 0) {
    // TODO hard coded parameter
    static plib::MemStore<DataEntry> mem(1000);
    persist = &mem;
  } else if (strcmp(method, "nvme") == 0) {
    // TODO hard coded parameters
    const char *dev = "/dev/nvme0n1p1";
    static plib::NVMeStore<DataEntry> nvme(9, dev, 256000000);
    persist = &nvme;
  } else if (strcmp(method, "tcp") == 0) {
    // TODO hard coded port
    static plib::TcpStore<DataEntry> tcp("localhost", 4000);
    persist = &tcp;
  } else {
    fprintf(stderr, "Warning: unknown persistence method %s!\n", method);
  }

  std::thread threads[num_threads];
  for (std::thread &t : threads) {
    t = std::thread(DoPersist, size / sizeof(DataEntry), 10000);
  }
  for (std::thread &t : threads) {
    t.join();
  }
  printf("%lu\n", sum_time / num_threads);
}

