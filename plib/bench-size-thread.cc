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
#include "group_committer.h"

using DataEntry = int64_t;

plib::VersionedPersistence<DataEntry> *persist = nullptr;
std::atomic<int64_t> sum_latency(0);

void DoPersist(int num_entries, int num_runs) {
  using namespace std::chrono;

  DataEntry mem[num_entries];
  high_resolution_clock::time_point t1 = high_resolution_clock::now();
  for (int i = 0; i < num_runs; ++i) {
    void *handle = persist->Submit(mem, num_entries);
    int err = persist->Commit(handle, 0, nullptr, num_entries);
    assert(!err);
  }
  high_resolution_clock::time_point t2 = high_resolution_clock::now();
  sum_latency += duration_cast<nanoseconds>(t2 - t1).count() / num_runs;
}

int main(int argc, const char *argv[]) {
  using namespace std::chrono;

  if (argc < 5) {
    printf("Usage: %s METHOD BLOCK_SIZE #THREADS #RUNS [#LANES GROUP_SIZE]\n",
           argv[0]);
    return 1;
  }

  const char *method = argv[1];
  int block_size = atoi(argv[2]);
  int num_threads = atoi(argv[3]);
  int num_runs = atoi(argv[4]);

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
  } else if (strcmp(method, "group") == 0) {
    // TODO hard coded parameters
    const char *dev = "/dev/nvme0n1p1";
    int nlanes = atoi(argv[5]);
    int group_size = atoi(argv[6]);
    static plib::GroupCommitter<DataEntry> group(dev, 9, nlanes, group_size);
    persist = &group;
  } else {
    fprintf(stderr, "Warning: unknown persistence method %s!\n", method);
  }

  std::thread threads[num_threads];
  for (std::thread &t : threads) {
    t = std::thread(DoPersist, block_size / sizeof(DataEntry), num_runs);
  }
  for (std::thread &t : threads) {
    t.join();
  }
  double thr = 1000 / (double)sum_latency * num_threads * block_size * num_threads; // MB/s
  printf("%lu\t%f\n", sum_latency / num_threads, thr);
}

