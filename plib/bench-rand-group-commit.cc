//
//  bench-group-commit.cc
//  vm_persistence
//
//  Created by Jinglei Ren on May 27, 2015.
//  Copyright (c) 2015 Jinglei Ren <jinglei@ren.systems>.
//

#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <cstring>

#include <atomic>
#include <chrono>
#include <thread>

#include "group_committer.h"
#include "writer.h"

std::atomic_int_fast64_t g_total_num(0);

std::chrono::high_resolution_clock::time_point t1, t2;
uint64_t n1, n2;

void DoPersist(plib::GroupCommitter *committer, const int size) {
  using namespace std::chrono;

  char mem[size];
  while (true) {
    uint64_t seq = g_total_num++;
    if (seq == n1) t1 = high_resolution_clock::now();
    else if (seq == n2) t2 = high_resolution_clock::now();
    else if (seq > n2) break;

    int err = committer->Commit(seq, mem, rand() % size + 100);
    assert(!err);
  }
}

int main(int argc, const char *argv[]) {
  using namespace std::chrono;
  if (argc < 7) {
    printf("Usage: %s METHOD NUM_TRANS SIZE_TRANS #THREADS #LANES GROUP_SIZE\n",
           argv[0]);
    return 1;
  }

  const char *method = argv[1];
  const int num_trans = atoi(argv[2]);
  const int size_trans = atoi(argv[3]);
  const int num_threads = atoi(argv[4]);
  const int num_lanes = atoi(argv[5]);
  const int group_size = atoi(argv[6]);

  plib::Writer *writer = nullptr;
  if (strcmp(method, "sleep") == 0) {
    writer = new plib::SleepWriter(50, 200);
  } else if (strcmp(method, "file") == 0) {
    //TODO hard coded parameter
    writer = new plib::FileWriter("file_writer.data");
  } else if (strcmp(method, "nvme") == 0) {
    //TODO hard coded parameter
    writer = new plib::NVMeWriter("/dev/nvme0n1p1", 9);
  } else {
    fprintf(stderr, "Warning: unknown persistence method %s!\n", method);
  }
  plib::GroupCommitter committer(num_lanes, group_size, *writer);
  // commit from the main thread
  char mem[group_size];
  int err = committer.Commit(0, mem, group_size);
  assert(!err);

  n1 = num_trans * 0.1;
  n2 = num_trans * 0.9;

  std::thread threads[num_threads];
  for (std::thread &t : threads) {
    t = std::thread(DoPersist, &committer, size_trans);
  }
  for (std::thread &t : threads) {
    t.join();
  }
  uint64_t nsec = duration_cast<nanoseconds>(t2 - t1).count();
  double thr = (n2 - n1) * size_trans * 1000 / (double)nsec; // MB/s
  uint64_t latency = num_threads * nsec / (n2 - n1) / 1000; // usec
  printf("%f\t%lu\n", thr, latency);

  if (writer) delete writer;
}

