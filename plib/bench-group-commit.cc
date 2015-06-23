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

std::atomic_uint_fast64_t g_total_num(0);

std::chrono::high_resolution_clock::time_point t1, t2;
uint64_t n1, n2;
double ckpt_bytes;

void Checkpointing(plib::Writer *writer,
    int ckpt_len, double throughput) {
  uint64_t addr = 100 * (uint64_t(1) << 30); // 100 GB
  char buffer[ckpt_len];
  uint64_t addr1 = 0, addr2 = 0;
  int interval = ckpt_len / throughput;
  while (true) {
    if (!addr1 && g_total_num > n1) {
      addr1 = addr;
    } else if (g_total_num > n2) {
      addr2 = addr;
      break;
    }
    writer->Write(buffer, ckpt_len, addr, NVME_RW_DSM_LATENCY_IDLE);
    addr += ckpt_len;
    if (throughput) {
      std::this_thread::sleep_for(std::chrono::microseconds(interval));
    }
  }
  ckpt_bytes = addr2 - addr1;
}

void DoPersist(plib::GroupCommitter *committer, int size) {
  using namespace std::chrono;

  char mem[size];
  while (true) {
    uint64_t seq = g_total_num++;
    if (seq == n1) t1 = high_resolution_clock::now();
    else if (seq == n2) t2 = high_resolution_clock::now();
    else if (seq > n2) break;

    if (!size) size = 100 + rand() % 12000;
    int err = committer->Commit(seq, mem, size, NVME_RW_DSM_LATENCY_LOW);
    assert(!err);
  }
}

int main(int argc, const char *argv[]) {
  using namespace std::chrono;
  if (argc != 7 && argc != 9) {
    printf("Usage: %s METHOD SIZE_TRANS #TRANS #THREADS BUFFER_SIZE #BUFFERS "
        "[CKPT_LEN THROUGHPUT]\n", argv[0]);
    return 1;
  }

  const char *method = argv[1];
  const int size_trans = atoi(argv[2]); // 0 denotes random
  const int num_trans = atoi(argv[3]);
  const int num_threads = atoi(argv[4]);
  const int buffer_size = atoi(argv[5]);
  const int num_buffers = atoi(argv[6]);
  const int ckpt_len = (argc == 9) ? atoi(argv[7]) : 0; // 0 denotes no ckpt
  const double ckpt_throughput = // MB/s
      (argc == 9) ? atof(argv[8]) : 0; // user-specified 0 indicates no limit

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
  plib::GroupCommitter committer(buffer_size, num_buffers, *writer);

  // commit from the main thread
  char mem[buffer_size * num_buffers];
  int err = committer.Commit(0, mem, buffer_size * num_buffers);
  assert(!err);

  n1 = num_trans * 0.1;
  n2 = num_trans * 0.9;

  std::thread ckpt_thread;
  if (ckpt_len) {
    ckpt_thread = std::thread(Checkpointing, writer, ckpt_len, ckpt_throughput);
  }

  std::thread threads[num_threads];
  for (std::thread &t : threads) {
    t = std::thread(DoPersist, &committer, size_trans);
  }
  for (std::thread &t : threads) {
    t.join();
  }
  if (ckpt_thread.joinable()) ckpt_thread.join();

  uint64_t nsec = duration_cast<nanoseconds>(t2 - t1).count();
  double thr = (n2 - n1) * size_trans * 1000 / (double)nsec; // MB/s
  uint64_t latency = num_threads * nsec / (n2 - n1) / 1000; // usec
  printf("%f\t%lu\n", thr, latency);

  if (ckpt_len) printf("%f\n", ckpt_bytes * 1000 / nsec); // MB/s

  if (writer) delete writer;
}

