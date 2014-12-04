#include <thread>
#include <string>
#include <vector>
#include <cstring>
#include <chrono>
#include <iostream>

#include "slib/mem_allocator.h"
#include "string_hashtable.h"
#include "generator.h"

#if defined(SLIB)
#define HASHTABLE SLibHashtable
#include "slib_hashtable.h"

#elif defined(SVM)
#include "svm_hashtable.h"
#define HASHTABLE SVMHashtable

#elif defined(TBB)
#include "tbb_hashtable.h"
#define HASHTABLE TBBHashtable

#else
#include "lock_hashtable.h"
#define HASHTABLE LockHashtable
#endif

using namespace std;
using std::chrono::high_resolution_clock;
using std::chrono::duration;
using std::chrono::duration_cast;

#define SVM_SIZE (0x100000000)

uint64_t Hash(uint64_t value) {
  return value;
}

std::string BuildKeyName(uint64_t key_num) {
  key_num = Hash(key_num);
  std::string str("user");
  str.append(std::to_string(key_num));
  return str;
}

void LoadData(StringHashtable<const char *> *table,
      int key_begin, int key_end, int *count) {
#ifdef SVM
  int err = sitevm_enter();
  assert(!err);
  HASHTABLE<const char *> *instance =
      (HASHTABLE<const char *> *)table->Instance();
  err = sitevm_open_and_update(instance->svm());
  assert(!err);
#endif
  *count = 0;
  for (int key = key_begin; key < key_end; ++key) {
    char *k = NewZeroHashString(BuildKeyName(key).c_str());
    char *v = NewZeroHashString(string(100, 'a' + key % 26).c_str());
    table->Insert(k, v);
    *count += 1;
  }
#ifdef SVM
  sitevm_exit();
#endif
}

void Workload(StringHashtable<const char *> *table,
      int num_ops, int max_key, int *count) {
#ifdef SVM
  int err = sitevm_enter();
  assert(!err);
  HASHTABLE<const char *> *instance =
      (HASHTABLE<const char *> *)table->Instance();
  err = sitevm_open_and_update(instance->svm());
  assert(!err);
#endif
  UniformIntGenerator op_chooser(0, 1);
  UniformIntGenerator key_chooser(0, max_key);
  *count = 0;
  for (int i = 0; i < num_ops; ++i) {
    uint64_t key = key_chooser.NextInt();
    char *k = NewZeroHashString(BuildKeyName(key).c_str());
    if (op_chooser.NextInt()) {
      const char *v = table->Get(k);
      string value(LoadString(v));
      assert(value.length() == 100);
      for (char c : value) {
        assert(key % 26 + 'a' == c);
      }
    } else {
      char *v = NewZeroHashString(string(100, 'A' + key % 26).c_str());
      FREE(table->Update(k, v));
    }
    *count += 1;
  }
#ifdef SVM
  sitevm_exit();
#endif
}

void JoinThreads(vector<thread> &threads);

bool CheckCounts(vector<int> &counts, int value);

int main(int argc, const char *argv[]) {

  if (argc != 4) {
    cout << "USAGE: " << argv[0] << "#threads #records #operations" << endl;
    return EINVAL;
  }

  const int num_threads = atoi(argv[1]);
  const int num_records = atoi(argv[2]);
  const int num_ops = atoi(argv[3]);

#ifdef SVM
  int err = sitevm_init();
  assert(!err);

  sitevm_seg_t* segment = sitevm_seg_create(NULL, SVM_SIZE);
  sitevm_malloc::init_sitevm_malloc(segment);

  err = sitevm_enter();
  assert(!err);
  err = sitevm_open_and_update(segment);
  assert(!err);
  StringHashtable<const char *> table(new HASHTABLE<const char *>(segment));
#else
  StringHashtable<const char *> table(new HASHTABLE<const char *>);
#endif

  vector<thread> threads(num_threads);
  vector<int> counts(num_threads);

  high_resolution_clock::time_point t1 = high_resolution_clock::now();
  int tr = num_records / num_threads;
  for (int i = 0; i < num_threads; ++i) {
    threads[i] = thread(LoadData, &table, tr * i, tr * (i + 1), &counts[i]);
  }
  JoinThreads(threads);
  assert(CheckCounts(counts, tr));
  high_resolution_clock::time_point t2 = high_resolution_clock::now();
  duration<double> time_span = duration_cast<duration<double>>(t2 - t1);
  cout << "\tSTATS-LoadData\t" << num_records / time_span.count() << endl;

  t1 = high_resolution_clock::now();
  int to = num_ops / num_threads;
  for (int i = 0; i < num_threads; ++i) {
    threads[i] = thread(Workload, &table, to, num_records - 1, &counts[i]);
  }
  JoinThreads(threads);
  assert(CheckCounts(counts, to));
  t2 = high_resolution_clock::now();
  time_span = duration_cast<duration<double>>(t2 - t1);
  cout << "\tSTATS-Workload\t" << num_ops / time_span.count() << endl;

  delete table.Instance();
}

void JoinThreads(vector<thread> &threads) {
  for (thread &t : threads) {
    t.join();
  }
}

bool CheckCounts(vector<int> &counts, int value) {
  for (const int &n : counts) {
    if (n != value) cout << "\tMISMATCH with " << value << ": " << n << endl;
  }
  return true;
}

