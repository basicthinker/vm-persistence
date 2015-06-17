//
//  notifier.h
//  vm_persistence
//
//  Created by Jinglei Ren on May 27, 2015.
//  Copyright (c) 2015 Jinglei Ren <jinglei@ren.systems>.
//

#ifndef VM_PERSISTENCE_PLIB_NOTIFIER_H_
#define VM_PERSISTENCE_PLIB_NOTIFIER_H_

#include <cassert>
#include <pthread.h>
#include <semaphore.h>

namespace plib {

class SleepingNotifier {
 public:
  SleepingNotifier();
  ~SleepingNotifier();
  int Wait();
  void Notify(int n = 1);
 private:
  sem_t sem_;
};

class SpinningNotifier {
 public:
  SpinningNotifier();
  int Wait();
  void Notify(int n = 1);
 private:
  std::atomic_bool waiting_ alignas(64);
  std::atomic_int permits_ alignas(64);
  alignas(64) const int padding = 0;
};

// Implementation of SleepingNotifier

inline SleepingNotifier::SleepingNotifier() {
  if (sem_init(&sem_, 0, 0)) {
    perror("[ERROR] SleepingNotifier::SleepingNotifier sem_init()");
  }
}

inline SleepingNotifier::~SleepingNotifier() {
  if (sem_destroy(&sem_)) {
    perror("[ERROR] SleepingNotifier::~SleepingNotifier sem_destroy()");
  }
}

inline int SleepingNotifier::Wait() {
  struct timespec timeout;
  if (clock_gettime(CLOCK_REALTIME, &timeout)) {
    perror("[ERROR] SleepingNotifier::Wait clock_gettime()");
    return -1;
  }
  timeout.tv_sec += 2;
  if (sem_timedwait(&sem_, &timeout)) {
    perror("[WARNING] SleepingNotifier::Wait sem_timedwait()");
    return -1;
  }
  return 0;
}

inline void SleepingNotifier::Notify(int n) {
  for (int i = 0; i < n; ++i) {
    if (sem_post(&sem_)) {
      perror("[ERROR] SleepingNotifier::Notify sem_post()");
    }
  }
}

// Implementation of SpinningNotifier

inline SpinningNotifier::SpinningNotifier() : waiting_(true), permits_(0) {}

inline int SpinningNotifier::Wait() {
  struct timespec t1;
  if (clock_gettime(CLOCK_REALTIME, &t1)) {
    perror("[ERROR] SpinningNotifier::Wait clock_gettime()");
    return -1;
  }
  t1.tv_sec += 2;
  struct timespec t2;
  while (true) {
    if (!waiting_) {
      if (--permits_ == 0) {
        waiting_ = true;
        assert(!permits_);
      }
      return 0;
    }
    if (clock_gettime(CLOCK_REALTIME, &t2)) {
      perror("[ERROR] SpinningNotifier::Wait clock_gettime()");
      return -1;
    }
    if (t2.tv_sec > t1.tv_sec ||
        (t2.tv_sec == t1.tv_sec && t2.tv_nsec >= t1.tv_nsec)) {
      fprintf(stderr, "[WARNING] SpinningNotifier::Wait timed out.\n");
      return -1;
    }
  }
}

inline void SpinningNotifier::Notify(int n) {
  permits_ += n;
  waiting_ = false;
}

} // namespace plib

#endif // VM_PERSISTENCE_PLIB_NOTIFIER_H_

