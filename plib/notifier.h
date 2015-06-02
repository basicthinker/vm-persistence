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
  void Wait();
  void Notify(int n = 1);
 private:
  sem_t sem_;
};

class SpinningNotifier {
 public:
  SpinningNotifier();
  ~SpinningNotifier();
  void Wait();
  void Notify(int n = 1);
 private:
  void Lock();
  void Unlock();

  pthread_spinlock_t lock_;
  int permits_;
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

inline void SleepingNotifier::Wait() {
  struct timespec timeout;
  if (clock_gettime(CLOCK_REALTIME, &timeout)) {
    perror("[ERROR] SleepingNotifier::Wait clock_gettime()");
    return;
  }
  timeout.tv_sec += 1;
  if (sem_timedwait(&sem_, &timeout)) {
    perror("[WARNING] SleepingNotifier::Wait sem_timedwait()");
  }
}

inline void SleepingNotifier::Notify(int n) {
  for (int i = 0; i < n; ++i) {
    if (sem_post(&sem_)) {
      perror("[ERROR] SleepingNotifier::Notify sem_post()");
    }
  }
}

// Implementation of SpinningNotifier

inline SpinningNotifier::SpinningNotifier() : permits_(0) {
  int err = pthread_spin_init(&lock_, 0);
  if (err) {
    fprintf(stderr, "[ERROR] SpinningNotifier::SpinningNotifier "
        "pthread_spin_init: %s\n", strerror(err));
  }
}

inline SpinningNotifier::~SpinningNotifier() {
  int err = pthread_spin_destroy(&lock_);
  if (err) {
    fprintf(stderr, "[ERROR] SpinningNotifier::SpinningNotifier "
        "pthread_spin_destroy: %s\n", strerror(err));
  }
}

inline void SpinningNotifier::Wait() {
  struct timespec t1;
  if (clock_gettime(CLOCK_REALTIME, &t1)) {
    perror("[ERROR] SpinningNotifier::Wait clock_gettime()");
    return;
  }
  t1.tv_sec += 1;
  struct timespec t2;
  while (true) {
    Lock();
    if (permits_ > 0) {
      --permits_;
      break;
    }
    if (clock_gettime(CLOCK_REALTIME, &t2)) {
      perror("[ERROR] SpinningNotifier::Wait clock_gettime()");
      break;
    }
    if (t2.tv_sec > t1.tv_sec ||
        (t2.tv_sec == t1.tv_sec && t2.tv_nsec >= t1.tv_nsec)) {
      fprintf(stderr, "[WARNING] SpinningNotifier::Wait timed out.\n");
      break;
    }
    Unlock();
  }
  Unlock();
}

inline void SpinningNotifier::Notify(int n) {
  Lock();
  assert(permits_ >= 0);
  permits_ += n;
  Unlock();
}

inline void SpinningNotifier::Lock() {
  int err = pthread_spin_lock(&lock_);
  assert(!err);
}

inline void SpinningNotifier::Unlock() {
  int err = pthread_spin_unlock(&lock_);
  assert(!err);
}

} // namespace plib

#endif // VM_PERSISTENCE_PLIB_NOTIFIER_H_

