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
  if (sem_wait(&sem_)) {
    perror("[ERROR] SleepingNotifier::Wait sem_waite()");
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
  while (true) {
    pthread_spin_lock(&lock_);
    if (permits_) {
      --permits_;
      pthread_spin_unlock(&lock_);
      return;
    }
    pthread_spin_unlock(&lock_);
  }
}

inline void SpinningNotifier::Notify(int n) {
  pthread_spin_lock(&lock_);
  assert(permits_ >= 0);
  permits_ += n;
  pthread_spin_unlock(&lock_);
}

} // namespace plib

#endif // VM_PERSISTENCE_PLIB_NOTIFIER_H_

