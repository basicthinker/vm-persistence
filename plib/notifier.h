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
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <thread>

namespace plib {

class SleepingNotifier {
 public:
  template<class Lambda>
  auto TakeAction(Lambda lambda);

  template<class Pre, class Lambda>
  void Wait(Pre pre, Lambda lambda);

  template<class Pre, class Lambda, class Timeout>
  void WaitFor(int seconds, Pre pre, Lambda lambda, Timeout timeout);

  void NotifyAll();

  static const bool kWait = false;
  static const bool kRelease = true;

 private:
  std::mutex mutex_;
  std::condition_variable condition_;
};

// Implementation of SleepingNotifier

template<class Lambda>
inline auto SleepingNotifier::TakeAction(Lambda lambda) {
  std::unique_lock<std::mutex> lock(mutex_);
  return lambda();
}

template<class Pre, class Lambda>
inline void SleepingNotifier::Wait(Pre pre, Lambda lambda) {
  std::unique_lock<std::mutex> lock(mutex_);
  if (pre() == kRelease) return;
  do {
    condition_.wait(lock);
  } while (lambda() == kWait);
}

template<class Pre, class Lambda, class Timeout>
inline void SleepingNotifier::WaitFor(int sec,
    Pre pre, Lambda lambda, Timeout timeout) {
  std::unique_lock<std::mutex> lock(mutex_);
  if (pre() == kRelease) return;
  std::cv_status status;
  auto time = std::chrono::system_clock::now() + std::chrono::seconds(sec);
  do {
    status = condition_.wait_until(lock, time);
  } while (status == std::cv_status::no_timeout && lambda() == kWait);
  if (status == std::cv_status::timeout) timeout();
}

inline void SleepingNotifier::NotifyAll() {
  condition_.notify_all();
}

} // namespace plib

#endif // VM_PERSISTENCE_PLIB_NOTIFIER_H_

