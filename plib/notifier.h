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

  template<class Pre, class Wakeup>
  void Wait(Pre pre, Wakeup wakeup);

  template<class Pre, class Wakeup, class Timeout>
  void WaitFor(int seconds, Pre pre, Wakeup wakeup, Timeout timeout);

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

template<class Pre, class Wakeup>
inline void SleepingNotifier::Wait(Pre pre, Wakeup wakeup) {
  std::unique_lock<std::mutex> lock(mutex_);
  if (pre() == kRelease) return;
  do {
    condition_.wait(lock);
  } while (wakeup() == kWait);
}

template<class Pre, class Wakeup, class Timeout>
inline void SleepingNotifier::WaitFor(int sec,
    Pre pre, Wakeup wakeup, Timeout timeout) {
  std::unique_lock<std::mutex> lock(mutex_);
  if (pre() == kRelease) return;
  std::cv_status status;
  auto time = std::chrono::system_clock::now() + std::chrono::seconds(sec);
  do {
    status = condition_.wait_until(lock, time);
  } while (status == std::cv_status::no_timeout && wakeup() == kWait);
  if (status == std::cv_status::timeout) timeout();
}

inline void SleepingNotifier::NotifyAll() {
  std::unique_lock<std::mutex> lock(mutex_);
  condition_.notify_all();
}

} // namespace plib

#endif // VM_PERSISTENCE_PLIB_NOTIFIER_H_

