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
  template<class Pre, class Lambda, class Post>
  auto Wait(Pre pre, Lambda lambda, Post post);

  template<class Pre>
  void NotifyAll(Pre pre);

  static const bool kWait = false;
  static const bool kRelease = true;

 private:
  std::mutex mutex_;
  std::condition_variable condition_;
};

// Implementation of SleepingNotifier

template<class Pre, class Lambda, class Post>
inline auto SleepingNotifier::Wait(Pre pre, Lambda lambda, Post post) {
  std::unique_lock<std::mutex> lock(mutex_);
  if (pre() == kRelease) return post();
  condition_.wait_for(lock, std::chrono::seconds(2), std::move(lambda));
  return post();
}

template<class Pre>
inline void SleepingNotifier::NotifyAll(Pre pre) {
  mutex_.lock();
  pre();
  mutex_.unlock();
  condition_.notify_all();
}

} // namespace plib

#endif // VM_PERSISTENCE_PLIB_NOTIFIER_H_

