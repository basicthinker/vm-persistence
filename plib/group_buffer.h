//
//  group_buffer.h
//  vm_persistence
//
//  Created by Jinglei Ren on May 11, 2015.
//  Copyright (c) 2015 Jinglei Ren <jinglei@ren.systems>.
//

#ifndef VM_PERSISTENCE_PLIB_GROUP_BUFFER_H_
#define VM_PERSISTENCE_PLIB_GROUP_BUFFER_H_

#include <vector>
#include <pthread.h>
#include <semaphore.h>

namespace plib {

struct BufferMeta {
  enum Flag { EMPTY, FULL, BUSY };

  Flag flag;
  int offset;
  std::vector<sem_t *> sems;

  BufferMeta() : flag(EMPTY), offset(0) {}
};

class GroupBuffer {
 public:
  GroupBuffer(int num_lanes, int single_size);
  ~GroupBuffer();

  int8_t *buffer(int index) const { return buffer_ + single_size_ * index; }
  int num_buffers() const { return num_lanes_ << 1; }

  int8_t *Lock(int len, sem_t *flush_sem);
  void Release(int8_t *mem, sem_t *commit_sem);

  int8_t *BeginFlush();
  bool EndFlush(int8_t *buffer); // Returns whether to clear

 private:
  void FindEmpty();
  bool AllEmpty();
  void NotifyThreads(int index);

  int8_t *buffer_;
  const int num_lanes_;
  const int single_size_;

  int index_;
  BufferMeta *meta_;
  pthread_spinlock_t lock_; // for any update on the overall buffer state
};

inline GroupBuffer::GroupBuffer(int num_lanes, int single_size) :
    num_lanes_(num_lanes), single_size_(single_size), index_(0) {
  buffer_ = (int8_t *)malloc(num_buffers() * single_size_);
  meta_ = new BufferMeta[num_buffers()];
  pthread_spin_init(&lock_, PTHREAD_PROCESS_PRIVATE);
}

inline GroupBuffer::~GroupBuffer() {
  pthread_spin_destroy(&lock_);
  delete[] meta_;
  free(buffer_);
}

// Assumes lock_ is locked and index_ is not EMPTY
inline void GroupBuffer::FindEmpty() {
  while (true) {
    int idx = -1;
    pthread_spin_unlock(&lock_);
    for (int i = 0; i < num_buffers(); ++i) {
      if (meta_[i].flag == BufferMeta::EMPTY) {
        idx = i;
        break;
      }
    }
    pthread_spin_lock(&lock_);
    if (meta_[index_].flag == BufferMeta::EMPTY) { // others have found
      return;
    }
    if (idx >= 0 && meta_[idx].flag == BufferMeta::EMPTY) {
      index_ = idx;
      assert(meta_[index_].offset == 0);
      return;
    }
  }
}

inline int8_t *GroupBuffer::Lock(int len, sem_t *flush_sem) {
  pthread_spin_lock(&lock_);
  if (meta_[index_].flag != BufferMeta::EMPTY) {
    FindEmpty();
  }
  while (meta_[index_].offset + len > single_size_) {
    meta_[index_].flag = BufferMeta::FULL;
    sem_post(flush_sem);
    FindEmpty();
  }

  int &offset = meta_[index_].offset;
  int8_t *p = buffer(index_) + offset;
#ifdef TRACE
  printf("<%lu> GroupBuffer::Lock on %d [%d] (%d)\n",
         std::hash<std::thread::id>()(std::this_thread::get_id()),
         index_, offset, meta_[index_].flag);
#endif
  offset += len;
  return p;
}

inline void GroupBuffer::Release(int8_t *mem, sem_t *commit_sem) {
  assert((mem - buffer_) / single_size_ == index_);
  meta_[index_].sems.push_back(commit_sem);
  pthread_spin_unlock(&lock_);
}

inline int8_t *GroupBuffer::BeginFlush() {
  int idx = -1;
  while (true) {
    for (int i = 0; i < num_buffers(); ++i) {
      if (meta_[i].flag == BufferMeta::FULL) {
        idx = i;
        break;
      }
    }
    pthread_spin_lock(&lock_);
    if (meta_[idx].flag == BufferMeta::FULL) {
      break;
    } else {
      pthread_spin_unlock(&lock_);
    }
  }

  meta_[idx].flag = BufferMeta::BUSY;
#ifdef TRACE
  printf("Waiting queues:");
  for (int i = 0; i < num_buffers(); ++i) {
    printf("\t%lu [%d] (%d)",
           meta_[i].sems.size(), meta_[i].offset, meta_[i].flag);
  }
  printf("\n<%lu> GroupBuffer::BeginFlush on %d (=>%d)\n",
         std::hash<std::thread::id>()(std::this_thread::get_id()),
         idx, meta_[idx].flag);
#endif
  pthread_spin_unlock(&lock_);
  return buffer(idx);
}

inline bool GroupBuffer::EndFlush(int8_t *buf) {
  int idx = (buf - buffer_) / single_size_;
  assert(idx < num_buffers());

  NotifyThreads(idx);

  assert(meta_[idx].flag == BufferMeta::BUSY);
  meta_[idx].offset = 0;
  meta_[idx].flag = BufferMeta::EMPTY;
#ifdef TRACE
  printf("<%lu> GroupBuffer::EndFlush on %d (%d)\n",
         std::hash<std::thread::id>()(std::this_thread::get_id()),
         idx, meta_[idx].flag);
#endif

  if (!AllEmpty()) return false; // no need for clear yet

  std::this_thread::sleep_for(std::chrono::microseconds(1));
 
  pthread_spin_lock(&lock_);
  if (!AllEmpty()) {
    pthread_spin_unlock(&lock_);
    return false;
  }

  printf("Final clear...\n");

  idx = -1;
  for (int i = 0; i < num_buffers(); ++i) {
    if (meta_[i].offset) {
      assert(idx == -1);
      idx = i;
    }
  }
  if (idx < 0) { // already clear
    pthread_spin_unlock(&lock_);
    return false;
  }

  int offset;
  do {
    offset = meta_[idx].offset;
    NotifyThreads(idx);
    pthread_spin_unlock(&lock_);
    std::this_thread::sleep_for(std::chrono::microseconds(1));
    pthread_spin_lock(&lock_);
    if (meta_[idx].flag != BufferMeta::EMPTY) {
      pthread_spin_unlock(&lock_);
      return false;
    }
  } while (offset != meta_[idx].offset);
  if (offset) meta_[idx].flag = BufferMeta::FULL;
  pthread_spin_unlock(&lock_);
  return offset;
}

// Utils

inline bool GroupBuffer::AllEmpty() {
  for (int i = 0; i < num_buffers(); ++i) {
    if (meta_[i].flag != BufferMeta::EMPTY) {
      return false;
    }
  }
  return true;
}

inline void GroupBuffer::NotifyThreads(int index) {
  for (sem_t *sem : meta_[index].sems) {
    sem_post(sem);
  }
  meta_[index].sems.clear();
}

} // namespace plib

#endif // VM_PERSISTENCE_PLIB_GROUP_BUFFER_H_

