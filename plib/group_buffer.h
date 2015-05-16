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
  std::vector<sem_t *> sems;

  BufferMeta() : flag(EMPTY) {}
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
  void EndFlush(int8_t *buffer);

 private:
  int FindEmpty();

  int8_t *buffer_;
  const int num_lanes_;
  const int single_size_;

  int index_;
  int offset_;
  BufferMeta *meta_;
  pthread_spinlock_t lock_; // for any update on the overall buffer state
};

inline GroupBuffer::GroupBuffer(int num_lanes, int single_size) :
    num_lanes_(num_lanes), single_size_(single_size),
    index_(0), offset_(0) {
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
inline int GroupBuffer::FindEmpty() {
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
    if (meta_[index_].flag == BufferMeta::EMPTY) {
      return index_;
    }
    if (idx > 0 && meta_[idx].flag == BufferMeta::EMPTY) {
      return idx;
    }
  }
}

inline int8_t *GroupBuffer::Lock(int len, sem_t *flush_sem) {
  pthread_spin_lock(&lock_);
  if (meta_[index_].flag != BufferMeta::EMPTY) {
    index_ = FindEmpty();
    offset_ = 0;
  } else {
    if (offset_ + len > single_size_) {
      meta_[index_].flag = BufferMeta::FULL;
      sem_post(flush_sem);
      index_ = FindEmpty();
      offset_ = 0;
    }
  }

  int8_t *p = buffer(index_) + offset_;
#ifdef TRACE
  printf("GroupBuffer::Lock on %d [%d] (%d)\n",
         index_, offset_, meta_[index_].flag);
#endif
  offset_ += len;
  return p;
}

inline void GroupBuffer::Release(int8_t *mem, sem_t *commit_sem) {
  assert((mem - buffer_) / single_size_ == index_);
  meta_[index_].sems.push_back(commit_sem);
#ifdef TRACE
  printf("GroupBuffer::Release on %d (%d)\n", index_, meta_[index_].flag);
#endif
  pthread_spin_unlock(&lock_);
}

inline int8_t *GroupBuffer::BeginFlush() {
  int i_busy = -1;
  pthread_spin_lock(&lock_);
  for (int i = 0; i < num_buffers(); ++i) {
    if (meta_[i].flag == BufferMeta::FULL) {
      meta_[i].flag = BufferMeta::BUSY;
      i_busy = i;
      break;
    }
  }

  if (i_busy < 0) {
    assert(meta_[index_].flag == BufferMeta::EMPTY);
    meta_[index_].flag = BufferMeta::BUSY;
    i_busy = index_;
  }
  pthread_spin_unlock(&lock_);
#ifdef TRACE
  printf("GroupBuffer::BeginFlush on %d (=>%d)\n", i_busy, meta_[i_busy].flag);
#endif
  return buffer(i_busy);
}

inline void GroupBuffer::EndFlush(int8_t *buf) {
  int idx = (buf - buffer_) / single_size_;
  assert(idx < num_buffers());

  for (sem_t *sem : meta_[idx].sems) {
    sem_post(sem);
  }
  meta_[idx].sems.clear();

  assert(meta_[idx].flag == BufferMeta::BUSY);
  meta_[idx].flag = BufferMeta::EMPTY;
#ifdef TRACE
  printf("GroupBuffer::EndFlush on %d (%d)\n", idx, meta_[idx].flag);
#endif
}

} // namespace plib

#endif // VM_PERSISTENCE_PLIB_GROUP_BUFFER_H_

