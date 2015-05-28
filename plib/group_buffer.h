//
//  group_buffer.h
//  vm_persistence
//
//  Created by Jinglei Ren on May 11, 2015.
//  Copyright (c) 2015 Jinglei Ren <jinglei@ren.systems>.
//

#ifndef VM_PERSISTENCE_PLIB_GROUP_BUFFER_H_
#define VM_PERSISTENCE_PLIB_GROUP_BUFFER_H_

#include <algorithm>
#include <atomic>

#include "waitlist.h"

namespace plib {

class GroupBuffer {
 public:
  // (1 << num_shifts) equals to the number of partitions.
  // (1 << partition_shifts) equals to the size of a partition.
  // (1 << waitlist_shifts) equals to the length of each waiting list.
  GroupBuffer(int num_shifts, int partition_shifts, int waitlist_shifts);
  ~GroupBuffer();

  int buffer_size() const { return buffer_mask_ + 1; }
  int num_partitions() const { return 1 << num_shifts_; }
  int partition_size() const { return partition_mask_ + 1; }
  int chunk_size() const { return chunk_mask_ + 1; }

  bool IsFlusher(uint64_t addr, int len) const;
  int8_t *GetBuffer(uint64_t addr, int &len) const;
  Waiter &GetWaiter(uint64_t addr);

  int CeilToChunk(int len) const;
  int NumChunks(int len) const;
  int ChunkIndex(uint64_t addr) const;

  int PartitionIndex(uint64_t addr) const;
  uint64_t PartitionOffset(uint64_t addr) const;

 private:
  int8_t *buffer_;
  Waitlist *waitlists_;

  const int num_shifts_;
  const int num_mask_;
  const int partition_shifts_;
  const uint64_t partition_mask_;
  const int chunk_shifts_;
  const uint64_t chunk_mask_;
  const int buffer_shifts_;
  const uint64_t buffer_mask_;
  const int waitlist_shifts_;
  const uint64_t waitlist_mask_;
};

inline GroupBuffer::GroupBuffer(
    int num_shifts, int partition_shifts, int waitlist_shifts) :

    buffer_(nullptr), waitlists_(nullptr),
    num_shifts_(num_shifts), num_mask_((1 << num_shifts) - 1),
    partition_shifts_(partition_shifts),
    partition_mask_((1 << partition_shifts_) - 1),
    chunk_shifts_(partition_shifts - Waiter::kShiftsToNumChunks),
    chunk_mask_((1 << chunk_shifts_) - 1),
    buffer_shifts_(num_shifts + partition_shifts),
    buffer_mask_((1 << buffer_shifts_) - 1),
    waitlist_shifts_(waitlist_shifts),
    waitlist_mask_((1 << waitlist_shifts_) - 1) {

  if (partition_shifts_ <= Waiter::kShiftsToNumChunks) return;
  buffer_ = (int8_t *)malloc(buffer_size());
  waitlists_ = new Waitlist[num_partitions()]();
  for (int i = 0; i < num_partitions(); ++i) {
    waitlists_[i].CreateList(1 << waitlist_shifts_);
    waitlists_[i][0].FlusherPost(); // TODO: move to Waiter's constructor 
  }
}

inline GroupBuffer::~GroupBuffer() {
  if (buffer_) free(buffer_);
  if (waitlists_) delete[] waitlists_;
}

inline int GroupBuffer::PartitionIndex(uint64_t addr) const {
  return addr >> partition_shifts_;
}

inline uint64_t GroupBuffer::PartitionOffset(uint64_t addr) const {
  return addr & partition_mask_;
}

inline bool GroupBuffer::IsFlusher(uint64_t addr, int len) const {
  return PartitionIndex(addr) != PartitionIndex(addr + len);
}

inline int8_t *GroupBuffer::GetBuffer(uint64_t addr, int &len) const {
  if (!buffer_) return nullptr;
  int rest = partition_size() - PartitionOffset(addr);
  len = std::min(len, rest);
  return buffer_ + (addr & buffer_mask_);
}

inline Waiter &GroupBuffer::GetWaiter(uint64_t addr) {
  int waiter_index = ((addr >> buffer_shifts_) & waitlist_mask_);
  return waitlists_[PartitionIndex(addr) & num_mask_][waiter_index];
}

inline int GroupBuffer::CeilToChunk(int len) const {
  int floor = len & ~chunk_mask_;
  return len == floor ? floor : floor + chunk_size();
}

inline int GroupBuffer::NumChunks(int len) const {
  assert((len & chunk_mask_) == 0);
  return len >> chunk_shifts_;
}

inline int GroupBuffer::ChunkIndex(uint64_t addr) const {
  return PartitionOffset(addr) >> chunk_shifts_;
}

} // namespace plib

#endif // VM_PERSISTENCE_PLIB_GROUP_BUFFER_H_

