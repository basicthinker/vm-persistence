//
//  buffer_array.h
//  vm_persistence
//
//  Created by Jinglei Ren on May 11, 2015.
//  Copyright (c) 2015 Jinglei Ren <jinglei@ren.systems>.
//

#ifndef VM_PERSISTENCE_PLIB_BUFFER_ARRAY_H_
#define VM_PERSISTENCE_PLIB_BUFFER_ARRAY_H_

#include <algorithm>
#include <atomic>

#include "buffer.h"

namespace plib {

class BufferArray {
 public:
  // (1 << buffer_shift) equals to the size of each buffer.
  // (1 << array_shift) equals to the number of buffers in the array.
  BufferArray(int buffer_shift, int array_shift);
  ~BufferArray();
  BufferArray(const BufferArray &) = delete;
  BufferArray &operator=(const BufferArray &) = delete;

  int buffer_size() const { return buffer_mask_ + 1; }
  int array_size() const { return array_mask_ + 1; }
  uint64_t BufferOffset(uint64_t addr) const { return addr & buffer_mask_; }
  uint64_t BufferTag(uint64_t addr) const { return addr & ~buffer_mask_; }

  Buffer *operator[](uint64_t addr);

 private:
  Buffer *array_;

  const int buffer_shift_;
  const uint64_t buffer_mask_;
  const int array_shift_;
  const uint64_t array_mask_;
};

inline BufferArray::BufferArray(
    int buffer_shift, int array_shift) :

    buffer_shift_(buffer_shift),
    buffer_mask_((uint64_t(1) << buffer_shift) - 1),
    array_shift_(array_shift),
    array_mask_((uint64_t(1) << array_shift) - 1) {

  array_ = (Buffer *)malloc(sizeof(Buffer) << array_shift_);
  for (int i = 0; i < array_size(); ++i) {
    ::new (array_ + i) Buffer(buffer_size(), array_size(), i);
  }
}

inline BufferArray::~BufferArray() {
  for (int i = 0; i < array_size(); ++i) {
    array_[i].~Buffer();
  }
  free(array_);
}

inline Buffer *BufferArray::operator[](uint64_t addr) {
  int index = (addr >> buffer_shift_) & array_mask_;
  return array_ + index;
}

} // namespace plib

#endif // VM_PERSISTENCE_PLIB_BUFFER_ARRAY_H_

