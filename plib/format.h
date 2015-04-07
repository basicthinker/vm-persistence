//
//  format.h
//  vm_persistence
//
//  Created by Jinglei Ren on Apr. 2, 2015.
//  Copyright (c) 2015 Jinglei Ren <jinglei@ren.systems>.
//

#ifndef VM_PERSISTENCE_PLIB_FORMAT_H_
#define VM_PERSISTENCE_PLIB_FORMAT_H_

#include <cstdint>

namespace plib {

// Utils

inline char *Serialize(char *mem, void *source, size_t nbytes) {
  memcpy(mem, source, nbytes);
  return mem + nbytes;
}

template <typename T>
inline char *Serialize(char *mem, const T &obj) {
  *(T *)mem = obj;
  return mem + sizeof(T);
}

inline uint64_t ToIndexedPosition(uint64_t pos, uint8_t index) {
  return (pos << 8) + index;
}

inline uint8_t ParseIndexedPosition(uint64_t *pos) {
  uint8_t index = *pos & 0xff;
  *pos >>= 8;
  return index;
}

// Meta format

inline size_t MetaLength(uint32_t n) {
  size_t len = 2 * sizeof(uint64_t) + sizeof(uint32_t); // header
  len += sizeof(uint64_t) * n;
  return len;
}

inline char *EncodeMeta(char *mem, uint64_t timestamp,
    const MetaEntry meta[], uint32_t n, int index, uint64_t pos) {
  char *cur = Serialize(mem, timestamp);
  cur = Serialize(cur, ToIndexedPosition(pos, index));
  cur = Serialize(cur, n);
  for (uint32_t i = 0; i < n; ++i) {
    cur = Serialize(cur, meta[i].address);
  }
  return cur;
}

} // namespace plib

#endif // VM_PERSISTENCE_PLIB_FORMAT_H_

