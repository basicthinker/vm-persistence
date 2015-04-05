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

template <typename T>
char *Serialize(char *mem, const T &obj) {
  *(T *)mem = obj;
  return mem + sizeof(T);
}

uint64_t ToIndexedPosition(uint64_t pos, uint8_t index) {
  return (pos << 8) + index;
}

uint8_t ParseIndexedPosition(uint64_t *pos) {
  uint8_t index = *pos & 0xff;
  *pos >>= 8;
  return index;
}

// Meta format

size_t MetaLength(int16_t n) {
  size_t len = sizeof(uint64_t) + sizeof(int16_t); // header
  len += (2 * sizeof(uint64_t) + sizeof(uint32_t)) * n;
  return len;
}

char *EncodeMeta(char *mem, uint64_t timestamp,
    const MetaEntry meta[], int16_t n, int index, uint64_t pos) {
  char *cur = Serialize(mem, timestamp);
  cur = Serialize(cur, n);
  for (int i = 0; i < n; ++i) {
    cur = Serialize(cur, meta[i].address);
    cur = Serialize(cur, meta[i].size);
    cur = Serialize(cur, ToIndexedPosition(pos, index));
    pos += meta[i].size;
  }
  return cur;
}

} // namespace plib

#endif // VM_PERSISTENCE_PLIB_FORMAT_H_

