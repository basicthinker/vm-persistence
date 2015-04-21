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
#include <cstring>

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
    uint64_t meta[], uint32_t n, uint8_t index, uint64_t pos) {
  char *cur = Serialize(mem, timestamp);
  cur = Serialize(cur, ToIndexedPosition(pos, index));
  cur = Serialize(cur, n);
  for (uint32_t i = 0; i < n; ++i) {
    cur = Serialize(cur, meta[i]);
  }
  return cur;
}

// SSD data striping

class FlashStriper {
  public:
    FlashStriper(int width_bits, int sector_bits) :
        width_(width_bits), sector_(sector_bits),
        width_mask_((1 << width_) - 1),
        unit_mask_((1 << (width_ + sector_)) - 1), index_mask_(~unit_mask_) {
    }

    uint64_t Translate(uint64_t lba) {
      uint64_t unit = lba & unit_mask_;
      unit = ((unit & width_mask_) << sector_) + (unit >> width_);
      return (lba & index_mask_) + unit;
    }

 private:
  int width_;
  int sector_;
  uint64_t width_mask_;
  uint64_t unit_mask_;
  uint64_t index_mask_;
};

} // namespace plib

#endif // VM_PERSISTENCE_PLIB_FORMAT_H_

