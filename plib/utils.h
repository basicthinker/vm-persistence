//
//  utils.h
//  vm_persistence
//
//  Created by Jinglei Ren on Apr. 2, 2015.
//  Copyright (c) 2015 Jinglei Ren <jinglei@ren.systems>.
//

#ifndef VM_PERSISTENCE_PLIB_UTILS_H_
#define VM_PERSISTENCE_PLIB_UTILS_H_

#include <cstdint>

namespace plib {

template <typename T>
char *Serialize(char *mem, const T &obj) {
  *(T *)mem = obj;
  return mem + sizeof(T);
}

uint64_t ToPersistAddr(uint64_t phy_addr, uint8_t index) {
  return (phy_addr << 8) + index;
}

uint8_t ParsePersistAddr(uint64_t *addr) {
  uint8_t index = *addr & 0xff;
  *addr >>= 8;
  return index;
}

} // namespace plib

#endif // VM_PERSISTENCE_PLIB_UTILS_H_

