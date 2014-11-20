// hash_string.h
// Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>

#ifndef VM_PERSISTENCE_BENCHMARK_HASH_STRING_H_
#define VM_PERSISTENCE_BENCHMARK_HASH_STRING_H_

#include <cstring>

uint32_t SDBMHash(const char *str) {
  uint32_t hash = 0;
  uint32_t c;
  while ((c = *str++) != '\0') {
    hash = c + (hash << 6) + (hash << 16) - hash;
  }
  return hash;
}

inline void StoreHash(char *hstr) {
  *((uint32_t *)hstr) = SDBMHash(hstr + sizeof(uint32_t));
}

inline void ZeroHash(char *hstr) {
  *((uint32_t *)hstr) = 0;
}

inline uint32_t LoadHash(const char *hstr) {
  return *((const uint32_t *)hstr);
}

inline char *LoadString(char *hstr) {
  return hstr + sizeof(uint32_t);
}

inline const char *LoadString(const char *hstr) {
  return hstr + sizeof(uint32_t);
}

inline size_t HashStringLength(const char *str) {
  return strlen(str) + sizeof(uint32_t);
}

#endif // VM_PERSISTENCE_BENCHMARK_HASH_STRING_H_

