// hash_string.h
// Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>

#ifndef VM_PERSISTENCE_BENCHMARK_HASH_STRING_H_
#define VM_PERSISTENCE_BENCHMARK_HASH_STRING_H_

#include <cstdlib>
#include <cstring>
#include <cassert>

#include "mem_allocator.h"

inline uint32_t SDBMHash(const char *str) {
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

inline char *NewZeroHashString(const char *str) {
  assert(str);
  const size_t len = strlen(str);
  const size_t hstr_len = sizeof(uint32_t) + len;
  char *hstr = (char *)MALLOC(hstr_len + 1);
  memcpy(LoadString(hstr), str, len);
  hstr[hstr_len] = '\0';
  ZeroHash(hstr);
  return hstr;
}

inline char *NewHashString(const char *str) {
  assert(str);
  const size_t len = strlen(str);
  const size_t hstr_len = sizeof(uint32_t) + len;
  char *hstr = (char *)MALLOC(hstr_len + 1);
  memcpy(LoadString(hstr), str, len);
  hstr[hstr_len] = '\0';
  StoreHash(hstr);
  return hstr;
}

#endif // VM_PERSISTENCE_BENCHMARK_HASH_STRING_H_

