//
//  mem_allocator.h
//
//  Created by Jinglei Ren on 12/23/14.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#ifndef VM_PERSISTENCE_MEM_ALLOCATOR_H_
#define VM_PERSISTENCE_MEM_ALLOCATOR_H_

#include <cstring>

#ifdef SVM
#include "sitevm/sitevm_malloc.h"
#define MALLOC(n) sitevm_malloc::smalloc(n)
#define FREE(p, n) do { \
      memset((void *)p, 255, n); \
      sitevm_malloc::sfree((void *)(p)); \
    } while (false)
#else
#define MALLOC(n) malloc(n)
#define FREE(p, n) free((void *)(p))
#endif

#define NEW(type, ...) ({ \
      type *p = (type *)MALLOC(sizeof(type)); \
      ::new((void*)(p)) (type)(__VA_ARGS__); \
      p; \
    })

template <typename T>
static inline void DELETE(T *p) {
  p->~T();
  FREE(p, sizeof(T));
}

#endif // VM_PERSISTENCE_MEM_ALLOCATOR_H_

