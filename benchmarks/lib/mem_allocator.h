// mem_allocator.h
// Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>

#ifndef VM_PERSISTENCE_BENCHMARK_MEM_ALLOCATOR_H_
#define VM_PERSISTENCE_BENCHMARK_MEM_ALLOCATOR_H_

#ifdef SVM
#define MALLOC(n) malloc(n)
#define FREE(p) free((void *)(p))
#else
#define MALLOC(n) malloc(n)
#define FREE(p) free((void *)(p))
#endif

#endif // VM_PERSISTENCE_BENCHMARK_MEM_ALLOCATOR_H_

