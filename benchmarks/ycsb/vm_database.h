// vm_database.h
// Implements interfaces for YCSB
// Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>

#ifndef VM_PERSISTENCE_BENCHMARK_YCSB_VM_DATABASE_H_
#define VM_PERSISTENCE_BENCHMARK_YCSB_VM_DATABASE_H_

class VMDatabase {
 public:
  char** Read(const char *table, const char *key, const char *fields[]);
  int Update(const char *table, const char *key,
    const char *fields[], const char *values[]);
  int Insert(const char *table, const char *key,
    const char *fields[], const char *values[]);
  int Delete(const char *table, const char *key);
};

#endif // VM_PERSISTENCE_BENCHMARK_YCSB_VM_DATABASE_H_

