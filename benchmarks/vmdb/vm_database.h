// vm_database.h
// Implements interfaces for YCSB
// Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>

#ifndef VM_PERSISTENCE_BENCHMARK_YCSB_VM_DATABASE_H_
#define VM_PERSISTENCE_BENCHMARK_YCSB_VM_DATABASE_H_

#include "hashtable.h"

class VMDatabase {
 public:
  VMDatabase();
  ~VMDatabase();

  const char **Read(const char *key, const char **fields);
  int Update(const char *key, const char **fields, const char **values);
  int Insert(const char *key, const char **fields, const char **values);
  int Delete(const char *key);

 private:
  typedef Hashtable<const char *> CStrHashtable;
  Hashtable<CStrHashtable *> *store_;

  const char *StoreCopy(const char *str);
  int ArrayLength(const char **array);
  int FreeElements(const char **array);
};

#endif // VM_PERSISTENCE_BENCHMARK_YCSB_VM_DATABASE_H_

