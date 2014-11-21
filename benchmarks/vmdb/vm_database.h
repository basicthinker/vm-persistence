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

  const char **Read(char *key, char **fields);
  int Update(char *key, char **fields, const char **values);
  int Insert(char *key, char **fields, const char **values);
  int Delete(char *key);

 private:
  typedef Hashtable<const char *> CStrHashtable;
  Hashtable<CStrHashtable *> *store_;

  int ArrayLength(char **array);
  int FreeElements(char **array);
};

#endif // VM_PERSISTENCE_BENCHMARK_YCSB_VM_DATABASE_H_

