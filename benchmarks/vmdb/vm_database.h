// vm_database.h
// Implements interfaces for YCSB
// Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>

#ifndef VM_PERSISTENCE_BENCHMARK_YCSB_VM_DATABASE_H_
#define VM_PERSISTENCE_BENCHMARK_YCSB_VM_DATABASE_H_

#include "string_hashtable.h"

class VMDatabase {
 public:
  VMDatabase();
  ~VMDatabase();

  const char **Read(char *key, char **fields);
  int Update(char *key, char **fields, const char **values);
  int Insert(char *key, char **fields, const char **values);
  int Delete(char *key);

  void InitClientThread();

 private:
  typedef StringHashtable<const char *> SubHashtable;

#ifdef SVM
  sitevm_seg_t* svm_;
#endif
  StringHashtable<SubHashtable *> *store_;

  int ArrayLength(char **array);
  int FreeElements(char **array);
};

#endif // VM_PERSISTENCE_BENCHMARK_YCSB_VM_DATABASE_H_

