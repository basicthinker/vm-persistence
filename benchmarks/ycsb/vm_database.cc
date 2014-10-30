// database.cc
// Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>

#include "vm_database.h"

using namespace std;

char** VMDatabase::Read(const char *table, const char *key,
    const char *fields[]) {
  return (char **)0;
}

int VMDatabase::Update(const char *table, const char *key,
    const char *fields[], const char *values[]) {
  return 0;
}

int VMDatabase::Insert(const char *table, const char *key,
    const char *fields[], const char *values[]) {
  return 0;
}

int VMDatabase::Delete(const char *table, const char *key) {
  return 0;
}

