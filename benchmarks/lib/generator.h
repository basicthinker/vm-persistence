#ifndef VM_PERSISTENCE_BENCHMARKS_GENERATOR_H_
#define VM_PERSISTENCE_BENCHMARKS_GENERATOR_H_

#include <random>

class IntGenerator {
 public:
  virtual uint64_t NextInt() = 0;
  virtual uint64_t LastInt() = 0;
};

class UniformIntGenerator : public IntGenerator {
 public:
  UniformIntGenerator(uint64_t min, uint64_t max) : dist_(min, max) { }
  uint64_t NextInt() { return last_int_ = dist_(generator_); }
  uint64_t LastInt() { return last_int_; }
 private:
  uint64_t last_int_;
  std::mt19937_64 generator_;
  std::uniform_int_distribution<uint64_t> dist_;
};

#endif // VM_PERSISTENCE_BENCHMARKS_GENERATOR_H_

