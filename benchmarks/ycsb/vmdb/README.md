## Quick Start

This section describes how to run YCSB on VMDB.

### 1. Compile VMDB

    git clone https://github.com/basicthinker/vm-persistence.git
    cd vm-persistence/benchmarks/vmdb
    make ycsb
    export LD_LIBRARY_PATH=`pwd`:$LD_LIBRARY_PATH

### 2. Set Up YCSB

    cd vm-persistence/benchmarks/ycsb
    mvn -pl com.yahoo.ycsb:core,com.yahoo.ycsb:vmdb-binding clean package

### 3. Load data and run tests

Example workload test:

    ./bin/ycsb run vmdb -s -P workloads/workloadd > outputRun.txt
    
