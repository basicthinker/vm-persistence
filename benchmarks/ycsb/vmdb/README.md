## Quick Start

This section describes how to run YCSB on VMDB.

### 1. Set Up YCSB

Git clone YCSB and compile:

    git clone https://github.com/basicthinker/vm-persistence.git
    cd vm-persistence/benchmarks/ycsb
    mvn -pl com.yahoo.ycsb:core,com.yahoo.ycsb:vmdb-binding clean package

### 2. Load data and run tests

Load the data:

    ./bin/ycsb load vmdb -s -P workloads/workloada > outputLoad.txt

Run the workload test:

    ./bin/ycsb run vmdb -s -P workloads/workloada > outputRun.txt
    
