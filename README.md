# Modified Tarantool Jepsen Test

This is a modified/forked version of the [Tarantool Jepsen Test suite](https://github.com/tarantool/jepsen.tarantool).
## Overview

This test suite uses the [Jepsen distributed systems testing library](https://jepsen.io) to test [Tarantool](https://github.com/tarantool/tarantool). It provides workloads that use [Elle](https://github.com/jepsen-io/elle) and [Knossos](https://github.com/jepsen-io/knossos) to find transactional anomalies up to strict serializability.

The tests include a variety of fault injections:
- Network partitions
- Process crashes
- Process pauses
- Clock skew
- Membership changes

## How to Use

### Prerequisites

You'll need a Jepsen cluster running Ubuntu.

The control node needs:

- A JVM with version 1.8 or higher
- JNA for SSH communication
- (optional) Gnuplot for performance plots
- (optional) Graphviz for transaction anomaly visualization

Install dependencies on Ubuntu:

    sudo apt install -y openjdk8-jdk graphviz gnuplot

Jepsen will automatically install all necessary dependencies (git, build tools, support libraries) and Tarantool itself on all DB nodes.

### Usage

To see all options and their default values:

    ./run-jepsen test --help

To run the `register` test with Tarantool 2.11 for 10 iterations, each running 600 seconds:

    ./run-jepsen test --username user --nodes-file nodes --workload register --version 2.11 --time-limit 600 --test-count 10

To run the `set` test with Tarantool built from the master branch for 100 seconds with 20 threads:

    ./run-jepsen test --username user --nodes-file nodes --workload set --concurrency 20 --time-limit 100

To focus on specific fault types:

    ./run-jepsen test --username user --nodes-file nodes --workload register --nemesis partition,kill

### Options

- `--concurrency` - Number of workers to run (integer, optionally followed by 'n' to multiply by node count)
- `--engine` - Tarantool data engine: `memtx` or `vinyl`
- `--leave-db-running` - Leave database running after tests (for debugging)
- `--logging-json` - Use JSON structured output in logs
- `--mvcc` - Enable MVCC engine
- `--nemesis` - Comma-separated list of fault types:
  - `none` - No faults
  - `standard` - Includes `partition` and `clock`
  - `all` - All available nemeses
  - `clock` - Manipulates clocks
  - `pause` - Pauses/resumes processes with SIGSTOP/SIGCONT
  - `kill` - Kills processes with SIGKILL
  - `partition` - Network partitioning
- `--nemesis-interval` - Time between fault injections
- `--node` - Node to test (can be specified multiple times)
- `--nodes` - Comma-separated list of node hostnames
- `--nodes-file` - File with node hostnames (one per line)
- `--username` - SSH username
- `--password` - Sudo password
- `--strict-host-key-checking` - Whether to check host keys
- `--ssh-private-key` - Path to SSH identity file
- `--test-count` - Number of test repetitions
- `--time-limit` - Test duration in seconds
- `--version` - Tarantool version to test (branch version or Git commit hash)
- `--workload` - Test workload:
  - `bank` - Bank transfers using SQL
  - `bank-multitable` - Multi-table bank transfers using SQL
  - `bank-lua` - Bank transfers using Lua functions
  - `bank-multitable-lua` - Multi-table bank transfers using Lua
  - `counter-inc` - Counter increments
  - `register` - Register with read/write/CAS operations
  - `set` - Unique number insertion and retrieval
