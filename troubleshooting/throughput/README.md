# Lab: Advanced Tuning & Troubleshooting: Throughput

## Exercise

* Improve the throughput of [`ThroughputJob`](src/main/java/com/ververica/flink/training/exercises/ThroughputJob.java).
  - Identify where backpressure originates.
  - You can also use a (local) profiler of your choice for identifying further optimisation potential.
    - Disable the local print sink for this (why?)

## Stats from Throughput Task row in Grafana

- [ThroughputJob](src/main/java/com/ververica/flink/training/exercises/ThroughputJob.java): ~160 K ops
- [ThroughputJobSolution1](src/solution/java/com/ververica/flink/training/solutions/ThroughputJobSolution1.java): ~215 K ops
- [ThroughputJobSolution2](src/solution/java/com/ververica/flink/training/solutions/ThroughputJobSolution2.java): ~930 K ops

-----

[**Back to Tuning & Troubleshooting Labs Overview**](../README.md)
