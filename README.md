# kafka-benchmark
[![Join the chat at https://gitter.im/rust-rdkafka/Lobby](https://badges.gitter.im/rust-rdkafka/Lobby.svg)](https://gitter.im/rust-rdkafka/Lobby?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

A tool to run programmable benchmarks on Kafka clusters.

This tool uses the high-performance
[rdkafka](https://github.com/fede1024/rust-rdkafka/) Rust client library for
Kafka (based on the C library
[librdkafka](https://github.com/edenhill/librdkafka)), and is able to produce
at extremely high speed to the cluster you want to benchmark.

Benchmark scenarios are configurable using a YAML file.

## Examples

This is an example run while producing messages to localhost, on an Intel(R)
Core(TM) i7-4712HQ CPU @ 2.30GHz:

```
→ cargo install kafka-benchmark
[...]

→ kafka-benchmark --config producer_benchmark_config.yaml
Scenario: byte_flow_bursts (message size: 10KB), repeat 5 times, 10s pause after each
* Produced 100000 messages (953.674 MB) in 4.791 seconds using 1 thread
    20872 messages/s
    199.055 MB/s
* Produced 100000 messages (953.674 MB) in 4.135 seconds using 1 thread
    24184 messages/s
    230.635 MB/s
* Produced 100000 messages (953.674 MB) in 4.150 seconds using 1 thread
    24096 messages/s
    229.801 MB/s
* Produced 100000 messages (953.674 MB) in 3.643 seconds using 1 thread
    27450 messages/s
    261.783 MB/s
* Produced 100000 messages (953.674 MB) in 4.344 seconds using 1 thread
    23020 messages/s
    219.538 MB/s
Average: 23737 messages/s, 226.375 MB/s

Scenario: msg_flow_bursts (message size: 10B), repeat 5 times, 2s pause after each
* Produced 5000000 messages (47.684 MB) in 5.718 seconds using 1 thread
    874432 messages/s
    8.339 MB/s
* Produced 5000000 messages (47.684 MB) in 5.720 seconds using 1 thread
    874126 messages/s
    8.336 MB/s
* Produced 5000000 messages (47.684 MB) in 5.747 seconds using 1 thread
    870019 messages/s
    8.297 MB/s
* Produced 5000000 messages (47.684 MB) in 5.546 seconds using 1 thread
    901551 messages/s
    8.598 MB/s
* Produced 5000000 messages (47.684 MB) in 7.334 seconds using 1 thread
    681756 messages/s
    6.502 MB/s
Average: 831476 messages/s, 7.930 MB/s
```

When producing to localhost, kafka-benchmark can send more than 900000 messages
per second on commodity hardware.
