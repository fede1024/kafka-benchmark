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
→ git clone https://github.com/fede1024/kafka-benchmark.git
[...]

→ cd kafka-benchmark/

→ cargo install
[...]

→ kafka-benchmark --config producer_benchmark_config.yaml --scenario msg_bursts_base
Scenario: msg_bursts_base, repeat 5 times, 10s pause after each
* Produced 20000000 messages (190.735 MB) in 5.045 seconds using 6 threads
    3964321 messages/s
    37.807 MB/s
* Produced 20000000 messages (190.735 MB) in 5.125 seconds using 6 threads
    3902439 messages/s
    37.217 MB/s
* Produced 20000000 messages (190.735 MB) in 5.032 seconds using 6 threads
    3974563 messages/s
    37.904 MB/s
* Produced 20000000 messages (190.735 MB) in 4.980 seconds using 6 threads
    4016064 messages/s
    38.300 MB/s
* Produced 20000000 messages (190.735 MB) in 5.036 seconds using 6 threads
    3971406 messages/s
    37.874 MB/s
Average: 3964950 messages/s, 37.813 MB/s

→ kafka-benchmark --config producer_benchmark_config.yaml --scenario byte_bursts
Scenario: byte_bursts, repeat 3 times, 20s pause after each
* Produced 200000 messages (1.863 GB) in 2.800 seconds using 6 threads
    71429 messages/s
    681.196 MB/s
* Produced 200000 messages (1.863 GB) in 2.529 seconds using 6 threads
    79083 messages/s
    754.191 MB/s
* Produced 200000 messages (1.863 GB) in 2.514 seconds using 6 threads
    79554 messages/s
    758.691 MB/s
Average: 76492 messages/s, 729.481 MB/s
```

When producing to localhost, kafka-benchmark can send almost 4 million messages
per second on commodity hardware.
