use super::config::{ConsumerBenchmark, ConsumerScenario, ConsumerType};
use super::units::{Bytes, Messages, Seconds};

use rdkafka::consumer::{BaseConsumer, Consumer, ConsumerContext};
use rdkafka::error::KafkaError;

use std::collections::HashSet;
use std::u64;
use std::time::Instant;


fn get_topic_partitions_count<X: ConsumerContext, C: Consumer<X>>(consumer: &C, topic_name: &str) -> Option<usize> {
    let metadata = consumer.fetch_metadata(Some(topic_name), 30000)
        .expect("Failed to fetch metadata");

    if metadata.topics().is_empty() {
        None
    } else {
        let partitions = metadata.topics()[0].partitions();
        if partitions.is_empty() {
            None  // Topic was auto-created
        } else {
            Some(partitions.len())
        }
    }
}

fn run_base_consumer_benchmark(benchmark: &ConsumerBenchmark, scenario: &ConsumerScenario) {
    let mut client_config = benchmark.generate_consumer_config(scenario);
    let mut partition_eof = HashSet::new();
    let consumer = client_config
        .set("enable.partition.eof", "true")
        .create::<BaseConsumer<_>>()
        .expect("Consumer creation failed");

    let partition_count = get_topic_partitions_count(&consumer, &scenario.topic)
        .expect("Topic not found");

    consumer.subscribe(&[&scenario.topic])
        .expect("Can't subscribe to specified topics");

    let limit = Messages::from(scenario.limit.unwrap_or(u64::MAX));
    let mut start_time = Instant::now();
    let mut messages = Messages::zero();
    let mut bytes = Bytes::zero();

    while messages < limit {
        match consumer.poll(1000) {
            None => {},
            Some(Ok(message)) => {
                if messages.is_zero() {
                    println!("First message received");
                    start_time = Instant::now();
                }
                messages += 1;
                bytes += message.payload_len() + message.key_len();
            },
            Some(Err(KafkaError::PartitionEOF(p))) => {
                partition_eof.insert(p);
                if partition_eof.len() >= partition_count {
                    break
                }
            },
            Some(Err(error)) => {
                println!("Error {:?}", error);
            }
        }
    }

    let time = Seconds(start_time.elapsed());

    println!("Received: {}, {}", messages, bytes);
    println!("Elapsed:  {} ({}, {})", time, messages / time, bytes / time)
}

fn run_stream_consumer_benchmark(_config: &ConsumerBenchmark, _scenario: &ConsumerScenario) {
    unimplemented!();
}

pub fn run(config: &ConsumerBenchmark, scenario_name: &str) {
    let scenario = config
        .scenarios
        .get(scenario_name)
        .expect("The specified scenario cannot be found");

    match scenario.consumer {
        ConsumerType::BaseConsumer => run_base_consumer_benchmark(config, scenario),
        ConsumerType::StreamConsumer => run_stream_consumer_benchmark(config, scenario),
    }
}
