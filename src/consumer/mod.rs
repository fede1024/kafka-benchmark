use super::config::{ConsumerBenchmark, ConsumerScenario, ConsumerType};
use super::units::{Bytes, Messages, Seconds};

use futures::Stream;
use rdkafka::config::FromClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer, ConsumerContext, StreamConsumer};
use rdkafka::error::KafkaError;

use std::collections::HashSet;
use std::time::{Duration, Instant};
use std::u64;


struct ConsumerBenchmarkStats {
    messages: Messages,
    bytes: Bytes,
    time: Seconds,
}

impl ConsumerBenchmarkStats {
    fn new(messages: u64, bytes: u64, time: Duration) -> ConsumerBenchmarkStats {
        ConsumerBenchmarkStats {
            messages: Messages::from(messages),
            bytes: Bytes::from(bytes),
            time: Seconds(time),
        }
    }

    fn print(&self) {
        println!("Received: {}, {}", self.messages, self.bytes);
        println!("Elapsed:  {} ({}, {})", self.time, self.messages / self.time, self.bytes / self.time)
    }
}

fn get_topic_partitions_count<X: ConsumerContext, C: Consumer<X>>(consumer: &C, topic_name: &str) -> Option<usize> {
    let metadata = consumer.fetch_metadata(Some(topic_name), Duration::from_secs(30))
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

fn initialize_consumer<T: FromClientConfig + Consumer>(scenario: &ConsumerScenario) -> T {
    let consumer: T = scenario.client_config()
        .set("enable.partition.eof", "true")
        .create()
        .expect("Consumer creation failed");
    consumer.subscribe(&[&scenario.topic])
        .expect("Can't subscribe to specified topics");
    consumer
}


fn run_base_consumer_benchmark(scenario: &ConsumerScenario) -> ConsumerBenchmarkStats {
    let consumer: BaseConsumer = initialize_consumer(scenario);
    let mut partition_eof = HashSet::new();
    let partition_count = get_topic_partitions_count(&consumer, &scenario.topic)
        .expect("Topic not found");

    let limit = if scenario.message_limit < 0 {
        u64::MAX
    } else {
        scenario.message_limit as u64
    };

    let mut start_time = Instant::now();
    let mut messages = 0;
    let mut bytes = 0;

    while messages < limit {
        match consumer.poll(Duration::from_secs(1)) {
            None => {},
            Some(Ok(message)) => {
                if messages == 0 {
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

    ConsumerBenchmarkStats::new(messages, bytes as u64, start_time.elapsed())
}

fn run_stream_consumer_benchmark(scenario: &ConsumerScenario) -> ConsumerBenchmarkStats {
    let consumer: StreamConsumer = initialize_consumer(scenario);
    let mut partition_eof = HashSet::new();
    let partition_count = get_topic_partitions_count(&consumer, &scenario.topic)
        .expect("Topic not found");

    let limit = if scenario.message_limit < 0 {
        u64::MAX
    } else {
        scenario.message_limit as u64
    };

    let mut start_time = Instant::now();
    let mut messages = 0;
    let mut bytes = 0;

    for message in consumer.start().wait() {
        match message {
            Err(e) => {
                println!("Error while reading from stream: {:?}", e);
            },
            Ok(Ok(message)) => {
                if messages == 0 {
                    println!("First message received");
                    start_time = Instant::now();
                }
                if messages >= limit {
                    break;
                }
                messages += 1;
                bytes += message.payload_len() + message.key_len();
            },
            Ok(Err(KafkaError::PartitionEOF(p))) => {
                partition_eof.insert(p);
                if partition_eof.len() >= partition_count {
                    break;
                }
            },
            Ok(Err(e)) => {
                println!("Kafka error: {}", e);
            },
        };
    }

    ConsumerBenchmarkStats::new(messages, bytes as u64, start_time.elapsed())
}

pub fn run(config: &ConsumerBenchmark, scenario_name: &str) {
    let scenario = config
        .scenarios
        .get(scenario_name)
        .expect("The specified scenario cannot be found");

    let stats = match scenario.consumer_type {
        ConsumerType::BaseConsumer => run_base_consumer_benchmark(scenario),
        ConsumerType::StreamConsumer => run_stream_consumer_benchmark(scenario),
    };

    stats.print();
}
