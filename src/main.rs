#[macro_use] extern crate clap;
#[macro_use] extern crate serde_derive;
extern crate rand;
extern crate serde;
extern crate serde_yaml;
extern crate rdkafka;
extern crate env_logger;

use rdkafka::ClientConfig;
use rdkafka::error::{RDKafkaError, KafkaError};
use rdkafka::Context;
use rdkafka::producer::{BaseProducer, ProducerContext, DeliveryReport};

use std::collections::HashMap;
use std::time::{Instant, Duration};
use std::thread;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::iter::{Iterator, IntoIterator};

mod config;
mod content;
mod output;

use config::{BenchmarkConfig, ProducerType, Scenario};
use content::CachedMessages;
use output::{BenchmarkStats, ScenarioStats, ThreadStats};


fn generate_producer_config(producer_config: &HashMap<String, String>) -> ClientConfig {
    producer_config.iter()
        .fold(ClientConfig::new(), |mut config, (key, value)| {config.set(key, value); config})
}

struct BenchmarkProducerContext {
    failure_counter: Arc<AtomicUsize>,
}

impl BenchmarkProducerContext {
    fn new() -> BenchmarkProducerContext {
        BenchmarkProducerContext { failure_counter: Arc::new(AtomicUsize::new(0)) }
    }
}

impl Context for BenchmarkProducerContext {}

impl ProducerContext for BenchmarkProducerContext {
    type DeliveryContext = ();

    fn delivery(&self, r: DeliveryReport, _: Self::DeliveryContext) {
        if !r.success() {
            self.failure_counter.fetch_add(1, Ordering::Relaxed);
        }
    }
}

fn base_producer_benchmark(scenario_name: &str, scenario: &Scenario) {
    let cache = Arc::new(CachedMessages::new(scenario.message_size, 1_000_000));
    println!("Scenario: {}, repeat {} times, {}s pause after each",
             scenario_name, scenario.repeat_times, scenario.repeat_pause);

    let mut benchmark_stats = BenchmarkStats::new(scenario);
    for i in 0..scenario.repeat_times {
        let mut scenario_stats = ScenarioStats::new(scenario);
        let threads = (0..scenario.threads).map(|thread_id| {
            let scenario = scenario.clone();
            let cache = cache.clone();
            thread::spawn(move || {
                base_producer_scenario(thread_id, &scenario, cache)
            })
        }).collect::<Vec<_>>();
        for thread in threads {
            let stats = thread.join();
            scenario_stats.add_thread_stats(&stats.unwrap());
        }
        scenario_stats.print();
        benchmark_stats.add_stat(scenario_stats);
        if i != scenario.repeat_times - 1 {
            thread::sleep(Duration::from_secs(scenario.repeat_pause as u64))
        }
    }
    benchmark_stats.print();
}

fn base_producer_scenario(thread_id: usize, scenario: &Scenario, cache: Arc<CachedMessages>) -> ThreadStats {
    let client_config = generate_producer_config(&scenario.producer_config);
    let producer_context = BenchmarkProducerContext::new();
    let failure_counter = producer_context.failure_counter.clone();
    let base_producer: BaseProducer<BenchmarkProducerContext> = client_config.create_with_context(producer_context)
        .expect("Producer creation failed");
    let producer = Arc::new(base_producer);
    producer.send_copy::<str, str>(&scenario.topic, None, Some("warmup"), None, None, None)
        .expect("Producer error");
    failure_counter.store(0, Ordering::Relaxed);
    producer.flush(10000);

    let start = Instant::now();
    let mut message_count = 0;
    let per_thread_messages = if thread_id == 0 {
        scenario.message_count - scenario.message_count / scenario.threads * (scenario.threads - 1)
    } else {
        scenario.message_count / scenario.threads
    };
    for content in cache.into_iter().take(per_thread_messages) {
        loop {
            match producer.send_copy::<[u8], [u8]>(&scenario.topic, Some(message_count % 3), Some(&content), None, None, None) {
                Err(KafkaError::MessageProduction(RDKafkaError::QueueFull)) => {
                    producer.poll(10);
                    continue;
                },
                Err(e) => { println!("Error {:?}", e); break },
                Ok(_) => break,
            }
        }
        message_count += 1;
        producer.poll(0);
    }
    producer.flush(120000);
    ThreadStats::new(start.elapsed(), failure_counter.load(Ordering::Relaxed))
}

fn future_producer_benchmark(_scenario: &Scenario) {
    unimplemented!()
}


fn main() {
    let matches = clap_app!(app =>
        (name: "producer_benchmark")
        (@arg config: --config +takes_value +required "The configuration file")
        (@arg scenario: --scenario +takes_value +required "The scenario you want to execute")
    ).get_matches();

    env_logger::init().expect("Failed to initialize logging");

    let config = BenchmarkConfig::from_file(matches.value_of("config").unwrap());
    let scenario_name = matches.value_of("scenario").unwrap();
    let scenario = config.scenarios.get(scenario_name)
        .expect("The specified scenario cannot be found");

    match scenario.producer {
        ProducerType::BaseProducer => base_producer_benchmark(scenario_name, scenario),
        ProducerType::FutureProducer => future_producer_benchmark(scenario),
    };
}
