#[macro_use] extern crate clap;
#[macro_use] extern crate serde_derive;
extern crate rand;
extern crate serde;
extern crate serde_yaml;
extern crate rdkafka;
extern crate env_logger;
extern crate futures;

use futures::Future;
use rdkafka::error::{KafkaError, RDKafkaError};
use rdkafka::Context;
use rdkafka::producer::{BaseProducer, DeliveryResult, FutureProducer, ProducerContext};
use rdkafka::producer::future_producer::DeliveryFuture;

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

    fn delivery(&self, r: &DeliveryResult, _: Self::DeliveryContext) {
        if r.is_err() {
            self.failure_counter.fetch_add(1, Ordering::Relaxed);
        }
    }
}

fn base_producer_thread(thread_id: usize, scenario: &Scenario, cache: &CachedMessages) -> ThreadStats {
    let client_config = scenario.generate_producer_config();
    let producer_context = BenchmarkProducerContext::new();
    let failure_counter = Arc::clone(&producer_context.failure_counter);
    let producer: BaseProducer<BenchmarkProducerContext> = client_config.create_with_context(producer_context)
        .expect("Producer creation failed");
    producer.send_copy::<str, str>(&scenario.topic, None, Some("warmup"), None, None, None)
        .expect("Producer error");
    failure_counter.store(0, Ordering::Relaxed);
    producer.flush(10_000);

    let per_thread_messages = if thread_id == 0 {
        scenario.message_count - scenario.message_count / scenario.threads * (scenario.threads - 1)
    } else {
        scenario.message_count / scenario.threads
    };
    let start = Instant::now();
    for (count, content) in cache.into_iter().take(per_thread_messages).enumerate() {
        loop {
            match producer.send_copy::<[u8], [u8]>(&scenario.topic, Some(count as i32 % 3), Some(content), None, None, None) {
                Err(KafkaError::MessageProduction(RDKafkaError::QueueFull)) => {
                    producer.poll(10);
                    continue;
                },
                Err(e) => { println!("Error {:?}", e); break },
                Ok(_) => break,
            }
        }
        producer.poll(0);
    }
    producer.flush(120_000);
    ThreadStats::new(start.elapsed(), failure_counter.load(Ordering::Relaxed))
}


fn wait_all(futures: Vec<DeliveryFuture>) -> usize {
    let mut failures = 0;
    for future in futures {
        match future.wait() {
            Ok(Err(e)) => {
                println!("Kafka error: {:?}", e);
                failures += 1;
            },
            Err(futures::Canceled) => {
                println!("Future cancelled");
                failures += 1;
            },
            Ok(_) => {},
        }
    }
    failures
}

fn future_producer_thread(thread_id: usize, scenario: &Scenario, cache: &CachedMessages) -> ThreadStats {
    let client_config = scenario.generate_producer_config();
    let producer: FutureProducer<_> = client_config.create().expect("Producer creation failed");
    let _ = producer.send_copy::<str, str>(&scenario.topic, None, Some("warmup"), None, None, 1000)
        .wait();

    let per_thread_messages = if thread_id == 0 {
        scenario.message_count - scenario.message_count / scenario.threads * (scenario.threads - 1)
    } else {
        scenario.message_count / scenario.threads
    };
    let start = Instant::now();
    let mut failures = 0;
    let mut futures = Vec::with_capacity(1_000_000);
    for (count, content) in cache.into_iter().take(per_thread_messages).enumerate() {
        futures.push(producer.send_copy::<[u8], [u8]>(&scenario.topic, Some(count as i32 % 3), Some(content), None, None, -1));
        if futures.len() >= 1_000_000 {
            failures += wait_all(futures);
            futures = Vec::with_capacity(1_000_000);
        }
    }
    failures += wait_all(futures);
    producer.flush(120_000);
    ThreadStats::new(start.elapsed(), failures)
}

fn run_benchmark(scenario_name: &str, scenario: &Scenario) {
    let cache = Arc::new(CachedMessages::new(scenario.message_size, 1_000_000));
    println!("Scenario: {}, repeat {} times, {}s pause after each",
             scenario_name, scenario.repeat_times, scenario.repeat_pause);

    let mut benchmark_stats = BenchmarkStats::new(scenario);
    for i in 0..scenario.repeat_times {
        let mut scenario_stats = ScenarioStats::new(scenario);
        let threads = (0..scenario.threads).map(|thread_id| {
            let scenario = scenario.clone();
            let cache = Arc::clone(&cache);
            thread::spawn(move || {
                match scenario.producer {
                    ProducerType::BaseProducer => base_producer_thread(thread_id, &scenario, &cache),
                    ProducerType::FutureProducer => future_producer_thread(thread_id, &scenario, &cache),
                }
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

    run_benchmark(scenario_name, scenario);
}
