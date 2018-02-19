use futures::{self, Future};
use rdkafka::ClientContext;
use rdkafka::error::{KafkaError, RDKafkaError};
use rdkafka::producer::future_producer::DeliveryFuture;
use rdkafka::producer::{BaseProducer, DeliveryResult, FutureProducer, ProducerContext};

use std::cmp;
use std::iter::{IntoIterator, Iterator};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::{Duration, Instant};

mod content;

use self::content::CachedMessages;
use super::config::{ProducerBenchmarkConfig, ProducerType, ProducerScenario};
use super::units::{Bytes, Messages, Seconds};

struct BenchmarkProducerContext {
    failure_counter: Arc<AtomicUsize>,
}

impl BenchmarkProducerContext {
    fn new() -> BenchmarkProducerContext {
        BenchmarkProducerContext {
            failure_counter: Arc::new(AtomicUsize::new(0)),
        }
    }
}

impl ClientContext for BenchmarkProducerContext {}

impl ProducerContext for BenchmarkProducerContext {
    type DeliveryOpaque = ();

    fn delivery(&self, r: &DeliveryResult, _: Self::DeliveryOpaque) {
        if r.is_err() {
            self.failure_counter.fetch_add(1, Ordering::Relaxed);
        }
    }
}

fn base_producer_thread(
    thread_id: u64,
    scenario: &ProducerScenario,
    cache: &CachedMessages,
) -> ThreadStats {
    let client_config = scenario.generate_producer_config();
    let producer_context = BenchmarkProducerContext::new();
    let failure_counter = Arc::clone(&producer_context.failure_counter);
    let producer: BaseProducer<BenchmarkProducerContext> = client_config
        .create_with_context(producer_context)
        .expect("Producer creation failed");
    producer
        .send_copy::<str, str>(&scenario.topic, None, Some("warmup"), None, (), None)
        .expect("Producer error");
    failure_counter.store(0, Ordering::Relaxed);
    producer.flush(10_000);

    let per_thread_messages = if thread_id == 0 {
        scenario.message_count - scenario.message_count / scenario.threads * (scenario.threads - 1)
    } else {
        scenario.message_count / scenario.threads
    };
    let start = Instant::now();
    for (count, content) in cache.into_iter().take(per_thread_messages as usize).enumerate() {
        loop {
            match producer.send_copy::<[u8], [u8]>(
                &scenario.topic,
                Some(count as i32 % 3),
                Some(content),
                None,
                (),
                None,
            ) {
                Err(KafkaError::MessageProduction(RDKafkaError::QueueFull)) => {
                    producer.poll(10);
                    continue;
                }
                Err(e) => {
                    println!("Error {:?}", e);
                    break;
                }
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
            }
            Err(futures::Canceled) => {
                println!("Future cancelled");
                failures += 1;
            }
            Ok(_) => {}
        }
    }
    failures
}

fn future_producer_thread(
    thread_id: u64,
    scenario: &ProducerScenario,
    cache: &CachedMessages,
) -> ThreadStats {
    let client_config = scenario.generate_producer_config();
    let producer: FutureProducer<_> = client_config.create().expect("Producer creation failed");
    let _ = producer
        .send_copy::<str, str>(&scenario.topic, None, Some("warmup"), None, None, 1000)
        .wait();

    let per_thread_messages = if thread_id == 0 {
        scenario.message_count - scenario.message_count / scenario.threads * (scenario.threads - 1)
    } else {
        scenario.message_count / scenario.threads
    };
    let start = Instant::now();
    let mut failures = 0;
    let mut futures = Vec::with_capacity(1_000_000);
    for (count, content) in cache.into_iter().take(per_thread_messages as usize).enumerate() {
        futures.push(producer.send_copy::<[u8], [u8]>(
            &scenario.topic,
            Some(count as i32 % 3),
            Some(content),
            None,
            None,
            -1,
        ));
        if futures.len() >= 1_000_000 {
            failures += wait_all(futures);
            futures = Vec::with_capacity(1_000_000);
        }
    }
    failures += wait_all(futures);
    producer.flush(120_000);
    ThreadStats::new(start.elapsed(), failures)
}

pub fn run(config: &ProducerBenchmarkConfig, scenario_name: &str) {
    let scenario = config
        .scenarios
        .get(scenario_name)
        .expect("The specified scenario cannot be found");

    let cache = Arc::new(CachedMessages::new(scenario.message_size, 1_000_000));
    println!(
        "Scenario: {}, repeat {} times, {} seconds pause after each",
        scenario_name, scenario.repeat_times, scenario.repeat_pause
    );

    let mut benchmark_stats = ProducerBenchmarkStats::new(scenario);
    for i in 0..scenario.repeat_times {
        let mut scenario_stats = ProducerRunStats::new(scenario);
        let threads = (0..scenario.threads)
            .map(|thread_id| {
                let scenario = scenario.clone();
                let cache = Arc::clone(&cache);
                thread::spawn(move || match scenario.producer {
                    ProducerType::BaseProducer => {
                        base_producer_thread(thread_id, &scenario, &cache)
                    }
                    ProducerType::FutureProducer => {
                        future_producer_thread(thread_id, &scenario, &cache)
                    }
                })
            })
            .collect::<Vec<_>>();
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


#[derive(Debug)]
pub struct ThreadStats {
    duration: Duration,
    failure_count: usize,
}

impl ThreadStats {
    pub fn new(duration: Duration, failure_count: usize) -> ThreadStats {
        ThreadStats {
            duration,
            failure_count,
        }
    }
}

#[derive(Debug)]
pub struct ProducerRunStats<'a> {
    scenario: &'a ProducerScenario,
    failure_count: usize,
    duration: Duration,
}

impl<'a> ProducerRunStats<'a> {
    pub fn new(scenario: &'a ProducerScenario) -> ProducerRunStats<'a> {
        ProducerRunStats {
            scenario,
            failure_count: 0,
            duration: Duration::from_secs(0),
        }
    }

    pub fn add_thread_stats(&mut self, thread_stats: &ThreadStats) {
        self.failure_count += thread_stats.failure_count;
        self.duration = cmp::max(self.duration, thread_stats.duration);
    }

    pub fn print(&self) {
        let time = Seconds(self.duration);
        let messages = Messages::from(self.scenario.message_count);
        let bytes = Bytes::from(self.scenario.message_count * self.scenario.message_size);

        if self.failure_count != 0 {
            println!(
                "Warning: {} messages failed to be delivered",
                self.failure_count
            );
        }

        println!(
            "* Produced {} ({}) in {} using {} thread{}\n    {}\n    {}",
            messages,
            bytes,
            time,
            self.scenario.threads,
            if self.scenario.threads > 1 { "s" } else { "" },
            messages / time,
            bytes / time
        );
    }
}

pub struct ProducerBenchmarkStats<'a> {
    scenario: &'a ProducerScenario,
    stats: Vec<ProducerRunStats<'a>>,
}

impl<'a> ProducerBenchmarkStats<'a> {
    pub fn new(scenario: &'a ProducerScenario) -> ProducerBenchmarkStats<'a> {
        ProducerBenchmarkStats {
            scenario,
            stats: Vec::new(),
        }
    }

    pub fn add_stat(&mut self, scenario_stat: ProducerRunStats<'a>) {
        self.stats.push(scenario_stat)
    }

    pub fn print(&self) {
        let time = Seconds(self.stats.iter().map(|stat| stat.duration).sum());
        let messages = Messages::from(self.scenario.message_count * self.stats.len() as u64);
        let bytes = Bytes(messages.0 * self.scenario.message_size as f64);

        println!("Average: {}, {}", messages / time, bytes / time);
    }
}
