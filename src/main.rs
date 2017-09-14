#[macro_use] extern crate clap;
#[macro_use] extern crate serde_derive;
extern crate rand;
extern crate serde_yaml;
extern crate rdkafka;
extern crate env_logger;

use rand::Rng;
use rdkafka::ClientConfig;
use rdkafka::error::{RDKafkaError, KafkaError};
use rdkafka::Context;
use rdkafka::producer::{BaseProducer, ProducerContext, DeliveryReport};
use rdkafka::util::duration_to_millis;

use std::collections::HashMap;
use std::fs::File;
use std::time::{Instant, Duration};
use std::thread;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::iter::{Iterator, IntoIterator};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BenchmarkConfig {
    scenarios: HashMap<String, Scenario>
}

impl BenchmarkConfig {
    fn from_file(path: &str) -> BenchmarkConfig {
        let input_file = File::open(path)
            .expect("Failed to open configuration file");
        serde_yaml::from_reader(input_file)
            .expect("Failed to parse configuration file")
    }
}

fn zero() -> usize { 0 }
fn one() -> usize { 1 }

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Scenario {
    #[serde(default = "one")]
    repeat_times: usize,
    #[serde(default = "zero")]
    repeat_pause: usize,
    #[serde(default = "one")]
    threads: usize,
    #[serde(default)]
    producer: ProducerType,
    message_size: usize,
    message_count: usize,
    topic: String,
    producer_config: HashMap<String, String>
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum ProducerType {
    BaseProducer,
    FutureProducer
}

impl Default for ProducerType {
    fn default() -> Self {
        ProducerType::BaseProducer
    }
}

struct CachedMessages(Vec<Vec<u8>>);

impl CachedMessages {
    fn new(msg_size: usize, cache_size: usize) -> CachedMessages {
        let messages = (0..(cache_size / msg_size)).map(|_|
            rand::thread_rng().gen_iter::<u8>().map(|v| v % 86 + 40).take(msg_size).collect::<Vec<u8>>()
        ).collect::<Vec<_>>();
        CachedMessages(messages)
    }
}

impl<'a> IntoIterator for &'a CachedMessages {
    type Item = &'a[u8];
    type IntoIter = CachedMessagesIterator<'a>;

    fn into_iter(self) -> Self::IntoIter {
        CachedMessagesIterator {
            index: rand::thread_rng().gen_range(0, self.0.len()),
            cache: &self
        }
    }
}

struct CachedMessagesIterator<'a> {
    index: usize,
    cache: &'a CachedMessages,
}

impl<'a> Iterator for CachedMessagesIterator<'a> {
    type Item = &'a[u8];

    fn next(&mut self) -> Option<Self::Item> {
        if self.index == self.cache.0.len() {
            self.index = 0;
        }
        self.index += 1;
        Some(self.cache.0[self.index-1].as_slice())
    }
}


fn generate_producer_config(producer_config: &HashMap<String, String>) -> ClientConfig {
    producer_config.iter()
        .fold(ClientConfig::new(), |mut config, (key, value)| {config.set(key, value); config})
}

struct BenchmarkProducerContext {
    delivered_counter: Arc<AtomicUsize>,
}

impl BenchmarkProducerContext {
    fn new() -> BenchmarkProducerContext {
        BenchmarkProducerContext { delivered_counter: Arc::new(AtomicUsize::new(0)) }
    }
}

impl Context for BenchmarkProducerContext {}

impl ProducerContext for BenchmarkProducerContext {
    type DeliveryContext = ();

    fn delivery(&self, r: DeliveryReport, _: Self::DeliveryContext) {
        if r.success() {
            self.delivered_counter.fetch_add(1, Ordering::Relaxed);
        }
    }
}

fn base_producer_benchmark(scenario_name: &str, scenario: &Scenario) {
    let cache = Arc::new(CachedMessages::new(scenario.message_size, 1_000_000));
    println!("Scenario: {}, repeat {} times, {}s pause after each",
             scenario_name, scenario.repeat_times, scenario.repeat_pause);

    let mut sent_total = 0;
    let mut duration_total = Duration::default();
    for i in 0..scenario.repeat_times {
        let (sent, duration) = base_producer_scenario(scenario, cache.clone());
        print_scenario_stats(scenario, sent, duration);
        sent_total += sent;
        duration_total += duration;
        if i != scenario.repeat_times - 1 {
            thread::sleep(Duration::from_secs(scenario.repeat_pause as u64))
        }
    }
    print_average_stats(scenario, sent_total, duration_total);
}

fn base_producer_scenario(scenario: &Scenario, cache: Arc<CachedMessages>) -> (usize, Duration) {
    let client_config = generate_producer_config(&scenario.producer_config);
    let producer_context = BenchmarkProducerContext::new();
    let delivered_message_counter = producer_context.delivered_counter.clone();
    let base_producer: BaseProducer<BenchmarkProducerContext> = client_config.create_with_context(producer_context)
        .expect("Producer creation failed");
    let producer = Arc::new(base_producer);
    producer.send_copy::<str, str>(&scenario.topic, None, Some("warmup"), None, None, None)
        .expect("Producer error");
    delivered_message_counter.fetch_sub(1, Ordering::Relaxed);
    producer.flush(1000);

    let start = Instant::now();
    let threads = (0..scenario.threads).map(|i| {
        let scenario = scenario.clone();
        let producer = producer.clone();
        let cache = cache.clone();
        thread::spawn(move || {
            let mut message_count = 0;
            let per_thread_messages = if i == 0 {
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
            producer.flush(10000);
        })
    }).collect::<Vec<_>>();
    for t in threads {
        if let Err(e) = t.join() {
            println!("Error in producer thread: {:?}", e);
        }
    }
    // producer.flush(10000);
    (delivered_message_counter.load(Ordering::Relaxed), start.elapsed())
}

fn print_scenario_stats(scenario: &Scenario, delivered_count: usize, duration: Duration) {
    let elapsed_ms = duration_to_millis(duration) as f64;
    let total_msg = delivered_count as f64;
    let total_bytes = total_msg * scenario.message_size as f64;
    let byte_rate_s = total_bytes / elapsed_ms * 1000f64;
    let msg_rate_s = total_msg / elapsed_ms * 1000f64;

    if scenario.message_count != delivered_count {
        println!("Not enough acknowledgements received. Expected {}, received {}",
                 scenario.message_count, delivered_count);
    }

    println!(
        "* Produced {} messages ({}) in {} using {} thread{}\n    {:.0} messages/s\n    {}/s",
        total_msg,
        Bytes(total_bytes as usize).to_human(),
        duration.to_human(),
        scenario.threads,
        if scenario.threads > 1 { "s" } else { "" },
        msg_rate_s,
        Bytes(byte_rate_s as usize).to_human()
    );
}

fn print_average_stats(scenario: &Scenario, delivered_count: usize, duration: Duration) {
    let elapsed_ms = duration_to_millis(duration) as f64;
    let total_msg = delivered_count as f64;
    let total_bytes = total_msg * scenario.message_size as f64;
    let byte_rate_s = total_bytes / elapsed_ms * 1000f64;
    let msg_rate_s = total_msg / elapsed_ms * 1000f64;

    println!("Average: {:.0} messages/s, {}/s", msg_rate_s, Bytes(byte_rate_s as usize).to_human());
}

fn future_producer_benchmark(_scenario: &Scenario) {
    unimplemented!()
}


struct Bytes(usize);


trait ToHuman {
    fn to_human(self) -> String;
}

impl ToHuman for Duration {
    fn to_human(self: Duration) -> String {
        format!("{:.3} seconds", duration_to_millis(self) as f32 / 1000.0)
    }
}

impl ToHuman for Bytes {
    fn to_human(self: Bytes) -> String {
        if self.0 >= 1<<30 {
            format!("{:.3} GB", self.0 as f32 / (1<<30) as f32)
        } else if self.0 >= 1<<20 {
            format!("{:.3} MB", self.0 as f32 / (1<<20) as f32)
        } else if self.0 >= 1<<10 {
            format!("{:.3} KB", self.0 as f32 / (1<<10) as f32)
        } else {
            format!("{} B", self.0)
        }
    }
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
