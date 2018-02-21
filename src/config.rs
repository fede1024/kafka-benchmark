use serde_yaml;

use rdkafka::ClientConfig;

use std::collections::HashMap;
use std::fs::File;


fn map_to_client_config(config_map: &HashMap<String, String>) -> ClientConfig {
    config_map
        .iter()
        .fold(ClientConfig::new(), |mut config, (key, value)| {
            config.set(key, value);
            config
        })
}

fn or_expect<T: Clone>(first: &Option<T>, second: &Option<T>, name: &str) -> T {
    first.as_ref()
        .cloned()
        .or_else(|| second.clone())
        .expect(&format!("Missing configuration parameter: {}", name))
}


//
// ********** PRODUCER CONFIG **********
//

/// The on-file producer benchmark format.
#[derive(Debug, Serialize, Deserialize)]
struct ProducerBenchmarkFileConfig {
    default: ProducerScenarioFileConfig,
    scenarios: HashMap<String, ProducerScenarioFileConfig>,
}

impl ProducerBenchmarkFileConfig {
    fn from_file(path: &str) -> ProducerBenchmarkFileConfig {
        let input_file = File::open(path).expect("Failed to open configuration file");
        serde_yaml::from_reader(input_file).expect("Failed to parse configuration file")
    }
}

/// The on-file producer scenario benchmark format.
#[derive(Debug, Serialize, Deserialize)]
struct ProducerScenarioFileConfig {
    repeat_times: Option<u64>,
    repeat_pause: Option<u64>,
    threads: Option<u64>,
    producer_type: Option<ProducerType>,
    message_size: Option<u64>,
    message_count: Option<u64>,
    topic: Option<String>,
    producer_config: Option<HashMap<String, String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ProducerType {
    BaseProducer,
    FutureProducer,
}

/// The producer scenario configuration used in benchmarks.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProducerScenario {
    pub repeat_times: u64,
    pub repeat_pause: u64,
    pub threads: u64,
    pub producer_type: ProducerType,
    pub message_size: u64,
    pub message_count: u64,
    pub topic: String,
    pub producer_config: HashMap<String, String>,
}

impl ProducerScenario {
    fn from_file_config(default: &ProducerScenarioFileConfig, scenario: &ProducerScenarioFileConfig) -> ProducerScenario {
        let mut producer_config = default.producer_config.clone().unwrap_or_default();
        if let Some(ref config) = scenario.producer_config {
            for (key, value) in config {
                producer_config.insert(key.clone(), value.clone());
            }
        }
        if producer_config.is_empty() {
            panic!("No producer configuration provided")
        }
        ProducerScenario {
            repeat_times: or_expect(&scenario.repeat_times, &default.repeat_times, "repeat_times"),
            repeat_pause: or_expect(&scenario.repeat_pause, &default.repeat_pause, "repeat_pause"),
            threads: or_expect(&scenario.threads, &default.threads, "threads"),
            producer_type: or_expect(&scenario.producer_type, &default.producer_type, "producer_type"),
            message_size: or_expect(&scenario.message_size, &default.message_size, "message_size"),
            message_count: or_expect(&scenario.message_count, &default.message_count, "message_count"),
            topic: or_expect(&scenario.topic, &default.topic, "topic"),
            producer_config,
        }
    }

    pub fn client_config(&self) -> ClientConfig {
        map_to_client_config(&self.producer_config)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProducerBenchmark {
    pub scenarios: HashMap<String, ProducerScenario>,
}

impl ProducerBenchmark {
    pub fn from_file(path: &str) -> ProducerBenchmark {
        let raw_config = ProducerBenchmarkFileConfig::from_file(path);
        let defaults = raw_config.default;
        ProducerBenchmark {
            scenarios: raw_config.scenarios.into_iter()
                .map(|(name, scenario)| (name, ProducerScenario::from_file_config(&defaults, &scenario)))
                .collect()
        }
    }
}


//
// ********** CONSUMER CONFIG **********
//

/// The on-file consumer benchmark format.
#[derive(Debug, Serialize, Deserialize)]
struct ConsumerBenchmarkFileConfig {
    default: ConsumerScenarioFileConfig,
    scenarios: HashMap<String, ConsumerScenarioFileConfig>,
}

impl ConsumerBenchmarkFileConfig {
    fn from_file(path: &str) -> ConsumerBenchmarkFileConfig {
        let input_file = File::open(path).expect("Failed to open configuration file");
        serde_yaml::from_reader(input_file).expect("Failed to parse configuration file")
    }
}

/// The on-file consumer scenario benchmark format.
#[derive(Debug, Serialize, Deserialize)]
struct ConsumerScenarioFileConfig {
    repeat_times: Option<u64>,
    repeat_pause: Option<u64>,
    consumer_type: Option<ConsumerType>,
    message_limit: Option<i64>,
    topic: Option<String>,
    consumer_config: Option<HashMap<String, String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ConsumerType {
    BaseConsumer,
    StreamConsumer,
}

/// The consumer scenario configuration used in benchmarks.
#[derive(Debug, Serialize, Deserialize)]
pub struct ConsumerScenario {
    pub repeat_times: u64,
    pub repeat_pause: u64,
    pub consumer_type: ConsumerType,
    pub message_limit: i64,
    pub topic: String,
    pub consumer_config: HashMap<String, String>,
}

impl ConsumerScenario {
    fn from_file_config(default: &ConsumerScenarioFileConfig, scenario: &ConsumerScenarioFileConfig) -> ConsumerScenario {
        let mut consumer_config = default.consumer_config.clone().unwrap_or_default();
        if let Some(ref config) = scenario.consumer_config {
            for (key, value) in config {
                consumer_config.insert(key.clone(), value.clone());
            }
        }
        if consumer_config.is_empty() {
            panic!("No consumer configuration provided")
        }
        ConsumerScenario {
            repeat_times: or_expect(&scenario.repeat_times, &default.repeat_times, "repeat_times"),
            repeat_pause: or_expect(&scenario.repeat_pause, &default.repeat_pause, "repeat_pause"),
            consumer_type: or_expect(&scenario.consumer_type, &default.consumer_type, "consumer"),
            message_limit: or_expect(&scenario.message_limit, &default.message_limit, "message_limit"),
            topic: or_expect(&scenario.topic, &default.topic, "topic"),
            consumer_config,
        }
    }

    pub fn client_config(&self) -> ClientConfig {
        map_to_client_config(&self.consumer_config)
    }
}

pub struct ConsumerBenchmark {
    pub scenarios: HashMap<String, ConsumerScenario>,
}

impl ConsumerBenchmark {
    pub fn from_file(path: &str) -> ConsumerBenchmark {
        let raw_config = ConsumerBenchmarkFileConfig::from_file(path);
        let defaults = raw_config.default;
        ConsumerBenchmark {
            scenarios: raw_config.scenarios.into_iter()
                .map(|(name, scenario)| (name, ConsumerScenario::from_file_config(&defaults, &scenario)))
                .collect()
        }
    }
}
