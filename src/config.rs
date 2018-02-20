use serde_yaml;
use serde;

use rdkafka::ClientConfig;

use std::collections::HashMap;
use std::fs::File;


pub fn from_yaml<T>(path: &str) -> T
where T: for<'de> serde::Deserialize<'de> {
    let input_file = File::open(path).expect("Failed to open configuration file");
    serde_yaml::from_reader(input_file).expect("Failed to parse configuration file")
}

fn map_to_client_config(config_map: &HashMap<String, String>) -> ClientConfig {
    config_map
        .iter()
        .fold(ClientConfig::new(), |mut config, (key, value)| {
            config.set(key, value);
            config
        })
}

fn zero() -> u64 {
    0
}

fn one() -> u64 {
    1
}

//
// ********** PRODUCER CONFIG **********
//
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProducerScenario {
    #[serde(default = "one")]
    pub repeat_times: u64,
    #[serde(default = "zero")]
    pub repeat_pause: u64,
    #[serde(default = "one")]
    pub threads: u64,
    #[serde(default)]
    pub producer: ProducerType,
    pub message_size: u64,
    pub message_count: u64,
    pub topic: String,
    pub producer_config: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProducerBenchmarkConfig {
    pub scenarios: HashMap<String, ProducerScenario>,
}

impl ProducerScenario {
    pub fn generate_producer_config(&self) -> ClientConfig {
        map_to_client_config(&self.producer_config)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ProducerType {
    BaseProducer,
    FutureProducer,
}

impl Default for ProducerType {
    fn default() -> Self {
        ProducerType::BaseProducer
    }
}

//
// ********** CONSUMER CONFIG **********
//


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

impl Default for ConsumerType {
    fn default() -> Self {
        ConsumerType::BaseConsumer
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ConsumerScenario {
    pub repeat_times: u64,
    pub repeat_pause: u64,
    pub consumer_type: ConsumerType,
    pub message_limit: i64,
    pub topic: String,
    pub consumer_config: HashMap<String, String>,
}

fn or_expect<T: Clone>(first: &Option<T>, second: &Option<T>, name: &str) -> T {
    first.as_ref()
        .cloned()
        .or_else(|| second.clone())
        .expect(&format!("Missing configuration parameter: {}", name))
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
        let mut scenarios = HashMap::new();

        for (name, scenario) in raw_config.scenarios {
            scenarios.insert(name.clone(), ConsumerScenario::from_file_config(&raw_config.default, &scenario));
        }

        ConsumerBenchmark {
            scenarios
        }
    }
}

// This api should be improved.
//impl ConsumerBenchmark {
//    pub fn generate_consumer_config(&self, scenario: &ConsumerScenario) -> ClientConfig {
//        let mut merged_config = self.default_consumer_config.clone();
//        for (key, value) in scenario.consumer_config.iter() {
//            merged_config.insert(key.clone(), value.clone());
//        }
//        map_to_client_config(&merged_config)
//    }
//}
