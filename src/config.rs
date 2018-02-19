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

// TODO: separate the structure matching the file configuration and
// the structure used internally. Or alternatively implement deserialization directly,
// without serde.

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumerScenario {
    #[serde(default = "one")]
    pub repeat_times: u64,
    #[serde(default = "zero")]
    pub repeat_pause: u64,
    #[serde(default)]
    pub consumer: ConsumerType,
    pub limit: Option<u64>,
    pub topic: String,
    #[serde(default)]
    pub consumer_config: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Range {
    /// Consume the whole topic
    All,
    /// Consume from the beginning, up to the specified number of messages.
    FirstNMessages(u64),
}

impl Default for Range {
    fn default() -> Self {
        Range::All
    }
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumerBenchmark {
    pub default_consumer_config: HashMap<String, String>,
    pub scenarios: HashMap<String, ConsumerScenario>,
}

// This api should be improved.
impl ConsumerBenchmark {
    pub fn generate_consumer_config(&self, scenario: &ConsumerScenario) -> ClientConfig {
        let mut merged_config = self.default_consumer_config.clone();
        for (key, value) in scenario.consumer_config.iter() {
            merged_config.insert(key.clone(), value.clone());
        }
        map_to_client_config(&merged_config)
    }
}
