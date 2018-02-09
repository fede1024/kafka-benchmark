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

fn zero() -> usize {
    0
}

fn one() -> usize {
    1
}

//
// ********** PRODUCER CONFIG **********
//
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProducerScenario {
    #[serde(default = "one")]
    pub repeat_times: usize,
    #[serde(default = "zero")]
    pub repeat_pause: usize,
    #[serde(default = "one")]
    pub threads: usize,
    #[serde(default)]
    pub producer: ProducerType,
    pub message_size: usize,
    pub message_count: usize,
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
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumerScenario {
    #[serde(default = "one")]
    pub repeat_times: usize,
    #[serde(default = "zero")]
    pub repeat_pause: usize,
    #[serde(default = "one")]
    pub threads: usize,
    #[serde(default)]
    pub consumer: ConsumerType,
    #[serde(default = "zero")]
    pub message_count: usize,  // Zero means consume the whole topic
    pub topic: String,
    pub consumer_config: HashMap<String, String>,
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
pub struct ConsumerBenchmarkConfig {
    pub scenarios: HashMap<String, ProducerScenario>,
}

impl ConsumerScenario {
    pub fn generate_consumer_config(&self) -> ClientConfig {
        map_to_client_config(&self.consumer_config)
    }
}
