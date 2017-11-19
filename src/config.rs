use serde_yaml;

use rdkafka::ClientConfig;

use std::collections::HashMap;
use std::fs::File;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchmarkConfig {
    pub scenarios: HashMap<String, Scenario>
}

impl BenchmarkConfig {
    pub fn from_file(path: &str) -> BenchmarkConfig {
        let input_file = File::open(path)
            .expect("Failed to open configuration file");
        serde_yaml::from_reader(input_file)
            .expect("Failed to parse configuration file")
    }
}

fn zero() -> usize { 0 }
fn one() -> usize { 1 }

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Scenario {
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
    pub producer_config: HashMap<String, String>
}

impl Scenario {
    pub fn generate_producer_config(&self) -> ClientConfig {
        self.producer_config.iter()
            .fold(ClientConfig::new(), |mut config, (key, value)| {config.set(key, value); config})
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ProducerType {
    BaseProducer,
    FutureProducer
}

impl Default for ProducerType {
    fn default() -> Self {
        ProducerType::BaseProducer
    }
}
