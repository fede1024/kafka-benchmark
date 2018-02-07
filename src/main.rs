#[macro_use]
extern crate clap;
extern crate env_logger;
extern crate futures;
extern crate rand;
extern crate rdkafka;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate serde_yaml;

mod producer;

fn main() {
    let matches = clap_app!(app =>
        (name: "kafka benchmark")
        (@arg benchmark_type: +takes_value +required "Benchmark type ('producer' or 'consumer')")
        (@arg config: +takes_value +required "The configuration file")
        (@arg scenario: +takes_value +required "The scenario you want to execute")
    ).get_matches();

    env_logger::init();

    let config_file = matches.value_of("config").unwrap();
    let scenario_name = matches.value_of("scenario").unwrap();

    match matches.value_of("benchmark_type").unwrap() {
        "consumer" => println!("Not yet implemented"),
        "producer" => producer::run(config_file, scenario_name),
        _ => println!("Undefined benchmark type. Please use 'producer' or 'consumer'"),
    }
}
