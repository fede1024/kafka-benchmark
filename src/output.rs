use rdkafka::util::duration_to_millis;

use config::Scenario;

use std::time::Duration;
use std::cmp;


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
        if self.0 >= 1 << 30 {
            format!("{:.3} GB", self.0 as f32 / (1 << 30) as f32)
        } else if self.0 >= 1 << 20 {
            format!("{:.3} MB", self.0 as f32 / (1 << 20) as f32)
        } else if self.0 >= 1 << 10 {
            format!("{:.3} KB", self.0 as f32 / (1 << 10) as f32)
        } else {
            format!("{} B", self.0)
        }
    }
}

#[derive(Debug)]
pub struct ThreadStats {
    duration: Duration,
    failure_count: usize
}

impl ThreadStats {
    pub fn new(duration: Duration, failure_count: usize) -> ThreadStats {
        ThreadStats { duration, failure_count }
    }
}

#[derive(Debug)]
pub struct ScenarioStats<'a> {
    scenario: &'a Scenario,
    failure_count: usize,
    duration: Duration
}

impl<'a> ScenarioStats<'a> {
    pub fn new(scenario: &'a Scenario) -> ScenarioStats<'a> {
        ScenarioStats {
            scenario,
            failure_count: 0,
            duration: Duration::from_secs(0)
        }
    }

    pub fn add_thread_stats(&mut self, thread_stats: &ThreadStats) {
        self.failure_count += thread_stats.failure_count;
        self.duration = cmp::max(self.duration, thread_stats.duration);
    }

    pub fn print(&self) {
        let elapsed_ms = duration_to_millis(self.duration) as f64;
        let total_msg = self.scenario.message_count as f64;
        let total_bytes = total_msg * self.scenario.message_size as f64;
        let byte_rate_s = total_bytes / elapsed_ms * 1000f64;
        let msg_rate_s = total_msg / elapsed_ms * 1000f64;

        if self.failure_count != 0 {
            println!("Warning: {} messages failed to be delivered", self.failure_count);
        }

        println!(
            "* Produced {} messages ({}) in {} using {} thread{}\n    {:.0} messages/s\n    {}/s",
            total_msg,
            Bytes(total_bytes as usize).to_human(),
            self.duration.to_human(),
            self.scenario.threads,
            if self.scenario.threads > 1 { "s" } else { "" },
            msg_rate_s,
            Bytes(byte_rate_s as usize).to_human()
        );
    }
}

pub struct BenchmarkStats<'a> {
    scenario: &'a Scenario,
    stats: Vec<ScenarioStats<'a>>
}

impl<'a> BenchmarkStats<'a> {
    pub fn new(scenario: &'a Scenario) -> BenchmarkStats<'a> {
        BenchmarkStats { scenario: scenario, stats: Vec::new() }
    }

    pub fn add_stat(&mut self, scenario_stat: ScenarioStats<'a>) {
        self.stats.push(scenario_stat)
    }

    pub fn print(&self) {
        let duration = self.stats.iter().map(|stat| stat.duration).sum();
        let total_msg = (self.scenario.message_count * self.stats.len()) as f64;

        let elapsed_ms = duration_to_millis(duration) as f64;
        let total_bytes = total_msg * self.scenario.message_size as f64;
        let byte_rate_s = total_bytes / elapsed_ms * 1000f64;
        let msg_rate_s = total_msg / elapsed_ms * 1000f64;

        println!("Average: {:.0} messages/s, {}/s", msg_rate_s, Bytes(byte_rate_s as usize).to_human());
    }
}
