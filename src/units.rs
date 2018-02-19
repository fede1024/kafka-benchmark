use rdkafka::util::duration_to_millis;

use std::fmt;
use std::ops::{Add, AddAssign, Div};
use std::time::Duration;

//
// ********** SECONDS **********
//

#[derive(Copy, Clone, Debug, Ord, PartialOrd, Eq, PartialEq)]
pub struct Seconds(pub Duration);

impl fmt::Display for Seconds {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if f.alternate() {
            write!(f, "{:.3}", duration_to_millis(self.0) as f64 / 1000.0)
        } else {
            write!(f, "{:.3} seconds", duration_to_millis(self.0) as f64 / 1000.0)
        }
    }
}

//
// ********** MESSAGES **********
//

#[derive(Copy, Clone, Debug, PartialOrd, PartialEq)]
pub struct Messages(pub f64);

impl Messages {
    pub fn zero() -> Messages {
        Messages(0f64)
    }

    pub fn is_zero(&self) -> bool {
        self.0 == 0f64
    }
}

impl Default for Messages {
    fn default() -> Messages {
        Messages(0f64)
    }
}

impl Add<usize> for Messages {
    type Output = Messages;

    fn add(self, rhs: usize) -> Messages {
        Messages(self.0 + rhs as f64)
    }
}

impl AddAssign<usize> for Messages {
    fn add_assign(&mut self, rhs: usize) {
        self.0 += rhs as f64;
    }
}

impl Div<f64> for Messages {
    type Output = Messages;

    fn div(self, rhs: f64) -> Messages {
        Messages(self.0 / rhs)
    }
}

impl From<u64> for Messages {
    fn from(amount: u64) -> Messages {
        Messages(amount as f64)
    }
}

impl fmt::Display for Messages {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if f.alternate() {
            write!(f, "{:.0}", self.0)
        } else {
            write!(f, "{:.0} messages", self.0)
        }
    }
}

//
// ********** BYTES **********
//
const GB: f64 = (1<<30) as f64;
const MB: f64 = (1<<20) as f64;
const KB: f64 = (1<<10) as f64;

#[derive(Copy, Clone, Debug, PartialOrd, PartialEq)]
pub struct Bytes(pub f64);

impl Bytes {
    pub fn zero() -> Bytes {
        Bytes(0f64)
    }
}

impl Default for Bytes {
    fn default() -> Bytes {
        Bytes(0f64)
    }
}

impl From<u64> for Bytes {
    fn from(amount: u64) -> Bytes {
        Bytes(amount as f64)
    }
}

impl Add<usize> for Bytes {
    type Output = Bytes;

    fn add(self, rhs: usize) -> Bytes {
        Bytes(self.0 + rhs as f64)
    }
}

impl AddAssign<usize> for Bytes {
    fn add_assign(&mut self, rhs: usize) {
        self.0 += rhs as f64;
    }
}

impl Div<f64> for Bytes {
    type Output = Bytes;

    fn div(self, rhs: f64) -> Bytes {
        Bytes(self.0 / rhs)
    }
}

impl fmt::Display for Bytes {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if self.0 >= GB {
            write!(f, "{:.3} GB", self.0 / GB)
        } else if self.0 >= MB {
            write!(f, "{:.3} MB", self.0 / MB)
        } else if self.0 >= KB {
            write!(f, "{:.3} KB", self.0 / KB)
        } else {
            write!(f, "{} B", self.0)
        }
    }
}

//
// ********** RATE **********
//

#[derive(Debug)]
pub struct Rate<T> {
    pub amount: T,
    pub duration: Duration,
}

impl<T> Rate<T> {
    pub fn new(amount: T, duration: Duration) -> Rate<T> {
        Rate { amount, duration }
    }
}

impl Div<Seconds> for Messages {
    type Output = Rate<Messages>;

    fn div(self, rhs: Seconds) -> Rate<Messages> {
        Rate::new(self, rhs.0)
    }
}

impl Div<Seconds> for Bytes {
    type Output = Rate<Bytes>;

    fn div(self, rhs: Seconds) -> Rate<Bytes> {
        Rate::new(self, rhs.0)
    }
}

impl<T: Div<f64, Output=T> + fmt::Display + Copy> fmt::Display for Rate<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let duration_s = duration_to_millis(self.duration) as f64 / 1000f64;
        if f.alternate() {
            write!(f, "{:#}", self.amount / duration_s)
        } else {
            write!(f, "{}/s", self.amount / duration_s)
        }
    }
}

