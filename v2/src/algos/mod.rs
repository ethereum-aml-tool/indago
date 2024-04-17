mod fifo;
mod haircut;
mod poison;
mod seniority;

pub struct Balance {
    pub total: u128,
    pub tainted: u128,
}

pub use fifo::Fifo;
pub use haircut::Haircut;
pub use poison::Poison;
pub use seniority::Seniority;
