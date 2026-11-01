//! A ring buffer with disk persistence. See `docs/design.md` for the design.

mod config;
mod core;
mod frame;
mod header;
mod io;
mod memory;
mod pos;
mod recovery;
mod ring;

#[cfg(test)]
mod tests;

pub use config::{ChecksumMode, Config, IoConfig, Preallocate, SyncMode};
pub use core::RecoveryInfo;
pub use ring::{AppendError, ReadError, Record, Ring, RingDriver};
