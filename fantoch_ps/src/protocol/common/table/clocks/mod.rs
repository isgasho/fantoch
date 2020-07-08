// This module contains the definition of `KeyClocks`.
mod keys;

// This module contains the definition of `QuorumClocks`.
mod quorum;

// Re-exports.
pub use keys::{AtomicKeyClocks, LockedKeyClocks, KeyClocks, SequentialKeyClocks};
pub use quorum::QuorumClocks;
