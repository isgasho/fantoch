// This module contains the implementation of a basic executor that executes operations as soon as
// it receives them.
mod basic;

// This module contains the implementation of a dependency graph executor.
mod graph;

// This module contains the implementation of a votes table executor.
mod table;

// Re-exports.
pub use basic::{BasicExecutionInfo, BasicExecutor};
pub use graph::{GraphExecutionInfo, GraphExecutor};
pub use table::{TableExecutionInfo, TableExecutor};

use crate::command::CommandResult;
use crate::config::Config;
use crate::id::Rifl;
use crate::kvs::{KVOpResult, Key};
use std::fmt::Debug;

pub trait Executor {
    type ExecutionInfo: Debug + Send + ExecutionInfoKey;

    fn new(config: Config) -> Self;

    fn register(&mut self, rifl: Rifl, key_count: usize);

    fn handle(&mut self, infos: Self::ExecutionInfo) -> ExecutorResult;

    fn parallel(&self) -> bool;

    fn show_metrics(&self) {
        // by default, nothing to show
    }
}

pub trait ExecutionInfoKey {
    /// If `None` is returned, then the execution info is sent to all executor processes.
    /// In particular, if the executor is not parallel, the execution info is sent to the single
    /// executor process.
    fn key(&self) -> Option<Key> {
        None
    }
}

// TODO maybe extend this with variants `Nothing`, `SingleReady`, `MultiReady`, etc
pub enum ExecutorResult {
    Ready(Vec<CommandResult>),
    Partial(Rifl, Key, KVOpResult),
}
