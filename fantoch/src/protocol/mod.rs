// This module contains the implementation of data structured used to hold info
// about commands.
mod info;

// This module contains the definition of `BaseProcess`.
mod base;

// This module contains the definition of a basic replication protocol that
// waits for f + 1 acks before committing a command. It's for sure inconsistent
// and most likely non-fault-tolerant until we base it on the synod module.
// TODO evolve the synod module so that is allows patterns like Coordinated
// Paxos and Simple Paxos from Mencius. With such patterns we can make this
// protocol fault-tolerant (but still inconsistent).
mod basic;

// This module contains common functionality from tracking when it's safe to
// garbage-collect a command, i.e., when it's been committed at all processes.
mod gc;

// Re-exports.
pub use base::BaseProcess;
pub use basic::Basic;
pub use info::{CommandsInfo, Info};

use crate::command::Command;
use crate::config::Config;
use crate::executor::Executor;
use crate::id::{Dot, ProcessId, ShardId};
use crate::metrics::Metrics;
use crate::time::SysTime;
use crate::HashSet;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Debug};
use std::time::Duration;

pub trait Protocol: Debug + Clone {
    type Message: Debug
        + Clone
        + Eq
        + PartialEq
        + Serialize
        + DeserializeOwned
        + Send
        + Sync
        + MessageIndex; // TODO why is Sync needed??
    type PeriodicEvent: Debug + Clone + Send + Sync + PeriodicEventIndex + Eq;
    type Executor: Executor + Send;

    /// Returns a new instance of the protocol and a list of periodic events.
    fn new(
        process_id: ProcessId,
        shard_id: ShardId,
        config: Config,
    ) -> (Self, Vec<(Self::PeriodicEvent, Duration)>);

    fn id(&self) -> ProcessId;

    fn shard_id(&self) -> ShardId;

    fn discover(&mut self, processes: Vec<(ProcessId, ShardId)>) -> bool;

    #[must_use]
    fn submit(
        &mut self,
        dot: Option<Dot>,
        cmd: Command,
        time: &dyn SysTime,
    ) -> Vec<Action<Self>>;

    #[must_use]
    fn handle(
        &mut self,
        from: ProcessId,
        from_shard_id: ShardId,
        msg: Self::Message,
        time: &dyn SysTime,
    ) -> Vec<Action<Self>>;

    #[must_use]
    fn handle_event(
        &mut self,
        event: Self::PeriodicEvent,
        time: &dyn SysTime,
    ) -> Vec<Action<Self>>;

    #[must_use]
    fn to_executor(
        &mut self,
    ) -> Vec<<Self::Executor as Executor>::ExecutionInfo>;

    fn parallel() -> bool;

    fn leaderless() -> bool;

    fn metrics(&self) -> &ProtocolMetrics;
}

pub type ProtocolMetrics = Metrics<ProtocolMetricsKind, u64>;

#[derive(Clone, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProtocolMetricsKind {
    FastPath,
    SlowPath,
    Stable,
}

impl Debug for ProtocolMetricsKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ProtocolMetricsKind::FastPath => write!(f, "fast_path"),
            ProtocolMetricsKind::SlowPath => write!(f, "slow_path"),
            ProtocolMetricsKind::Stable => write!(f, "stable"),
        }
    }
}

pub trait MessageIndex {
    /// This trait is used to decide to which worker some messages should be
    /// forwarded to, ensuring that messages with the same index are forwarded
    /// to the same process. If `None` is returned, then the message is sent to
    /// all workers. In particular, if the protocol is not parallel, the
    /// message is sent to the single protocol worker.
    ///
    /// There only 2 types of indexes are supported:
    /// - Some((reserved, index)): `index` will be used to compute working index
    ///   making sure that index is higher than `reserved`
    /// - None: no indexing; message will be sent to all workers
    fn index(&self) -> Option<(usize, usize)>;
}

pub trait PeriodicEventIndex {
    /// Same as `MessageIndex`.
    fn index(&self) -> Option<(usize, usize)>;
}

#[derive(Debug, Clone, PartialEq)]
pub enum Action<P: Protocol> {
    ToSend {
        target: HashSet<ProcessId>,
        msg: <P as Protocol>::Message,
    },
    ToForward {
        msg: <P as Protocol>::Message,
    },
}
