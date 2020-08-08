use crate::executor::table::MultiVotesTable;
use crate::protocol::common::table::VoteRange;
use fantoch::command::Command;
use fantoch::config::Config;
use fantoch::executor::{
    Executor, ExecutorMetrics, ExecutorResult, MessageKey, Pending,
};
use fantoch::id::{Dot, ProcessId, Rifl, ShardId};
use fantoch::kvs::{KVOp, KVStore, Key};
use serde::{Deserialize, Serialize};
use tracing::instrument;

pub struct TableExecutor {
    execute_at_commit: bool,
    table: MultiVotesTable,
    store: KVStore,
    pending: Pending,
    metrics: ExecutorMetrics,
    to_clients: Vec<ExecutorResult>,
}

impl Executor for TableExecutor {
    type ExecutionInfo = TableExecutionInfo;

    fn new(
        process_id: ProcessId,
        shard_id: ShardId,
        config: Config,
        executors: usize,
    ) -> Self {
        // TODO this is specific to newt
        let (_, _, stability_threshold) = config.newt_quorum_sizes();
        let table = MultiVotesTable::new(
            process_id,
            shard_id,
            config.n(),
            stability_threshold,
        );
        let store = KVStore::new();
        // aggregate results if the number of executors is 1
        let aggregate = executors == 1;
        let pending = Pending::new(aggregate, process_id, shard_id);
        let metrics = ExecutorMetrics::new();
        let to_clients = Vec::new();

        Self {
            execute_at_commit: config.execute_at_commit(),
            table,
            store,
            pending,
            metrics,
            to_clients,
        }
    }

    fn wait_for(&mut self, cmd: &Command) {
        // start command in pending
        assert!(self.pending.wait_for(cmd));
    }

    fn wait_for_rifl(&mut self, rifl: Rifl) {
        self.pending.wait_for_rifl(rifl);
    }

    fn handle(&mut self, info: Self::ExecutionInfo) {
        // handle each new info by updating the votes table and execute ready
        // commands
        match info {
            TableExecutionInfo::Votes {
                dot,
                clock,
                rifl,
                key,
                op,
                votes,
            } => {
                if self.execute_at_commit {
                    self.execute(key, std::iter::once((rifl, op)));
                } else {
                    let to_execute =
                        self.table.add_votes(dot, clock, rifl, &key, op, votes);
                    self.execute(key, to_execute);
                }
            }
            TableExecutionInfo::DetachedVotes { key, votes } => {
                if !self.execute_at_commit {
                    let to_execute = self.table.add_detached_votes(&key, votes);
                    self.execute(key, to_execute);
                }
            }
        }
    }

    fn to_clients(&mut self) -> Option<ExecutorResult> {
        self.to_clients.pop()
    }

    fn parallel() -> bool {
        true
    }

    fn metrics(&self) -> &ExecutorMetrics {
        &self.metrics
    }
}

impl TableExecutor {
    #[instrument(skip(self, key, to_execute))]
    fn execute<I>(&mut self, key: Key, to_execute: I)
    where
        I: Iterator<Item = (Rifl, KVOp)>,
    {
        to_execute.for_each(|(rifl, op)| {
            // execute op in the `KVStore`
            let op_result = self.store.execute(&key, op);

            // add partial result to `Pending`
            if let Some(executor_result) =
                self.pending.add_partial(rifl, || (key.clone(), op_result))
            {
                self.to_clients.push(executor_result);
            }
        })
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TableExecutionInfo {
    Votes {
        dot: Dot,
        clock: u64,
        rifl: Rifl,
        key: Key,
        op: KVOp,
        votes: Vec<VoteRange>,
    },
    DetachedVotes {
        key: Key,
        votes: Vec<VoteRange>,
    },
}

impl TableExecutionInfo {
    pub fn votes(
        dot: Dot,
        clock: u64,
        rifl: Rifl,
        key: Key,
        op: KVOp,
        votes: Vec<VoteRange>,
    ) -> Self {
        TableExecutionInfo::Votes {
            dot,
            clock,
            rifl,
            key,
            op,
            votes,
        }
    }

    pub fn detached_votes(key: Key, votes: Vec<VoteRange>) -> Self {
        TableExecutionInfo::DetachedVotes { key, votes }
    }
}

impl MessageKey for TableExecutionInfo {
    fn key(&self) -> Option<&Key> {
        let key = match self {
            TableExecutionInfo::Votes { key, .. } => key,
            TableExecutionInfo::DetachedVotes { key, .. } => key,
        };
        Some(&key)
    }
}
