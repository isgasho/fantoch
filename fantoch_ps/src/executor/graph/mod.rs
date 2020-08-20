// This module contains the definition of `TarjanSCCFinder` and `FinderResult`.
mod tarjan;

/// This module contains the definition of `VertexIndex` and `PendingIndex`.
mod index;

/// This modules contains the definition of `GraphExecutor` and
/// `GraphExecutionInfo`.
mod executor;

/// This module contains the definition of `LevelExecutedClock`.
mod level;

// Re-exports.
pub use executor::{GraphExecutionInfo, GraphExecutor};

use self::index::{PendingIndex, VertexIndex};
use self::level::LevelExecutedClock;
use self::tarjan::{FinderResult, TarjanSCCFinder, Vertex, SCC};
use fantoch::command::Command;
use fantoch::config::Config;
use fantoch::executor::{ExecutorMetrics, ExecutorMetricsKind};
use fantoch::id::{Dot, ProcessId, ShardId};
use fantoch::log;
use fantoch::time::SysTime;
use fantoch::util;
use fantoch::{HashMap, HashSet};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::sync::Arc;
use threshold::{AEClock, VClock};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RequestReply {
    Info {
        dot: Dot,
        cmd: Command,
        clock: VClock<ProcessId>,
    },
    Executed {
        dot: Dot,
    },
}

impl RequestReply {
    fn dot(&self) -> &Dot {
        match self {
            Self::Info { dot, .. } => dot,
            Self::Executed { dot, .. } => dot,
        }
    }
}

#[derive(Clone)]
pub struct DependencyGraph {
    executor_index: usize,
    process_id: ProcessId,
    shard_id: ShardId,
    executed_clock: AEClock<ProcessId>,
    // only used in partial replication
    executed_clock_snapshot: Arc<RwLock<AEClock<ProcessId>>>,
    // only used in partial replication
    level_executed_clock: LevelExecutedClock,
    vertex_index: VertexIndex,
    pending_index: PendingIndex,
    finder: TarjanSCCFinder,
    metrics: ExecutorMetrics,
    // worker 0 (handles commands):
    // - adds new commands `to_execute`
    // - `out_requests` dependencies to be able to order commands
    to_execute: Vec<Command>,
    out_requests: HashMap<ShardId, HashSet<Dot>>,
    // worker 1 (handles requests):
    // - may have `buffered_in_requests` when doesn't have the command yet
    // - produces `out_request_replies` when it has the command
    buffered_in_requests: HashMap<ShardId, HashSet<Dot>>,
    out_request_replies: HashMap<ShardId, Vec<RequestReply>>,
}

enum FinderInfo {
    // set of dots in found SCCs
    Found(Vec<Dot>),
    // set of dots in found SCCs (it's possible to find SCCs even though the
    // search for another dot failed), missing dependencies and set of dots
    // visited while searching for SCCs
    MissingDependencies(Vec<Dot>, HashSet<Dot>, HashSet<Dot>),
    // in case we try to find SCCs on dots that are no longer pending
    NotPending,
}

impl DependencyGraph {
    /// Create a new `Graph`.
    pub fn new(
        process_id: ProcessId,
        shard_id: ShardId,
        config: &Config,
    ) -> Self {
        // this value will be overwritten
        let executor_index = 0;
        // create executed clock
        let ids: Vec<_> = util::all_process_ids(config.shards(), config.n())
            .map(|(process_id, _)| process_id)
            .collect();
        let executed_clock = AEClock::with(ids.clone());
        let executed_clock_snapshot = Arc::new(RwLock::new(AEClock::with(ids)));
        // create level executed clock
        let level_executed_clock =
            LevelExecutedClock::new(process_id, shard_id, config);
        // create indexes
        let vertex_index = VertexIndex::new(process_id);
        let pending_index = PendingIndex::new(process_id, shard_id, *config);
        // create finder
        let finder = TarjanSCCFinder::new(process_id, shard_id, *config);
        let metrics = ExecutorMetrics::new();
        // create to execute
        let to_execute = Vec::new();
        // create requests and request replies
        let out_requests = Default::default();
        let buffered_in_requests = Default::default();
        let out_request_replies = Default::default();
        DependencyGraph {
            executor_index,
            process_id,
            shard_id,
            executed_clock,
            executed_clock_snapshot,
            level_executed_clock,
            vertex_index,
            pending_index,
            finder,
            metrics,
            to_execute,
            out_requests,
            buffered_in_requests,
            out_request_replies,
        }
    }

    fn set_executor_index(&mut self, index: usize) {
        self.executor_index = index;
    }

    /// Returns a new command ready to be executed.
    #[must_use]
    pub fn command_to_execute(&mut self) -> Option<Command> {
        self.to_execute.pop()
    }

    /// Returns a request.
    #[must_use]
    pub fn requests(&mut self) -> HashMap<ShardId, HashSet<Dot>> {
        std::mem::take(&mut self.out_requests)
    }

    /// Returns a set of request replies.
    #[must_use]
    pub fn request_replies(&mut self) -> HashMap<ShardId, Vec<RequestReply>> {
        std::mem::take(&mut self.out_request_replies)
    }

    #[cfg(test)]
    fn commands_to_execute(&mut self) -> Vec<Command> {
        std::mem::take(&mut self.to_execute)
    }

    fn metrics(&self) -> &ExecutorMetrics {
        &self.metrics
    }

    fn cleanup(&mut self, time: &dyn SysTime) {
        log!(
            "p{}: @{} Graph::cleanup | time = {}",
            self.process_id,
            self.executor_index,
            time.millis()
        );
        if self.executor_index == 0 {
            self.level_executed_clock
                .maybe_level(&mut self.executed_clock, time);
            // if main executor, update snapshot
            *self.executed_clock_snapshot.write() = self.executed_clock.clone();
        } else {
            // otherwise, simply check pending remote requests
            self.check_pending_requests(time);
        }
    }

    /// Add a new command with its clock to the queue.
    pub fn handle_add(
        &mut self,
        dot: Dot,
        cmd: Command,
        clock: VClock<ProcessId>,
        time: &dyn SysTime,
    ) {
        assert_eq!(self.executor_index, 0);
        log!(
            "p{}: @{} Graph::handle_add {:?} {:?} | time = {}",
            self.process_id,
            self.executor_index,
            dot,
            clock,
            time.millis()
        );

        // create new vertex for this command
        let vertex = Vertex::new(dot, cmd, clock, time);

        if self.vertex_index.index(vertex).is_some() {
            panic!(
                "p{}: @{} Graph::handle_add tried to index already indexed {:?}",
                self.process_id, self.executor_index, dot
            );
        }

        // get current command ready count and count newly ready commands
        let initial_ready = self.to_execute.len();
        let mut total_found = 0;

        // try to find new SCCs
        match self.find_scc(dot, &mut total_found, time) {
            FinderInfo::Found(dots) => {
                // try to execute other commands if new SCCs were found
                self.check_pending(dots, &mut total_found, time);
            }
            FinderInfo::MissingDependencies(dots, dep_dot, _visited) => {
                // update the pending
                self.index_pending(dep_dot, dot, time);
                // try to execute other commands if new SCCs were found
                self.check_pending(dots, &mut total_found, time);
            }
            FinderInfo::NotPending => {
                panic!("just added dot must be pending");
            }
        }

        // check that all newly ready commands have been incorporated
        assert_eq!(self.to_execute.len(), initial_ready + total_found);

        log!(
            "p{}: @{} Graph::log executed {:?} | pending {:?} | time = {}",
            self.process_id,
            self.executor_index,
            self.executed_clock,
            self.vertex_index
                .dots()
                .collect::<std::collections::BTreeSet<_>>(),
            time.millis()
        );
    }

    pub fn handle_add_mine(&mut self, dot: Dot, _time: &dyn SysTime) {
        assert_eq!(self.executor_index, 0);
        log!(
            "p{}: @{} Graph::handle_add_mine {:?} | time = {}",
            self.process_id,
            self.executor_index,
            dot,
            _time.millis()
        );
        self.pending_index.add_mine(dot);
    }

    fn handle_request(
        &mut self,
        from: ShardId,
        dots: HashSet<Dot>,
        _time: &dyn SysTime,
    ) {
        assert!(self.executor_index > 0);
        log!(
            "p{}: @{} Graph::handle_request {:?} from {:?} | time = {}",
            self.process_id,
            self.executor_index,
            dots,
            from,
            _time.millis()
        );
        // save in requests metric
        self.metrics.aggregate(ExecutorMetricsKind::InRequests, 1);
        // simply buffer the request
        self.buffered_in_requests
            .entry(from)
            .or_default()
            .extend(dots);
    }

    fn process_requests(
        &mut self,
        from: ShardId,
        dots: impl Iterator<Item = Dot>,
        _time: &dyn SysTime,
    ) {
        assert!(self.executor_index > 0);
        for dot in dots {
            log!(
                "p{}: @{} Graph::process_requests {:?} from {:?} | time = {}",
                self.process_id,
                self.executor_index,
                dot,
                from,
                _time.millis()
            );
            if let Some(vertex) = self.vertex_index.find(&dot) {
                let vertex = vertex.read();

                // only send the vertex if the shard that requested this vertex
                // does not replicate it
                if vertex.cmd.replicated_by(&from) {
                    log!(
                        "p{}: @{} Graph::process_requests {:?} is replicated by {:?} (WARN) | time = {}",
                        self.process_id,
                        self.executor_index,
                        dot,
                        from,
                        _time.millis()
                    )
                } else {
                    self.out_request_replies.entry(from).or_default().push(
                        RequestReply::Info {
                            dot,
                            cmd: vertex.cmd.clone(),
                            clock: vertex.clock.clone(),
                        },
                    )
                }
            } else {
                // if we don't have it, then check if it's executed (in our
                // snapshot)
                if self
                    .executed_clock_snapshot
                    .read()
                    .contains(&dot.source(), dot.sequence())
                {
                    log!(
                        "p{}: @{} Graph::process_requests {:?} is already executed | time = {}",
                        self.process_id,
                        self.executor_index,
                        dot,
                        _time.millis()
                    );
                    self.out_request_replies
                        .entry(from)
                        .or_default()
                        .push(RequestReply::Executed { dot });
                } else {
                    log!(
                        "p{}: @{} Graph::process_requests from {:?} for a dot {:?} we don't have | time = {}",
                        self.process_id,
                        self.executor_index,
                        from,
                        dot,
                        _time.millis()
                    );
                    // buffer request again
                    self.buffered_in_requests
                        .entry(from)
                        .or_default()
                        .insert(dot);
                }
            }
        }
    }

    pub fn handle_request_reply(
        &mut self,
        infos: Vec<RequestReply>,
        time: &dyn SysTime,
    ) {
        assert_eq!(self.executor_index, 0);
        let mut accepted_replies = 0;
        for info in infos {
            log!(
                "p{}: @{} Graph::handle_request_reply {:?} | time = {}",
                self.process_id,
                self.executor_index,
                info,
                time.millis()
            );
            if self.pending_index.is_mine(info.dot()) {
                log!(
                    "p{}: @{} Graph::handle_request_reply ignore {:?} as it is mine | time = {}",
                    self.process_id,
                    self.executor_index,
                    info,
                    time.millis()
                );
                continue;
            }
            match info {
                RequestReply::Info { dot, cmd, clock } => {
                    // count number of accepted replies
                    accepted_replies += 1;

                    self.handle_add(dot, cmd, clock, time)
                }
                RequestReply::Executed { dot } => {
                    // add to executed if not mine
                    if !self.pending_index.is_mine(&dot) {
                        // count number of accepted replies
                        accepted_replies += 1;

                        // update executed clock
                        self.executed_clock.add(&dot.source(), dot.sequence());
                        // check pending
                        let dots = vec![dot];
                        let mut total_found = 0;
                        self.check_pending(dots, &mut total_found, time);
                    }
                }
            }
        }
        // save in request replies metric
        self.metrics
            .aggregate(ExecutorMetricsKind::InRequestReplies, accepted_replies);
    }

    #[must_use]
    fn find_scc(
        &mut self,
        dot: Dot,
        total_found: &mut usize,
        time: &dyn SysTime,
    ) -> FinderInfo {
        assert_eq!(self.executor_index, 0);
        log!(
            "p{}: @{} Graph::find_scc {:?} | time = {}",
            self.process_id,
            self.executor_index,
            dot,
            time.millis()
        );
        // execute tarjan's algorithm
        let mut found = 0;
        let finder_result = self.strong_connect(dot, &mut found);

        // update total found
        *total_found += found;

        // get sccs
        let sccs = self.finder.sccs();

        // save new SCCs
        let mut dots = Vec::with_capacity(found);
        sccs.into_iter().for_each(|scc| {
            self.save_scc(scc, &mut dots, time);
        });

        // reset finder state and get visited dots
        let visited = self.finder.finalize(&self.vertex_index);

        // NOTE: what follows must be done even if
        // `FinderResult::MissingDependency` was returned - it's possible that
        // while running the finder for some dot `X` we actually found SCCs with
        // another dots, even though the find for this dot `X` failed!

        // save new SCCs if any were found
        match finder_result {
            FinderResult::Found => FinderInfo::Found(dots),
            FinderResult::MissingDependencies(deps) => {
                FinderInfo::MissingDependencies(dots, deps, visited)
            }
            FinderResult::NotPending => FinderInfo::NotPending,
            FinderResult::NotFound => panic!(
                "either there's a missing dependency, or we should find an SCC"
            ),
        }
    }

    fn save_scc(&mut self, scc: SCC, dots: &mut Vec<Dot>, time: &dyn SysTime) {
        assert_eq!(self.executor_index, 0);

        // save chain size metric
        self.metrics
            .collect(ExecutorMetricsKind::ChainSize, scc.len() as u64);

        scc.into_iter().for_each(|dot| {
            log!(
                "p{}: @{} Graph::save_scc removing {:?} from indexes | time = {}",
                self.process_id,
                self.executor_index,
                dot,
                time.millis()
            );

            // remove from vertex index
            let vertex = self
                .vertex_index
                .remove(&dot)
                .expect("dots from an SCC should exist");

            // update the set of ready dots
            dots.push(dot);

            // get command
            let (duration, cmd) = vertex.into_command(time);

            // save execution delay metric
            self.metrics
                .collect(ExecutorMetricsKind::ExecutionDelay, duration);

            // add command to commands to be executed
            self.to_execute.push(cmd);
        })
    }

    fn index_pending(
        &mut self,
        missing_deps: HashSet<Dot>,
        dot: Dot,
        _time: &dyn SysTime,
    ) {
        let mut requests = 0;
        for dep_dot in missing_deps {
            if let Some(target_shard) = self.pending_index.index(dep_dot, dot) {
                log!(
                    "p{}: @{} Graph::index_pending will ask {:?} to {:?} | time = {}",
                    self.process_id,
                    self.executor_index,
                    dep_dot,
                    target_shard,
                    _time.millis()
                );
                requests += 1;
                self.out_requests
                    .entry(target_shard)
                    .or_default()
                    .insert(dep_dot);
            }
        }
        // save out requests metric
        self.metrics
            .aggregate(ExecutorMetricsKind::OutRequests, requests);
    }

    fn check_pending(
        &mut self,
        mut dots: Vec<Dot>,
        total_found: &mut usize,
        time: &dyn SysTime,
    ) {
        assert_eq!(self.executor_index, 0);
        while let Some(dot) = dots.pop() {
            // get pending commands that depend on this dot
            if let Some(pending) = self.pending_index.remove(&dot) {
                log!(
                    "p{}: @{} Graph::try_pending {:?} depended on {:?} | time = {}",
                    self.process_id,
                    self.executor_index,
                    pending,
                    dot,
                    time.millis()
                );
                self.try_pending(pending, &mut dots, total_found, time);
            }
        }
        // once there are no more dots to try, no command in pending should be
        // possible to be executed, so we give up!
    }

    fn try_pending(
        &mut self,
        pending: HashSet<Dot>,
        dots: &mut Vec<Dot>,
        total_found: &mut usize,
        time: &dyn SysTime,
    ) {
        assert_eq!(self.executor_index, 0);
        // try to find new SCCs for each of those commands
        let mut visited = HashSet::new();

        for dot in pending {
            // only try to find new SCCs from non-visited commands
            if !visited.contains(&dot) {
                match self.find_scc(dot, total_found, time) {
                    FinderInfo::Found(new_dots) => {
                        // reset visited
                        visited.clear();

                        // if new SCCs were found, now there are more
                        // child dots to check
                        dots.extend(new_dots);
                    }
                    FinderInfo::MissingDependencies(
                        new_dots,
                        missing_deps,
                        new_visited,
                    ) => {
                        if !new_dots.is_empty() {
                            // if we found a new SCC, reset visited;
                            visited.clear();
                        } else {
                            // otherwise, try other pending commands,
                            // but don't try those that were visited in
                            // this search
                            visited.extend(new_visited);
                        }

                        // if new SCCs were found, now there are more
                        // child dots to check
                        dots.extend(new_dots);

                        // update pending
                        self.index_pending(missing_deps, dot, time);
                    }
                    FinderInfo::NotPending => {
                        // this happens if the pending dot is no longer
                        // pending
                    }
                }
            }
        }
    }

    fn strong_connect(&mut self, dot: Dot, found: &mut usize) -> FinderResult {
        assert_eq!(self.executor_index, 0);
        // get the vertex
        match self.vertex_index.find(&dot) {
            Some(vertex) => self.finder.strong_connect(
                dot,
                &vertex,
                &mut self.executed_clock,
                &self.vertex_index,
                found,
            ),
            None => {
                // in this case this `dot` is no longer pending
                FinderResult::NotPending
            }
        }
    }

    fn check_pending_requests(&mut self, time: &dyn SysTime) {
        let buffered = std::mem::take(&mut self.buffered_in_requests);
        for (from, dots) in buffered {
            self.process_requests(from, dots.into_iter(), time);
        }
    }
}

impl fmt::Debug for DependencyGraph {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "vertex index:")?;
        write!(f, "{:#?}", self.vertex_index)?;
        write!(f, "pending index:")?;
        write!(f, "{:#?}", self.pending_index)?;
        write!(f, "executed:")?;
        write!(f, "{:?}", self.executed_clock)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util;
    use fantoch::id::{ClientId, Rifl, ShardId};
    use fantoch::kvs::{KVOp, Key};
    use fantoch::time::RunTime;
    use fantoch::HashMap;
    use permutator::{Combination, Permutation};
    use rand::seq::SliceRandom;
    use std::cell::RefCell;
    use std::cmp::Ordering;
    use std::collections::{BTreeMap, BTreeSet};
    use std::iter::FromIterator;
    use threshold::{AEClock, AboveExSet, EventSet};

    #[test]
    fn simple() {
        // create queue
        let process_id = 1;
        let shard_id = 0;
        let n = 2;
        let f = 1;
        let config = Config::new(n, f);
        let mut queue = DependencyGraph::new(process_id, shard_id, &config);
        let time = RunTime;

        // cmd 0
        let dot_0 = Dot::new(1, 1);
        let cmd_0 =
            Command::put(Rifl::new(1, 1), String::from("A"), String::new());
        let clock_0 = util::vclock(vec![0, 1]);

        // cmd 1
        let dot_1 = Dot::new(2, 1);
        let cmd_1 =
            Command::put(Rifl::new(2, 1), String::from("A"), String::new());
        let clock_1 = util::vclock(vec![1, 0]);

        // add cmd 0
        queue.handle_add(dot_0, cmd_0.clone(), clock_0, &time);
        // check commands ready to be executed
        assert!(queue.commands_to_execute().is_empty());

        // add cmd 1
        queue.handle_add(dot_1, cmd_1.clone(), clock_1, &time);
        // check commands ready to be executed
        assert_eq!(queue.commands_to_execute(), vec![cmd_0, cmd_1]);
    }

    /// We have 5 commands by the same process (process A) that access the same
    /// key. We have `n = 5` and `f = 1` and thus the fast quorum size of 3.
    /// The fast quorum used by process A is `{A, B, C}`. We have the
    /// following order of commands in the 3 processes:
    /// - A: (1,1) (1,2) (1,3) (1,4) (1,5)
    /// - B: (1,3) (1,4) (1,1) (1,2) (1,5)
    /// - C: (1,1) (1,2) (1,4) (1,5) (1,3)
    ///
    /// The above results in the following final dependencies:
    /// - dep[(1,1)] = {(1,4)}
    /// - dep[(1,2)] = {(1,4)}
    /// - dep[(1,3)] = {(1,5)}
    /// - dep[(1,4)] = {(1,3)}
    /// - dep[(1,5)] = {(1,4)}
    ///
    /// The executor then receives the commit notifications of (1,3) (1,4) and
    /// (1,5) and, if transitive conflicts are assumed, these 3 commands
    /// form an SCC. This is because with this assumption we only check the
    /// highest conflicting command per replica, and thus (1,3) (1,4) and
    /// (1,5) have "all the dependencies".
    ///
    /// Then, the executor executes whichever missing command comes first, since
    /// (1,1) and (1,2) "only depend on an SCC already formed". This means that
    /// if two executors receive (1,3) (1,4) (1,5), and then one receives (1,1)
    /// and (1,2) while the other receives (1,2) and (1,1), they will execute
    /// (1,1) and (1,2) in differents order, leading to an inconsistency.
    ///
    /// This example is impossible if commands from the same process are
    /// processed (on the replicas computing dependencies) in their submission
    /// order. With this, a command never depends on later commands from the
    /// same process, which seems to be enough to prevent this issue. This means
    /// that parallelizing the processing of messages needs to be on a
    /// per-process basis, i.e. commands by the same process are always
    /// processed by the same worker.
    #[ignore]
    #[test]
    fn transitive_conflicts_assumption_regression_test_1() {
        // config
        let n = 5;
        let transitive_conflicts = true;

        // cmd 1
        let dot_1 = Dot::new(1, 1);
        let clock_1 = util::vclock(vec![4, 0, 0, 0, 0]);

        // cmd 2
        let dot_2 = Dot::new(1, 2);
        let clock_2 = util::vclock(vec![4, 0, 0, 0, 0]);

        // cmd 3
        let dot_3 = Dot::new(1, 3);
        let clock_3 = util::vclock(vec![5, 0, 0, 0, 0]);

        // cmd 4
        let dot_4 = Dot::new(1, 4);
        let clock_4 = util::vclock(vec![3, 0, 0, 0, 0]);

        // cmd 5
        let dot_5 = Dot::new(1, 5);
        let clock_5 = util::vclock(vec![4, 0, 0, 0, 0]);

        let order_a = vec![
            (dot_3, None, clock_2.clone()),
            (dot_4, None, clock_3.clone()),
            (dot_5, None, clock_4.clone()),
            (dot_1, None, clock_1.clone()),
            (dot_2, None, clock_1.clone()),
        ];
        let order_b = vec![
            (dot_3, None, clock_3),
            (dot_4, None, clock_4),
            (dot_5, None, clock_5),
            (dot_2, None, clock_2),
            (dot_1, None, clock_1),
        ];
        let order_a = check_termination(n, order_a, transitive_conflicts);
        let order_b = check_termination(n, order_b, transitive_conflicts);
        assert_eq!(order_a, order_b);
    }

    /// Simple example showing why encoding of dependencies matters for the
    /// `transitive_conflicts` optimization to be correct (which, makes the name
    /// of the optimization not great):
    /// - 3 replicas (A, B, C), and 3 commands
    ///   * command (A, 1), keys = {x}
    ///   * command (A, 2), keys = {y}
    ///   * command (B, 1), keys = {x, y}
    ///
    /// First, (A, 1) is submitted and gets no dependencies:
    /// - {A -> 0, B -> 0, C -> 0}
    /// Then, (A, 2) is submitted and also gets no dependencies:
    /// - {A -> 0, B -> 0, C -> 0}
    /// Finally, (B, 1) is submitted and gets (A, 2) as a dependency:
    /// - {A -> 2, B -> 0, C -> 0}
    /// It only gets (A, 2) because we only return the highest conflicting
    /// command from each replica.
    ///
    /// With the optimization, the order in which commands are received by the
    /// ordering component affects results:
    /// - (A, 1), (A, 2), (B, 1): commands are executed in the order they're
    ///   received, producing correct results
    /// - (A, 2), (B, 1), (A, 1): (B, 1) is executed before (A, 1) and shouldn't
    ///
    /// Without the optimization, (B, 1) would be forced to wait for (A, 1) in
    /// the last case, producing a correct result.
    ///
    /// It looks like the optimization would be correct if, instead of returning
    /// the highest conflicting command per replica, we would return the highest
    /// conflict command per replica *per key*.
    #[ignore]
    #[test]
    fn transitive_conflicts_assumption_regression_test_2() {
        // config
        let n = 3;
        let transitive_conflicts = true;

        let keys = |keys: Vec<&str>| {
            keys.into_iter()
                .map(|key| key.to_string())
                .collect::<BTreeSet<_>>()
        };

        // cmd 1,1
        let dot_1_1 = Dot::new(1, 1);
        let keys_1_1 = keys(vec!["A"]);
        let clock_1_1 = util::vclock(vec![0, 0, 0]);

        // cmd 1,2
        let dot_1_2 = Dot::new(1, 2);
        let keys_1_2 = keys(vec!["B"]);
        let clock_1_2 = util::vclock(vec![0, 0, 0]);

        // cmd 2,1
        let dot_2_1 = Dot::new(2, 1);
        let keys_2_1 = keys(vec!["A", "B"]);
        let clock_2_1 = util::vclock(vec![2, 0, 0]);

        let order_a = vec![
            (dot_1_1, Some(keys_1_1.clone()), clock_1_1.clone()),
            (dot_1_2, Some(keys_1_2.clone()), clock_1_2.clone()),
            (dot_2_1, Some(keys_2_1.clone()), clock_2_1.clone()),
        ];
        let order_b = vec![
            (dot_1_2, Some(keys_1_2), clock_1_2),
            (dot_2_1, Some(keys_2_1), clock_2_1),
            (dot_1_1, Some(keys_1_1), clock_1_1),
        ];
        let order_a = check_termination(n, order_a, transitive_conflicts);
        let order_b = check_termination(n, order_b, transitive_conflicts);
        assert_eq!(order_a, order_b);
    }

    #[test]
    fn self_cycle_test() {
        // config
        let n = 1;
        let transitive_conflicts = false;

        // cmd 1
        let dot_1 = Dot::new(1, 1);
        let clock_1 = util::vclock(vec![3]);

        // cmd 2
        let dot_2 = Dot::new(1, 2);
        let clock_2 = util::vclock(vec![1]);

        // cmd 3
        let dot_3 = Dot::new(1, 3);
        let clock_3 = util::vclock(vec![2]);

        let args = vec![
            (dot_1, None, clock_1),
            (dot_2, None, clock_2),
            (dot_3, None, clock_3),
        ];
        shuffle_it(n, transitive_conflicts, args);
    }

    #[test]
    /// We have 3 commands by the same process:
    /// - the first (1,1) accesses key A and thus has no dependencies
    /// - the second (1,2) accesses key B and thus has no dependencies
    /// - the third (1,3) accesses key B and thus it depends on the second
    ///   command (1,2)
    ///
    /// The commands are then received in the following order:
    /// - (1,2) executed as it has no dependencies
    /// - (1,3) can't be executed: this command depends on (1,2) and if we don't
    ///   assume the transitivity of conflicts, it also depends on (1,1) that
    ///   hasn't been executed
    /// - (1,1) executed as it has no dependencies
    ///
    /// Now when (1,1) is executed, we would hope that (1,3) would also be.
    /// However, since (1,1) accesses key A, when it is executed, the previous
    /// pending mechanism doesn't try the pending operation (1,3) because it
    /// accesses a different key (B). This means that such mechanism to
    /// track pending commands per key does not work when we don't assume
    /// the transitivity of conflicts.
    ///
    /// This was fixed by simply tracking one missing dependency per pending
    /// command. When that dependency is executed, if the command still can't be
    /// executed it's because there's another missing dependency, and now it
    /// will wait for that one.
    fn pending_on_different_key_regression_test() {
        // create config
        let n = 1;
        let f = 1;
        let mut config = Config::new(n, f);

        // cmd 1
        let dot_1 = Dot::new(1, 1);
        let cmd_1 =
            Command::put(Rifl::new(1, 1), String::from("A"), String::new());
        let clock_1 = util::vclock(vec![0]);

        // cmd 2
        let dot_2 = Dot::new(1, 2);
        let cmd_2 =
            Command::put(Rifl::new(2, 1), String::from("B"), String::new());
        let clock_2 = util::vclock(vec![0]);

        // cmd 3
        let dot_3 = Dot::new(1, 3);
        let cmd_3 =
            Command::put(Rifl::new(3, 1), String::from("B"), String::new());
        let clock_3 = util::vclock(vec![2]);

        for transitive_conflicts in vec![false, true] {
            config.set_transitive_conflicts(transitive_conflicts);

            // create queue
            let process_id = 1;
            let shard_id = 0;
            let mut queue = DependencyGraph::new(process_id, shard_id, &config);
            let time = RunTime;

            // add cmd 2
            queue.handle_add(dot_2, cmd_2.clone(), clock_2.clone(), &time);
            assert_eq!(queue.commands_to_execute(), vec![cmd_2.clone()]);

            // add cmd 3
            queue.handle_add(dot_3, cmd_3.clone(), clock_3.clone(), &time);
            if transitive_conflicts {
                // if we assume transitive conflicts, then cmd 3 can be executed
                assert_eq!(queue.commands_to_execute(), vec![cmd_3.clone()]);
            } else {
                // otherwise, it can't as it also depends cmd 1
                assert!(queue.commands_to_execute().is_empty());
            }

            // add cmd 1
            queue.handle_add(dot_1, cmd_1.clone(), clock_1.clone(), &time);
            // cmd 1 can always be executed
            if transitive_conflicts {
                assert_eq!(queue.commands_to_execute(), vec![cmd_1.clone()]);
            } else {
                // the following used to fail because our previous mechanism to
                // track pending commands didn't work without the assumption of
                // transitive conflicts
                let ready = queue.commands_to_execute();
                assert_eq!(ready.len(), 2);
                assert!(ready.contains(&cmd_3));
                assert!(ready.contains(&cmd_1));
            }
        }
    }

    #[test]
    fn simple_test_add_1() {
        // the actual test_add_1 is:
        // {1, 2}, [2, 2]
        // {1, 1}, [3, 2]
        // {1, 5}, [6, 2]
        // {1, 6}, [6, 3]
        // {1, 3}, [3, 3]
        // {2, 2}, [0, 2]
        // {2, 1}, [4, 3]
        // {1, 4}, [6, 2]
        // {2, 3}, [6, 3]
        // in the simple version, {1, 5} and {1, 6} are removed

        // {1, 2}, [2, 2]
        let dot_a = Dot::new(1, 2);
        let clock_a = util::vclock(vec![2, 2]);

        // {1, 1}, [3, 2]
        let dot_b = Dot::new(1, 1);
        let clock_b = util::vclock(vec![3, 2]);

        // {1, 3}, [3, 3]
        let dot_c = Dot::new(1, 3);
        let clock_c = util::vclock(vec![3, 3]);

        // {2, 2}, [0, 2]
        let dot_d = Dot::new(2, 2);
        let clock_d = util::vclock(vec![0, 2]);

        // {2, 1}, [4, 3]
        let dot_e = Dot::new(2, 1);
        let clock_e = util::vclock(vec![4, 3]);

        // {1, 4}, [4, 2]
        let dot_f = Dot::new(1, 4);
        let clock_f = util::vclock(vec![4, 2]);

        // {2, 3}, [4, 3]
        let dot_g = Dot::new(2, 3);
        let clock_g = util::vclock(vec![4, 3]);

        // create args
        let args = vec![
            (dot_a, None, clock_a),
            (dot_b, None, clock_b),
            (dot_c, None, clock_c),
            (dot_d, None, clock_d),
            (dot_e, None, clock_e),
            (dot_f, None, clock_f),
            (dot_g, None, clock_g),
        ];

        let n = 2;
        let transitive_conflicts = false;
        shuffle_it(n, transitive_conflicts, args);
    }

    #[test]
    fn test_add_2() {
        // {2, 4}, [3, 4]
        let dot_a = Dot::new(2, 4);
        let clock_a = util::vclock(vec![3, 4]);

        // {2, 3}, [0, 3]
        let dot_b = Dot::new(2, 3);
        let clock_b = util::vclock(vec![0, 3]);

        // {1, 3}, [3, 3]
        let dot_c = Dot::new(1, 3);
        let clock_c = util::vclock(vec![3, 3]);

        // {1, 1}, [3, 4]
        let dot_d = Dot::new(1, 1);
        let clock_d = util::vclock(vec![3, 4]);

        // {2, 2}, [0, 2]
        let dot_e = Dot::new(2, 2);
        let clock_e = util::vclock(vec![0, 2]);

        // {1, 2}, [3, 3]
        let dot_f = Dot::new(1, 2);
        let clock_f = util::vclock(vec![3, 3]);

        // {2, 1}, [3, 3]
        let dot_g = Dot::new(2, 1);
        let clock_g = util::vclock(vec![3, 3]);

        // create args
        let args = vec![
            (dot_a, None, clock_a),
            (dot_b, None, clock_b),
            (dot_c, None, clock_c),
            (dot_d, None, clock_d),
            (dot_e, None, clock_e),
            (dot_f, None, clock_f),
            (dot_g, None, clock_g),
        ];

        let n = 2;
        let transitive_conflicts = false;
        shuffle_it(n, transitive_conflicts, args);
    }

    #[test]
    fn test_add_3() {
        // {3, 2}, [1, 0, 2]
        let dot_a = Dot::new(3, 2);
        let clock_a = util::vclock(vec![1, 0, 2]);

        // {3, 3}, [1, 1, 3]
        let dot_b = Dot::new(3, 3);
        let clock_b = util::vclock(vec![1, 1, 3]);

        // {3, 1}, [1, 1, 3]
        let dot_c = Dot::new(3, 1);
        let clock_c = util::vclock(vec![1, 1, 3]);

        // {1, 1}, [1, 0, 0]
        let dot_d = Dot::new(1, 1);
        let clock_d = util::vclock(vec![1, 0, 0]);

        // {2, 1}, [1, 1, 2]
        let dot_e = Dot::new(2, 1);
        let clock_e = util::vclock(vec![1, 1, 2]);

        // create args
        let args = vec![
            (dot_a, None, clock_a),
            (dot_b, None, clock_b),
            (dot_c, None, clock_c),
            (dot_d, None, clock_d),
            (dot_e, None, clock_e),
        ];

        let n = 3;
        let transitive_conflicts = false;
        shuffle_it(n, transitive_conflicts, args);
    }

    #[test]
    fn test_add_4() {
        // {1, 5}, [5]
        let dot_a = Dot::new(1, 5);
        let clock_a = util::vclock(vec![5]);

        // {1, 4}, [6]
        let dot_b = Dot::new(1, 4);
        let clock_b = util::vclock(vec![6]);

        // {1, 1}, [5]
        let dot_c = Dot::new(1, 1);
        let clock_c = util::vclock(vec![5]);

        // {1, 2}, [6]
        let dot_d = Dot::new(1, 2);
        let clock_d = util::vclock(vec![6]);

        // {1, 3}, [5]
        let dot_e = Dot::new(1, 3);
        let clock_e = util::vclock(vec![5]);

        // {1, 6}, [6]
        let dot_f = Dot::new(1, 6);
        let clock_f = util::vclock(vec![6]);

        // create args
        let args = vec![
            (dot_a, None, clock_a),
            (dot_b, None, clock_b),
            (dot_c, None, clock_c),
            (dot_d, None, clock_d),
            (dot_e, None, clock_e),
            (dot_f, None, clock_f),
        ];

        let n = 1;
        let transitive_conflicts = false;
        shuffle_it(n, transitive_conflicts, args);
    }

    #[test]
    fn test_add_5() {
        // {1, 1}, [1, 1]
        let dot_a = Dot::new(1, 1);
        let clock_a = util::vclock(vec![1, 1]);

        // {1, 2}, [2, 0]
        let dot_b = Dot::new(1, 2);
        let clock_b = util::vclock(vec![2, 0]);

        // {2, 1}, [1, 1]
        let dot_c = Dot::new(2, 1);
        let clock_c = util::vclock(vec![1, 1]);

        // create args
        let args = vec![
            (dot_a, None, clock_a),
            (dot_b, None, clock_b),
            (dot_c, None, clock_c),
        ];

        let n = 2;
        let transitive_conflicts = false;
        shuffle_it(n, transitive_conflicts, args);
    }

    #[test]
    fn test_add_6() {
        // {1, 1}, [1, 0]
        let dot_a = Dot::new(1, 1);
        let clock_a = util::vclock(vec![1, 0]);

        // {1, 2}, [4, 1]
        let dot_b = Dot::new(1, 2);
        let clock_b = util::vclock(vec![4, 1]);

        // {1, 3}, [3, 0]
        let dot_c = Dot::new(1, 3);
        let clock_c = util::vclock(vec![3, 0]);

        // {1, 4}, [4, 0]
        let dot_d = Dot::new(1, 4);
        let clock_d = util::vclock(vec![4, 0]);

        // {2, 1}, [1, 1]
        let dot_e = Dot::new(2, 1);
        let clock_e = util::vclock(vec![1, 1]);

        // {2, 2}, [3, 2]
        let dot_f = Dot::new(2, 2);
        let clock_f = util::vclock(vec![3, 2]);

        // create args
        let args = vec![
            (dot_a, None, clock_a),
            (dot_b, None, clock_b),
            (dot_c, None, clock_c),
            (dot_d, None, clock_d),
            (dot_e, None, clock_e),
            (dot_f, None, clock_f),
        ];

        let n = 2;
        let transitive_conflicts = false;
        shuffle_it(n, transitive_conflicts, args);
    }

    #[test]
    fn test_add_random() {
        let shard_id = 0;
        let n = 2;
        let transitive_conflicts = false;
        let iterations = 10;
        let events_per_process = 3;

        (0..iterations).for_each(|_| {
            let args = random_adds(shard_id, n, events_per_process);
            shuffle_it(n, transitive_conflicts, args);
        });
    }

    fn random_adds(
        shard_id: ShardId,
        n: usize,
        events_per_process: usize,
    ) -> Vec<(Dot, Option<BTreeSet<Key>>, VClock<ProcessId>)> {
        let mut possible_keys: Vec<_> =
            ('A'..='D').map(|key| key.to_string()).collect();

        // create dots
        let dots: Vec<_> = util::process_ids(shard_id, n)
            .flat_map(|process_id| {
                (1..=events_per_process)
                    .map(move |event| Dot::new(process_id, event as u64))
            })
            .collect();

        // compute keys and bottom clocks
        let clocks: HashMap<_, _> = dots
            .clone()
            .into_iter()
            .map(|dot| {
                // select two random keys from the set of possible keys:
                // - this makes sure that the conflict relation is not
                //   transitive
                possible_keys.shuffle(&mut rand::thread_rng());
                let mut keys = BTreeSet::new();
                assert!(keys.insert(possible_keys[0].clone()));
                assert!(keys.insert(possible_keys[1].clone()));
                // create bottom clock
                let clock = VClock::with(util::process_ids(shard_id, n));
                (dot, (Some(keys), RefCell::new(clock)))
            })
            .collect();

        // for each pair of dots
        dots.combination(2).for_each(|dots| {
            let left = dots[0];
            let right = dots[1];

            // find their data
            let (left_keys, left_clock) =
                clocks.get(left).expect("left dot data must exist");
            let (right_keys, right_clock) =
                clocks.get(right).expect("right dot data must exist");

            // unwrap keys
            let left_keys = left_keys.as_ref().expect("left keys should exist");
            let right_keys =
                right_keys.as_ref().expect("right keys should exist");

            // check if the commands conflict (i.e. if the keys being accessed
            // intersect)
            let conflict = left_keys.intersection(&right_keys).next().is_some();

            // if the commands conflict, then make sure at least one is a
            // dependency of the other
            if conflict {
                // borrow their clocks mutably
                let mut left_clock = left_clock.borrow_mut();
                let mut right_clock = right_clock.borrow_mut();

                if left.source() == right.source() {
                    // if dots belong to the same process, make the latest
                    // depend on the oldest
                    match left.sequence().cmp(&right.sequence()) {
                        Ordering::Less => {
                            right_clock.add(&left.source(), left.sequence());
                        }
                        Ordering::Greater => {
                            left_clock.add(&right.source(), right.sequence());
                        }
                        _ => unreachable!("dots must be different"),
                    };
                } else {
                    // otherwise, make them depend on each other (maybe both
                    // ways)
                    match rand::random::<usize>() % 3 {
                        0 => {
                            // left depends on right
                            left_clock.add(&right.source(), right.sequence());
                        }
                        1 => {
                            // right depends on left
                            right_clock.add(&left.source(), left.sequence());
                        }
                        2 => {
                            // both
                            left_clock.add(&right.source(), right.sequence());
                            right_clock.add(&left.source(), left.sequence());
                        }
                        _ => panic!("usize % 3 must < 3"),
                    }
                }
            }
        });

        clocks
            .into_iter()
            .map(|(dot, (keys, clock_cell))| {
                let clock = clock_cell.into_inner();
                (dot, keys, clock)
            })
            .collect()
    }

    fn shuffle_it(
        n: usize,
        transitive_conflicts: bool,
        mut args: Vec<(Dot, Option<BTreeSet<Key>>, VClock<ProcessId>)>,
    ) {
        let total_order =
            check_termination(n, args.clone(), transitive_conflicts);
        println!("transitive_conflicts = {:?}", transitive_conflicts);
        args.permutation().for_each(|permutation| {
            println!("permutation = {:?}", permutation);
            let sorted =
                check_termination(n, permutation, transitive_conflicts);
            assert_eq!(total_order, sorted);
        });
    }

    fn check_termination(
        n: usize,
        args: Vec<(Dot, Option<BTreeSet<Key>>, VClock<ProcessId>)>,
        transitive_conflicts: bool,
    ) -> BTreeMap<Key, Vec<Rifl>> {
        // create queue
        let process_id = 1;
        let shard_id = 0;
        let f = 1;
        let mut config = Config::new(n, f);
        config.set_transitive_conflicts(transitive_conflicts);
        let mut queue = DependencyGraph::new(process_id, shard_id, &config);
        let time = RunTime;
        let mut all_rifls = HashSet::new();
        let mut sorted = BTreeMap::new();

        args.into_iter().for_each(|(dot, keys, clock)| {
            // create command rifl from its dot
            let rifl = Rifl::new(dot.source() as ClientId, dot.sequence());

            // create command:
            // - set single CONF key if no keys were provided
            let keys = keys.unwrap_or_else(|| {
                BTreeSet::from_iter(vec![String::from("CONF")])
            });
            let ops = keys.into_iter().map(|key| {
                let value = String::from("");
                (key, KVOp::Put(value))
            });
            let cmd = Command::from(rifl, ops);

            // add to the set of all rifls
            assert!(all_rifls.insert(rifl));

            // add it to the queue
            queue.handle_add(dot, cmd, clock, &time);

            // get ready to execute
            let to_execute = queue.commands_to_execute();

            // for each command ready to be executed
            to_execute.iter().for_each(|cmd| {
                // get its rifl
                let rifl = cmd.rifl();

                // remove it from the set of rifls
                assert!(all_rifls.remove(&cmd.rifl()));

                // and add it to the sorted results
                cmd.keys(shard_id).for_each(|key| {
                    sorted
                        .entry(key.clone())
                        .or_insert_with(Vec::new)
                        .push(rifl);
                })
            });
        });

        // the set of all rifls should be empty
        if !all_rifls.is_empty() {
            panic!("the set of all rifls should be empty");
        }

        // return sorted commands
        sorted
    }

    #[test]
    fn sccs_found_and_missing_dep() {
        /*
        MCommit((4, 31), Clock { clock: {60, 50, 50, 30, 60} })
        MCommit((4, 32), Clock { clock: {60, 50, 50, 31, 60} })
        MCommit((4, 33), Clock { clock: {60, 50, 50, 32, 60} })
        MCommit((4, 34), Clock { clock: {60, 50, 50, 33, 60} })
        MCommit((4, 35), Clock { clock: {60, 50, 50, 34, 60} })
        MCommit((4, 36), Clock { clock: {60, 50, 50, 35, 60} })
        MCommit((4, 37), Clock { clock: {60, 50, 50, 36, 60} })
        MCommit((4, 38), Clock { clock: {60, 50, 50, 37, 60} })
        MCommit((4, 39), Clock { clock: {60, 50, 50, 38, 60} })
        MCommit((4, 40), Clock { clock: {60, 50, 50, 39, 60} })
        MCommit((5, 70), Clock { clock: {60, 50, 50, 40, 61} })
        ...
        stuff happens and at some point (5, 70) is tried because one of its dependencies, (2, 50), is delivered
        ...
        Graph:save_scc removing (2, 50) from indexes
        Graph:save_scc executed clock Clock { clock: {60, 50, 50, 30, 60} }
        Graph::try_pending {(5, 70), ...} depended on (2, 50)
        Graph:find_scc (5, 70)
        */
        // in order for this test to pass, sccs must be found by the finder; the
        // following loop stops once that happens
        std::iter::repeat(()).any(|_| check_sccs_found_with_missing_dep());
    }

    fn check_sccs_found_with_missing_dep() -> bool {
        let conflicting_command = || {
            let rifl = Rifl::new(1, 1);
            Command::put(rifl, String::from("CONF"), String::new())
        };

        // create queue
        let process_id = 4;
        let shard_id = 0;
        let n = 5;
        let f = 1;
        let mut config = Config::new(n, f);
        let transitive_conflicts = false;
        config.set_transitive_conflicts(transitive_conflicts);
        let mut queue = DependencyGraph::new(process_id, shard_id, &config);
        let time = RunTime;

        // (5, 70): only (5, 61) is missing
        let missing_dot = Dot::new(5, 61);

        let root_dot = Dot::new(5, 70);
        queue.vertex_index.index(Vertex::new(
            root_dot,
            conflicting_command(),
            util::vclock(vec![60, 50, 50, 40, 61]),
            &time,
        ));

        // (4, 31)
        queue.vertex_index.index(Vertex::new(
            Dot::new(4, 31),
            conflicting_command(),
            util::vclock(vec![60, 50, 50, 30, 60]),
            &time,
        ));
        // (4, 32)
        queue.vertex_index.index(Vertex::new(
            Dot::new(4, 32),
            conflicting_command(),
            util::vclock(vec![60, 50, 50, 31, 60]),
            &time,
        ));
        // (4, 33)
        queue.vertex_index.index(Vertex::new(
            Dot::new(4, 33),
            conflicting_command(),
            util::vclock(vec![60, 50, 50, 32, 60]),
            &time,
        ));
        // (4, 34)
        queue.vertex_index.index(Vertex::new(
            Dot::new(4, 34),
            conflicting_command(),
            util::vclock(vec![60, 50, 50, 33, 60]),
            &time,
        ));
        // (4, 35)
        queue.vertex_index.index(Vertex::new(
            Dot::new(4, 35),
            conflicting_command(),
            util::vclock(vec![60, 50, 50, 34, 60]),
            &time,
        ));
        // (4, 36)
        queue.vertex_index.index(Vertex::new(
            Dot::new(4, 36),
            conflicting_command(),
            util::vclock(vec![60, 50, 50, 35, 60]),
            &time,
        ));
        // (4, 37)
        queue.vertex_index.index(Vertex::new(
            Dot::new(4, 37),
            conflicting_command(),
            util::vclock(vec![60, 50, 50, 36, 60]),
            &time,
        ));
        // (4, 38)
        queue.vertex_index.index(Vertex::new(
            Dot::new(4, 38),
            conflicting_command(),
            util::vclock(vec![60, 50, 50, 37, 60]),
            &time,
        ));
        // (4, 39)
        queue.vertex_index.index(Vertex::new(
            Dot::new(4, 39),
            conflicting_command(),
            util::vclock(vec![60, 50, 50, 38, 60]),
            &time,
        ));
        // (4, 40)
        queue.vertex_index.index(Vertex::new(
            Dot::new(4, 40),
            conflicting_command(),
            util::vclock(vec![60, 50, 50, 39, 60]),
            &time,
        ));

        // create executed clock
        queue.executed_clock = AEClock::from(
            util::vclock(vec![60, 50, 50, 30, 60]).into_iter().map(
                |(process_id, max_set)| {
                    (process_id, AboveExSet::from_events(max_set.event_iter()))
                },
            ),
        );

        // create ready commands counter and try to find an SCC
        let mut ready_commands = 0;
        let finder_info = queue.find_scc(root_dot, &mut ready_commands, &time);

        if let FinderInfo::MissingDependencies(
            to_be_executed,
            missing_deps,
            _,
        ) = finder_info
        {
            // check the missing dot
            assert_eq!(
                missing_deps.len(),
                1,
                "there's a single missing dependency"
            );
            assert_eq!(missing_deps.into_iter().next().unwrap(), missing_dot);

            // check that ready commands are actually delivered
            assert_eq!(ready_commands, to_be_executed.len());

            // return whether sccs have been found
            !to_be_executed.is_empty()
        } else {
            panic!("FinderInfo::MissingDependency not found");
        }
    }
}
