use crate::command::Command;
use crate::config::Config;
use crate::executor::{Executor, TableExecutor};
use crate::id::{Dot, ProcessId};
use crate::log;
use crate::planet::{Planet, Region};
use crate::protocol::common::{
    info::{Commands, Info},
    table::{KeysClocks, ProcessVotes, QuorumClocks, Votes},
};
use crate::protocol::{BaseProcess, Process, ToSend};
use std::cmp;
use std::mem;

type ExecutionInfo = <TableExecutor as Executor>::ExecutionInfo;

pub struct Newt {
    bp: BaseProcess,
    keys_clocks: KeysClocks,
    cmds: Commands<CommandInfo>,
    to_executor: Vec<ExecutionInfo>,
}

impl Process for Newt {
    type Message = Message;
    type Executor = TableExecutor;

    /// Creates a new `Newt` process.
    fn new(process_id: ProcessId, region: Region, planet: Planet, config: Config) -> Self {
        // compute fast and write quorum sizes
        let (fast_quorum_size, write_quorum_size, _) = config.newt_quorum_sizes();

        // create protocol data-structures
        let bp = BaseProcess::new(
            process_id,
            region,
            planet,
            config,
            fast_quorum_size,
            write_quorum_size,
        );
        let keys_clocks = KeysClocks::new(process_id);
        let cmds = Commands::new(process_id, config.n(), config.f(), fast_quorum_size);
        let to_executor = Vec::new();

        // create `Newt`
        Self {
            bp,
            keys_clocks,
            cmds,
            to_executor,
        }
    }

    /// Returns the process identifier.
    fn id(&self) -> ProcessId {
        self.bp.process_id
    }

    /// Updates the processes known by this process.
    fn discover(&mut self, processes: Vec<(ProcessId, Region)>) -> bool {
        self.bp.discover(processes)
    }

    /// Submits a command issued by some client.
    fn submit(&mut self, cmd: Command) -> ToSend<Self::Message> {
        self.handle_submit(cmd)
    }

    /// Handles protocol messages.
    fn handle(&mut self, from: ProcessId, msg: Self::Message) -> Option<ToSend<Message>> {
        match msg {
            Message::MCollect {
                dot,
                cmd,
                quorum,
                clock,
            } => self.handle_mcollect(from, dot, cmd, quorum, clock),
            Message::MCollectAck {
                dot,
                clock,
                process_votes,
            } => self.handle_mcollectack(from, dot, clock, process_votes),
            Message::MCommit {
                dot,
                cmd,
                clock,
                votes,
            } => self.handle_mcommit(dot, cmd, clock, votes),
            Message::MPhantom { dot, process_votes } => self.handle_mphantom(dot, process_votes),
        }
    }

    /// Returns new commands results to be sent to clients.
    fn to_executor(&mut self) -> Vec<ExecutionInfo> {
        let mut to_executor = Vec::new();
        mem::swap(&mut to_executor, &mut self.to_executor);
        to_executor
    }

    fn show_metrics(&self) {
        self.bp.show_metrics();
    }
}

impl Newt {
    /// Handles a submit operation by a client.
    fn handle_submit(&mut self, cmd: Command) -> ToSend<Message> {
        // compute the command identifier
        let dot = self.bp.next_dot();

        // compute its clock
        let clock = self.keys_clocks.clock(&cmd) + 1;

        // create `MCollect` and target
        let mcollect = Message::MCollect {
            dot,
            cmd,
            clock,
            quorum: self.bp.fast_quorum(),
        };
        let target = self.bp.fast_quorum();

        // return `ToSend`
        ToSend {
            from: self.id(),
            target,
            msg: mcollect,
        }
    }

    fn handle_mcollect(
        &mut self,
        from: ProcessId,
        dot: Dot,
        cmd: Command,
        quorum: Vec<ProcessId>,
        remote_clock: u64,
    ) -> Option<ToSend<Message>> {
        log!(
            "p{}: MCollect({:?}, {:?}, {}) from {}",
            self.id(),
            dot,
            cmd,
            remote_clock,
            from
        );

        // get cmd info
        let info = self.cmds.get(dot);

        // discard message if no longer in START
        if info.status != Status::START {
            return None;
        }

        // TODO can we somehow combine the next 2 operations in order to save map lookups?

        // compute command clock
        let clock = cmp::max(remote_clock, self.keys_clocks.clock(&cmd) + 1);
        // compute votes consumed by this command
        let process_votes = self.keys_clocks.process_votes(&cmd, clock);
        // check that there's one vote per key
        assert_eq!(process_votes.len(), cmd.key_count());

        // update command info
        info.status = Status::COLLECT;
        info.cmd = Some(cmd);
        info.quorum = quorum;
        info.clock = clock;

        // create `MCollectAck` and target
        let mcollectack = Message::MCollectAck {
            dot,
            clock,
            process_votes,
        };
        let target = vec![from];

        // return `ToSend`
        Some(ToSend {
            from: self.id(),
            target,
            msg: mcollectack,
        })
    }

    fn handle_mcollectack(
        &mut self,
        from: ProcessId,
        dot: Dot,
        clock: u64,
        remote_votes: ProcessVotes,
    ) -> Option<ToSend<Message>> {
        log!(
            "p{}: MCollectAck({:?}, {}, {:?}) from {}",
            self.id(),
            dot,
            clock,
            remote_votes,
            from
        );

        // get cmd info
        let info = self.cmds.get(dot);

        if info.status != Status::COLLECT {
            // do nothing if we're no longer COLLECT
            return None;
        }

        // update votes with remote votes
        info.votes.add(remote_votes);

        // update quorum clocks while computing max clock and its number of occurences
        let (max_clock, max_count) = info.quorum_clocks.add(from, clock);

        // optimization: bump all keys clocks in `cmd` to be `max_clock`
        // - this prevents us from generating votes (either when clients submit new operations or
        //   when handling `MCollect` from other processes) that could potentially delay the
        //   execution of this command
        match info.cmd.as_ref() {
            Some(cmd) => {
                let local_votes = self.keys_clocks.process_votes(cmd, max_clock);
                // update votes with local votes
                info.votes.add(local_votes);
            }
            None => {
                panic!("there should be a command payload in the MCollectAck handler");
            }
        }

        // check if we have all necessary replies
        if info.quorum_clocks.all() {
            // fast path condition:
            // - if `max_clock` was reported by at least f processes
            if max_count >= self.bp.config.f() {
                self.bp.fast_path();
                // reset local votes as we're going to receive them right away; this also prevents a
                // `info.votes.clone()`
                let votes = Self::reset_votes(&mut info.votes);

                // create `MCommit` and target
                // TODO create a slim-MCommit that only sends the payload to the non-fast-quorum
                // members, or send the payload to all in a slim-MConsensus
                let mcommit = Message::MCommit {
                    dot,
                    cmd: info.cmd.clone(),
                    clock: max_clock,
                    votes,
                };
                let target = self.bp.all();

                // return `ToSend`
                Some(ToSend {
                    from: self.id(),
                    target,
                    msg: mcommit,
                })
            } else {
                self.bp.slow_path();
                // TODO slow path
                todo!("slow path not implemented yet")
            }
        } else {
            None
        }
    }

    fn handle_mcommit(
        &mut self,
        dot: Dot,
        cmd: Option<Command>,
        clock: u64,
        mut votes: Votes,
    ) -> Option<ToSend<Message>> {
        log!("p{}: MCommit({:?}, {}, {:?})", self.id(), dot, clock, votes);

        // get cmd info
        let info = self.cmds.get(dot);

        if info.status == Status::COMMIT {
            // do nothing if we're already COMMIT
            // TODO what about the executed status?
            return None;
        }

        // update command info:
        info.status = Status::COMMIT;
        info.cmd = cmd;
        info.clock = clock;

        // get current votes (probably from phantom messages) merge them with received votes so that
        // all together can be added to a votes table
        let current_votes = Self::reset_votes(&mut info.votes);
        votes.merge(current_votes);

        // generate phantom votes if committed clock is higher than the local key's clock
        let mut to_send = None;
        if let Some(cmd) = info.cmd.as_ref() {
            // if not a no op, check if we can generate more votes that can speed-up execution
            let process_votes = self.keys_clocks.process_votes(cmd, info.clock);

            // create `MPhantom` if there are new votes
            if !process_votes.is_empty() {
                let mphantom = Message::MPhantom { dot, process_votes };
                let target = self.bp.all();
                to_send = Some(ToSend {
                    from: self.bp.process_id,
                    target,
                    msg: mphantom,
                });
            }
        }

        // create execution info if not a noop
        // TODO if noOp, should we add `Votes` to the table, or there will be no votes?
        if let Some(cmd) = info.cmd.clone() {
            // create execution info
            let execution_info = ExecutionInfo::votes(dot, cmd, info.clock, votes);
            self.to_executor.push(execution_info);
        }

        // return `ToSend`
        to_send
    }

    fn handle_mphantom(
        &mut self,
        dot: Dot,
        process_votes: ProcessVotes,
    ) -> Option<ToSend<Message>> {
        log!("p{}: MPhantom({:?}, {:?})", self.id(), dot, process_votes);

        // get cmd info
        let info = self.cmds.get(dot);

        // TODO if there's ever a Status::EXECUTE, this check might be incorrect
        if info.status == Status::COMMIT {
            // create execution info
            let execution_info = ExecutionInfo::phantom_votes(process_votes);
            self.to_executor.push(execution_info);
        } else {
            // if not committed yet, update votes with remote votes
            info.votes.add(process_votes);
        }

        // nothing to send
        None
    }

    // Replaces the value `local_votes` with empty votes, returning the previous votes.
    fn reset_votes(local_votes: &mut Votes) -> Votes {
        let mut votes = Votes::new();
        mem::swap(&mut votes, local_votes);
        votes
    }
}

// `CommandInfo` contains all information required in the life-cyle of a
// `Command`
struct CommandInfo {
    status: Status,
    quorum: Vec<ProcessId>,
    cmd: Option<Command>, // `None` if noOp
    clock: u64,
    // `votes` is used by the coordinator to aggregate `ProcessVotes` from fast
    // quorum members
    votes: Votes,
    // `quorum_clocks` is used by the coordinator to compute the highest clock
    // reported by fast quorum members and the number of times it was reported
    quorum_clocks: QuorumClocks,
}

impl Info for CommandInfo {
    fn new(_: ProcessId, _: usize, _: usize, fast_quorum_size: usize) -> Self {
        Self {
            status: Status::START,
            quorum: vec![],
            cmd: None,
            clock: 0,
            votes: Votes::new(),
            quorum_clocks: QuorumClocks::new(fast_quorum_size),
        }
    }
}

// `Newt` protocol messages
#[derive(Debug, Clone, PartialEq)]
pub enum Message {
    MCollect {
        dot: Dot,
        cmd: Command,
        quorum: Vec<ProcessId>,
        clock: u64,
    },
    MCollectAck {
        dot: Dot,
        clock: u64,
        process_votes: ProcessVotes,
    },
    MCommit {
        dot: Dot,
        cmd: Option<Command>,
        clock: u64,
        votes: Votes,
    },
    MPhantom {
        dot: Dot,
        process_votes: ProcessVotes,
    },
}

/// `Status` of commands.
#[derive(PartialEq)]
enum Status {
    START,
    COLLECT,
    COMMIT,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::{Client, Workload};
    use crate::sim::Simulation;
    use crate::time::SimTime;

    #[test]
    fn newt_flow() {
        // create simulation
        let mut simulation = Simulation::new();

        // processes ids
        let process_id_1 = 1;
        let process_id_2 = 2;
        let process_id_3 = 3;

        // regions
        let europe_west2 = Region::new("europe-west2");
        let europe_west3 = Region::new("europe-west2");
        let us_west1 = Region::new("europe-west2");

        // processes
        let processes = vec![
            (process_id_1, europe_west2.clone()),
            (process_id_2, europe_west3.clone()),
            (process_id_3, us_west1.clone()),
        ];

        // planet
        let planet = Planet::new("latency/");

        // create system time
        let time = SimTime::new();

        // n and f
        let n = 3;
        let f = 1;
        let config = Config::new(n, f);

        // executors
        let executor_1 = TableExecutor::new(&config);
        let executor_2 = TableExecutor::new(&config);
        let executor_3 = TableExecutor::new(&config);

        // newts
        let mut newt_1 = Newt::new(process_id_1, europe_west2.clone(), planet.clone(), config);
        let mut newt_2 = Newt::new(process_id_2, europe_west3.clone(), planet.clone(), config);
        let mut newt_3 = Newt::new(process_id_3, us_west1.clone(), planet.clone(), config);

        // discover processes in all newts
        newt_1.discover(processes.clone());
        newt_2.discover(processes.clone());
        newt_3.discover(processes.clone());

        // register processes
        simulation.register_process(newt_1, executor_1);
        simulation.register_process(newt_2, executor_2);
        simulation.register_process(newt_3, executor_3);

        // client workload
        let conflict_rate = 100;
        let total_commands = 10;
        let workload = Workload::new(conflict_rate, total_commands);

        // create client 1 that is connected to newt 1
        let client_id = 1;
        let client_region = europe_west2.clone();
        let mut client_1 = Client::new(client_id, client_region, planet.clone(), workload);

        // discover processes in client 1
        assert!(client_1.discover(processes));

        // start client
        let (target, cmd) = client_1
            .start(&time)
            .expect("there should be a first operation");

        // check that `target` is newt 1
        assert_eq!(target, process_id_1);

        // register clients
        simulation.register_client(client_1);

        // register command in executor and submit it in newt 1
        let mcollect = {
            let (mut process, mut executor) = simulation.get_process_mut(target);
            executor.register(&cmd);
            process.submit(cmd)
        };

        // check that the mcollect is being sent to 2 processes
        let ToSend { target, .. } = mcollect.clone();
        assert_eq!(target.len(), 2 * f);
        assert_eq!(target, vec![1, 2]);

        // handle mcollects
        let mut mcollectacks = simulation.forward_to_processes(mcollect);

        // check that there are 2 mcollectacks
        assert_eq!(mcollectacks.len(), 2 * f);

        // handle the first mcollectack
        let mcommits = simulation
            .forward_to_processes(mcollectacks.pop().expect("there should be an mcollect ack"));
        // no mcommit yet
        assert!(mcommits.is_empty());

        // handle the second mcollectack
        let mut mcommits = simulation
            .forward_to_processes(mcollectacks.pop().expect("there should be an mcollect ack"));
        // there's a commit now
        assert_eq!(mcommits.len(), 1);

        // check that the mcommit is sent to everyone
        let mcommit = mcommits.pop().expect("there should be an mcommit");
        let ToSend { target, .. } = mcommit.clone();
        assert_eq!(target.len(), n);

        // all processes handle it
        let mut mphantoms = simulation.forward_to_processes(mcommit);
        // there should be one mphantom (from process 3)
        assert_eq!(mphantoms.len(), 1);

        // get mphantom
        let mphantom = mphantoms.pop().expect("there should an mphantom");
        let ToSend { from, target, msg } = mphantom.clone();

        match msg {
            Message::MPhantom { .. } => {
                assert_eq!(from, process_id_3);
                assert_eq!(target, vec![process_id_1, process_id_2, process_id_3]);
            }
            _ => panic!("Message::MPhantom not found!"),
        }

        let mut ready = {
            // process 1 should have something to the executor
            let (mut process, mut executor) = simulation.get_process_mut(process_id_1);
            let to_executor = process.to_executor();
            assert_eq!(to_executor.len(), 1);

            // handle in executor and check there's a single command ready
            let ready = executor.handle(to_executor);
            assert_eq!(ready.len(), 1);
            ready
        };

        // get that command
        let cmd_result = ready.pop().expect("there should a command ready");

        // -------------------------
        // forward now the mphantoms
        let to_sends = simulation.forward_to_processes(mphantom);
        // check there's nothing to send
        assert!(to_sends.is_empty());

        {
            // process 1 should have something to the executor
            let (mut process, mut executor) = simulation.get_process_mut(process_id_1);
            let to_executor = process.to_executor();
            assert_eq!(to_executor.len(), 1);

            // handle in executor and check that it didn't generate another command
            let ready = executor.handle(to_executor);
            assert!(ready.is_empty());
        }
        // -------------------------

        // handle the previous command result
        let (target, cmd) = simulation
            .forward_to_client(cmd_result, &time)
            .expect("there should a new submit");

        let (mut process, _) = simulation.get_process_mut(target);
        let ToSend { msg, .. } = process.submit(cmd);
        if let Message::MCollect { dot, .. } = msg {
            assert_eq!(dot, Dot::new(process_id_1, 2));
        } else {
            panic!("Message::MCollect not found!");
        }
    }
}
