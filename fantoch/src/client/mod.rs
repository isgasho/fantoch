// This module contains the definition of `Workload`
pub mod workload;

// This module contains the definition of `Pending`
pub mod pending;

// This module contains the definition of `ClientData`
pub mod data;

// Re-exports.
pub use data::ClientData;
pub use pending::Pending;
pub use workload::Workload;

use crate::command::{Command, CommandResult};
use crate::id::ProcessId;
use crate::id::{ClientId, RiflGen};
use crate::log;
use crate::time::SysTime;

pub struct Client {
    /// id of this client
    client_id: ClientId,
    /// id of the process this client is connected to
    process_id: Option<ProcessId>,
    /// rifl id generator
    rifl_gen: RiflGen,
    /// workload configuration
    workload: Workload,
    /// map from pending command RIFL to its start time
    pending: Pending,
    /// mapping from
    data: ClientData,
}

impl Client {
    /// Creates a new client.
    pub fn new(client_id: ClientId, workload: Workload) -> Self {
        // create client
        Self {
            client_id,
            process_id: None,
            rifl_gen: RiflGen::new(client_id),
            workload,
            pending: Pending::new(),
            data: ClientData::new(),
        }
    }

    /// Returns the client identifier.
    pub fn id(&self) -> ClientId {
        self.client_id
    }

    /// "Connect" to the closest process.
    pub fn discover(&mut self, processes: Vec<ProcessId>) -> bool {
        // set the closest process
        self.process_id = processes.into_iter().next();

        // check if we have a closest process
        self.process_id.is_some()
    }

    /// Generates the next command in this client's workload.
    pub fn next_cmd(
        &mut self,
        time: &dyn SysTime,
    ) -> Option<(ProcessId, Command)> {
        self.process_id.and_then(|process_id| {
            // generate next command in the workload if some process_id
            self.workload.next_cmd(&mut self.rifl_gen).map(|cmd| {
                // if a new command was generated, start it in pending
                self.pending.start(cmd.rifl(), time);
                (process_id, cmd)
            })
        })
    }

    /// Handle executed command and return a boolean indicating whether we have
    /// generated all commands and receive all the corresponding command
    /// results.
    pub fn handle(
        &mut self,
        cmd_result: CommandResult,
        time: &dyn SysTime,
    ) -> bool {
        // end command in pending and save command latency
        let (latency, end_time) = self.pending.end(cmd_result.rifl(), time);
        log!(
            "rifl {:?} ended after {} micros at {}",
            cmd_result.rifl(),
            latency.as_micros(),
            end_time
        );
        self.data.record(latency, end_time);

        // we're done once:
        // - the workload is finished and
        // - pending is empty
        self.workload.finished() && self.pending.is_empty()
    }

    pub fn data(&self) -> &ClientData {
        &self.data
    }

    /// Returns the number of commands already issued.
    pub fn issued_commands(&self) -> usize {
        self.workload.issued_commands()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::planet::{Planet, Region};
    use crate::time::SimTime;
    use crate::util;
    use std::time::Duration;

    // Generates some client.
    fn gen_client(total_commands: usize) -> Client {
        // workload
        let conflict_rate = 100;
        let payload_size = 100;
        let workload =
            Workload::new(conflict_rate, total_commands, payload_size);

        // client
        let id = 1;
        Client::new(id, workload)
    }

    #[test]
    fn discover() {
        // create planet
        let planet = Planet::new();

        // processes
        let processes = vec![
            (0, Region::new("asia-east1")),
            (1, Region::new("australia-southeast1")),
            (2, Region::new("europe-west1")),
        ];

        // client
        let region = Region::new("europe-west2");
        let total_commands = 0;
        let mut client = gen_client(total_commands);

        // check discover with empty vec
        let sorted = util::sort_processes_by_distance(&region, &planet, vec![]);
        assert!(!client.discover(sorted));
        assert_eq!(client.process_id, None);

        // check discover with processes
        let sorted =
            util::sort_processes_by_distance(&region, &planet, processes);
        assert!(client.discover(sorted));
        assert_eq!(client.process_id, Some(2));
    }

    #[test]
    fn client_flow() {
        // create planet
        let planet = Planet::new();

        // processes
        let processes = vec![
            (0, Region::new("asia-east1")),
            (1, Region::new("australia-southeast1")),
            (2, Region::new("europe-west1")),
        ];

        // client
        let region = Region::new("europe-west2");
        let total_commands = 2;
        let mut client = gen_client(total_commands);

        // discover
        let sorted =
            util::sort_processes_by_distance(&region, &planet, processes);
        client.discover(sorted);

        // create system time
        let mut time = SimTime::new();

        // creates a fake command result from a command
        let fake_result = |cmd: Command| CommandResult::new(cmd.rifl(), 0);

        // start client at time 0
        let (process_id, cmd) = client
            .next_cmd(&time)
            .expect("there should a first operation");
        // process_id should be 2
        assert_eq!(process_id, 2);

        // handle result at time 10
        time.add_millis(10);
        client.handle(fake_result(cmd), &time);
        let next = client.next_cmd(&time);

        // check there's next command
        assert!(next.is_some());
        let (process_id, cmd) = next.unwrap();
        // process_id should be 2
        assert_eq!(process_id, 2);

        // handle result at time 15
        time.add_millis(5);
        client.handle(fake_result(cmd), &time);
        let next = client.next_cmd(&time);

        // check there's no next command
        assert!(next.is_none());

        // check latency
        let mut latency: Vec<_> = client.data().latency_data().collect();
        latency.sort();
        assert_eq!(
            latency,
            vec![Duration::from_millis(5), Duration::from_millis(10)]
        );

        // check throughput
        let mut throughput: Vec<_> = client.data().throughput_data().collect();
        throughput.sort();
        assert_eq!(throughput, vec![(10, 1), (15, 1)],);
    }
}
