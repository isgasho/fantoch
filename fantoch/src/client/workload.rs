use crate::client::key_gen::KeyGenState;
use crate::client::{KeyGen, ShardGen};
use crate::command::Command;
use crate::id::{RiflGen, ShardId};
use crate::kvs::{KVOp, Value};
use crate::log;
use crate::{HashMap, HashSet};
use rand::{distributions::Alphanumeric, Rng};
use serde::{Deserialize, Serialize};
use std::hash::Hash;
use std::iter;

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct Workload {
    /// number of shards accessed per command
    shards_per_command: usize,
    /// shard generator
    shard_gen: ShardGen,
    /// number of keys in each shard accessed by the command
    keys_per_shard: usize,
    // key generator
    key_gen: KeyGen,
    /// number of commands to be submitted in this workload
    commands_per_client: usize,
    /// size of payload in command (in bytes)
    payload_size: usize,
    /// number of commands already issued in this workload
    command_count: usize,
}

impl Workload {
    pub fn new(
        shards_per_command: usize,
        shard_gen: ShardGen,
        keys_per_shard: usize,
        key_gen: KeyGen,
        commands_per_client: usize,
        payload_size: usize,
    ) -> Self {
        // check for valid workloads
        match key_gen {
            KeyGen::ConflictRate { conflict_rate } => {
                if conflict_rate == 100 && keys_per_shard > 1 {
                    panic!("invalid workload; can't generate more than one key per shard when the conflict_rate is 100");
                }
                if keys_per_shard > 2 {
                    panic!("invalid workload; can't generate more than two keys per shard with the conflict_rate key generator");
                }
            }
            _ => (),
        }
        Self {
            shards_per_command,
            shard_gen,
            keys_per_shard,
            key_gen,
            commands_per_client,
            payload_size,
            command_count: 0,
        }
    }

    /// Returns the number of shards accessed by commands generated by this
    /// workload.
    pub fn shards_per_command(&self) -> usize {
        self.shards_per_command
    }

    /// Returns the shard generator.
    pub fn shard_gen(&self) -> ShardGen {
        self.shard_gen
    }

    /// Returns the number of keys in each shard accessed by commands generated
    /// by this workload.
    pub fn keys_per_shard(&self) -> usize {
        self.keys_per_shard
    }

    /// Returns the key generator.
    pub fn key_gen(&self) -> KeyGen {
        self.key_gen
    }

    /// Returns the total number of commands to be generated by this workload.
    pub fn commands_per_client(&self) -> usize {
        self.commands_per_client
    }

    /// Returns the payload size of the commands to be generated by this
    /// workload.
    pub fn payload_size(&self) -> usize {
        self.payload_size
    }

    /// Generate the next command.
    pub fn next_cmd(
        &mut self,
        rifl_gen: &mut RiflGen,
        key_gen_state: &mut KeyGenState,
    ) -> Option<(ShardId, Command)> {
        // check if we should generate more commands
        if self.command_count < self.commands_per_client {
            if self.command_count % 1000 == 0 {
                log!(
                    "client {:?}: {} of {}",
                    rifl_gen.source(),
                    self.command_count,
                    self.commands_per_client
                );
            }

            // increment command count
            self.command_count += 1;
            // generate new command
            Some(self.gen_cmd(rifl_gen, key_gen_state))
        } else {
            log!("client {:?} is done!", rifl_gen.source());
            None
        }
    }

    /// Returns the number of commands already issued.
    pub fn issued_commands(&self) -> usize {
        self.command_count
    }

    /// Returns a boolean indicating whether the workload has finished, i.e. all
    /// commands have been issued.
    pub fn finished(&self) -> bool {
        self.command_count == self.commands_per_client
    }

    /// Generate a command.
    fn gen_cmd(
        &mut self,
        rifl_gen: &mut RiflGen,
        key_gen_state: &mut KeyGenState,
    ) -> (ShardId, Command) {
        // generate rifl
        let rifl = rifl_gen.next_id();

        // generate all the key-value pairs
        let mut ops = HashMap::new();

        // generate unique shards, and also compute the target shard (which
        // should be one of the shards involved in the command)
        let mut target_shard = None;
        let shard_ids = Self::gen_unique(self.shards_per_command, || {
            let shard_id = self.shard_gen.gen_shard();
            // target shard will be the first identifier generated
            target_shard = target_shard.or(Some(shard_id));
            shard_id
        });
        let target_shard =
            target_shard.expect("there should be a target shard");

        for shard_id in shard_ids {
            // create entry for shard
            let shard_ops = ops.entry(shard_id).or_insert_with(HashMap::new);

            // generate unique keys
            let keys = Self::gen_unique(self.keys_per_shard, || {
                key_gen_state.gen_cmd_key()
            });

            for key in keys {
                let value = self.gen_cmd_value();
                shard_ops.insert(key, KVOp::Put(value));
            }
        }

        // create commadn
        (target_shard, Command::new(rifl, ops))
    }

    /// Generate a command payload with the payload size provided.
    fn gen_cmd_value(&self) -> Value {
        let mut rng = rand::thread_rng();
        iter::repeat(())
            .map(|_| rng.sample(Alphanumeric))
            .take(self.payload_size)
            .collect()
    }

    fn gen_unique<V, F>(count: usize, mut gen: F) -> HashSet<V>
    where
        F: FnMut() -> V,
        V: Eq + Hash,
    {
        let mut values = HashSet::with_capacity(count);
        while values.len() != count {
            values.insert(gen());
        }
        values
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::key_gen::CONFLICT_COLOR;
    use crate::kvs::KVOp;

    #[test]
    fn gen_cmd_key() {
        // create rilf gen
        let client_id = 1;
        let mut rifl_gen = RiflGen::new(client_id);

        // general config
        let shards_per_command = 1;
        let shard_gen = ShardGen::Random { shards: 1 };
        let keys_per_shard = 1;
        let total_commands = 100;
        let payload_size = 100;

        // create conflicting workload
        let conflict_rate = 100;
        let key_gen = KeyGen::ConflictRate { conflict_rate };
        let mut key_gen_state = key_gen.initial_state(client_id);
        let mut workload = Workload::new(
            shards_per_command,
            shard_gen,
            keys_per_shard,
            key_gen,
            total_commands,
            payload_size,
        );
        let (target_shard, command) =
            workload.gen_cmd(&mut rifl_gen, &mut key_gen_state);
        assert_eq!(target_shard, 0);
        assert_eq!(
            command.keys(target_shard).collect::<Vec<_>>(),
            vec![CONFLICT_COLOR]
        );

        // create non-conflicting workload
        let conflict_rate = 0;
        let key_gen = KeyGen::ConflictRate { conflict_rate };
        let mut key_gen_state = key_gen.initial_state(client_id);
        let mut workload = Workload::new(
            shards_per_command,
            shard_gen,
            keys_per_shard,
            key_gen,
            total_commands,
            payload_size,
        );
        let (target_shard, command) =
            workload.gen_cmd(&mut rifl_gen, &mut key_gen_state);
        assert_eq!(target_shard, 0);
        assert_eq!(command.keys(target_shard).collect::<Vec<_>>(), vec!["1"]);
    }

    #[test]
    fn next_cmd() {
        // create rilf gen
        let client_id = 1;
        let mut rifl_gen = RiflGen::new(client_id);

        // general config
        let shards_per_command = 1;
        let shard_gen = ShardGen::Random { shards: 1 };
        let keys_per_shard = 1;
        let total_commands = 10000;
        let payload_size = 10;

        // create workload
        let conflict_rate = 100;
        let key_gen = KeyGen::ConflictRate { conflict_rate };
        let mut key_gen_state = key_gen.initial_state(client_id);
        let mut workload = Workload::new(
            shards_per_command,
            shard_gen,
            keys_per_shard,
            key_gen,
            total_commands,
            payload_size,
        );

        // check total and issued commands
        assert_eq!(workload.commands_per_client(), total_commands);
        assert_eq!(workload.issued_commands(), 0);

        // the first `total_commands` commands are `Some`
        for i in 1..=total_commands {
            if let Some((target_shard, cmd)) =
                workload.next_cmd(&mut rifl_gen, &mut key_gen_state)
            {
                // since there's a single shard, keys should be on shard 0
                assert_eq!(target_shard, 0);
                let (key, value) = cmd.into_iter(target_shard).next().unwrap();
                // since the conflict is 100, the key should be `CONFLICT_COLOR`
                assert_eq!(key, CONFLICT_COLOR);
                // check that the value size is `payload_size`
                if let KVOp::Put(payload) = value {
                    assert_eq!(payload.len(), payload_size);
                } else {
                    panic!("workload should generate PUT commands");
                }

                // check total and issued commands
                assert_eq!(workload.commands_per_client(), total_commands);
                assert_eq!(workload.issued_commands(), i);
            } else {
                panic!("there should be a next command in this workload");
            }
        }

        // check the workload is finished
        assert!(workload.finished());

        // after this, no more commands are generated
        for _ in 1..=10 {
            assert!(workload
                .next_cmd(&mut rifl_gen, &mut key_gen_state)
                .is_none());
        }

        // check the workload is still finished
        assert!(workload.finished());
    }

    #[test]
    fn conflict_rate() {
        for conflict_rate in vec![1, 2, 10, 50] {
            // create rilf gen
            let client_id = 1;
            let mut rifl_gen = RiflGen::new(client_id);

            // total commands
            let shards_per_command = 1;
            let shard_gen = ShardGen::Random { shards: 1 };
            let keys_per_shard = 1;
            let total_commands = 100000;
            let payload_size = 0;

            // create workload
            let key_gen = KeyGen::ConflictRate { conflict_rate };
            let mut key_gen_state = key_gen.initial_state(client_id);
            let mut workload = Workload::new(
                shards_per_command,
                shard_gen,
                keys_per_shard,
                key_gen,
                total_commands,
                payload_size,
            );

            // count conflicting commands
            let mut conflict_color_count = 0;

            while let Some((target_shard, cmd)) =
                workload.next_cmd(&mut rifl_gen, &mut key_gen_state)
            {
                // since there's a single shard, keys should be on shard 0
                assert_eq!(target_shard, 0);
                // get command key and check if it's conflicting
                let (key, _) = cmd.into_iter(target_shard).next().unwrap();
                if key == CONFLICT_COLOR {
                    conflict_color_count += 1;
                }
            }

            // compute percentage of conflicting commands
            let percentage =
                (conflict_color_count * 100) as f64 / total_commands as f64;
            assert_eq!(percentage.round() as usize, conflict_rate);
        }
    }
}
