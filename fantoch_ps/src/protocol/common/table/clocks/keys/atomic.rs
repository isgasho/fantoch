use super::KeyClocks;
use crate::protocol::common::shared::Shared;
use crate::protocol::common::table::{VoteRange, Votes};
use fantoch::command::Command;
use fantoch::id::ProcessId;
use fantoch::kvs::Key;
use std::cmp;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct AtomicKeyClocks {
    id: ProcessId,
    // TODO remove arc
    clocks: Arc<Shared<AtomicU64>>,
}

impl KeyClocks for AtomicKeyClocks {
    /// Create a new `AtomicKeyClocks` instance.
    fn new(id: ProcessId) -> Self {
        // create shared clocks
        let clocks = Shared::new();
        // wrap them in an arc
        let clocks = Arc::new(clocks);

        Self { id, clocks }
    }

    fn init_clocks(&mut self, cmd: &Command) {
        cmd.keys().for_each(|key| {
            // get initializes the key to the default value, and that's exactly
            // what we want
            let _ = self.clocks.get(key);
        });
    }

    fn bump_and_vote(&mut self, cmd: &Command, min_clock: u64) -> (u64, Votes) {
        // single round of votes:
        // - vote on each key and compute the highest clock seen
        // - this means that if we have more than one key, then we don't
        //   necessarily end up with all key clocks equal
        let mut votes = Votes::with_capacity(cmd.key_count());
        let highest = cmd
            .keys()
            .map(|key| {
                // bump the `key` clock
                let clock = self.clocks.get(key);
                let previous_value = Self::bump(&clock, min_clock);

                // create vote range and save it
                let current_value = cmp::max(min_clock, previous_value + 1);
                let vr =
                    VoteRange::new(self.id, previous_value + 1, current_value);
                votes.set(key.clone(), vec![vr]);

                // return "current" clock value
                current_value
            })
            .max()
            .expect("there should be a maximum sequence");
        (highest, votes)
    }

    fn vote(&mut self, cmd: &Command, up_to: u64) -> Votes {
        // create votes
        let mut votes = Votes::with_capacity(cmd.key_count());
        for key in cmd.keys() {
            let clock = self.clocks.get(key);
            Self::maybe_bump(self.id, key, &clock, up_to, &mut votes);
        }
        votes
    }

    fn vote_all(&mut self, up_to: u64) -> Votes {
        let key_count = self.clocks.len();
        // create votes
        let mut votes = Votes::with_capacity(key_count);

        self.clocks.iter().for_each(|entry| {
            let key = entry.key();
            let clock = entry.value();
            Self::maybe_bump(self.id, key, &clock, up_to, &mut votes);
        });

        votes
    }

    fn parallel() -> bool {
        true
    }
}

impl AtomicKeyClocks {
    // Bump the clock to at least `min_clock`.
    fn bump(clock: &AtomicU64, min_clock: u64) -> u64 {
        let fetch_update =
            clock.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                Some(cmp::max(min_clock, value + 1))
            });
        match fetch_update {
            Ok(previous_value) => previous_value,
            Err(_) => {
                panic!("atomic bump should always succeed");
            }
        }
    }

    // Bump the clock to `up_to` if lower than `up_to`.
    fn maybe_bump(
        id: ProcessId,
        key: &Key,
        clock: &AtomicU64,
        up_to: u64,
        votes: &mut Votes,
    ) {
        let fetch_update =
            clock.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                if value < up_to {
                    Some(up_to)
                } else {
                    None
                }
            });
        if let Ok(previous_value) = fetch_update {
            let vr = VoteRange::new(id, previous_value + 1, up_to);
            votes.set(key.clone(), vec![vr]);
        }
    }
}
