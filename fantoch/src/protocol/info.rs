use crate::id::{Dot, ProcessId, ShardId};
use crate::protocol::gc::GCTrack;
use crate::util;
use crate::HashMap;
use threshold::VClock;

pub trait Info {
    fn new(
        process_id: ProcessId,
        shard_id: ShardId,
        n: usize,
        f: usize,
        fast_quorum_size: usize,
    ) -> Self;
}

// `CommandsInfo` contains `CommandInfo` for each `Dot`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandsInfo<I> {
    process_id: ProcessId,
    shard_id: ShardId,
    n: usize,
    f: usize,
    fast_quorum_size: usize,
    dot_to_info: HashMap<Dot, I>,
    gc_track: GCTrack,
}

impl<I> CommandsInfo<I>
where
    I: Info,
{
    pub fn new(
        process_id: ProcessId,
        shard_id: ShardId,
        n: usize,
        f: usize,
        fast_quorum_size: usize,
    ) -> Self {
        Self {
            process_id,
            shard_id,
            n,
            f,
            fast_quorum_size,
            dot_to_info: HashMap::new(),
            gc_track: GCTrack::new(process_id, shard_id, n),
        }
    }

    /// Returns the `Info` associated with `Dot`.
    /// If no `Info` is associated, an empty `Info` is returned.
    pub fn get(&mut self, dot: Dot) -> &mut I {
        // TODO borrow everything we need so that the borrow checker does not
        // complain
        let process_id = self.process_id;
        let shard_id = self.shard_id;
        let n = self.n;
        let f = self.f;
        let fast_quorum_size = self.fast_quorum_size;
        self.dot_to_info.entry(dot).or_insert_with(|| {
            I::new(process_id, shard_id, n, f, fast_quorum_size)
        })
    }

    /// Records that a command has been committed.
    pub fn commit(&mut self, dot: Dot) {
        self.gc_track.commit(dot);
    }

    /// Records that set of `committed` commands by process `from`.
    pub fn committed_by(
        &mut self,
        from: ProcessId,
        committed: VClock<ProcessId>,
    ) {
        self.gc_track.committed_by(from, committed);
    }

    /// Returns committed clock and newly stable dots.
    pub fn committed(&mut self) -> VClock<ProcessId> {
        self.gc_track.committed()
    }

    /// Returns newly stable dots.
    pub fn stable(&mut self) -> Vec<(ProcessId, u64, u64)> {
        self.gc_track.stable()
    }

    /// Performs garbage collection of stable dots.
    /// Returns how many stable does were removed.
    pub fn gc(&mut self, stable: Vec<(ProcessId, u64, u64)>) -> usize {
        util::dots(stable)
            .filter(|dot| {
                // remove dot:
                // - the dot may not exist locally if there are multiple workers
                //   and this worker is not responsible for such dot
                self.dot_to_info.remove(&dot).is_some()
            })
            .count()
    }

    /// Removes a command has been committed.
    pub fn gc_single(&mut self, dot: Dot) {
        assert!(self.dot_to_info.remove(&dot).is_some());
    }
}
