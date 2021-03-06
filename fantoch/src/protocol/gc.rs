use crate::id::{Dot, ProcessId, ShardId};
use crate::trace;
use crate::util;
use crate::HashMap;
use threshold::{AEClock, EventSet, VClock};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GCTrack {
    process_id: ProcessId,
    shard_id: ShardId,
    n: usize,
    // the next 3 variables will be updated by the single process responsible
    // for GC
    my_clock: AEClock<ProcessId>,
    all_but_me: HashMap<ProcessId, VClock<ProcessId>>,
    previous_stable: VClock<ProcessId>,
}

impl GCTrack {
    pub fn new(process_id: ProcessId, shard_id: ShardId, n: usize) -> Self {
        // clocks from all processes but self
        let all_but_me = HashMap::with_capacity(n - 1);

        Self {
            process_id,
            shard_id,
            n,
            my_clock: Self::bottom_aeclock(shard_id, n),
            all_but_me,
            previous_stable: Self::bottom_vclock(shard_id, n),
        }
    }

    /// Returns a clock representing the set of commands recorded locally.
    /// Note that there might be more commands recorded than the ones being
    /// represented by the returned clock.
    pub fn clock(&self) -> VClock<ProcessId> {
        self.my_clock.frontier()
    }

    /// Records this command.
    pub fn add_to_clock(&mut self, dot: Dot) {
        self.my_clock.add(&dot.source(), dot.sequence());
        // make sure we don't record dots from other shards
        debug_assert_eq!(self.my_clock.len(), self.n);
    }

    /// Updates local clock. It assumes that the clock passed as argument is
    /// monotonic.
    pub fn update_clock(&mut self, clock: AEClock<ProcessId>) {
        self.my_clock = clock;
        debug_assert_eq!(self.my_clock.len(), self.n);
    }

    /// Records the set of commands by process `from`.
    pub fn update_clock_of(
        &mut self,
        from: ProcessId,
        clock: VClock<ProcessId>,
    ) {
        if let Some(current) = self.all_but_me.get_mut(&from) {
            // accumulate new knowledge; simply replacing it doesn't work since
            // messages can be reordered
            current.join(&clock);
        } else {
            self.all_but_me.insert(from, clock);
        }
    }

    /// Computes the new set of stable dots.
    // #[instrument(skip(self))]
    pub fn stable(&mut self) -> Vec<(ProcessId, u64, u64)> {
        // compute new stable clock
        let mut new_stable = self.stable_clock();
        trace!("GCTrack::stable_clock {:?}", new_stable);

        // compute new stable dots; while at it, update the previous stable
        // clock and return newly stable dots
        // - here we make sure we never go down on the previous clock, which
        //   would be possible if messages are reordered in the network or if
        //   we're multiplexing
        let dots = self
            .previous_stable
            .iter()
            .filter_map(|(process_id, previous)| {
                let current =
                    if let Some(current) = new_stable.get_mut(process_id) {
                        current
                    } else {
                        panic!(
                            "actor {} should exist in the newly stable clock",
                            process_id
                        )
                    };

                // compute representation of stable dots.
                let start = previous.frontier() + 1;
                let end = current.frontier();

                // make sure new clock doesn't go backwards
                current.join(previous);

                if start > end {
                    None
                } else {
                    // return stable dots representation
                    // - note that `start == end` also represents a stable dot
                    Some((*process_id, start, end))
                }
            })
            .collect();

        // update the previous stable clock and return newly stable dots
        self.previous_stable = new_stable;
        dots
    }

    // TODO we should design a fault-tolerant version of this
    // #[instrument(skip(self))]
    fn stable_clock(&mut self) -> VClock<ProcessId> {
        if self.all_but_me.len() != self.n - 1 {
            // if we don't have info from all processes, then there are no
            // stable dots.
            return Self::bottom_vclock(self.shard_id, self.n);
        }

        // start from our own frontier
        let mut stable = self.my_clock.frontier();
        // and intersect with all the other clocks
        self.all_but_me.values().for_each(|clock| {
            stable.meet(clock);
        });
        stable
    }

    fn bottom_aeclock(shard_id: ShardId, n: usize) -> AEClock<ProcessId> {
        AEClock::with(util::process_ids(shard_id, n))
    }

    fn bottom_vclock(shard_id: ShardId, n: usize) -> VClock<ProcessId> {
        VClock::with(util::process_ids(shard_id, n))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use threshold::MaxSet;

    // create vector clock with two entries: process 1 and process 2
    fn vclock(p1: u64, p2: u64) -> VClock<ProcessId> {
        VClock::from(vec![(1, MaxSet::from(p1)), (2, MaxSet::from(p2))])
    }

    fn stable_dots(repr: Vec<(ProcessId, u64, u64)>) -> Vec<Dot> {
        crate::util::dots(repr).collect()
    }

    #[test]
    fn gc_flow() {
        let n = 2;
        let shard_id = 0;
        // create new gc track for the our process: 1
        let mut gc = GCTrack::new(1, shard_id, n);

        // let's also create a gc track for process 2
        let mut gc2 = GCTrack::new(2, shard_id, n);

        // there's nothing committed and nothing stable
        assert_eq!(gc.clock(), vclock(0, 0));
        assert_eq!(gc.stable_clock(), vclock(0, 0));
        assert_eq!(stable_dots(gc.stable()), vec![]);

        // let's create a bunch of dots
        let dot11 = Dot::new(1, 1);
        let dot12 = Dot::new(1, 2);
        let dot13 = Dot::new(1, 3);

        // and commit dot12 locally
        gc.add_to_clock(dot12);

        // this doesn't change anything
        assert_eq!(gc.clock(), vclock(0, 0));
        assert_eq!(gc.stable_clock(), vclock(0, 0));
        assert_eq!(stable_dots(gc.stable()), vec![]);

        // however, if we also commit dot11, the committed clock will change
        gc.add_to_clock(dot11);
        assert_eq!(gc.clock(), vclock(2, 0));
        assert_eq!(gc.stable_clock(), vclock(0, 0));
        assert_eq!(stable_dots(gc.stable()), vec![]);

        // if we update with the committed clock from process 2 nothing changes
        gc.update_clock_of(2, gc2.clock());
        assert_eq!(gc.clock(), vclock(2, 0));
        assert_eq!(gc.stable_clock(), vclock(0, 0));
        assert_eq!(stable_dots(gc.stable()), vec![]);

        // let's commit dot11 and dot13 at process 2
        gc2.add_to_clock(dot11);
        gc2.add_to_clock(dot13);

        // now dot11 is stable at process 1
        gc.update_clock_of(2, gc2.clock());
        assert_eq!(gc.clock(), vclock(2, 0));
        assert_eq!(gc.stable_clock(), vclock(1, 0));
        assert_eq!(stable_dots(gc.stable()), vec![dot11]);

        // if we call stable again, no new dot is returned
        assert_eq!(gc.stable_clock(), vclock(1, 0));
        assert_eq!(stable_dots(gc.stable()), vec![]);

        // let's commit dot13 at process 1 and dot12 at process 2
        gc.add_to_clock(dot13);
        gc2.add_to_clock(dot12);

        // now both dot12 and dot13 are stable at process 1
        gc.update_clock_of(2, gc2.clock());
        assert_eq!(gc.clock(), vclock(3, 0));
        assert_eq!(gc.stable_clock(), vclock(3, 0));
        assert_eq!(stable_dots(gc.stable()), vec![dot12, dot13]);
        assert_eq!(stable_dots(gc.stable()), vec![]);
    }
}
