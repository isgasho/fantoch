// This module contains common data-structures between protocols.
pub mod common;

// This module contains the definition of `Atlas`.
mod atlas;

// This module contains the definition of `EPaxos`.
mod epaxos;

// This module contains the definition of `Newt`.
mod newt;

// This module contains the definition of `FPaxos`.
mod fpaxos;

// This module contains common functionality for partial replication.
mod partial;

// Re-exports.
pub use atlas::{AtlasLocked, AtlasSequential};
pub use epaxos::{EPaxosLocked, EPaxosSequential};
pub use fpaxos::FPaxos;
pub use newt::{NewtAtomic, NewtFineLocked, NewtLocked, NewtSequential};

#[cfg(test)]
mod tests {
    use super::*;
    use fantoch::client::{KeyGen, ShardGen, Workload};
    use fantoch::config::Config;
    use fantoch::id::ProcessId;
    use fantoch::planet::Planet;
    use fantoch::protocol::{Protocol, ProtocolMetricsKind};
    use fantoch::run::tests::{run_test_with_inspect_fun, tokio_test_runtime};
    use fantoch::sim::Runner;
    use fantoch::HashMap;
    use std::time::Duration;

    // global test config
    const SHARD_COUNT: usize = 1;
    const SHARDS_PER_COMMAND: usize = 1;
    const COMMANDS_PER_CLIENT: usize = 100;
    const CONFLICT_RATE: usize = 50;
    const CLIENTS_PER_PROCESS: usize = 10;

    macro_rules! config {
        ($n:expr, $f:expr) => {
            Config::new($n, $f)
        };
        ($n:expr, $f:expr, $leader:expr) => {{
            let mut config = Config::new($n, $f);
            config.set_leader($leader);
            config
        }};
    }

    macro_rules! newt_config {
        ($n:expr, $f:expr) => {{
            let mut config = Config::new($n, $f);
            // always set `newt_detached_send_interval`
            config.set_newt_detached_send_interval(Duration::from_millis(100));
            config
        }};
        ($n:expr, $f:expr, $clock_bump_interval:expr) => {{
            let mut config = newt_config!($n, $f);
            config.set_newt_tiny_quorums(true);
            config.set_newt_clock_bump_interval($clock_bump_interval);
            config
        }};
    }

    /// Computes the number of commands per client and clients per process
    /// according to "CI" env var; if set to true, run the tests with a smaller
    /// load
    fn small_load_in_ci() -> (usize, usize) {
        if let Ok(value) = std::env::var("CI") {
            // if ci is set, it should be a bool
            let ci =
                value.parse::<bool>().expect("CI env var should be a bool");
            if ci {
                // 10 commands per client and 1 client per process
                (10, 1)
            } else {
                panic!("CI env var is set and it's not true");
            }
        } else {
            (COMMANDS_PER_CLIENT, CLIENTS_PER_PROCESS)
        }
    }

    // ---- newt tests ---- //
    #[test]
    fn sim_newt_3_1_test() {
        let slow_paths = sim_test::<NewtSequential>(
            newt_config!(3, 1),
            COMMANDS_PER_CLIENT,
            CLIENTS_PER_PROCESS,
        );
        assert_eq!(slow_paths, 0);
    }

    #[test]
    fn sim_real_time_newt_3_1_test() {
        // NOTE: with n = 3 we don't really need real time clocks to get the
        // best results
        let clock_bump_interval = Duration::from_millis(50);
        let slow_paths = sim_test::<NewtSequential>(
            newt_config!(3, 1, clock_bump_interval),
            COMMANDS_PER_CLIENT,
            CLIENTS_PER_PROCESS,
        );
        assert_eq!(slow_paths, 0);
    }

    #[test]
    fn sim_newt_5_1_test() {
        let slow_paths = sim_test::<NewtSequential>(
            newt_config!(5, 1),
            COMMANDS_PER_CLIENT,
            CLIENTS_PER_PROCESS,
        );
        assert_eq!(slow_paths, 0);
    }

    #[test]
    fn sim_newt_5_2_test() {
        let slow_paths = sim_test::<NewtSequential>(
            newt_config!(5, 2),
            COMMANDS_PER_CLIENT,
            CLIENTS_PER_PROCESS,
        );
        assert!(slow_paths > 0);
    }

    #[test]
    fn sim_real_time_newt_5_1_test() {
        let clock_bump_interval = Duration::from_millis(50);
        let slow_paths = sim_test::<NewtSequential>(
            newt_config!(5, 1, clock_bump_interval),
            COMMANDS_PER_CLIENT,
            CLIENTS_PER_PROCESS,
        );
        assert_eq!(slow_paths, 0);
    }

    #[test]
    fn run_newt_3_1_sequential_test() {
        // newt sequential can only handle one worker but many executors
        let workers = 1;
        let executors = 4;
        let slow_paths = run_test::<NewtSequential>(
            newt_config!(3, 1),
            SHARD_COUNT,
            workers,
            executors,
            SHARDS_PER_COMMAND,
            COMMANDS_PER_CLIENT,
            CLIENTS_PER_PROCESS,
        );
        assert_eq!(slow_paths, 0);
    }

    #[test]
    fn run_newt_3_1_atomic_test() {
        // newt atomic can handle as many workers as we want but we may want to
        // only have one executor
        let workers = 4;
        let executors = 1;
        let slow_paths = run_test::<NewtAtomic>(
            newt_config!(3, 1),
            SHARD_COUNT,
            workers,
            executors,
            SHARDS_PER_COMMAND,
            COMMANDS_PER_CLIENT,
            CLIENTS_PER_PROCESS,
        );
        assert_eq!(slow_paths, 0);
    }

    #[test]
    fn run_newt_3_1_locked_test() {
        // newt locked can handle as many workers as we want but we may want to
        // only have one executor
        let workers = 4;
        let executors = 1;
        let slow_paths = run_test::<NewtLocked>(
            newt_config!(3, 1),
            SHARD_COUNT,
            workers,
            executors,
            SHARDS_PER_COMMAND,
            COMMANDS_PER_CLIENT,
            CLIENTS_PER_PROCESS,
        );
        assert_eq!(slow_paths, 0);
    }

    #[test]
    fn run_real_time_newt_3_1_atomic_test() {
        let workers = 2;
        let executors = 2;
        let (commands_per_client, clients_per_process) = small_load_in_ci();
        let clock_bump_interval = Duration::from_millis(500);
        let slow_paths = run_test::<NewtAtomic>(
            newt_config!(3, 1, clock_bump_interval),
            SHARD_COUNT,
            workers,
            executors,
            SHARDS_PER_COMMAND,
            commands_per_client,
            clients_per_process,
        );
        assert_eq!(slow_paths, 0);
    }

    #[test]
    fn run_real_time_newt_3_1_locked_test() {
        let workers = 2;
        let executors = 2;
        let (commands_per_client, clients_per_process) = small_load_in_ci();
        let clock_bump_interval = Duration::from_millis(500);
        let slow_paths = run_test::<NewtLocked>(
            newt_config!(3, 1, clock_bump_interval),
            SHARD_COUNT,
            workers,
            executors,
            SHARDS_PER_COMMAND,
            commands_per_client,
            clients_per_process,
        );
        assert_eq!(slow_paths, 0);
    }

    #[test]
    fn run_newt_5_1_sequential_test() {
        // newt sequential can only handle one worker but many executors
        let workers = 1;
        let executors = 4;
        let slow_paths = run_test::<NewtSequential>(
            newt_config!(5, 1),
            SHARD_COUNT,
            workers,
            executors,
            SHARDS_PER_COMMAND,
            COMMANDS_PER_CLIENT,
            CLIENTS_PER_PROCESS,
        );
        assert_eq!(slow_paths, 0);
    }

    #[test]
    fn run_newt_5_1_atomic_test() {
        // newt atomic can handle as many workers as we want but we may want to
        // only have one executor
        let workers = 4;
        let executors = 1;
        let slow_paths = run_test::<NewtAtomic>(
            newt_config!(5, 1),
            SHARD_COUNT,
            workers,
            executors,
            SHARDS_PER_COMMAND,
            COMMANDS_PER_CLIENT,
            CLIENTS_PER_PROCESS,
        );
        assert_eq!(slow_paths, 0);
    }

    #[test]
    fn run_newt_5_1_locked_test() {
        // newt locked can handle as many workers as we want but we may want to
        // only have one executor
        let workers = 4;
        let executors = 1;
        let slow_paths = run_test::<NewtLocked>(
            newt_config!(5, 1),
            SHARD_COUNT,
            workers,
            executors,
            SHARDS_PER_COMMAND,
            COMMANDS_PER_CLIENT,
            CLIENTS_PER_PROCESS,
        );
        assert_eq!(slow_paths, 0);
    }

    #[test]
    fn run_real_time_newt_5_1_atomic_test() {
        let workers = 2;
        let executors = 2;
        let (commands_per_client, clients_per_process) = small_load_in_ci();
        let clock_bump_interval = Duration::from_millis(500);
        let slow_paths = run_test::<NewtAtomic>(
            newt_config!(5, 1, clock_bump_interval),
            SHARD_COUNT,
            workers,
            executors,
            SHARDS_PER_COMMAND,
            commands_per_client,
            clients_per_process,
        );
        assert_eq!(slow_paths, 0);
    }

    #[test]
    fn run_real_time_newt_5_1_locked_test() {
        let workers = 2;
        let executors = 2;
        let (commands_per_client, clients_per_process) = small_load_in_ci();
        let clock_bump_interval = Duration::from_millis(500);
        let slow_paths = run_test::<NewtLocked>(
            newt_config!(5, 1, clock_bump_interval),
            SHARD_COUNT,
            workers,
            executors,
            SHARDS_PER_COMMAND,
            commands_per_client,
            clients_per_process,
        );
        assert_eq!(slow_paths, 0);
    }

    // ---- newt (partial replication) tests ---- //
    #[test]
    fn run_newt_3_1_atomic_partial_replication_one_shard_per_command_test() {
        let shard_count = 2;
        let workers = 2;
        let executors = 2;
        let slow_paths = run_test::<NewtAtomic>(
            newt_config!(3, 1),
            shard_count,
            workers,
            executors,
            SHARDS_PER_COMMAND,
            COMMANDS_PER_CLIENT,
            CLIENTS_PER_PROCESS,
        );
        assert_eq!(slow_paths, 0);
    }

    #[test]
    fn run_newt_3_1_atomic_partial_replication_two_shards_per_command_test() {
        let shard_count = 2;
        let workers = 2;
        let executors = 2;
        let shards_per_command = 2;
        let (commands_per_client, clients_per_process) = small_load_in_ci();
        let slow_paths = run_test::<NewtAtomic>(
            newt_config!(3, 1),
            shard_count,
            workers,
            executors,
            shards_per_command,
            commands_per_client,
            clients_per_process,
        );
        assert_eq!(slow_paths, 0);
    }

    #[test]
    fn run_newt_3_1_atomic_partial_replication_two_shards_per_command_three_shards_test(
    ) {
        let shard_count = 3;
        let workers = 2;
        let executors = 2;
        let shards_per_command = 2;
        let (commands_per_client, clients_per_process) = small_load_in_ci();
        let slow_paths = run_test::<NewtAtomic>(
            newt_config!(3, 1),
            shard_count,
            workers,
            executors,
            shards_per_command,
            commands_per_client,
            clients_per_process,
        );
        assert_eq!(slow_paths, 0);
    }

    #[test]
    fn run_newt_5_2_atomic_partial_replication_two_shards_per_command_test() {
        let shard_count = 2;
        let workers = 2;
        let executors = 2;
        let shards_per_command = 2;
        let (commands_per_client, clients_per_process) = small_load_in_ci();
        let slow_paths = run_test::<NewtAtomic>(
            newt_config!(5, 2),
            shard_count,
            workers,
            executors,
            shards_per_command,
            commands_per_client,
            clients_per_process,
        );
        assert!(slow_paths > 0);
    }

    // ---- atlas tests ---- //
    #[test]
    fn sim_atlas_3_1_test() {
        let slow_paths = sim_test::<AtlasSequential>(
            config!(3, 1),
            COMMANDS_PER_CLIENT,
            CLIENTS_PER_PROCESS,
        );
        assert_eq!(slow_paths, 0);
    }

    #[test]
    fn sim_atlas_5_1_test() {
        let slow_paths = sim_test::<AtlasSequential>(
            config!(3, 1),
            COMMANDS_PER_CLIENT,
            CLIENTS_PER_PROCESS,
        );
        assert_eq!(slow_paths, 0);
    }

    #[test]
    fn sim_atlas_5_2_test() {
        let slow_paths = sim_test::<AtlasSequential>(
            config!(5, 2),
            COMMANDS_PER_CLIENT,
            CLIENTS_PER_PROCESS,
        );
        assert!(slow_paths > 0);
    }

    #[test]
    fn run_atlas_3_1_sequential_test() {
        // atlas sequential can only handle one worker and one executor
        let workers = 1;
        let executors = 1;
        let slow_paths = run_test::<AtlasSequential>(
            config!(3, 1),
            SHARD_COUNT,
            workers,
            executors,
            SHARDS_PER_COMMAND,
            COMMANDS_PER_CLIENT,
            CLIENTS_PER_PROCESS,
        );
        assert_eq!(slow_paths, 0);
    }

    #[test]
    fn run_atlas_3_1_locked_test() {
        // atlas locked can handle as many workers as we want but only one
        // executor
        let workers = 4;
        let executors = 1;
        let slow_paths = run_test::<AtlasLocked>(
            config!(3, 1),
            SHARD_COUNT,
            workers,
            executors,
            SHARDS_PER_COMMAND,
            COMMANDS_PER_CLIENT,
            CLIENTS_PER_PROCESS,
        );
        assert_eq!(slow_paths, 0);
    }

    // ---- atlas (partial replication) tests ---- //
    #[test]
    fn run_atlas_3_1_locked_partial_replication_one_shard_per_command_test() {
        let shard_count = 2;
        let workers = 2;
        let executors = 1;
        let (commands_per_client, clients_per_process) = small_load_in_ci();
        let slow_paths = run_test::<AtlasLocked>(
            newt_config!(3, 1),
            shard_count,
            workers,
            executors,
            SHARDS_PER_COMMAND,
            commands_per_client,
            clients_per_process,
        );
        assert_eq!(slow_paths, 0);
    }

    #[test]
    fn run_atlas_3_1_locked_partial_replication_two_shards_per_command_test() {
        let shard_count = 2;
        let workers = 2;
        let executors = 1;
        let shards_per_command = 2;
        let (commands_per_client, clients_per_process) = small_load_in_ci();
        let slow_paths = run_test::<AtlasLocked>(
            newt_config!(3, 1),
            shard_count,
            workers,
            executors,
            shards_per_command,
            commands_per_client,
            clients_per_process,
        );
        assert_eq!(slow_paths, 0);
    }

    #[ignore]
    #[test]
    fn run_atlas_3_1_locked_partial_replication_two_shards_per_command_three_shards_test(
    ) {
        let shard_count = 3;
        let workers = 2;
        let executors = 1;
        let shards_per_command = 2;
        let (commands_per_client, clients_per_process) = small_load_in_ci();
        let slow_paths = run_test::<AtlasLocked>(
            newt_config!(3, 1),
            shard_count,
            workers,
            executors,
            shards_per_command,
            commands_per_client,
            clients_per_process,
        );
        assert_eq!(slow_paths, 0);
    }

    #[test]
    fn run_atlas_5_2_locked_partial_replication_two_shards_per_command_test() {
        let shard_count = 2;
        let workers = 2;
        let executors = 1;
        let shards_per_command = 2;
        let (commands_per_client, clients_per_process) = small_load_in_ci();
        let slow_paths = run_test::<AtlasLocked>(
            newt_config!(5, 2),
            shard_count,
            workers,
            executors,
            shards_per_command,
            commands_per_client,
            clients_per_process,
        );
        assert!(slow_paths > 0);
    }

    // ---- epaxos tests ---- //
    #[test]
    fn sim_epaxos_3_1_test() {
        let slow_paths = sim_test::<EPaxosSequential>(
            config!(3, 1),
            COMMANDS_PER_CLIENT,
            CLIENTS_PER_PROCESS,
        );
        assert_eq!(slow_paths, 0);
    }

    #[test]
    fn sim_epaxos_5_2_test() {
        let slow_paths = sim_test::<EPaxosSequential>(
            config!(5, 2),
            COMMANDS_PER_CLIENT,
            CLIENTS_PER_PROCESS,
        );
        assert!(slow_paths > 0);
    }

    #[test]
    fn run_epaxos_3_1_sequential_test() {
        // epaxos sequential can only handle one worker and one executor
        let workers = 1;
        let executors = 1;
        let slow_paths = run_test::<EPaxosSequential>(
            config!(3, 1),
            SHARD_COUNT,
            workers,
            executors,
            SHARDS_PER_COMMAND,
            COMMANDS_PER_CLIENT,
            CLIENTS_PER_PROCESS,
        );
        assert_eq!(slow_paths, 0);
    }

    #[test]
    fn run_epaxos_locked_test() {
        // epaxos locked can handle as many workers as we want but only one
        // executor
        let workers = 4;
        let executors = 1;
        let slow_paths = run_test::<EPaxosLocked>(
            config!(3, 1),
            SHARD_COUNT,
            workers,
            executors,
            SHARDS_PER_COMMAND,
            COMMANDS_PER_CLIENT,
            CLIENTS_PER_PROCESS,
        );
        assert_eq!(slow_paths, 0);
    }

    // ---- fpaxos tests ---- //
    #[test]
    fn sim_fpaxos_3_1_test() {
        let leader = 1;
        sim_test::<FPaxos>(
            config!(3, 1, leader),
            COMMANDS_PER_CLIENT,
            CLIENTS_PER_PROCESS,
        );
    }

    #[test]
    fn sim_fpaxos_5_2_test() {
        let leader = 1;
        sim_test::<FPaxos>(
            config!(5, 2, leader),
            COMMANDS_PER_CLIENT,
            CLIENTS_PER_PROCESS,
        );
    }

    #[test]
    fn run_fpaxos_3_1_sequential_test() {
        let leader = 1;
        // run fpaxos in sequential mode
        let workers = 1;
        let executors = 1;
        run_test::<FPaxos>(
            config!(3, 1, leader),
            SHARD_COUNT,
            workers,
            executors,
            SHARDS_PER_COMMAND,
            COMMANDS_PER_CLIENT,
            CLIENTS_PER_PROCESS,
        );
    }

    #[test]
    fn run_fpaxos_3_1_parallel_test() {
        let leader = 1;
        // run fpaxos in paralel mode (in terms of workers, since execution is
        // never parallel)
        let workers = 3;
        let executors = 1;
        run_test::<FPaxos>(
            config!(3, 1, leader),
            SHARD_COUNT,
            workers,
            executors,
            SHARDS_PER_COMMAND,
            COMMANDS_PER_CLIENT,
            CLIENTS_PER_PROCESS,
        );
    }

    #[allow(dead_code)]
    fn metrics_inspect<P>(worker: &P) -> (usize, usize, usize)
    where
        P: Protocol,
    {
        let fast_paths = worker
            .metrics()
            .get_aggregated(ProtocolMetricsKind::FastPath)
            .cloned()
            .unwrap_or_default() as usize;
        let slow_paths = worker
            .metrics()
            .get_aggregated(ProtocolMetricsKind::SlowPath)
            .cloned()
            .unwrap_or_default() as usize;
        let stable_count = worker
            .metrics()
            .get_aggregated(ProtocolMetricsKind::Stable)
            .cloned()
            .unwrap_or_default() as usize;
        (fast_paths, slow_paths, stable_count)
    }

    fn run_test<P>(
        mut config: Config,
        shard_count: usize,
        workers: usize,
        executors: usize,
        shards_per_command: usize,
        commands_per_client: usize,
        clients_per_process: usize,
    ) -> usize
    where
        P: Protocol + Send + 'static,
    {
        // make sure stability is running
        config.set_gc_interval(Duration::from_millis(100));

        // set number of shards
        config.set_shards(shard_count);

        // create workload
        let shard_gen = ShardGen::Random { shard_count };
        let keys_per_shard = 2;
        let key_gen = KeyGen::ConflictRate {
            conflict_rate: CONFLICT_RATE,
        };
        let payload_size = 1;
        let workload = Workload::new(
            shards_per_command,
            shard_gen,
            keys_per_shard,
            key_gen,
            commands_per_client,
            payload_size,
        );

        // run until the clients end + another 10 seconds
        let tracer_show_interval = None;
        let extra_run_time = Some(Duration::from_secs(10));
        let metrics = tokio_test_runtime()
            .block_on(run_test_with_inspect_fun::<P, (usize, usize, usize)>(
                config,
                workload,
                clients_per_process,
                workers,
                executors,
                tracer_show_interval,
                Some(metrics_inspect),
                extra_run_time,
            ))
            .expect("run should complete successfully")
            .into_iter()
            .map(|(process_id, process_metrics)| {
                // aggregate worker metrics
                let mut total_fast_paths = 0;
                let mut total_slow_paths = 0;
                let mut total_stable_count = 0;
                process_metrics.into_iter().for_each(
                    |(fast_paths, slow_paths, stable_count)| {
                        total_fast_paths += fast_paths;
                        total_slow_paths += slow_paths;
                        total_stable_count += stable_count;
                    },
                );
                (
                    process_id,
                    (total_fast_paths, total_slow_paths, total_stable_count),
                )
            })
            .collect();

        check_metrics(
            config,
            shards_per_command,
            commands_per_client,
            clients_per_process,
            metrics,
        )
    }

    fn sim_test<P: Protocol + Eq>(
        mut config: Config,
        commands_per_client: usize,
        clients_per_process: usize,
    ) -> usize {
        // make sure stability is running
        config.set_gc_interval(Duration::from_millis(100));

        // planet
        let planet = Planet::new();

        // clients workload
        let shards_per_command = 1;
        let shard_gen = ShardGen::Random { shard_count: 1 };
        let keys_per_shard = 2;
        let payload_size = 1;
        let key_gen = KeyGen::ConflictRate {
            conflict_rate: CONFLICT_RATE,
        };
        let workload = Workload::new(
            shards_per_command,
            shard_gen,
            keys_per_shard,
            key_gen,
            commands_per_client,
            payload_size,
        );

        // process and client regions
        let mut regions = planet.regions();
        regions.truncate(config.n());
        let process_regions = regions.clone();
        let client_regions = regions.clone();

        // create runner
        let mut runner: Runner<P> = Runner::new(
            planet,
            config,
            workload,
            clients_per_process,
            process_regions,
            client_regions,
        );

        // run simulation until the clients end + another 2 seconds
        let extra_sim_time = Some(Duration::from_secs(2));
        let (metrics, _) = runner.run(extra_sim_time);

        // fetch slow paths and stable count from metrics
        let metrics = metrics
            .into_iter()
            .map(|(process_id, process_metrics)| {
                // get fast paths
                let fast_paths = process_metrics
                    .get_aggregated(ProtocolMetricsKind::FastPath)
                    .cloned()
                    .unwrap_or_default()
                    as usize;

                // get slow paths
                let slow_paths = process_metrics
                    .get_aggregated(ProtocolMetricsKind::SlowPath)
                    .cloned()
                    .unwrap_or_default()
                    as usize;

                // get stable count
                let stable_count = process_metrics
                    .get_aggregated(ProtocolMetricsKind::Stable)
                    .cloned()
                    .unwrap_or_default()
                    as usize;

                (process_id, (fast_paths, slow_paths, stable_count))
            })
            .collect();

        check_metrics(
            config,
            shards_per_command,
            commands_per_client,
            clients_per_process,
            metrics,
        )
    }

    fn check_metrics(
        config: Config,
        shards_per_command: usize,
        commands_per_client: usize,
        clients_per_process: usize,
        metrics: HashMap<ProcessId, (usize, usize, usize)>,
    ) -> usize {
        // total commands per shard

        // total fast and slow paths count
        let mut total_fast_paths = 0;
        let mut total_slow_paths = 0;
        let mut total_stable = 0;

        // check process stats
        metrics.into_iter().for_each(
            |(process_id, (fast_paths, slow_paths, stable))| {
                println!(
                    "process id = {} | fast = {} | slow = {} | stable = {}",
                    process_id, fast_paths, slow_paths, stable
                );
                total_fast_paths += fast_paths;
                total_slow_paths += slow_paths;
                total_stable += stable;
            },
        );

        // compute the total number of commands
        let total_commands_per_shard = shards_per_command
            * commands_per_client
            * clients_per_process
            * config.n();
        let total_commands = total_commands_per_shard * config.shards();

        // check that all commands were committed (only for leaderless
        // protocols)
        if config.leader().is_none() {
            assert_eq!(
                total_fast_paths + total_slow_paths,
                total_commands,
                "not all commands were committed"
            );
        }

        // check GC:
        // - if there's a leader (i.e. FPaxos), GC will only prune commands at
        //   f+1 acceptors
        // - otherwise, GC will prune comands at all processes
        let gc_at = if config.leader().is_some() {
            config.f() + 1
        } else {
            config.n()
        } * config.shards();

        // since GC only happens at the targetted shard, here divide by the
        // number of `shards_per_command`
        assert_eq!(
            gc_at * total_commands_per_shard / shards_per_command,
            total_stable,
            "not all processes gced"
        );

        // return number of slow paths
        total_slow_paths
    }
}
