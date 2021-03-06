use super::execution_logger;
use crate::command::Command;
use crate::config::Config;
use crate::id::{Dot, ProcessId, ShardId};
use crate::protocol::{Action, Executed, Protocol};
use crate::run::prelude::*;
use crate::run::rw::Connection;
use crate::run::task;
use crate::time::RunTime;
use crate::HashMap;
use crate::{trace, warn};
use color_eyre::Report;
use rand::Rng;
use std::fmt::Debug;
use std::net::IpAddr;
use std::sync::Arc;
use tokio::net::{TcpListener, ToSocketAddrs};
use tokio::task::JoinHandle;
use tokio::time::{self, Duration};

pub async fn connect_to_all<A, P>(
    process_id: ProcessId,
    shard_id: ShardId,
    config: Config,
    listener: TcpListener,
    addresses: Vec<(A, Option<Duration>)>,
    to_workers: ReaderToWorkers<P>,
    to_executors: ToExecutors<P>,
    connect_retries: usize,
    tcp_nodelay: bool,
    tcp_buffer_size: usize,
    tcp_flush_interval: Option<Duration>,
    channel_buffer_size: usize,
    multiplexing: usize,
) -> Result<
    (
        HashMap<ProcessId, (ShardId, IpAddr, Option<Duration>)>,
        HashMap<ProcessId, Vec<WriterSender<P>>>,
    ),
    Report,
>
where
    A: ToSocketAddrs + Debug,
    P: Protocol + 'static,
{
    // check that (n-1 + shards-1) addresses were set
    let total = config.n() - 1 + config.shard_count() - 1;
    assert_eq!(
        addresses.len(),
        total,
        "addresses count should be (n-1 + shards-1)"
    );

    // compute the number of expected connections
    let total_connections = total * multiplexing;

    // spawn listener
    let mut from_listener = task::spawn_producer(channel_buffer_size, |tx| {
        super::listener_task(listener, tcp_nodelay, tcp_buffer_size, tx)
    });

    // create list of in and out connections:
    // - even though TCP is full-duplex, due to the current tokio
    //   non-parallel-tcp-socket-read-write limitation, we going to use in
    //   streams for reading and out streams for writing, which can be done in
    //   parallel
    let mut outgoing = Vec::with_capacity(total_connections);
    let mut incoming = Vec::with_capacity(total_connections);

    // connect to all addresses (outgoing)
    for (address, delay) in addresses {
        // create `multiplexing` connections per address
        for _ in 0..multiplexing {
            let mut connection = super::connect(
                &address,
                tcp_nodelay,
                tcp_buffer_size,
                connect_retries,
            )
            .await?;
            // maybe set delay
            if let Some(delay) = delay {
                connection.set_delay(delay);
            }
            // save connection if connected successfully
            outgoing.push(connection);
        }
    }

    // receive from listener all connected (incoming)
    for _ in 0..total_connections {
        let connection = from_listener
            .recv()
            .await
            .expect("should receive connection from listener");
        incoming.push(connection);
    }

    let res = handshake::<P>(
        process_id,
        shard_id,
        to_workers,
        to_executors,
        tcp_flush_interval,
        channel_buffer_size,
        incoming,
        outgoing,
    )
    .await;
    Ok(res)
}

async fn handshake<P>(
    process_id: ProcessId,
    shard_id: ShardId,
    to_workers: ReaderToWorkers<P>,
    to_executors: ToExecutors<P>,
    tcp_flush_interval: Option<Duration>,
    channel_buffer_size: usize,
    mut connections_0: Vec<Connection>,
    mut connections_1: Vec<Connection>,
) -> (
    HashMap<ProcessId, (ShardId, IpAddr, Option<Duration>)>,
    HashMap<ProcessId, Vec<WriterSender<P>>>,
)
where
    P: Protocol + 'static,
{
    // say hi to all on both connections
    say_hi(process_id, shard_id, &mut connections_0).await;
    say_hi(process_id, shard_id, &mut connections_1).await;
    trace!("said hi to all processes");

    // receive hi from all on both connections
    let id_to_connection_0 = receive_hi(connections_0).await;
    let id_to_connection_1 = receive_hi(connections_1).await;

    // start readers and writers
    start_readers::<P>(to_workers, to_executors, id_to_connection_0);
    start_writers::<P>(
        shard_id,
        tcp_flush_interval,
        channel_buffer_size,
        id_to_connection_1,
    )
    .await
}

async fn say_hi(
    process_id: ProcessId,
    shard_id: ShardId,
    connections: &mut Vec<Connection>,
) {
    let hi = ProcessHi {
        process_id,
        shard_id,
    };
    // send hi on each connection
    for connection in connections.iter_mut() {
        if let Err(e) = connection.send(&hi).await {
            warn!("error while sending hi to connection: {:?}", e)
        }
    }
}

async fn receive_hi(
    connections: Vec<Connection>,
) -> Vec<(ProcessId, ShardId, Connection)> {
    let mut id_to_connection = Vec::with_capacity(connections.len());

    // receive hi from each connection
    for mut connection in connections {
        if let Some(ProcessHi {
            process_id,
            shard_id,
        }) = connection.recv().await
        {
            id_to_connection.push((process_id, shard_id, connection));
        } else {
            panic!("error receiving hi");
        }
    }
    id_to_connection
}

/// Starts a reader task per connection received. A `ReaderToWorkers` is passed
/// to each reader so that these can forward immediately to the correct worker
/// process.
fn start_readers<P>(
    to_workers: ReaderToWorkers<P>,
    to_executors: ToExecutors<P>,
    connections: Vec<(ProcessId, ShardId, Connection)>,
) where
    P: Protocol + 'static,
{
    for (process_id, shard_id, connection) in connections {
        task::spawn(reader_task::<P>(
            to_workers.clone(),
            to_executors.clone(),
            process_id,
            shard_id,
            connection,
        ));
    }
}

async fn start_writers<P>(
    shard_id: ShardId,
    tcp_flush_interval: Option<Duration>,
    channel_buffer_size: usize,
    connections: Vec<(ProcessId, ShardId, Connection)>,
) -> (
    HashMap<ProcessId, (ShardId, IpAddr, Option<Duration>)>,
    HashMap<ProcessId, Vec<WriterSender<P>>>,
)
where
    P: Protocol + 'static,
{
    let mut ips = HashMap::with_capacity(connections.len());
    // mapping from process id to channel broadcast writer should write to
    let mut writers = HashMap::with_capacity(connections.len());

    // start on writer task per connection
    for (peer_id, peer_shard_id, connection) in connections {
        // save shard id, ip and connection delay
        let ip = connection
            .ip_addr()
            .expect("ip address should be set for outgoing connection");
        let delay = connection.delay();
        ips.insert(peer_id, (peer_shard_id, ip, delay));

        // get connection delay
        let connection_delay = connection.delay();

        // get list set of writers to this process and create writer channels
        let txs = writers.entry(peer_id).or_insert_with(Vec::new);
        let (mut writer_tx, writer_rx) = task::channel(channel_buffer_size);

        // name the channel accordingly
        writer_tx.set_name(format!(
            "to_writer_{}_process_{}",
            txs.len(),
            peer_id
        ));

        // don't use a flush interval if this peer is in my region: a peer is in
        // my region if it has a different shard id
        let tcp_flush_interval = if peer_shard_id != shard_id {
            None
        } else {
            tcp_flush_interval
        };

        // spawn the writer task
        task::spawn(writer_task::<P>(
            tcp_flush_interval,
            connection,
            writer_rx,
        ));

        let tx = if let Some(delay) = connection_delay {
            // if connection has a delay, spawn a delay task for this writer
            let (mut delay_tx, delay_rx) = task::channel(channel_buffer_size);

            // name the channel accordingly
            delay_tx.set_name(format!(
                "to_delay_{}_process_{}",
                txs.len(),
                peer_id
            ));

            // spawn delay task
            task::spawn(super::delay::delay_task(delay_rx, writer_tx, delay));

            // in this case, messages are first forward to the delay task, which
            // then forwards them to the writer task
            delay_tx
        } else {
            // if there's no connection delay, then send the messages directly
            // to the writer task
            writer_tx
        };

        // and add a new writer channel
        txs.push(tx);
    }

    (ips, writers)
}

/// Reader task.
async fn reader_task<P>(
    mut reader_to_workers: ReaderToWorkers<P>,
    mut to_executors: ToExecutors<P>,
    process_id: ProcessId,
    shard_id: ShardId,
    mut connection: Connection,
) where
    P: Protocol + 'static,
{
    loop {
        match connection.recv::<POEMessage<P>>().await {
            Some(msg) => match msg {
                POEMessage::Protocol(msg) => {
                    let forward = reader_to_workers
                        .forward((process_id, shard_id, msg))
                        .await;
                    if let Err(e) = forward {
                        warn!("[reader] error notifying process task with new msg: {:?}",e);
                    }
                }
                POEMessage::Executor(execution_info) => {
                    trace!("[reader] to executor {:?}", execution_info);
                    // notify executor
                    if let Err(e) = to_executors.forward(execution_info).await {
                        warn!("[reader] error while notifying executor with new execution info: {:?}", e);
                    }
                }
            },
            None => {
                warn!("[reader] error receiving message from connection");
                break;
            }
        }
    }
}

/// Writer task.
async fn writer_task<P>(
    tcp_flush_interval: Option<Duration>,
    mut connection: Connection,
    mut parent: WriterReceiver<P>,
) where
    P: Protocol + 'static,
{
    // track whether there's been a flush error on this connection
    let mut flush_error = false;
    // if flush interval higher than 0, then flush periodically; otherwise,
    // flush on every write
    if let Some(tcp_flush_interval) = tcp_flush_interval {
        // create interval
        let mut interval = time::interval(tcp_flush_interval);
        loop {
            tokio::select! {
                msg = parent.recv() => {
                    if let Some(msg) = msg {
                        // connection write *doesn't* flush
                        if let Err(e) = connection.write(&*msg).await {
                            warn!("[writer] error writing message in connection: {:?}", e);
                        }
                    } else {
                        warn!("[writer] error receiving message from parent");
                        break;
                    }
                }
                _ = interval.tick() => {
                    // flush socket
                    if let Err(e) = connection.flush().await {
                        // make sure we only log the error once
                        if !flush_error {
                            warn!("[writer] error flushing connection: {:?}", e);
                            flush_error = true;
                        }
                    }
                }
            }
        }
    } else {
        loop {
            if let Some(msg) = parent.recv().await {
                // connection write *does* flush
                if let Err(e) = connection.send(&*msg).await {
                    warn!(
                        "[writer] error sending message to connection: {:?}",
                        e
                    );
                }
            } else {
                warn!("[writer] error receiving message from parent");
                break;
            }
        }
    }
    warn!("[writer] exiting after failure");
}

/// Starts process workers.
pub fn start_processes<P, R>(
    process: P,
    reader_to_workers_rxs: Vec<ReaderReceiver<P>>,
    client_to_workers_rxs: Vec<SubmitReceiver>,
    periodic_to_workers_rxs: Vec<PeriodicEventReceiver<P, R>>,
    executors_to_workers_rxs: Vec<ExecutedReceiver>,
    to_writers: HashMap<ProcessId, Vec<WriterSender<P>>>,
    reader_to_workers: ReaderToWorkers<P>,
    to_executors: ToExecutors<P>,
    process_channel_buffer_size: usize,
    execution_log: Option<String>,
    to_metrics_logger: Option<ProtocolMetricsSender>,
) -> Vec<JoinHandle<()>>
where
    P: Protocol + Send + 'static,
    R: Debug + Clone + Send + 'static,
{
    let to_execution_logger = execution_log.map(|execution_log| {
        // if the execution log was set, then start the execution logger
        let mut tx = task::spawn_consumer(process_channel_buffer_size, |rx| {
            execution_logger::execution_logger_task::<P>(execution_log, rx)
        });
        tx.set_name("to_execution_logger");
        tx
    });

    // zip rxs'
    let incoming = reader_to_workers_rxs
        .into_iter()
        .zip(client_to_workers_rxs.into_iter())
        .zip(periodic_to_workers_rxs.into_iter())
        .zip(executors_to_workers_rxs.into_iter());

    // create executor workers
    incoming
        .enumerate()
        .map(
            |(
                worker_index,
                (((from_readers, from_clients), from_periodic), from_executors),
            )| {
                // create task
                let task = process_task::<P, R>(
                    worker_index,
                    process.clone(),
                    from_readers,
                    from_clients,
                    from_periodic,
                    from_executors,
                    to_writers.clone(),
                    reader_to_workers.clone(),
                    to_executors.clone(),
                    to_execution_logger.clone(),
                    to_metrics_logger.clone(),
                );
                task::spawn(task)
                // // if this is a reserved worker, run it on its own runtime
                // if worker_index < super::INDEXES_RESERVED {
                //     let thread_name =
                //         format!("worker_{}_runtime", worker_index);
                //     tokio::task::spawn_blocking(|| {
                //         // create tokio runtime
                //         let mut runtime = tokio::runtime::Builder::new()
                //             .threaded_scheduler()
                //             .core_threads(1)
                //             .thread_name(thread_name)
                //             .build()
                //             .expect("tokio runtime build should work");
                //         runtime.block_on(task)
                //     });
                //     None
                // } else {
                //     Some(task::spawn(task))
                // }
            },
        )
        .collect()
}

async fn process_task<P, R>(
    worker_index: usize,
    mut process: P,
    mut from_readers: ReaderReceiver<P>,
    mut from_clients: SubmitReceiver,
    mut from_periodic: PeriodicEventReceiver<P, R>,
    mut from_executors: ExecutedReceiver,
    mut to_writers: HashMap<ProcessId, Vec<WriterSender<P>>>,
    mut reader_to_workers: ReaderToWorkers<P>,
    mut to_executors: ToExecutors<P>,
    mut to_execution_logger: Option<ExecutionInfoSender<P>>,
    mut to_metrics_logger: Option<ProtocolMetricsSender>,
) where
    P: Protocol + 'static,
    R: Debug + 'static,
{
    // create time
    let time = RunTime;

    // create interval (for metrics notification)
    let mut interval = time::interval(super::metrics_logger::METRICS_INTERVAL);

    loop {
        // TODO maybe used select_biased
        tokio::select! {
            msg = from_readers.recv() => {
                selected_from_processes(worker_index, msg, &mut process, &mut to_writers, &mut reader_to_workers, &mut to_executors, &mut to_execution_logger, &time).await
            }
            event = from_periodic.recv() => {
                selected_from_periodic_task(worker_index, event, &mut process, &mut to_writers, &mut reader_to_workers, &mut to_executors, &mut to_execution_logger, &time).await
            }
            executed = from_executors.recv() => {
                selected_from_executors(worker_index, executed, &mut process, &mut to_writers, &mut reader_to_workers, &mut to_executors, &mut to_execution_logger, &time).await
            }
            cmd = from_clients.recv() => {
                selected_from_clients(worker_index, cmd, &mut process, &mut to_writers, &mut reader_to_workers, &mut to_executors, &mut to_execution_logger, &time).await
            }
            _ = interval.tick()  => {
                if let Some(to_metrics_logger) = to_metrics_logger.as_mut() {
                    // send metrics to logger (in case there's one)
                    let protocol_metrics = process.metrics().clone();
                    if let Err(e) = to_metrics_logger.send((worker_index, protocol_metrics)).await {
                        warn!("[server] error while sending metrics to metrics logger: {:?}", e);
                    }
                }
            }
        }
    }
}

async fn selected_from_processes<P>(
    worker_index: usize,
    msg: Option<(ProcessId, ShardId, P::Message)>,
    process: &mut P,
    to_writers: &mut HashMap<ProcessId, Vec<WriterSender<P>>>,
    reader_to_workers: &mut ReaderToWorkers<P>,
    to_executors: &mut ToExecutors<P>,
    to_execution_logger: &mut Option<ExecutionInfoSender<P>>,
    time: &RunTime,
) where
    P: Protocol + 'static,
{
    trace!("[server] reader message: {:?}", msg);
    if let Some((from_id, from_shard_id, msg)) = msg {
        handle_from_processes(
            worker_index,
            from_id,
            from_shard_id,
            msg,
            process,
            to_writers,
            reader_to_workers,
            to_executors,
            to_execution_logger,
            time,
        )
        .await
    } else {
        warn!(
            "[server] error while receiving new process message from readers"
        );
    }
}

async fn handle_from_processes<P>(
    worker_index: usize,
    from_id: ProcessId,
    from_shard_id: ShardId,
    msg: P::Message,
    process: &mut P,
    to_writers: &mut HashMap<ProcessId, Vec<WriterSender<P>>>,
    reader_to_workers: &mut ReaderToWorkers<P>,
    to_executors: &mut ToExecutors<P>,
    to_execution_logger: &mut Option<ExecutionInfoSender<P>>,
    time: &RunTime,
) where
    P: Protocol + 'static,
{
    // handle message in process and potentially new actions
    process.handle(from_id, from_shard_id, msg, time);
    send_to_processes_and_executors(
        worker_index,
        process,
        to_writers,
        reader_to_workers,
        to_executors,
        to_execution_logger,
        time,
    )
    .await;
}

// TODO maybe run in parallel
async fn send_to_processes_and_executors<P>(
    worker_index: usize,
    process: &mut P,
    to_writers: &mut HashMap<ProcessId, Vec<WriterSender<P>>>,
    reader_to_workers: &mut ReaderToWorkers<P>,
    to_executors: &mut ToExecutors<P>,
    to_execution_logger: &mut Option<ExecutionInfoSender<P>>,
    time: &RunTime,
) where
    P: Protocol + 'static,
{
    while let Some(action) = process.to_processes() {
        match action {
            Action::ToSend { target, msg } => {
                // check if should handle message locally
                if target.contains(&process.id()) {
                    // handle msg locally if self in `target`
                    handle_message_from_self::<P>(
                        worker_index,
                        msg.clone(),
                        process,
                        reader_to_workers,
                        time,
                    )
                    .await;
                }

                // prevent unnecessary cloning of messages, since send only
                // requires a reference to the message
                let msg_to_send = Arc::new(POEMessage::Protocol(msg));

                // send message to writers in target
                for (to, channels) in to_writers.iter_mut() {
                    if target.contains(to) {
                        send_to_one_writer::<P>(
                            "server",
                            msg_to_send.clone(),
                            channels,
                        )
                        .await
                    }
                }
            }
            Action::ToForward { msg } => {
                // handle msg locally if self in `target`
                handle_message_from_self(
                    worker_index,
                    msg,
                    process,
                    reader_to_workers,
                    time,
                )
                .await;
            }
        }
    }

    // notify executors
    for execution_info in process.to_executors_iter() {
        // if there's an execution logger, then also send execution info to it
        if let Some(to_execution_logger) = to_execution_logger {
            if let Err(e) =
                to_execution_logger.send(execution_info.clone()).await
            {
                warn!("[server] error while sending new execution info to execution logger: {:?}", e);
            }
        }
        // notify executor
        if let Err(e) = to_executors.forward(execution_info).await {
            warn!(
                "[server] error while sending new execution info to executor: {:?}",
                e
            );
        }
    }
}

async fn handle_message_from_self<P>(
    worker_index: usize,
    msg: P::Message,
    process: &mut P,
    reader_to_workers: &mut ReaderToWorkers<P>,
    time: &RunTime,
) where
    P: Protocol + 'static,
{
    // create msg to be forwarded
    let to_forward = (process.id(), process.shard_id(), msg);
    // only handle message from self in this worker if the destination worker is
    // us; this means that "messages to self are delivered immediately" is only
    // true for self messages to the same worker
    if reader_to_workers.only_to_self(&to_forward, worker_index) {
        process.handle(to_forward.0, to_forward.1, to_forward.2, time)
    } else {
        if let Err(e) = reader_to_workers.forward(to_forward).await {
            warn!("[server] error notifying process task with msg from self: {:?}", e);
        }
    }
}

pub async fn send_to_one_writer<P>(
    tag: &'static str,
    msg: Arc<POEMessage<P>>,
    writers: &mut Vec<WriterSender<P>>,
) where
    P: Protocol + 'static,
{
    // pick a random one
    let writer_index = rand::thread_rng().gen_range(0, writers.len());

    if let Err(e) = writers[writer_index].send(msg).await {
        warn!(
            "[{}] error while sending to writer {}: {:?}",
            tag, writer_index, e
        );
    }
}

async fn selected_from_clients<P>(
    worker_index: usize,
    cmd: Option<(Option<Dot>, Command)>,
    process: &mut P,
    to_writers: &mut HashMap<ProcessId, Vec<WriterSender<P>>>,
    reader_to_workers: &mut ReaderToWorkers<P>,
    to_executors: &mut ToExecutors<P>,
    to_execution_logger: &mut Option<ExecutionInfoSender<P>>,
    time: &RunTime,
) where
    P: Protocol + 'static,
{
    trace!("[server] from clients: {:?}", cmd);
    if let Some((dot, cmd)) = cmd {
        handle_from_clients(
            worker_index,
            dot,
            cmd,
            process,
            to_writers,
            reader_to_workers,
            to_executors,
            to_execution_logger,
            time,
        )
        .await
    } else {
        warn!("[server] error while receiving new command from clients");
    }
}

async fn handle_from_clients<P>(
    worker_index: usize,
    dot: Option<Dot>,
    cmd: Command,
    process: &mut P,
    to_writers: &mut HashMap<ProcessId, Vec<WriterSender<P>>>,
    reader_to_workers: &mut ReaderToWorkers<P>,
    to_executors: &mut ToExecutors<P>,
    to_execution_logger: &mut Option<ExecutionInfoSender<P>>,
    time: &RunTime,
) where
    P: Protocol + 'static,
{
    // submit command in process
    process.submit(dot, cmd, time);
    send_to_processes_and_executors(
        worker_index,
        process,
        to_writers,
        reader_to_workers,
        to_executors,
        to_execution_logger,
        time,
    )
    .await;
}

async fn selected_from_periodic_task<P, R>(
    worker_index: usize,
    event: Option<FromPeriodicMessage<P, R>>,
    process: &mut P,
    to_writers: &mut HashMap<ProcessId, Vec<WriterSender<P>>>,
    reader_to_workers: &mut ReaderToWorkers<P>,
    to_executors: &mut ToExecutors<P>,
    to_execution_logger: &mut Option<ExecutionInfoSender<P>>,
    time: &RunTime,
) where
    P: Protocol + 'static,
    R: Debug + 'static,
{
    trace!("[server] from periodic task: {:?}", event);
    if let Some(event) = event {
        handle_from_periodic_task(
            worker_index,
            event,
            process,
            to_writers,
            reader_to_workers,
            to_executors,
            to_execution_logger,
            time,
        )
        .await
    } else {
        warn!("[server] error while receiving new event from periodic task");
    }
}

async fn handle_from_periodic_task<P, R>(
    worker_index: usize,
    msg: FromPeriodicMessage<P, R>,
    process: &mut P,
    to_writers: &mut HashMap<ProcessId, Vec<WriterSender<P>>>,
    reader_to_workers: &mut ReaderToWorkers<P>,
    to_executors: &mut ToExecutors<P>,
    to_execution_logger: &mut Option<ExecutionInfoSender<P>>,
    time: &RunTime,
) where
    P: Protocol + 'static,
    R: Debug + 'static,
{
    match msg {
        FromPeriodicMessage::Event(event) => {
            // handle event in process
            process.handle_event(event, time);
            send_to_processes_and_executors(
                worker_index,
                process,
                to_writers,
                reader_to_workers,
                to_executors,
                to_execution_logger,
                time,
            )
            .await;
        }
        FromPeriodicMessage::Inspect(f, mut tx) => {
            let outcome = f(&process);
            if let Err(e) = tx.send(outcome).await {
                warn!("[server] error while sending inspect result: {:?}", e);
            }
        }
    }
}

async fn selected_from_executors<P>(
    worker_index: usize,
    executed: Option<Executed>,
    process: &mut P,
    to_writers: &mut HashMap<ProcessId, Vec<WriterSender<P>>>,
    reader_to_workers: &mut ReaderToWorkers<P>,
    to_executors: &mut ToExecutors<P>,
    to_execution_logger: &mut Option<ExecutionInfoSender<P>>,
    time: &RunTime,
) where
    P: Protocol + 'static,
{
    trace!("[server] from executors: {:?}", executed);
    if let Some(executed) = executed {
        handle_from_executors(
            worker_index,
            executed,
            process,
            to_writers,
            reader_to_workers,
            to_executors,
            to_execution_logger,
            time,
        )
        .await
    } else {
        warn!("[server] error while receiving message from executors");
    }
}

async fn handle_from_executors<P>(
    worker_index: usize,
    executed: Executed,
    process: &mut P,
    to_writers: &mut HashMap<ProcessId, Vec<WriterSender<P>>>,
    reader_to_workers: &mut ReaderToWorkers<P>,
    to_executors: &mut ToExecutors<P>,
    to_execution_logger: &mut Option<ExecutionInfoSender<P>>,
    time: &RunTime,
) where
    P: Protocol + 'static,
{
    process.handle_executed(executed, time);
    send_to_processes_and_executors(
        worker_index,
        process,
        to_writers,
        reader_to_workers,
        to_executors,
        to_execution_logger,
        time,
    )
    .await;
}
