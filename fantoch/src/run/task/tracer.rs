use tokio::time::Duration;

#[cfg(not(feature = "prof"))]
pub async fn tracer_task(interval: Option<Duration>) {
    match interval {
        Some(_) => {
            panic!("[tracer_task] tracer show interval was set but the 'prof' feature is disabled");
        }
        None => {
            println!("[tracer_task] disabled since the 'prof' feature is not enabled");
        }
    }
}

#[cfg(feature = "prof")]
pub async fn tracer_task(interval: Option<Duration>) {
    use crate::log;
    use fantoch_prof::ProfSubscriber;

    // if no interval, do not trace
    if interval.is_none() {
        println!("[tracer_task] tracer show interval was not set even though the 'prof' feature is enabled");
        return;
    }

    // set tracing subscriber
    let subscriber = ProfSubscriber::new();
    tracing::subscriber::set_global_default(subscriber.clone()).unwrap_or_else(
        |e| println!("tracing global default subscriber already set: {:?}", e),
    );

    // create tokio interval
    let interval = interval.unwrap();
    log!("[tracer_task] interval {:?}", interval);
    let mut interval = tokio::time::interval(interval);

    loop {
        // wait tick
        let _ = interval.tick().await;
        // show metrics
        println!("{:?}", subscriber);
    }
}
