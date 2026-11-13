//! The worker pool: `n` tasks, each claiming one step at a time from the Postgres queue.
//!
//! Per step: claim → heartbeat ticker (every `HEARTBEAT_S`) → run the step in its own task,
//! bounded by its deadline (`limits::step_deadline`; past it: Retryable `step_timeout`) →
//! `complete_and_enqueue` or `fail`. A step whose heartbeat reports `LeaseLost` is aborted and its
//! result dropped (another worker owns it now; rulings R12). A panicking step does not kill the
//! worker: it is recorded as a `Retryable` failure (`worker_panic`). The deadline counts work only:
//! waits for a provider/convert/upload slot are excluded and bounded on their own
//! (`step_clock`, rulings R35). On shutdown a worker stops claiming, gives the step it holds
//! `shutdown_grace` to finish, then aborts it and releases it (`steps::release`: pending, the run
//! refunded up to `MAX_RELEASE_REFUNDS` times, `attempt` never decremented) so another pod picks
//! it up at once (rulings R39). A run abandoned with `slot_wait_timeout` is re-queued without
//! using an attempt (`steps::requeue_slot_wait`).

use std::sync::Arc;
use std::time::Duration;

use futures::future::BoxFuture;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

use crate::db::steps::{self, ClaimedStep};
use crate::error::{IngestError, StepError};
use crate::limits::{HEARTBEAT_S, SLOT_WAIT_MAX_S, step_deadline};
use crate::pipeline::{Ctx, StepResult, run_step};
use crate::providers::ErrorClass;
use crate::step_clock::{StepClock, Verdict};
use tokio::time::Instant;

/// A scratch dir older than this at worker start is stale (no step runs past 15 min).
pub const SCRATCH_MAX_AGE: Duration = Duration::from_secs(3600);

/// On shutdown, the time a running step still gets to finish before it is aborted and released.
/// Well inside the API's worker drain (`api::DRAIN_TIMEOUT`, 30 s) and the pod's
/// `terminationGracePeriodSeconds` (120 s).
pub const SHUTDOWN_GRACE: Duration = Duration::from_secs(10);

/// How long an idle worker waits before polling the queue again.
pub const IDLE_POLL: Duration = Duration::from_millis(500);

/// Worker timing. Production uses the defaults; tests shorten them.
#[derive(Debug, Clone, Copy)]
pub struct WorkerOpts {
    pub poll: Duration,
    pub heartbeat: Duration,
    /// Work ceiling for every step; `None` = per kind, [`step_deadline`]. Time spent waiting
    /// for a shared slot is not counted ([`crate::step_clock`]).
    pub deadline: Option<Duration>,
    /// Longest single wait for a shared slot ([`SLOT_WAIT_MAX_S`]).
    pub slot_wait_max: Duration,
    /// On shutdown, how long a running step may still finish before it is aborted and
    /// released (`steps::release`) for another pod.
    pub shutdown_grace: Duration,
}

impl Default for WorkerOpts {
    fn default() -> Self {
        Self {
            poll: IDLE_POLL,
            heartbeat: Duration::from_secs(HEARTBEAT_S),
            deadline: None,
            slot_wait_max: Duration::from_secs(SLOT_WAIT_MAX_S),
            shutdown_grace: SHUTDOWN_GRACE,
        }
    }
}

/// Runs one claimed step. Production uses [`run_step`]; tests inject their own.
pub type StepRunner =
    Arc<dyn Fn(Arc<Ctx>, ClaimedStep) -> BoxFuture<'static, StepResult> + Send + Sync>;

/// The production runner: [`run_step`].
pub fn default_runner() -> StepRunner {
    Arc::new(|ctx: Arc<Ctx>, step: ClaimedStep| {
        Box::pin(async move { run_step(&ctx, &step).await })
    })
}

/// Runs `n` workers until `shutdown` is cancelled and every worker has finished its current step.
pub async fn run_workers(ctx: Arc<Ctx>, n: usize, shutdown: CancellationToken) {
    run_workers_with(ctx, n, shutdown, WorkerOpts::default(), default_runner()).await;
}

/// [`run_workers`] with explicit timing and step runner.
pub async fn run_workers_with(
    ctx: Arc<Ctx>,
    n: usize,
    shutdown: CancellationToken,
    opts: WorkerOpts,
    runner: StepRunner,
) {
    // Scratch dirs a killed worker left behind (audio Convert); a clean exit removes its own.
    let swept = tokio::task::spawn_blocking(|| {
        crate::pipeline::audio::sweep_scratch_dirs(&std::env::temp_dir(), SCRATCH_MAX_AGE)
    })
    .await
    .unwrap_or(0);
    if swept > 0 {
        tracing::info!(swept, "removed stale scratch dirs");
    }
    let mut set = JoinSet::new();
    for worker in 0..n.max(1) {
        set.spawn(worker_loop(
            ctx.clone(),
            worker,
            shutdown.clone(),
            opts,
            runner.clone(),
        ));
    }
    while let Some(r) = set.join_next().await {
        if let Err(e) = r {
            tracing::error!(error = %e, "worker task ended abnormally");
        }
    }
}

/// How often the delete sweeper ([`run_delete_sweeper`]) looks for stale `deleting` jobs.
pub const DELETE_SWEEP_INTERVAL: Duration = Duration::from_secs(60);

/// Resumes deletes nobody is working on any more: at start, then every `interval`, until
/// `shutdown`. A job `deleting` for longer than `stale_after` (`jobs::DELETING_STALE_AFTER` in
/// production; a live delete refreshes `deleting_since` every `jobs::DELETE_HEARTBEAT`) is
/// finished by [`jobs::sweep_deleting`]: the pod died mid-delete, the request's delete task was
/// lost, or its blob deletes failed (then retried here until storage_service is back). Safe with
/// several pods (each job is claimed with `FOR UPDATE SKIP LOCKED`).
///
/// [`jobs::sweep_deleting`]: crate::db::jobs::sweep_deleting
pub async fn run_delete_sweeper(
    ctx: Arc<Ctx>,
    interval: Duration,
    stale_after: Duration,
    shutdown: CancellationToken,
) {
    loop {
        match crate::db::jobs::sweep_deleting(&ctx.pool, &ctx.storage, stale_after).await {
            Ok(0) => {}
            Ok(n) => tracing::info!(jobs = n, "delete sweeper finished stale deletes"),
            Err(e) => tracing::warn!(error = %e, "delete sweeper pass failed"),
        }
        tokio::select! {
            _ = shutdown.cancelled() => return,
            _ = tokio::time::sleep(interval) => {}
        }
    }
}

async fn worker_loop(
    ctx: Arc<Ctx>,
    worker: usize,
    shutdown: CancellationToken,
    opts: WorkerOpts,
    runner: StepRunner,
) {
    tracing::debug!(worker, "worker started");
    while !shutdown.is_cancelled() {
        match steps::claim_next(&ctx.pool, ctx.cfg.limits.max_attempts).await {
            Ok(Some(step)) => {
                let span = tracing::info_span!(
                    "step",
                    worker,
                    id = step.id,
                    job = %step.job_id,
                    kind = step.kind.as_str(),
                    key = %step.key,
                    attempt = step.attempt
                );
                process(&ctx, step, opts, &runner, &shutdown)
                    .instrument(span)
                    .await;
            }
            Ok(None) => idle(&shutdown, opts.poll).await,
            Err(e) => {
                tracing::warn!(worker, error = %e, "claiming a step failed");
                idle(&shutdown, opts.poll).await;
            }
        }
    }
    tracing::debug!(worker, "worker stopped");
}

async fn idle(shutdown: &CancellationToken, poll: Duration) {
    tokio::select! {
        _ = shutdown.cancelled() => {}
        _ = tokio::time::sleep(poll) => {}
    }
}

/// How a step's run ended, as the supervising loop in [`process`] saw it.
enum Outcome {
    Done(Result<StepResult, tokio::task::JoinError>),
    TimedOut(StepError),
    /// One wait for a shared slot exceeded `slot_wait_max`; the step never got to work.
    SlotWait(StepError),
    LeaseLost,
    ShutDown,
}

async fn process(
    ctx: &Arc<Ctx>,
    step: ClaimedStep,
    opts: WorkerOpts,
    runner: &StepRunner,
    shutdown: &CancellationToken,
) {
    let lost = CancellationToken::new();
    let ticker = tokio::spawn(heartbeat_ticker(
        ctx.clone(),
        step.clone(),
        opts.heartbeat,
        lost.clone(),
    ));
    let deadline = opts.deadline.unwrap_or_else(|| step_deadline(step.kind));
    let clock = StepClock::new();
    let mut changed = clock.subscribe();
    let run = clock.clone().scope(runner(ctx.clone(), step.clone()));
    let kind = step.kind;
    let mut work = tokio::spawn(run.in_current_span());
    let start = Instant::now();
    // Once shutdown starts, the step gets `shutdown_grace` to finish; then it is released.
    let mut shutdown_at: Option<Instant> = None;
    let outcome = loop {
        let mut wake = match clock.verdict(start, deadline, opts.slot_wait_max, Instant::now()) {
            Verdict::WakeAt(t) => t,
            Verdict::DeadlineExceeded { worked } => {
                let waited = clock.waited(Instant::now());
                break Outcome::TimedOut(StepError {
                    class: ErrorClass::Retryable,
                    code: "step_timeout".into(),
                    message: format!(
                        "step {} did not finish within {} s of work ({:.0} s worked, {:.0} s \
                         waiting for a slot not counted)",
                        kind.as_str(),
                        deadline.as_secs_f64(),
                        worked.as_secs_f64(),
                        waited.as_secs_f64()
                    ),
                    provider: None,
                    retry_after_s: None,
                });
            }
            Verdict::WaitExceeded { waited } => {
                break Outcome::SlotWait(StepError {
                    class: ErrorClass::Retryable,
                    code: "slot_wait_timeout".into(),
                    message: format!(
                        "step {} waited {:.0} s for a provider/convert/upload slot (limit {} s)",
                        kind.as_str(),
                        waited.as_secs_f64(),
                        opts.slot_wait_max.as_secs()
                    ),
                    provider: None,
                    retry_after_s: None,
                });
            }
        };
        if let Some(at) = shutdown_at {
            let release_at = at + opts.shutdown_grace;
            if Instant::now() >= release_at {
                break Outcome::ShutDown;
            }
            wake = wake.min(release_at);
        }
        tokio::select! {
            r = &mut work => break Outcome::Done(r),
            _ = lost.cancelled() => break Outcome::LeaseLost,
            _ = shutdown.cancelled(), if shutdown_at.is_none() => {
                shutdown_at = Some(Instant::now());
            }
            _ = tokio::time::sleep_until(wake) => {}
            _ = changed.changed() => {}
        }
    };
    if !matches!(outcome, Outcome::Done(_)) {
        // Make sure the run is gone (no write of it can land) before recording anything.
        work.abort();
        let _ = (&mut work).await;
    }
    ticker.abort();
    // A paid provider call of this run whose record failed (`metering::Unrecorded`): written with
    // the result when the step completes, else given one more try (and logged if it is lost).
    let mut carried = ctx.unrecorded.take(&step);
    if !matches!(outcome, Outcome::Done(Ok(Ok(_))))
        && let Some(m) = carried.take()
    {
        crate::metering::record_or_log_lost(&ctx.pool, &step, &m, "the step did not complete")
            .await;
    }
    let result = match outcome {
        Outcome::LeaseLost => {
            tracing::info!(
                step = step.id,
                attempt = step.attempt,
                "lease lost; step aborted and its result dropped"
            );
            return;
        }
        Outcome::ShutDown => {
            match steps::release(&ctx.pool, &step, ctx.cfg.limits.max_attempts).await {
                Ok(()) => tracing::info!(
                    step = step.id,
                    kind = step.kind.as_str(),
                    "shutting down: step aborted and released for another worker"
                ),
                Err(IngestError::LeaseLost { .. }) => {}
                // The lease expires and the reaper re-queues the step.
                Err(e) => {
                    tracing::warn!(step = step.id, error = %e, "releasing the step failed")
                }
            }
            return;
        }
        Outcome::SlotWait(e) => {
            match steps::requeue_slot_wait(&ctx.pool, &step, &e, ctx.cfg.limits.max_attempts).await
            {
                Ok(()) => {}
                Err(IngestError::LeaseLost { .. }) => {}
                Err(e) => {
                    tracing::warn!(step = step.id, error = %e, "re-queueing the step failed")
                }
            }
            return;
        }
        Outcome::TimedOut(e) => Err(e),
        Outcome::Done(Ok(r)) => r,
        Outcome::Done(Err(e)) if e.is_panic() => {
            let msg = panic_message(e.into_panic());
            tracing::error!(step = step.id, kind = step.kind.as_str(), panic = %msg, "step panicked");
            Err(StepError {
                class: ErrorClass::Retryable,
                code: "worker_panic".into(),
                message: format!("step {} panicked: {msg}", step.kind.as_str()),
                provider: None,
                retry_after_s: None,
            })
        }
        Outcome::Done(Err(e)) => Err(StepError {
            class: ErrorClass::Retryable,
            code: "worker_aborted".into(),
            message: format!("step {} task ended: {e}", step.kind.as_str()),
            provider: None,
            retry_after_s: None,
        }),
    };
    let write = match result {
        Ok((mut out, next)) => {
            out.unrecorded = carried.clone();
            let r = steps::complete_and_enqueue(&ctx.pool, &step, out, &next).await;
            if r.is_err()
                && let Some(m) = &carried
            {
                crate::metering::record_or_log_lost(
                    &ctx.pool,
                    &step,
                    m,
                    "the step's result was not written",
                )
                .await;
            }
            r
        }
        Err(err) => steps::fail(&ctx.pool, &step, &err, ctx.cfg.limits.max_attempts).await,
    };
    match write {
        Ok(()) => {}
        Err(IngestError::LeaseLost { step: id, attempt }) => {
            tracing::info!(
                step = id,
                attempt,
                "lease lost before the result was recorded; dropped"
            );
        }
        // The lease expires and the reaper re-queues the step.
        Err(e) => tracing::warn!(step = step.id, error = %e, "recording the step result failed"),
    }
}

/// Extends the lease every `every` until aborted; cancels `lost` on `LeaseLost`. Other errors
/// (e.g. the DB is briefly unreachable) are logged and retried on the next tick.
async fn heartbeat_ticker(
    ctx: Arc<Ctx>,
    step: ClaimedStep,
    every: Duration,
    lost: CancellationToken,
) {
    let mut tick = tokio::time::interval(every);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    tick.tick().await; // the first tick is immediate; the claim just set the lease
    loop {
        tick.tick().await;
        match steps::heartbeat(&ctx.pool, &step).await {
            Ok(()) => {}
            Err(IngestError::LeaseLost { .. }) => {
                lost.cancel();
                return;
            }
            Err(e) => tracing::warn!(step = step.id, error = %e, "heartbeat failed"),
        }
    }
}

fn panic_message(p: Box<dyn std::any::Any + Send>) -> String {
    p.downcast_ref::<&str>()
        .map(|s| s.to_string())
        .or_else(|| p.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "non-string panic payload".to_string())
}
