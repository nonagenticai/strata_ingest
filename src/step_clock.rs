//! A step's run clock: the per-kind deadline (`limits::step_deadline`) counts only the time the
//! step actually works, not the time it waits for a shared slot — a provider permit or rpm slot
//! ([`crate::providers::Limited::acquire`]), the pod's Convert slot, a storage upload slot.
//! A wait is bounded separately (`limits::SLOT_WAIT_MAX_S`, final review I2 / rulings R35): a
//! step queued behind three 150 s OCR calls on a one-permit GPU provider is served instead of
//! dying `step_timeout` before its own call starts.
//!
//! The worker runs each step inside [`StepClock::scope`]; code that waits for a slot wraps the
//! wait in [`waiting`]. Outside a scope (unit tests calling a step directly) [`waiting`] is a
//! plain await.

use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::watch;
use tokio::time::Instant;

tokio::task_local! {
    static CLOCK: StepClock;
}

/// Shared by the worker (which watches it) and the step task (which pauses it).
#[derive(Clone, Debug)]
pub struct StepClock {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    state: Mutex<State>,
    /// Bumped on every pause/resume so the watching worker re-evaluates its wake-up time.
    changed: watch::Sender<u64>,
}

#[derive(Debug, Default)]
struct State {
    /// Nested waits (a wait inside a wait) pause once.
    depth: u32,
    paused_since: Option<Instant>,
    paused_total: Duration,
}

/// What the watching worker should do now.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    /// Check again at `Instant` (or when the clock changes).
    WakeAt(Instant),
    /// The step worked longer than its deadline (waits excluded).
    DeadlineExceeded { worked: Duration },
    /// One wait for a slot lasted longer than the wait bound.
    WaitExceeded { waited: Duration },
}

impl Default for StepClock {
    fn default() -> Self {
        Self::new()
    }
}

impl StepClock {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Inner {
                state: Mutex::new(State::default()),
                changed: watch::channel(0).0,
            }),
        }
    }

    /// Runs `f` with this clock as the current step's clock.
    pub async fn scope<F: Future>(self, f: F) -> F::Output {
        CLOCK.scope(self, f).await
    }

    /// Notified on every pause/resume.
    pub fn subscribe(&self) -> watch::Receiver<u64> {
        self.inner.changed.subscribe()
    }

    /// Total time spent waiting so far (the current wait included).
    pub fn waited(&self, now: Instant) -> Duration {
        let s = self.lock();
        s.paused_total + s.paused_since.map_or(Duration::ZERO, |p| now - p)
    }

    /// The verdict for a step that started at `start`, may work `deadline` and wait at most
    /// `wait_max` per wait.
    pub fn verdict(
        &self,
        start: Instant,
        deadline: Duration,
        wait_max: Duration,
        now: Instant,
    ) -> Verdict {
        let s = self.lock();
        if let Some(since) = s.paused_since {
            let waited = now - since;
            if waited >= wait_max {
                return Verdict::WaitExceeded { waited };
            }
            // The deadline does not run while waiting.
            return Verdict::WakeAt(since + wait_max);
        }
        let worked = (now - start).saturating_sub(s.paused_total);
        if worked >= deadline {
            return Verdict::DeadlineExceeded { worked };
        }
        Verdict::WakeAt(now + (deadline - worked))
    }

    fn pause(&self) {
        let mut s = self.lock();
        s.depth += 1;
        if s.depth == 1 {
            s.paused_since = Some(Instant::now());
        }
        drop(s);
        self.inner.changed.send_modify(|n| *n = n.wrapping_add(1));
    }

    fn resume(&self) {
        let mut s = self.lock();
        s.depth = s.depth.saturating_sub(1);
        if s.depth == 0
            && let Some(since) = s.paused_since.take()
        {
            s.paused_total += since.elapsed();
        }
        drop(s);
        self.inner.changed.send_modify(|n| *n = n.wrapping_add(1));
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        // Plain timestamps: a panic elsewhere cannot leave them inconsistent.
        self.inner.state.lock().unwrap_or_else(|p| p.into_inner())
    }
}

/// Resumes the clock when dropped, so a cancelled wait does not leave it paused.
struct Resume(StepClock);

impl Drop for Resume {
    fn drop(&mut self) {
        self.0.resume();
    }
}

/// Awaits `wait` (acquiring a shared slot) with the current step's deadline paused.
pub async fn waiting<F: Future>(wait: F) -> F::Output {
    let _resume = CLOCK.try_with(|c| c.clone()).ok().map(|c| {
        c.pause();
        Resume(c)
    });
    wait.await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn waits_do_not_count_against_the_deadline() {
        let clock = StepClock::new();
        let start = Instant::now();
        let (dl, max) = (Duration::from_secs(10), Duration::from_secs(100));
        let c = clock.clone();
        clock
            .clone()
            .scope(async move {
                tokio::time::sleep(Duration::from_secs(4)).await;
                waiting(tokio::time::sleep(Duration::from_secs(50))).await;
                let now = Instant::now();
                assert_eq!(
                    c.verdict(start, dl, max, now),
                    Verdict::WakeAt(now + Duration::from_secs(6)),
                    "4 s worked, 50 s waited"
                );
                assert_eq!(c.waited(now), Duration::from_secs(50));
                tokio::time::sleep(Duration::from_secs(6)).await;
                assert!(matches!(
                    c.verdict(start, dl, max, Instant::now()),
                    Verdict::DeadlineExceeded { .. }
                ));
            })
            .await;
    }

    #[tokio::test(start_paused = true)]
    async fn a_wait_is_bounded_on_its_own() {
        let clock = StepClock::new();
        let start = Instant::now();
        let c = clock.clone();
        let (dl, max) = (Duration::from_secs(10), Duration::from_secs(30));
        let probe = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(20)).await;
            let v1 = c.verdict(start, dl, max, Instant::now());
            tokio::time::sleep(Duration::from_secs(15)).await;
            (v1, c.verdict(start, dl, max, Instant::now()))
        });
        clock
            .scope(waiting(tokio::time::sleep(Duration::from_secs(60))))
            .await;
        let (v1, v2) = probe.await.unwrap();
        assert_eq!(
            v1,
            Verdict::WakeAt(start + max),
            "past the deadline, inside the wait"
        );
        assert!(matches!(v2, Verdict::WaitExceeded { .. }), "{v2:?}");
    }

    #[tokio::test]
    async fn waiting_outside_a_scope_is_a_plain_await() {
        assert_eq!(waiting(async { 7 }).await, 7);
    }
}
