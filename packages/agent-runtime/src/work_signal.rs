use std::{sync::Arc, time::Duration};
use tokio::sync::watch;

/// Generation-based wakeup hints. Durable database scans remain authoritative.
#[derive(Clone, Debug)]
pub struct WorkSignal {
    generation: Arc<watch::Sender<u64>>,
}

impl Default for WorkSignal {
    fn default() -> Self {
        let (generation, _) = watch::channel(0);
        Self {
            generation: Arc::new(generation),
        }
    }
}

impl WorkSignal {
    pub fn snapshot(&self) -> u64 {
        *self.generation.borrow()
    }

    pub fn notify(&self) {
        self.generation
            .send_modify(|value| *value = value.wrapping_add(1));
    }

    pub async fn wait(&self, observed: u64, fallback: Duration) {
        let mut receiver = self.generation.subscribe();
        if *receiver.borrow() != observed {
            return;
        }
        let _ = tokio::time::timeout(fallback, receiver.changed()).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn notification_before_wait_is_not_lost() {
        let signal = WorkSignal::default();
        let observed = signal.snapshot();
        signal.notify();
        tokio::time::timeout(
            Duration::from_millis(20),
            signal.wait(observed, Duration::from_secs(1)),
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn active_wait_wakes_immediately() {
        let signal = WorkSignal::default();
        let observed = signal.snapshot();
        let waiting = tokio::spawn({
            let signal = signal.clone();
            async move { signal.wait(observed, Duration::from_secs(1)).await }
        });
        tokio::task::yield_now().await;
        signal.notify();
        tokio::time::timeout(Duration::from_millis(20), waiting)
            .await
            .unwrap()
            .unwrap();
    }
}
