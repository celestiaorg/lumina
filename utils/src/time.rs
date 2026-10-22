pub use self::imp::{
    Elapsed, Instant, Interval, Sleep, SystemTime, SystemTimeError, Timeout, sleep, timeout,
};

#[cfg(not(target_arch = "wasm32"))]
mod imp {
    use std::task::{Context, Poll};
    use std::time::Duration;

    pub use std::time::{Instant, SystemTime, SystemTimeError};
    pub use tokio::time::error::Elapsed;
    pub use tokio::time::{Sleep, Timeout, sleep, timeout};

    /// Type allowing to wait on a sequence of instants with a certain duration between each instant.
    pub struct Interval(tokio::time::Interval);

    impl Interval {
        /// Create a new `Interval` with provided duration between firings
        pub fn new(dur: Duration) -> Self {
            let mut inner = tokio::time::interval(dur);

            // In Tokio the first tick returns immediately, in order to
            // create an identical cross-platform behavior the first tick
            // needs to be after the specified duration.
            //
            // We have two ways of doing this: `tick` once, which requires
            // `.await`, or call `reset`.
            inner.reset();

            Interval(inner)
        }

        /// Completes when the next instant in the interval has been reached.
        pub async fn tick(&mut self) {
            self.0.tick().await;
        }

        /// Polls for the next instant in the interval to be reached.
        pub fn poll_tick(&mut self, cx: &mut Context) -> Poll<()> {
            self.0.poll_tick(cx).map(drop)
        }
    }
}

#[cfg(target_arch = "wasm32")]
mod imp {
    use std::future::Future;
    use std::pin::Pin;
    use std::task::{Context, Poll};
    use std::time::Duration;

    use futures::StreamExt;
    use gloo_timers::future::{IntervalStream, TimeoutFuture};
    use pin_project::pin_project;
    use send_wrapper::SendWrapper;

    pub use web_time::{Instant, SystemTime, SystemTimeError};

    /// Type allowing to wait on a sequence of instants with a certain duration between each instant.
    pub struct Interval(SendWrapper<IntervalStream>);

    impl Interval {
        /// Create a new `Interval` with provided duration between firings
        pub fn new(dur: Duration) -> Self {
            // If duration was less than a millisecond, then make
            // it 1 millisecond.
            let millis = u32::try_from(dur.as_millis().max(1)).unwrap_or(u32::MAX);

            Interval(SendWrapper::new(IntervalStream::new(millis)))
        }

        /// Completes when the next instant in the interval has been reached.
        pub async fn tick(&mut self) {
            self.0.next().await;
        }

        /// Polls for the next instant in the interval to be reached.
        pub fn poll_tick(&mut self, cx: &mut Context) -> Poll<()> {
            self.0.poll_next_unpin(cx).map(drop)
        }
    }

    /// Future returned by `sleep`.
    #[pin_project]
    #[must_use = "futures do nothing unless you `.await` or poll them"]
    #[derive(Debug)]
    pub struct Sleep {
        #[pin]
        deadline: Instant,
        #[pin]
        fut: Option<SendWrapper<TimeoutFuture>>,
    }

    impl Future for Sleep {
        type Output = ();

        fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
            let mut this = self.project();

            loop {
                if let Some(fut) = this.fut.as_mut().as_pin_mut()
                    && fut.poll(cx).is_pending()
                {
                    return Poll::Pending;
                }

                let now = Instant::now();

                if now >= *this.deadline {
                    this.fut.take();
                    return Poll::Ready(());
                }

                // `Sleep` guarantees that it will sleep **at least** an X amount of time.
                // When we convert a duration to milliseconds there is a possibility to
                // saturate some sub-milliseconds amount. Therefore, to satisfy the sleep
                // guarantees, we add 1 millisecond more.
                let remaining_millis = this.deadline.duration_since(now).as_millis() + 1;

                // JavaScript's `set_timeout` can not take a delay bigger than `i32::MAX`.
                let delay_millis = remaining_millis.min(i32::MAX as u128);
                *this.fut = Some(SendWrapper::new(TimeoutFuture::new(delay_millis as u32)));

                // Loop again to register the new future.
            }
        }
    }

    /// Waits until `duration` has elapsed.
    pub fn sleep(duration: Duration) -> Sleep {
        Sleep {
            deadline: Instant::now() + duration,
            fut: None,
        }
    }

    /// This error is returned when a timeout expires before the function was able to finish.
    #[derive(Debug)]
    pub struct Elapsed;

    /// Future returned by `timeout`.
    #[pin_project]
    #[must_use = "futures do nothing unless you `.await` or poll them"]
    #[derive(Debug)]
    pub struct Timeout<T> {
        #[pin]
        value: T,
        #[pin]
        timeout: Sleep,
    }

    impl<T> Future for Timeout<T>
    where
        T: Future,
    {
        type Output = Result<T::Output, Elapsed>;

        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            let me = self.project();

            if let Poll::Ready(v) = me.value.poll(cx) {
                return Poll::Ready(Ok(v));
            }

            match me.timeout.poll(cx) {
                Poll::Ready(()) => Poll::Ready(Err(Elapsed)),
                Poll::Pending => Poll::Pending,
            }
        }
    }

    /// Requires a `Future` to complete before the specified duration has elapsed.
    pub fn timeout<F>(duration: Duration, future: F) -> Timeout<F>
    where
        F: Future,
    {
        Timeout {
            value: future,
            timeout: sleep(duration),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::async_test;
    use std::time::Duration;

    #[async_test]
    async fn interval() {
        let now = Instant::now();
        let mut interval = Interval::new(Duration::from_millis(100));

        interval.tick().await;
        let elapsed = now.elapsed();
        assert!(elapsed > Duration::from_millis(100) && elapsed < Duration::from_millis(105));

        interval.tick().await;
        let elapsed = now.elapsed();
        assert!(elapsed > Duration::from_millis(200) && elapsed < Duration::from_millis(205));
    }
}
