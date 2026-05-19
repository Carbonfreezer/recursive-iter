//! # recursive-iter
//!
//! A lightweight utility for turning recursive functions into lazy [`Iterator`]s
//! without collecting all results into memory first.
//!
//! Rust does not have stable generator / `yield` syntax. The usual workaround —
//! collecting every result into a [`Vec`] before returning — can be prohibitive when
//! the result set is large (e.g. enumerating all 2^n – 1 moves of the Tower of Hanoi
//! for large n). This crate provides [`generate_iterator`], which runs the recursive
//! function in a background thread and streams results to the caller in fixed-size
//! batches via a bounded channel, keeping peak memory usage at roughly
//! `2 × batch_size` elements regardless of how many elements the function produces.
//!
//! ## Early termination
//!
//! The iterator can be short-circuited with [`Iterator::take`] or any other
//! early-exit combinator. When the consumer drops the iterator before the producer
//! thread has finished, the next full batch triggers a panic inside the worker
//! thread, which terminates it immediately. Because the thread is detached, the
//! panic does not propagate to the caller. This is intentional: without it, the
//! producer would continue an unbounded computation whose results are silently
//! discarded.

use std::mem::take;
use std::sync::mpsc::{SyncSender, sync_channel};

/// Accumulates elements produced by a recursive function and forwards them in
/// fixed-size batches to the consumer via a bounded [`std::sync::mpsc`] channel.
///
/// You do not construct a `Batcher` directly; [`generate_iterator`] creates one
/// and passes a mutable reference to your closure.
///
/// # Backpressure
///
/// The underlying channel has a capacity of one batch. The producer thread
/// therefore blocks as soon as it is one full batch ahead of the consumer,
/// bounding memory usage to approximately `2 × batch_size` live elements.
pub struct Batcher<T> {
    current_batch: Vec<T>,
    sender: SyncSender<Vec<T>>,
    batch_size: usize,
}

impl<T> Batcher<T> {
    /// Creates a new `Batcher` that fills batches of `batch_size` elements and
    /// sends them through `sender`.
    fn new(batch_size: usize, sender: SyncSender<Vec<T>>) -> Batcher<T> {
        Batcher {
            current_batch: Vec::with_capacity(batch_size),
            sender,
            batch_size,
        }
    }

    /// Adds a single element to the current batch.
    ///
    /// Once the batch reaches `batch_size` elements it is moved into the channel
    /// (blocking until the consumer has taken the previous batch) and a fresh
    /// allocation is prepared for the next batch.
    ///
    /// If the consumer has dropped the iterator, the send will panic, terminating
    /// the worker thread immediately. Because the thread is detached, the panic
    /// does not propagate to the caller. This is intentional: it stops the
    /// producer from continuing a potentially unbounded computation whose results
    /// are no longer needed.
    ///
    /// Call this wherever you would write `yield element` in a language that
    /// supports generator syntax.
    pub fn add_element(&mut self, element: T) {
        self.current_batch.push(element);
        if self.current_batch.len() >= self.batch_size {
            let content = take(&mut self.current_batch);
            self.current_batch.reserve(self.batch_size);
            self.sender.send(content).unwrap();
        }
    }
}

/// Flushes any remaining elements when the producer function returns.
///
/// Without this, elements in the last partial batch would be silently discarded.
/// If the consumer has already dropped the iterator, the flush is silently
/// skipped — no panic occurs.
impl<T> Drop for Batcher<T> {
    fn drop(&mut self) {
        if !self.current_batch.is_empty() {
            let _ = self.sender.send(take(&mut self.current_batch));
        }
    }
}

/// Runs `start_function` in a background thread and returns a lazy [`Iterator`]
/// over every element it produces via [`Batcher::add_element`].
///
/// This is the primary entry point of the crate. It acts as a stable-Rust
/// replacement for generator / `yield` syntax for recursive functions that would
/// otherwise need to collect all results into a [`Vec`] before the caller can
/// begin processing them.
///
/// # Parameters
///
/// - `batch_size`: Number of elements per batch. Larger values reduce
///   synchronisation overhead; smaller values improve latency for the first
///   element. A value between 256 and 4096 is a reasonable starting point.
/// - `start_function`: A closure (or function pointer) that receives a
///   `&mut Batcher<T>` and calls [`Batcher::add_element`] for every element it
///   wants to emit. Typically this is a thin wrapper around a recursive function.
///
/// # Type bounds
///
/// `T` must be [`Send`] and `'static` because it is transferred across a thread
/// boundary. The compiler will reject non-`Send` types (e.g. [`std::rc::Rc`])
/// with a clear error pointing to the call site.
///
/// # Example
///
/// ```rust
/// use recursive_iter::{generate_iterator, Batcher};
///
/// fn hanoi(from: u8, to: u8, n: u8, out: &mut Batcher<(u8, u8)>) {
///     let via = 3 - from - to;
///     if n > 1 { hanoi(from, via, n - 1, out); }
///     out.add_element((from, to));
///     if n > 1 { hanoi(via, to, n - 1, out); }
/// }
///
/// // Iterate over all 2^20 - 1 moves without allocating a Vec of that size.
/// let moves = generate_iterator(1000, |b| hanoi(0, 2, 20, b));
/// let first_ten: Vec<_> = moves.take(10).collect();
/// ```
pub fn generate_iterator<T: Send + 'static>(
    batch_size: usize,
    start_function: impl FnOnce(&mut Batcher<T>) + Send + 'static,
) -> impl Iterator<Item = T> {
    let (tx, rx) = sync_channel(1);
    let mut batcher = Batcher::<T>::new(batch_size, tx);
    std::thread::spawn(move || start_function(&mut batcher));
    rx.into_iter().flatten()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hanoi_solver(start_tower: u8, end_tower: u8, slices: u8, batcher: &mut Batcher<(u8, u8)>) {
        let between = 3 - start_tower - end_tower;
        if slices > 1 {
            hanoi_solver(start_tower, between, slices - 1, batcher);
        }
        batcher.add_element((start_tower, end_tower));
        if slices > 1 {
            hanoi_solver(between, end_tower, slices - 1, batcher);
        }
    }

    fn flat_hanoi_solver(start_tower: u8, end_tower: u8, slices: u8, solution: &mut Vec<(u8, u8)>) {
        let between = 3 - start_tower - end_tower;
        if slices > 1 {
            flat_hanoi_solver(start_tower, between, slices - 1, solution);
        }
        solution.push((start_tower, end_tower));
        if slices > 1 {
            flat_hanoi_solver(between, end_tower, slices - 1, solution);
        }
    }

    #[test]
    fn test_hanoi_solver() {
        const NUM_SLICES: u8 = 20;
        let mut plain_vec = Vec::with_capacity(2usize.pow(NUM_SLICES as u32) - 1);
        flat_hanoi_solver(0, 2, NUM_SLICES, &mut plain_vec);

        let iter = generate_iterator(1000, |batch| hanoi_solver(0, 2, NUM_SLICES, batch));
        let base_vec: Vec<(u8, u8)> = iter.collect();

        assert_eq!(base_vec, plain_vec, "Solutions are not the same");
    }

    #[test]
    fn test_take_functionality() {
        // Early termination must not panic — the worker thread silently stops
        // when the consumer drops the iterator.
        let iter = generate_iterator(1000, |batch| hanoi_solver(0, 2, 100, batch));
        let _: Vec<(u8, u8)> = iter.take(10).collect();
    }
}
