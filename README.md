# recursive-iter

A lightweight Rust utility for turning recursive functions into lazy iterators —
a stable-Rust stand-in for generator / `yield` syntax.

## Motivation

Rust does not have stable generator syntax. When a recursive function needs to
produce a sequence of values the usual workaround is to collect everything into a
`Vec` before returning:

```rust
fn hanoi(from: u8, to: u8, n: u8, out: &mut Vec<(u8, u8)>) {
    let via = 3 - from - to;
    if n > 1 { hanoi(from, via, n - 1, out); }
    out.push((from, to));
    if n > 1 { hanoi(via, to, n - 1, out); }
}

let mut moves = Vec::new();
hanoi(0, 2, 30, &mut moves); // allocates ~10^9 elements before you see any
```

For large inputs this is either too slow to start or simply runs out of memory.
`recursive-iter` solves this by running the recursive function in a background
thread and streaming its output to the caller in fixed-size batches via a bounded
channel. The caller receives a regular `Iterator` and can begin processing
immediately.

## Usage

Add a `&mut Batcher<T>` parameter to your recursive function, return
`Result<(), ShouldTerminateAsSoonAsPossible>`, and replace every `out.push(x)`
with `out.add_element(x)?`. Then wrap the call in `generate_iterator`:

```rust
use recursive_iter::{generate_iterator, Batcher, ShouldTerminateAsSoonAsPossible};

fn hanoi(
    from: u8, to: u8, n: u8,
    out: &mut Batcher<(u8, u8)>,
) -> Result<(), ShouldTerminateAsSoonAsPossible> {
    let via = 3 - from - to;
    if n > 1 { hanoi(from, via, n - 1, out)?; }
    out.add_element((from, to))?;
    if n > 1 { hanoi(via, to, n - 1, out)?; }
    Ok(())
}

// Lazy: the background thread starts producing moves while you consume them.
let moves = generate_iterator(1000, |b| hanoi(0, 2, 30, b));

for (from, to) in moves {
    println!("Move disk from peg {} to peg {}", from, to);
}
```

### Early termination

Because `generate_iterator` returns a standard `Iterator`, you can use any
combinator that stops consuming early:

```rust
let first_ten: Vec<_> = generate_iterator(1000, |b| hanoi(0, 2, 30, b))
    .take(10)
    .collect();
```

When the consumer drops the iterator, the next `add_element` call that would
send a full batch returns `Err(ShouldTerminateAsSoonAsPossible)`. The `?`
operator propagates this up through the recursion, unwinding the call stack and
terminating the worker thread cleanly — no panic, no unbounded computation
continuing in the background.

## How it works

```
 caller thread                    worker thread
 ─────────────────                ─────────────────────────────────
 generate_iterator(...)           start_function(&mut batcher)
   └─ returns Iterator              └─ recursive calls to add_element
        │                                 │
        │   sync_channel(capacity=1)      │
        │ <══════════════════════════════ │  send batch (blocks when 1 batch ahead)
        │                                 │
   flatten() yields items           ... continues recursion ...
```

- **Bounded memory:** the channel holds at most one batch in flight. Peak
  allocation is `≈ 2 × batch_size` elements regardless of how many the function
  ultimately produces.
- **Backpressure:** the producer thread blocks whenever it is more than one
  batch ahead of the consumer, so a slow consumer automatically throttles the
  producer.
- **Clean early termination:** when the consumer drops the iterator,
  `add_element` returns `Err(ShouldTerminateAsSoonAsPossible)`. Propagating
  this with `?` unwinds the recursion immediately — no panic, no wasted work.
- **Type safety:** `T: Send + 'static` is required because `T` crosses a thread
  boundary. Non-`Send` types (e.g. `Rc<T>`) are rejected by the compiler with a
  clear error at the call site.

## Choosing a batch size

| Scenario | Suggested `batch_size` |
|---|---|
| Elements are cheap and plentiful | 1 000 – 4 000 |
| Elements are large structs | 64 – 256 |
| You want the first element fast | 1 – 16 |
| Throughput matters more than latency | 4 000 – 16 000 |

The channel synchronisation cost is paid once per batch, so larger batches
improve throughput at the expense of first-element latency.

## Limitations

- One background thread is spawned per `generate_iterator` call.
- The approach is not suitable for `async` contexts; use `async-stream` or
  similar crates if you need a `Stream` instead of an `Iterator`.
