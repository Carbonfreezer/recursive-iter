use std::mem::take;
use std::sync::mpsc::{SyncSender, sync_channel};

pub struct Batcher<T> {
    current_batch: Vec<T>,
    sender: SyncSender<Vec<T>>,
    batch_size: usize,
}

impl<T> Batcher<T> {
    fn new(batch_size: usize, sender: SyncSender<Vec<T>>) -> Batcher<T> {
        Batcher {
            current_batch: Vec::with_capacity(batch_size),
            sender,
            batch_size,
        }
    }

    pub fn add_element(&mut self, pair: T) {
        self.current_batch.push(pair);
        if self.current_batch.len() >= self.batch_size {
            let content = take(&mut self.current_batch);
            self.current_batch.reserve(self.batch_size);
            self.sender.send(content).unwrap();
        }
    }
}

impl<T> Drop for Batcher<T> {
    fn drop(&mut self) {
        if !self.current_batch.is_empty() {
            self.sender.send(take(&mut self.current_batch)).unwrap();
        }
    }
}

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

        assert_eq!(base_vec, plain_vec, "Soluitions are not the same");
    }
}
