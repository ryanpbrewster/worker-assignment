use std::{collections::{HashMap, hash_map::DefaultHasher}, hash::{Hash, Hasher}};

#[derive(Debug, Eq, PartialEq, Hash, Copy, Clone)]
pub struct Worker(u32);

#[derive(Debug, Eq, PartialEq, Hash, Copy, Clone)]
pub struct Shard(u32);

pub fn naive_assign(
    workers: Vec<Worker>,
    shards: Vec<Shard>,
    redundancy: usize,
) -> HashMap<Shard, Vec<Worker>> {
    assert_eq!(redundancy, 1);
    let mut acc = HashMap::new();
    for shard in shards {
        acc.insert(shard, vec![workers[h(shard) % workers.len()]]);
    }
    acc
}

fn h<T: Hash>(t: T) -> usize {
    let mut hasher = DefaultHasher::new();
    t.hash(&mut hasher);
    hasher.finish() as usize
}

#[cfg(test)]
mod tests {
    use crate::{Worker, Shard, naive_assign};

    #[test]
    fn it_works() {
        let workers: Vec<Worker> = vec![Worker(100), Worker(200), Worker(300), Worker(400)];
        let shards: Vec<Shard> = (1..=8).map(|i| Shard(i)).collect();
        assert_eq!(
            naive_assign(workers, shards, 1),
            vec![
                (Shard(1), vec![Worker(100)]),
                (Shard(2), vec![Worker(200)]),
                (Shard(3), vec![Worker(300)]),
                (Shard(4), vec![Worker(100)]),
                (Shard(5), vec![Worker(400)]),
                (Shard(6), vec![Worker(400)]),
                (Shard(7), vec![Worker(300)]),
                (Shard(8), vec![Worker(400)]),
            ].into_iter().collect()
        );
    }
}
