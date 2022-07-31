use std::{
    collections::{hash_map::DefaultHasher, BTreeMap},
    hash::{Hash, Hasher},
};

#[derive(Debug, Eq, PartialEq, Hash, Copy, Clone, PartialOrd, Ord)]
pub struct Worker(u32);

pub fn naive_assign(
    workers: &[Worker],
    shards: &[u32],
    redundancy: usize,
) -> BTreeMap<Worker, Vec<u32>> {
    let mut acc: BTreeMap<Worker, Vec<u32>> = BTreeMap::new();
    for &s in shards {
        let idx = h(s);
        for i in 0..redundancy {
            acc.entry(workers[(idx + i) % workers.len()])
                .or_default()
                .push(s);
        }
    }
    acc
}

pub fn rendevoux_assign(
    workers: &[Worker],
    shards: &[u32],
    redundancy: usize,
) -> BTreeMap<Worker, Vec<u32>> {
    let mut acc: BTreeMap<Worker, Vec<u32>> = BTreeMap::new();
    for &s in shards {
        let mut ranked = workers.to_owned();
        ranked.sort_by_key(|w| h((s, w)));
        for i in 0..redundancy {
            acc.entry(ranked[i])
                .or_default()
                .push(s);
        }
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
    use crate::{naive_assign, Worker};

    #[test]
    fn naive_assign_smoke_test() {
        let workers: Vec<Worker> = vec![Worker(100), Worker(200), Worker(300), Worker(400)];
        let shards: Vec<u32> = (1..=8).collect();

        assert_eq!(
            naive_assign(&workers, &shards, 1),
            vec![
                (Worker(100), vec![1, 4]),
                (Worker(200), vec![2]),
                (Worker(300), vec![3, 7]),
                (Worker(400), vec![5, 6, 8]),
            ]
            .into_iter()
            .collect()
        );

        assert_eq!(
            naive_assign(&workers, &shards, 2),
            vec![
                (Worker(100), vec![1, 4, 5, 6, 8]),
                (Worker(200), vec![1, 2, 4]),
                (Worker(300), vec![2, 3, 7]),
                (Worker(400), vec![3, 5, 6, 7, 8]),
            ]
            .into_iter()
            .collect()
        );
    }

    #[test]
    fn naive_reassign() {
        let workers: Vec<Worker> = vec![Worker(100), Worker(200), Worker(300), Worker(400)];
        let shards: Vec<u32> = (1..=8).collect();

        assert_eq!(
            naive_assign(&workers, &shards, 1),
            vec![
                (Worker(100), vec![1, 4]),
                (Worker(200), vec![2]),
                (Worker(300), vec![3, 7]),
                (Worker(400), vec![5, 6, 8]),
            ]
            .into_iter()
            .collect()
        );

        assert_eq!(
            naive_assign(&workers[1..], &shards, 1),
            vec![
                (Worker(200), vec![1]),
                (Worker(300), vec![2, 3, 5, 7]),
                (Worker(400), vec![4, 6, 8]),
            ]
            .into_iter()
            .collect()
        );
    }
}
