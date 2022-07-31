use std::{
    collections::{hash_map::DefaultHasher, BTreeMap, BTreeSet},
    hash::{Hash, Hasher},
};

#[derive(Debug, Eq, PartialEq, Hash, Copy, Clone, PartialOrd, Ord)]
pub struct Worker(u32);

pub fn naive_assign(
    workers: &[Worker],
    shards: &[u32],
    redundancy: usize,
) -> BTreeMap<Worker, BTreeSet<u32>> {
    let mut acc: BTreeMap<Worker, BTreeSet<u32>> = BTreeMap::new();
    for &s in shards {
        let idx = h(s);
        for i in 0..redundancy {
            acc.entry(workers[(idx + i) % workers.len()])
                .or_default()
                .insert(s);
        }
    }
    acc
}

pub fn rendevoux_assign(
    workers: &[Worker],
    shards: &[u32],
    redundancy: usize,
) -> BTreeMap<Worker, BTreeSet<u32>> {
    let mut acc: BTreeMap<Worker, BTreeSet<u32>> = BTreeMap::new();
    for &s in shards {
        let mut ranked = workers.to_owned();
        ranked.sort_by_key(|w| h((s, w)));
        for w in ranked.into_iter().take(redundancy) {
            acc.entry(w).or_default().insert(s);
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
    use std::collections::{BTreeMap, BTreeSet};

    use crate::{naive_assign, rendevoux_assign, Worker};

    #[test]
    fn naive_smoke_test() {
        let workers: Vec<Worker> = vec![Worker(100), Worker(200), Worker(300), Worker(400)];
        let shards: Vec<u32> = (1..=8).collect();

        assert_eq!(
            naive_assign(&workers, &shards, 1),
            vec![
                (Worker(100), vec![1, 4].into_iter().collect()),
                (Worker(200), vec![2].into_iter().collect()),
                (Worker(300), vec![3, 7].into_iter().collect()),
                (Worker(400), vec![5, 6, 8].into_iter().collect()),
            ]
            .into_iter()
            .collect()
        );

        assert_eq!(
            naive_assign(&workers, &shards, 2),
            vec![
                (Worker(100), vec![1, 4, 5, 6, 8].into_iter().collect()),
                (Worker(200), vec![1, 2, 4].into_iter().collect()),
                (Worker(300), vec![2, 3, 7].into_iter().collect()),
                (Worker(400), vec![3, 5, 6, 7, 8].into_iter().collect()),
            ]
            .into_iter()
            .collect()
        );
    }

    #[test]
    fn rendevous_smoke_test() {
        let workers: Vec<Worker> = vec![Worker(100), Worker(200), Worker(300), Worker(400)];
        let shards: Vec<u32> = (1..=8).collect();

        assert_eq!(
            rendevoux_assign(&workers, &shards, 1),
            vec![
                (Worker(100), vec![4, 7].into_iter().collect()),
                (Worker(200), vec![2, 6, 8].into_iter().collect()),
                (Worker(300), vec![5].into_iter().collect()),
                (Worker(400), vec![1, 3].into_iter().collect()),
            ]
            .into_iter()
            .collect()
        );

        assert_eq!(
            rendevoux_assign(&workers, &shards, 2),
            vec![
                (Worker(100), vec![2, 4, 7].into_iter().collect()),
                (Worker(200), vec![2, 3, 4, 5, 6, 7, 8].into_iter().collect()),
                (Worker(300), vec![1, 5].into_iter().collect()),
                (Worker(400), vec![1, 3, 6, 8].into_iter().collect()),
            ]
            .into_iter()
            .collect()
        );
    }

    #[test]
    fn reassignment_score_test() {
        // This test checks to see how expensive it is to add/remove a single
        // worker, in terms of how many shards get moved around.
        let workers: Vec<Worker> = (0..100).map(|i| Worker(100 * i)).collect();
        let shards: Vec<u32> = (1..=800).collect();

        assert_eq!(
            score_diff(
                naive_assign(&workers[0..], &shards, 1),
                naive_assign(&workers[1..], &shards, 1),
            ),
            1_590
        );
        assert_eq!(
            score_diff(
                rendevoux_assign(&workers[0..], &shards, 1),
                rendevoux_assign(&workers[1..], &shards, 1),
            ),
            18
        );
    }

    fn score_diff(a: BTreeMap<Worker, BTreeSet<u32>>, b: BTreeMap<Worker, BTreeSet<u32>>) -> usize {
        let mut score = 0;
        let keys: BTreeSet<Worker> = a.keys().chain(b.keys()).cloned().collect();
        for k in keys {
            let aa = a.get(&k).cloned().unwrap_or_default();
            let bb = b.get(&k).cloned().unwrap_or_default();
            score += aa.len() + bb.len() - 2 * aa.intersection(&bb).count();
        }
        score
    }
}
