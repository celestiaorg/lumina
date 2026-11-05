//! Input shared by the rsema1d fuzz targets.

use arbitrary::Arbitrary;
use rayon::{ThreadPool, ThreadPoolBuilder};
use rsema1d::{ExtendedData, Parameters, RowMatrix, default_work_budget, encode_with_work_budget};
use std::num::NonZeroUsize;
use std::sync::OnceLock;

const MAX_THREADS: usize = 8;

/// Data to encode: small parameters and pseudo-random rows that start with the fuzzer's bytes.
#[derive(Debug, Arbitrary)]
pub struct Square {
    k: u8,
    n: u8,
    chunks: u8,
    threads: u8,
    budget: Option<u32>,
    seed: u64,
    prefix: Vec<u8>,
}

impl Square {
    pub fn params(&self) -> Parameters {
        Parameters::new(
            usize::from(self.k % 64) + 1,
            usize::from(self.n % 64) + 1,
            (usize::from(self.chunks % 40) + 1) * 64,
        )
        .unwrap()
    }

    /// The pool to run the code under test in; thread count changes how work is split.
    pub fn pool(&self) -> &'static ThreadPool {
        static POOLS: [OnceLock<ThreadPool>; MAX_THREADS] =
            [const { OnceLock::new() }; MAX_THREADS];
        let threads = usize::from(self.threads) % MAX_THREADS + 1;
        POOLS[threads - 1].get_or_init(|| {
            ThreadPoolBuilder::new()
                .num_threads(threads)
                .build()
                .unwrap()
        })
    }

    /// Work budget for encoding and reconstruction; small ones force narrow stripes.
    /// Call inside [`Square::pool`], since the default depends on the pool.
    pub fn work_budget(&self) -> NonZeroUsize {
        self.budget.map_or_else(default_work_budget, |b| {
            NonZeroUsize::new(b as usize).unwrap_or(NonZeroUsize::MIN)
        })
    }

    pub fn encode(&self) -> ExtendedData {
        let params = self.params();
        let len = params.k * params.row_size;
        let mut x = self.seed | 1;
        let mut data: Vec<u8> = (0..len)
            .map(|_| {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                x as u8
            })
            .collect();
        let prefix = self.prefix.len().min(len);
        data[..prefix].copy_from_slice(&self.prefix[..prefix]);
        let rows = RowMatrix::with_shape(data, params.k, params.row_size).unwrap();
        self.pool()
            .install(|| encode_with_work_budget(&rows, &params, self.work_budget()))
            .unwrap()
    }
}
