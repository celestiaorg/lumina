//! Shared fixtures for the `celestia-types` fuzz targets.

use std::sync::LazyLock;

use celestia_types::consts::appconsts::SHARE_SIZE;
use celestia_types::nmt::{NS_SIZE, Namespace};
use celestia_types::{DataAvailabilityHeader, ExtendedDataSquare};

/// Namespace of the data in [`EDS`]. Requests for other namespaces exercise absence proofs.
pub const NAMESPACE: Namespace = Namespace::const_v0([1; 10]);

/// Width of the original data square of [`EDS`].
pub const ODS_WIDTH: usize = 4;

/// A deterministic square that responses are verified against.
///
/// Half of the ODS is in [`NAMESPACE`], the rest is tail padding.
pub static EDS: LazyLock<ExtendedDataSquare> = LazyLock::new(|| {
    let shares = (0..ODS_WIDTH * ODS_WIDTH)
        .map(|i| {
            let ns = if i < ODS_WIDTH * ODS_WIDTH / 2 {
                NAMESPACE
            } else {
                Namespace::TAIL_PADDING
            };
            [ns.as_bytes(), &[0; SHARE_SIZE - NS_SIZE][..]].concat()
        })
        .collect();

    ExtendedDataSquare::from_ods(shares).unwrap()
});

/// [`DataAvailabilityHeader`] of [`EDS`].
pub static DAH: LazyLock<DataAvailabilityHeader> =
    LazyLock::new(|| DataAvailabilityHeader::from_eds(&EDS));
