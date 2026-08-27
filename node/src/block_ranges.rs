//! Ranges utilities

use std::borrow::Borrow;
use std::fmt::{Debug, Display};
use std::ops::{
    Add, AddAssign, BitAnd, BitAndAssign, BitOr, BitOrAssign, Not, RangeInclusive, Sub, SubAssign,
};

use serde::Serialize;
use smallvec::SmallVec;

/// Type alias of [`RangeInclusive<u64>`].
///
/// [`RangeInclusive<u64>`]: std::ops::RangeInclusive
pub type BlockRange = RangeInclusive<u64>;

/// Errors that can be produced by [`BlockRanges`].
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum BlockRangesError {
    /// Block ranges must be sorted.
    #[error("Block ranges are not sorted")]
    UnsortedBlockRanges,

    /// Not a valid block range.
    #[error("Invalid block range: {}", .0.display())]
    InvalidBlockRange(BlockRange),

    /// Provided range overlaps with another one.
    #[error("Insertion constrain not satisfied: Provided range ({}) overlaps with {}", .0.display(), .1.display())]
    BlockRangeOverlap(BlockRange, BlockRange),

    /// Provided range do not have any adjacent neightbors.
    #[error(
        "Insertion constrain not satified: Provided range ({}) do not have any adjacent neighbors", .0.display()
    )]
    NoAdjacentNeighbors(BlockRange),
}

type Result<T, E = BlockRangesError> = std::result::Result<T, E>;

pub(crate) trait BlockRangeExt {
    fn display(&self) -> BlockRangeDisplay<'_>;
    fn validate(&self) -> Result<()>;
    fn len(&self) -> u64;
    fn is_adjacent(&self, other: &BlockRange) -> bool;
    fn is_overlapping(&self, other: &BlockRange) -> bool;
    fn is_left_of(&self, other: &BlockRange) -> bool;
    fn is_right_of(&self, other: &BlockRange) -> bool;
    fn headn(&self, limit: u64) -> Self;
    fn tailn(&self, limit: u64) -> Self;
}

pub(crate) struct BlockRangeDisplay<'a>(&'a RangeInclusive<u64>);

impl Display for BlockRangeDisplay<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}-{}", self.0.start(), self.0.end())
    }
}

impl BlockRangeExt for BlockRange {
    fn display(&self) -> BlockRangeDisplay<'_> {
        BlockRangeDisplay(self)
    }

    fn validate(&self) -> Result<()> {
        if *self.start() > 0 && self.start() <= self.end() {
            Ok(())
        } else {
            Err(BlockRangesError::InvalidBlockRange(self.to_owned()))
        }
    }

    fn len(&self) -> u64 {
        match self.end().checked_sub(*self.start()) {
            Some(difference) => difference + 1,
            None => 0,
        }
    }

    fn is_adjacent(&self, other: &BlockRange) -> bool {
        debug_assert!(self.validate().is_ok());
        debug_assert!(other.validate().is_ok());

        // End of `self` touches start of `other`
        //
        // self:  |------|
        // other:         |------|
        if *self.end() == other.start().saturating_sub(1) {
            return true;
        }

        // Start of `self` touches end of `other`
        //
        // self:          |------|
        // other: |------|
        if self.start().saturating_sub(1) == *other.end() {
            return true;
        }

        false
    }

    fn is_overlapping(&self, other: &BlockRange) -> bool {
        debug_assert!(self.validate().is_ok());
        debug_assert!(other.validate().is_ok());

        // `self` overlaps with start of `other`
        //
        // self:  |------|
        // other:     |------|
        if self.start() < other.start() && other.contains(self.end()) {
            return true;
        }

        // `self` overlaps with end of `other`
        //
        // self:      |------|
        // other: |------|
        if self.end() > other.end() && other.contains(self.start()) {
            return true;
        }

        // `self` is subset of `other`
        //
        // self:    |--|
        // other: |------|
        if self.start() >= other.start() && self.end() <= other.end() {
            return true;
        }

        // `self` is superset of `other`
        //
        // self:  |------|
        // other:   |--|
        if self.start() <= other.start() && self.end() >= other.end() {
            return true;
        }

        false
    }

    /// Returns `true` if the whole range of `self` is on the left of `other`.
    fn is_left_of(&self, other: &BlockRange) -> bool {
        debug_assert!(self.validate().is_ok());
        debug_assert!(other.validate().is_ok());
        self.end() < other.start()
    }

    /// Returns `true` if the whole range of `self` is on the right of `other`.
    fn is_right_of(&self, other: &BlockRange) -> bool {
        debug_assert!(self.validate().is_ok());
        debug_assert!(other.validate().is_ok());
        other.end() < self.start()
    }

    /// Truncate the range so that it contains at most `limit` elements, removing from the tail
    fn headn(&self, limit: u64) -> Self {
        if self.is_empty() {
            return RangeInclusive::new(1, 0);
        }
        let start = *self.start();
        let end = *self.end();

        let Some(adjusted_start) = end.saturating_sub(limit).checked_add(1) else {
            // overflow can happen only if limit == 0, which is an empty range anyway
            return RangeInclusive::new(1, 0);
        };

        u64::max(start, adjusted_start)..=end
    }

    /// Truncate the range so that it contains at most `limit` elements, removing from the head
    fn tailn(&self, limit: u64) -> Self {
        if self.is_empty() {
            return RangeInclusive::new(1, 0);
        }
        let start = *self.start();
        let end = *self.end();

        let Some(adjusted_end) = start.saturating_add(limit).checked_sub(1) else {
            return RangeInclusive::new(1, 0);
        };

        start..=u64::min(end, adjusted_end)
    }
}

/// Represents possibly multiple non-overlapping, sorted ranges of header heights
#[derive(Debug, Clone, PartialEq, Default, Serialize)]
#[serde(transparent)]
pub struct BlockRanges(SmallVec<[BlockRange; 2]>);

/// Custom `Deserialize` that validates `BlockRanges`.
impl<'de> serde::Deserialize<'de> for BlockRanges {
    fn deserialize<D>(deserializer: D) -> Result<BlockRanges, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        let raw_ranges = SmallVec::<[RangeInclusive<u64>; 2]>::deserialize(deserializer)?;
        let ranges = BlockRanges::from_vec(raw_ranges)
            .map_err(|e| serde::de::Error::custom(e.to_string()))?;
        Ok(ranges)
    }
}

impl BlockRanges {
    /// Create a new, empty `BlockRanges`.
    pub fn new() -> BlockRanges {
        BlockRanges(SmallVec::new())
    }

    /// Create a `BlockRanges` from a [`SmallVec`].
    pub fn from_vec(ranges: SmallVec<[BlockRange; 2]>) -> Result<Self> {
        let mut prev: Option<&RangeInclusive<u64>> = None;

        for range in &ranges {
            range.validate()?;

            if prev.is_some_and(|prev| range.start() <= prev.end()) {
                return Err(BlockRangesError::UnsortedBlockRanges);
            }

            prev = Some(range);
        }

        Ok(BlockRanges(ranges))
    }

    /// Returns internal representation.
    pub fn into_inner(self) -> SmallVec<[BlockRange; 2]> {
        self.0
    }

    /// Return whether `BlockRanges` contains provided height.
    pub fn contains(&self, height: u64) -> bool {
        self.0.iter().any(|r| r.contains(&height))
    }

    /// Return how many blocks exist in `BlockRanges`.
    pub fn len(&self) -> u64 {
        self.0.iter().map(|r| r.len()).sum()
    }

    /// Return whether range is empty.
    pub fn is_empty(&self) -> bool {
        self.0.iter().all(|r| r.is_empty())
    }

    /// Return highest height in the range.
    pub fn head(&self) -> Option<u64> {
        self.0.last().map(|r| *r.end())
    }

    pub(crate) fn headn(&self, limit: u64) -> BlockRanges {
        let mut truncated = BlockRanges::new();
        let mut len = 0;

        for range in self.0.iter().rev() {
            if len == limit {
                break;
            }

            let r = range.headn(limit - len);

            len += r.len();
            truncated
                .insert_relaxed(r)
                .expect("BlockRanges always holds valid ranges");

            debug_assert_eq!(truncated.len(), len);
            debug_assert!(len <= limit);
        }

        truncated
    }

    /// Return lowest height in the range.
    pub fn tail(&self) -> Option<u64> {
        self.0.first().map(|r| *r.start())
    }

    #[allow(unused)]
    pub(crate) fn tailn(&self, limit: u64) -> BlockRanges {
        let mut truncated = BlockRanges::new();
        let mut len = 0;

        for range in self.0.iter() {
            if len == limit {
                break;
            }

            let r = range.tailn(limit - len);

            len += r.len();
            truncated
                .insert_relaxed(r)
                .expect("BlockRanges always holds valid ranges");

            debug_assert_eq!(truncated.len(), len);
            debug_assert!(len <= limit);
        }

        truncated
    }

    /// Returns first and last index of ranges overlapping or touching provided `range`.
    fn find_affected_ranges(&self, range: impl Borrow<BlockRange>) -> Option<(usize, usize)> {
        let range = range.borrow();
        debug_assert!(range.validate().is_ok());

        let mut start_idx = None;
        let mut end_idx = None;

        for (i, r) in self.0.iter().enumerate() {
            if r.is_overlapping(range) || r.is_adjacent(range) {
                if start_idx.is_none() {
                    start_idx = Some(i);
                }

                end_idx = Some(i);
            } else if end_idx.is_some() {
                // Ranges are sorted, we can skip checking the rest.
                break;
            }
        }

        Some((start_idx?, end_idx?))
    }

    /// Checks if `to_insert` is valid range to be inserted into the header store.
    ///
    /// This *must* be used when implementing [`Store::insert`].
    ///
    /// The constraints are as follows:
    ///
    /// * Insertion range must be valid.
    /// * Insertion range must not overlap with any of the existing ranges.
    /// * Insertion range must be appended to the left or to the right of an existing range.
    /// * Insertion is always allowed on empty `BlockRanges`.
    /// * New HEAD range can be created at the front if it doesn't overlap with existing one.
    ///
    /// Returns:
    ///
    /// * `Err(_)` if constraints are not met.
    /// * `Ok(true, false)` if `to_insert` range is going to be merged with its left neighbor.
    /// * `Ok(false, true)` if `to_insert` range is going to be merged with the right neighbor.
    /// * `Ok(true, true)` if `to_insert` range is going to be merged with both neighbors.
    /// * `Ok(false, false)` if `to_insert` range is going to be inserted as a new HEAD range (without merging).
    ///
    /// [`Store::insert`]: crate::store::Store::insert
    pub fn check_insertion_constraints(
        &self,
        to_insert: impl Borrow<BlockRange>,
    ) -> Result<(bool, bool)> {
        let to_insert = to_insert.borrow();
        to_insert.validate()?;

        let Some(head_range) = self.0.last() else {
            // Allow insersion on empty store.
            return Ok((false, false));
        };

        if head_range.is_left_of(to_insert) {
            // Allow adding a new HEAD
            let prev_exists = head_range.is_adjacent(to_insert);
            return Ok((prev_exists, false));
        }

        let Some((first_idx, last_idx)) = self.find_affected_ranges(to_insert) else {
            return Err(BlockRangesError::NoAdjacentNeighbors(to_insert.to_owned()));
        };

        let first = &self.0[first_idx];
        let last = &self.0[last_idx];
        let num_of_ranges = last_idx - first_idx + 1;

        match num_of_ranges {
            0 => unreachable!(),
            1 => {
                if first.is_overlapping(to_insert) {
                    Err(BlockRangesError::BlockRangeOverlap(
                        to_insert.to_owned(),
                        calc_overlap(to_insert, first, last),
                    ))
                } else if first.is_left_of(to_insert) {
                    Ok((true, false))
                } else {
                    Ok((false, true))
                }
            }
            2 => {
                if first.is_adjacent(to_insert) && last.is_adjacent(to_insert) {
                    Ok((true, true))
                } else {
                    Err(BlockRangesError::BlockRangeOverlap(
                        to_insert.to_owned(),
                        calc_overlap(to_insert, first, last),
                    ))
                }
            }
            _ => Err(BlockRangesError::BlockRangeOverlap(
                to_insert.to_owned(),
                calc_overlap(to_insert, first, last),
            )),
        }
    }

    /// Returns the head height and removes it from the ranges.
    pub fn pop_head(&mut self) -> Option<u64> {
        let last = self.0.last_mut()?;
        let head = *last.end();

        if last.len() == 1 {
            self.0.remove(self.0.len() - 1);
        } else {
            *last = *last.start()..=*last.end() - 1;
        }

        Some(head)
    }

    /// Returns the tail (lowest) height and removes it from the ranges.
    pub fn pop_tail(&mut self) -> Option<u64> {
        let first = self.0.first_mut()?;
        let tail = *first.start();

        if first.len() == 1 {
            self.0.remove(0);
        } else {
            *first = *first.start() + 1..=*first.end();
        }

        Some(tail)
    }

    /// Insert a new range.
    ///
    /// This fails only if `range` is not valid. It allows inserting an overlapping range.
    pub fn insert_relaxed(&mut self, range: impl Borrow<BlockRange>) -> Result<()> {
        let range = range.borrow();
        range.validate()?;

        match self.find_affected_ranges(range) {
            // `range` must be merged with other ranges
            Some((start_idx, end_idx)) => {
                let start = *self.0[start_idx].start().min(range.start());
                let end = *self.0[end_idx].end().max(range.end());

                self.0.drain(start_idx..=end_idx);
                self.0.insert(start_idx, start..=end);
            }
            // `range` can not be merged with other ranges
            None => {
                for (i, r) in self.0.iter().enumerate() {
                    if range.end() < r.start() {
                        self.0.insert(i, range.to_owned());
                        return Ok(());
                    }
                }

                self.0.push(range.to_owned());
            }
        }

        Ok(())
    }

    /// Remove a range.
    ///
    /// This fails only if `range` is not valid. It allows removing non-existing range.
    pub fn remove_relaxed(&mut self, range: impl Borrow<BlockRange>) -> Result<()> {
        let range = range.borrow();
        range.validate()?;

        let Some((start_idx, end_idx)) = self.find_affected_ranges(range) else {
            // Nothing to remove
            return Ok(());
        };

        // Remove old ranges
        let old_ranges = self
            .0
            .drain(start_idx..=end_idx)
            .collect::<SmallVec<[_; 2]>>();

        // ranges:       |-----|  |----|  |----|
        // remove:           |--------------|
        // after remove: |---|              |--|
        let first_range = old_ranges.first().expect("non empty");
        let last_range = old_ranges.last().expect("non empty");

        if range.end() < last_range.end() {
            // Add the right range
            self.0
                .insert(start_idx, *range.end() + 1..=*last_range.end());
        }

        if first_range.start() < range.start() {
            // Add the left range
            self.0
                .insert(start_idx, *first_range.start()..=*range.start() - 1);
        }

        Ok(())
    }

    pub(crate) fn edges(&self) -> BlockRanges {
        let mut edges = BlockRanges::new();

        for range in self.0.iter() {
            let start = *range.start();
            let end = *range.end();

            edges
                .insert_relaxed(start..=start)
                .expect("BlockRanges always holds valid ranges");
            edges
                .insert_relaxed(end..=end)
                .expect("BlockRanges always holds valid ranges");
        }

        edges
    }

    /// Returns `([left], middle, [right])` that can be used for binary search.
    pub(crate) fn partitions(&self) -> Option<(BlockRanges, u64, BlockRanges)> {
        let len = self.len();

        if len == 0 {
            return None;
        }

        let mut left = BlockRanges::new();
        let mut right = BlockRanges::new();

        let middle = len / 2;
        let mut left_len = 0;

        for range in self.0.iter() {
            let range_len = range.len();

            if left_len + range_len <= middle {
                // If the whole `range` is up to the `middle`
                left.insert_relaxed(range.to_owned())
                    .expect("BlockRanges always holds valid ranges");
                left_len += range_len;
            } else if left_len < middle {
                // If `range` overlaps with `middle`
                let start = *range.start();
                let end = *range.end();

                let left_end = start + middle - left_len;
                let left_range = start..=left_end;

                left_len += left_range.len();
                left.insert_relaxed(left_range)
                    .expect("BlockRanges always holds valid ranges");

                if left_end < end {
                    right
                        .insert_relaxed(left_end + 1..=end)
                        .expect("BlockRanges always holds valid ranges");
                }
            } else {
                // If the whole `range` is after the `middle`
                right
                    .insert_relaxed(range.to_owned())
                    .expect("BlockRanges always holds valid ranges");
            }
        }

        let middle_height = if left_len < right.len() {
            right.pop_tail()?
        } else {
            left.pop_head()?
        };

        Some((left, middle_height, right))
    }

    /// Returns the height that is on the left of `height` parameter.
    pub(crate) fn left_of(&self, height: u64) -> Option<u64> {
        for r in self.0.iter().rev() {
            if r.is_left_of(&(height..=height)) {
                return Some(*r.end());
            } else if r.contains(&height) && *r.start() != height {
                return Some(height - 1);
            }
        }

        None
    }

    /// Returns the height that is on the left of `height` parameter.
    pub(crate) fn right_of(&self, height: u64) -> Option<u64> {
        for r in self.0.iter() {
            if r.is_right_of(&(height..=height)) {
                return Some(*r.start());
            } else if r.contains(&height) && *r.end() != height {
                return Some(height + 1);
            }
        }

        None
    }
}

impl TryFrom<RangeInclusive<u64>> for BlockRanges {
    type Error = BlockRangesError;

    fn try_from(value: RangeInclusive<u64>) -> Result<Self, Self::Error> {
        let mut ranges = BlockRanges::new();
        ranges.insert_relaxed(value)?;
        Ok(ranges)
    }
}

impl<const N: usize> TryFrom<[RangeInclusive<u64>; N]> for BlockRanges {
    type Error = BlockRangesError;

    fn try_from(value: [RangeInclusive<u64>; N]) -> Result<Self, Self::Error> {
        BlockRanges::from_vec(value.into_iter().collect())
    }
}

impl TryFrom<&[RangeInclusive<u64>]> for BlockRanges {
    type Error = BlockRangesError;

    fn try_from(value: &[RangeInclusive<u64>]) -> Result<Self, Self::Error> {
        BlockRanges::from_vec(value.iter().cloned().collect())
    }
}

impl AsRef<[RangeInclusive<u64>]> for BlockRanges {
    fn as_ref(&self) -> &[RangeInclusive<u64>] {
        &self.0
    }
}

impl AddAssign for BlockRanges {
    fn add_assign(&mut self, rhs: BlockRanges) {
        self.add_assign(&rhs);
    }
}

impl AddAssign<&BlockRanges> for BlockRanges {
    fn add_assign(&mut self, rhs: &BlockRanges) {
        for range in rhs.0.iter() {
            self.insert_relaxed(range)
                .expect("BlockRanges always holds valid ranges");
        }
    }
}

impl Add for BlockRanges {
    type Output = Self;

    fn add(mut self, rhs: BlockRanges) -> Self::Output {
        self.add_assign(&rhs);
        self
    }
}

impl Add<&BlockRanges> for BlockRanges {
    type Output = Self;

    fn add(mut self, rhs: &BlockRanges) -> Self::Output {
        self.add_assign(rhs);
        self
    }
}

impl SubAssign for BlockRanges {
    fn sub_assign(&mut self, rhs: BlockRanges) {
        self.sub_assign(&rhs);
    }
}

impl SubAssign<&BlockRanges> for BlockRanges {
    fn sub_assign(&mut self, rhs: &BlockRanges) {
        for range in rhs.0.iter() {
            self.remove_relaxed(range)
                .expect("BlockRanges always holds valid ranges");
        }
    }
}

impl Sub for BlockRanges {
    type Output = Self;

    fn sub(mut self, rhs: BlockRanges) -> Self::Output {
        self.sub_assign(&rhs);
        self
    }
}

impl Sub<&BlockRanges> for BlockRanges {
    type Output = Self;

    fn sub(mut self, rhs: &BlockRanges) -> Self::Output {
        self.sub_assign(rhs);
        self
    }
}

impl Not for BlockRanges {
    type Output = Self;

    fn not(self) -> Self::Output {
        let mut inverse = BlockRanges::new();

        // Start with full range
        inverse.insert_relaxed(1..=u64::MAX).expect("valid ranges");

        // And remove whatever we have
        for range in self.0.iter() {
            inverse
                .remove_relaxed(range)
                .expect("BlockRanges always holds valid ranges");
        }

        inverse
    }
}

impl BitOrAssign for BlockRanges {
    fn bitor_assign(&mut self, rhs: BlockRanges) {
        self.add_assign(&rhs);
    }
}

impl BitOrAssign<&BlockRanges> for BlockRanges {
    fn bitor_assign(&mut self, rhs: &BlockRanges) {
        self.add_assign(rhs);
    }
}

impl BitOr for BlockRanges {
    type Output = Self;

    fn bitor(mut self, rhs: BlockRanges) -> Self::Output {
        self.add_assign(&rhs);
        self
    }
}

impl BitOr<&BlockRanges> for BlockRanges {
    type Output = Self;

    fn bitor(mut self, rhs: &BlockRanges) -> Self::Output {
        self.add_assign(rhs);
        self
    }
}

impl BitAndAssign for BlockRanges {
    fn bitand_assign(&mut self, rhs: BlockRanges) {
        self.bitand_assign(&rhs);
    }
}

impl BitAndAssign<&BlockRanges> for BlockRanges {
    fn bitand_assign(&mut self, rhs: &BlockRanges) {
        // !(!A | !B) == (A & B)
        *self = !(!self.clone() | !rhs.clone());
    }
}

impl BitAnd for BlockRanges {
    type Output = Self;

    fn bitand(mut self, rhs: BlockRanges) -> Self::Output {
        self.bitand_assign(&rhs);
        self
    }
}

impl BitAnd<&BlockRanges> for BlockRanges {
    type Output = Self;

    fn bitand(mut self, rhs: &BlockRanges) -> Self::Output {
        self.bitand_assign(rhs);
        self
    }
}

impl Iterator for BlockRanges {
    type Item = u64;

    fn next(&mut self) -> Option<Self::Item> {
        self.pop_tail()
    }
}

impl DoubleEndedIterator for BlockRanges {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.pop_head()
    }
}

impl Display for BlockRanges {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[")?;
        for (idx, range) in self.0.iter().enumerate() {
            if idx == 0 {
                write!(f, "{}", range.display())?;
            } else {
                write!(f, ", {}", range.display())?;
            }
        }
        write!(f, "]")
    }
}

fn calc_overlap(
    to_insert: &BlockRange,
    first_range: &BlockRange,
    last_range: &BlockRange,
) -> BlockRange {
    let start = first_range.start().max(to_insert.start());
    let end = last_range.end().min(to_insert.end());
    *start..=*end
}

#[cfg(test)]
// One-element arrays here really are arrays of ranges, not a shorthand for a range of values.
#[allow(clippy::single_range_in_vec_init)]
mod tests {
    use super::*;

    use crate::test_utils::new_block_ranges;

    #[test]
    fn range_len() {
        assert_eq!((0u64..=0).len(), 1);
        assert_eq!((0u64..=5).len(), 6);
        assert_eq!((1u64..=2).len(), 2);
        assert_eq!(RangeInclusive::new(2u64, 1).len(), 0);
        assert_eq!((10001u64..=20000).len(), 10000);
    }

    #[test]
    fn block_ranges_empty() {
        assert!(new_block_ranges([]).is_empty());
        assert!(!new_block_ranges([1..=3]).is_empty());
    }

    #[test]
    fn block_ranges_head() {
        assert_eq!(new_block_ranges([]).head(), None);
        assert_eq!(new_block_ranges([1..=3]).head(), Some(3));
        assert_eq!(new_block_ranges([1..=3, 6..=9]).head(), Some(9));
        assert_eq!(new_block_ranges([1..=3, 5..=5, 8..=9]).head(), Some(9));
    }

    #[test]
    fn block_ranges_tail() {
        assert_eq!(new_block_ranges([]).tail(), None);
        assert_eq!(new_block_ranges([1..=3]).tail(), Some(1));
        assert_eq!(new_block_ranges([1..=3, 6..=9]).tail(), Some(1));
        assert_eq!(new_block_ranges([1..=3, 5..=5, 8..=9]).tail(), Some(1));
    }

    #[test]
    fn check_range_insert_append() {
        let (prev_exists, next_exists) = new_block_ranges([])
            .check_insertion_constraints(1..=5)
            .unwrap();
        assert!(!prev_exists);
        assert!(!next_exists);

        let (prev_exists, next_exists) = new_block_ranges([1..=4])
            .check_insertion_constraints(5..=5)
            .unwrap();
        assert!(prev_exists);
        assert!(!next_exists);

        let (prev_exists, next_exists) = new_block_ranges([1..=5])
            .check_insertion_constraints(6..=9)
            .unwrap();
        assert!(prev_exists);
        assert!(!next_exists);

        let (prev_exists, next_exists) = new_block_ranges([6..=8])
            .check_insertion_constraints(2..=5)
            .unwrap();
        assert!(!prev_exists);
        assert!(next_exists);

        // Allow inserting a new HEAD range
        let (prev_exists, next_exists) = new_block_ranges([1..=5])
            .check_insertion_constraints(7..=9)
            .unwrap();
        assert!(!prev_exists);
        assert!(!next_exists);
    }

    #[test]
    fn check_range_insert_with_consolidation() {
        let (prev_exists, next_exists) = new_block_ranges([1..=3, 6..=9])
            .check_insertion_constraints(4..=5)
            .unwrap();
        assert!(prev_exists);
        assert!(next_exists);

        let (prev_exists, next_exists) = new_block_ranges([1..=2, 5..=5, 8..=9])
            .check_insertion_constraints(3..=4)
            .unwrap();
        assert!(prev_exists);
        assert!(next_exists);

        let (prev_exists, next_exists) = new_block_ranges([1..=2, 4..=4, 8..=9])
            .check_insertion_constraints(5..=7)
            .unwrap();
        assert!(prev_exists);
        assert!(next_exists);
    }

    #[test]
    fn check_range_insert_overlapping() {
        let result = new_block_ranges([1..=2])
            .check_insertion_constraints(1..=1)
            .unwrap_err();
        assert_eq!(result, BlockRangesError::BlockRangeOverlap(1..=1, 1..=1));

        let result = new_block_ranges([1..=2])
            .check_insertion_constraints(2..=2)
            .unwrap_err();
        assert_eq!(result, BlockRangesError::BlockRangeOverlap(2..=2, 2..=2));

        let result = new_block_ranges([1..=4])
            .check_insertion_constraints(2..=8)
            .unwrap_err();
        assert_eq!(result, BlockRangesError::BlockRangeOverlap(2..=8, 2..=4));

        let result = new_block_ranges([1..=4])
            .check_insertion_constraints(2..=3)
            .unwrap_err();
        assert_eq!(result, BlockRangesError::BlockRangeOverlap(2..=3, 2..=3));

        let result = new_block_ranges([5..=9])
            .check_insertion_constraints(1..=5)
            .unwrap_err();
        assert_eq!(result, BlockRangesError::BlockRangeOverlap(1..=5, 5..=5));

        let result = new_block_ranges([5..=8])
            .check_insertion_constraints(2..=8)
            .unwrap_err();
        assert_eq!(result, BlockRangesError::BlockRangeOverlap(2..=8, 5..=8));

        let result = new_block_ranges([1..=3, 6..=9])
            .check_insertion_constraints(3..=6)
            .unwrap_err();
        assert_eq!(result, BlockRangesError::BlockRangeOverlap(3..=6, 3..=6));

        let result = new_block_ranges([1..=3, 5..=6])
            .check_insertion_constraints(3..=9)
            .unwrap_err();
        assert_eq!(result, BlockRangesError::BlockRangeOverlap(3..=9, 3..=6));

        let result = new_block_ranges([2..=3, 5..=6])
            .check_insertion_constraints(1..=5)
            .unwrap_err();
        assert_eq!(result, BlockRangesError::BlockRangeOverlap(1..=5, 2..=5));
    }

    #[test]
    fn check_range_insert_invalid_placement() {
        let result = new_block_ranges([1..=2, 7..=9])
            .check_insertion_constraints(4..=4)
            .unwrap_err();
        assert_eq!(result, BlockRangesError::NoAdjacentNeighbors(4..=4));

        let result = new_block_ranges([1..=2, 8..=9])
            .check_insertion_constraints(4..=6)
            .unwrap_err();
        assert_eq!(result, BlockRangesError::NoAdjacentNeighbors(4..=6));

        let result = new_block_ranges([4..=5, 7..=8])
            .check_insertion_constraints(1..=2)
            .unwrap_err();
        assert_eq!(result, BlockRangesError::NoAdjacentNeighbors(1..=2));
    }

    #[test]
    fn test_header_range_creation_ok() {
        new_block_ranges([1..=3, 5..=8]);
        new_block_ranges([]);
        new_block_ranges([1..=1, 1000000..=2000000]);
    }

    #[test]
    #[should_panic]
    fn test_header_range_creation_overlap() {
        new_block_ranges([1..=3, 2..=5]);
    }

    #[test]
    #[should_panic]
    fn test_header_range_creation_inverse() {
        new_block_ranges([1..=3, RangeInclusive::new(9, 5)]);
    }

    #[test]
    #[should_panic]
    fn test_header_range_creation_wrong_order() {
        new_block_ranges([10..=15, 1..=5]);
    }

    #[test]
    fn pop_head() {
        let mut ranges = new_block_ranges([]);
        assert_eq!(ranges.pop_head(), None);

        let mut ranges = new_block_ranges([1..=4, 6..=8, 10..=10]);
        assert_eq!(ranges.pop_head(), Some(10));
        assert_eq!(ranges.pop_head(), Some(8));
        assert_eq!(ranges.pop_head(), Some(7));
        assert_eq!(ranges.pop_head(), Some(6));
        assert_eq!(ranges.pop_head(), Some(4));
        assert_eq!(ranges.pop_head(), Some(3));
        assert_eq!(ranges.pop_head(), Some(2));
        assert_eq!(ranges.pop_head(), Some(1));
        assert_eq!(ranges.pop_head(), None);
    }

    #[test]
    fn pop_tail() {
        let mut ranges = new_block_ranges([]);
        assert_eq!(ranges.pop_tail(), None);

        let mut ranges = new_block_ranges([1..=4, 6..=8, 10..=10]);
        assert_eq!(ranges.pop_tail(), Some(1));
        assert_eq!(ranges.pop_tail(), Some(2));
        assert_eq!(ranges.pop_tail(), Some(3));
        assert_eq!(ranges.pop_tail(), Some(4));
        assert_eq!(ranges.pop_tail(), Some(6));
        assert_eq!(ranges.pop_tail(), Some(7));
        assert_eq!(ranges.pop_tail(), Some(8));
        assert_eq!(ranges.pop_tail(), Some(10));
        assert_eq!(ranges.pop_tail(), None);
    }

    #[test]
    fn block_ranges_iterator() {
        let ranges = new_block_ranges([1..=5, 10..=15]);
        let heights: Vec<_> = ranges.collect();
        assert_eq!(heights, vec![1, 2, 3, 4, 5, 10, 11, 12, 13, 14, 15]);

        let empty_heights: Vec<u64> = new_block_ranges([]).collect();
        assert_eq!(empty_heights, Vec::<u64>::new())
    }

    #[test]
    fn block_ranges_double_ended_iterator() {
        let ranges = new_block_ranges([1..=5, 10..=15]);
        let heights: Vec<_> = ranges.rev().collect();
        assert_eq!(heights, vec![15, 14, 13, 12, 11, 10, 5, 4, 3, 2, 1]);

        let empty_heights: Vec<u64> = new_block_ranges([]).collect();
        assert_eq!(empty_heights, Vec::<u64>::new())
    }

    #[test]
    fn validate_check() {
        (1..=1).validate().unwrap();
        (1..=2).validate().unwrap();
        (0..=0).validate().unwrap_err();
        (0..=1).validate().unwrap_err();
        #[allow(clippy::reversed_empty_ranges)]
        (2..=1).validate().unwrap_err();
    }

    #[test]
    fn adjacent_check() {
        assert!((3..=5).is_adjacent(&(1..=2)));
        assert!((3..=5).is_adjacent(&(6..=8)));

        assert!(!(3..=5).is_adjacent(&(1..=1)));
        assert!(!(3..=5).is_adjacent(&(7..=8)));
    }

    #[test]
    fn overlapping_check() {
        // equal
        assert!((3..=5).is_overlapping(&(3..=5)));

        // partial set
        assert!((3..=5).is_overlapping(&(1..=4)));
        assert!((3..=5).is_overlapping(&(1..=3)));
        assert!((3..=5).is_overlapping(&(4..=8)));
        assert!((3..=5).is_overlapping(&(5..=8)));

        // subset
        assert!((3..=5).is_overlapping(&(4..=4)));
        assert!((3..=5).is_overlapping(&(3..=4)));
        assert!((3..=5).is_overlapping(&(4..=5)));

        // superset
        assert!((3..=5).is_overlapping(&(1..=5)));
        assert!((3..=5).is_overlapping(&(1..=6)));
        assert!((3..=5).is_overlapping(&(3..=6)));
        assert!((3..=5).is_overlapping(&(4..=6)));

        // not overlapping
        assert!(!(3..=5).is_overlapping(&(1..=1)));
        assert!(!(3..=5).is_overlapping(&(7..=8)));
    }

    #[test]
    fn is_left_of_check() {
        // range is on the left of
        assert!((1..=2).is_left_of(&(3..=4)));
        assert!((1..=1).is_left_of(&(3..=4)));

        // range is on the right of
        assert!(!(3..=4).is_left_of(&(1..=2)));
        assert!(!(3..=4).is_left_of(&(1..=1)));

        // overlapping is not accepted
        assert!(!(1..=3).is_left_of(&(3..=4)));
        assert!(!(1..=5).is_left_of(&(3..=4)));
        assert!(!(3..=4).is_left_of(&(1..=3)));
    }

    #[test]
    fn left_of() {
        let r = new_block_ranges([10..=20, 30..=30, 40..=50]);

        assert_eq!(r.left_of(60), Some(50));
        assert_eq!(r.left_of(50), Some(49));
        assert_eq!(r.left_of(45), Some(44));
        assert_eq!(r.left_of(40), Some(30));
        assert_eq!(r.left_of(39), Some(30));
        assert_eq!(r.left_of(30), Some(20));
        assert_eq!(r.left_of(29), Some(20));
        assert_eq!(r.left_of(20), Some(19));
        assert_eq!(r.left_of(15), Some(14));
        assert_eq!(r.left_of(10), None);
        assert_eq!(r.left_of(9), None);
    }

    #[test]
    fn is_right_of_check() {
        // range is on the right of
        assert!((3..=4).is_right_of(&(1..=2)));
        assert!((3..=4).is_right_of(&(1..=1)));

        // range is on the left of
        assert!(!(1..=2).is_right_of(&(3..=4)));
        assert!(!(1..=1).is_right_of(&(3..=4)));

        // overlapping is not accepted
        assert!(!(1..=3).is_right_of(&(3..=4)));
        assert!(!(1..=5).is_right_of(&(3..=4)));
        assert!(!(3..=4).is_right_of(&(1..=3)));
    }

    #[test]
    fn right_of() {
        let r = new_block_ranges([10..=20, 30..=30, 40..=50]);

        assert_eq!(r.right_of(1), Some(10));
        assert_eq!(r.right_of(9), Some(10));
        assert_eq!(r.right_of(10), Some(11));
        assert_eq!(r.right_of(15), Some(16));
        assert_eq!(r.right_of(19), Some(20));
        assert_eq!(r.right_of(20), Some(30));
        assert_eq!(r.right_of(29), Some(30));
        assert_eq!(r.right_of(30), Some(40));
        assert_eq!(r.right_of(39), Some(40));
        assert_eq!(r.right_of(40), Some(41));
        assert_eq!(r.right_of(45), Some(46));
        assert_eq!(r.right_of(49), Some(50));
        assert_eq!(r.right_of(50), None);
        assert_eq!(r.right_of(60), None);
    }

    #[test]
    fn headn() {
        assert_eq!((1..=10).headn(u64::MAX), 1..=10);
        assert_eq!((1..=10).headn(20), 1..=10);
        assert_eq!((1..=10).headn(10), 1..=10);
        assert_eq!((1..=10).headn(5), 6..=10);
        assert_eq!((1..=10).headn(1), 10..=10);
        assert!((1..=10).headn(0).is_empty());

        assert_eq!((0..=u64::MAX).headn(u64::MAX), 1..=u64::MAX);
        assert_eq!((0..=u64::MAX).headn(1), u64::MAX..=u64::MAX);
        assert!((0..=u64::MAX).headn(0).is_empty());

        assert_eq!(
            new_block_ranges([1..=10]).headn(u64::MAX),
            new_block_ranges([1..=10])
        );
        assert_eq!(
            new_block_ranges([1..=10]).headn(20),
            new_block_ranges([1..=10])
        );
        assert_eq!(
            new_block_ranges([1..=10]).headn(10),
            new_block_ranges([1..=10])
        );
        assert_eq!(
            new_block_ranges([1..=10]).headn(5),
            new_block_ranges([6..=10])
        );
        assert_eq!(
            new_block_ranges([1..=10]).headn(1),
            new_block_ranges([10..=10])
        );
        assert!(new_block_ranges([1..=10]).headn(0).is_empty());

        assert_eq!(
            new_block_ranges([1..=5, 10..=14]).headn(u64::MAX),
            new_block_ranges([1..=5, 10..=14])
        );
        assert_eq!(
            new_block_ranges([1..=5, 10..=14]).headn(20),
            new_block_ranges([1..=5, 10..=14])
        );
        assert_eq!(
            new_block_ranges([1..=5, 10..=14]).headn(10),
            new_block_ranges([1..=5, 10..=14])
        );
        assert_eq!(
            new_block_ranges([1..=5, 10..=14]).headn(4),
            new_block_ranges([11..=14])
        );
        assert_eq!(
            new_block_ranges([1..=5, 10..=14]).headn(5),
            new_block_ranges([10..=14])
        );
        assert_eq!(
            new_block_ranges([1..=5, 10..=14]).headn(6),
            new_block_ranges([5..=5, 10..=14])
        );
        assert_eq!(
            new_block_ranges([1..=5, 10..=14]).headn(7),
            new_block_ranges([4..=5, 10..=14])
        );
        assert_eq!(
            new_block_ranges([1..=5, 10..=14]).headn(1),
            new_block_ranges([14..=14])
        );
        assert!(new_block_ranges([1..=5, 10..=14]).headn(0).is_empty());
    }

    #[test]
    fn tailn() {
        assert_eq!((1..=10).tailn(20), 1..=10);
        assert_eq!((1..=10).tailn(10), 1..=10);
        assert_eq!((1..=10).tailn(5), 1..=5);
        assert_eq!((1..=10).tailn(1), 1..=1);
        assert!((1..=10).tailn(0).is_empty());

        assert_eq!((0..=u64::MAX).tailn(u64::MAX), 0..=(u64::MAX - 1));
        assert_eq!((0..=u64::MAX).tailn(1), 0..=0);
        assert!((0..=u64::MAX).tailn(0).is_empty());

        assert_eq!(
            new_block_ranges([1..=10]).tailn(u64::MAX),
            new_block_ranges([1..=10])
        );
        assert_eq!(
            new_block_ranges([1..=10]).tailn(20),
            new_block_ranges([1..=10])
        );
        assert_eq!(
            new_block_ranges([1..=10]).tailn(10),
            new_block_ranges([1..=10])
        );
        assert_eq!(
            new_block_ranges([1..=10]).tailn(5),
            new_block_ranges([1..=5])
        );
        assert_eq!(
            new_block_ranges([1..=10]).tailn(1),
            new_block_ranges([1..=1])
        );
        assert!(new_block_ranges([1..=10]).tailn(0).is_empty());

        assert_eq!(
            new_block_ranges([1..=5, 10..=14]).tailn(u64::MAX),
            new_block_ranges([1..=5, 10..=14])
        );
        assert_eq!(
            new_block_ranges([1..=5, 10..=14]).tailn(20),
            new_block_ranges([1..=5, 10..=14])
        );
        assert_eq!(
            new_block_ranges([1..=5, 10..=14]).tailn(10),
            new_block_ranges([1..=5, 10..=14])
        );
        assert_eq!(
            new_block_ranges([1..=5, 10..=14]).tailn(4),
            new_block_ranges([1..=4])
        );
        assert_eq!(
            new_block_ranges([1..=5, 10..=14]).tailn(5),
            new_block_ranges([1..=5])
        );
        assert_eq!(
            new_block_ranges([1..=5, 10..=14]).tailn(6),
            new_block_ranges([1..=5, 10..=10])
        );
        assert_eq!(
            new_block_ranges([1..=5, 10..=14]).tailn(7),
            new_block_ranges([1..=5, 10..=11])
        );
        assert_eq!(
            new_block_ranges([1..=5, 10..=14]).tailn(1),
            new_block_ranges([1..=1])
        );
        assert!(new_block_ranges([1..=5, 10..=14]).tailn(0).is_empty());
    }

    #[test]
    fn find_affected_ranges() {
        let ranges = new_block_ranges([30..=50, 80..=100, 130..=150]);

        assert_eq!(ranges.find_affected_ranges(28..=28), None);
        assert_eq!(ranges.find_affected_ranges(1..=15), None);
        assert_eq!(ranges.find_affected_ranges(1..=28), None);
        assert_eq!(ranges.find_affected_ranges(3..=28), None);
        assert_eq!(ranges.find_affected_ranges(1..=29), Some((0, 0)));
        assert_eq!(ranges.find_affected_ranges(1..=30), Some((0, 0)));
        assert_eq!(ranges.find_affected_ranges(1..=49), Some((0, 0)));
        assert_eq!(ranges.find_affected_ranges(1..=50), Some((0, 0)));
        assert_eq!(ranges.find_affected_ranges(1..=51), Some((0, 0)));

        assert_eq!(ranges.find_affected_ranges(40..=51), Some((0, 0)));
        assert_eq!(ranges.find_affected_ranges(50..=51), Some((0, 0)));
        assert_eq!(ranges.find_affected_ranges(51..=51), Some((0, 0)));

        assert_eq!(ranges.find_affected_ranges(40..=79), Some((0, 1)));
        assert_eq!(ranges.find_affected_ranges(50..=79), Some((0, 1)));
        assert_eq!(ranges.find_affected_ranges(51..=79), Some((0, 1)));
        assert_eq!(ranges.find_affected_ranges(50..=80), Some((0, 1)));

        assert_eq!(ranges.find_affected_ranges(52..=52), None);
        assert_eq!(ranges.find_affected_ranges(52..=78), None);
        assert_eq!(ranges.find_affected_ranges(52..=79), Some((1, 1)));
        assert_eq!(ranges.find_affected_ranges(52..=80), Some((1, 1)));
        assert_eq!(ranges.find_affected_ranges(52..=129), Some((1, 2)));
        assert_eq!(ranges.find_affected_ranges(99..=129), Some((1, 2)));
        assert_eq!(ranges.find_affected_ranges(100..=129), Some((1, 2)));
        assert_eq!(ranges.find_affected_ranges(101..=129), Some((1, 2)));
        assert_eq!(ranges.find_affected_ranges(101..=128), Some((1, 1)));
        assert_eq!(ranges.find_affected_ranges(51..=129), Some((0, 2)));

        assert_eq!(ranges.find_affected_ranges(102..=128), None);
        assert_eq!(ranges.find_affected_ranges(102..=120), None);
        assert_eq!(ranges.find_affected_ranges(120..=128), None);

        assert_eq!(ranges.find_affected_ranges(40..=129), Some((0, 2)));
        assert_eq!(ranges.find_affected_ranges(40..=140), Some((0, 2)));
        assert_eq!(ranges.find_affected_ranges(20..=140), Some((0, 2)));
        assert_eq!(ranges.find_affected_ranges(20..=150), Some((0, 2)));
        assert_eq!(ranges.find_affected_ranges(20..=151), Some((0, 2)));
        assert_eq!(ranges.find_affected_ranges(20..=160), Some((0, 2)));

        assert_eq!(ranges.find_affected_ranges(120..=129), Some((2, 2)));
        assert_eq!(ranges.find_affected_ranges(120..=128), None);
        assert_eq!(ranges.find_affected_ranges(120..=130), Some((2, 2)));
        assert_eq!(ranges.find_affected_ranges(120..=131), Some((2, 2)));
        assert_eq!(ranges.find_affected_ranges(140..=145), Some((2, 2)));
        assert_eq!(ranges.find_affected_ranges(140..=150), Some((2, 2)));
        assert_eq!(ranges.find_affected_ranges(140..=155), Some((2, 2)));
        assert_eq!(ranges.find_affected_ranges(152..=155), None);
        assert_eq!(ranges.find_affected_ranges(152..=178), None);
        assert_eq!(ranges.find_affected_ranges(152..=152), None);

        assert_eq!(new_block_ranges([]).find_affected_ranges(1..=1), None);

        assert_eq!(new_block_ranges([1..=2]).find_affected_ranges(6..=9), None);
        assert_eq!(new_block_ranges([4..=8]).find_affected_ranges(1..=2), None);
        assert_eq!(
            new_block_ranges([4..=8, 20..=30]).find_affected_ranges(1..=2),
            None
        );
        assert_eq!(
            new_block_ranges([4..=8, 20..=30]).find_affected_ranges(10..=12),
            None
        );
        assert_eq!(
            new_block_ranges([4..=8, 20..=30]).find_affected_ranges(32..=32),
            None
        );
    }

    #[test]
    fn insert_relaxed_disjoined() {
        let mut r = BlockRanges::default();
        r.insert_relaxed(10..=10).unwrap();
        assert_eq!(&r.0[..], &[10..=10][..]);

        let ranges = new_block_ranges([30..=50, 80..=100, 130..=150]);

        let mut r = ranges.clone();
        r.insert_relaxed(1..=1).unwrap();
        assert_eq!(&r.0[..], &[1..=1, 30..=50, 80..=100, 130..=150][..]);

        let mut r = ranges.clone();
        r.insert_relaxed(1..=28).unwrap();
        assert_eq!(&r.0[..], &[1..=28, 30..=50, 80..=100, 130..=150][..]);

        let mut r = ranges.clone();
        r.insert_relaxed(10..=28).unwrap();
        assert_eq!(&r.0[..], &[10..=28, 30..=50, 80..=100, 130..=150][..]);

        let mut r = ranges.clone();
        r.insert_relaxed(52..=78).unwrap();
        assert_eq!(&r.0[..], &[30..=50, 52..=78, 80..=100, 130..=150][..]);

        let mut r = ranges.clone();
        r.insert_relaxed(102..=128).unwrap();
        assert_eq!(&r.0[..], &[30..=50, 80..=100, 102..=128, 130..=150][..]);

        let mut r = ranges.clone();
        r.insert_relaxed(152..=152).unwrap();
        assert_eq!(&r.0[..], &[30..=50, 80..=100, 130..=150, 152..=152][..]);

        let mut r = ranges.clone();
        r.insert_relaxed(152..=170).unwrap();
        assert_eq!(&r.0[..], &[30..=50, 80..=100, 130..=150, 152..=170][..]);

        let mut r = ranges.clone();
        r.insert_relaxed(160..=170).unwrap();
        assert_eq!(&r.0[..], &[30..=50, 80..=100, 130..=150, 160..=170][..]);
    }

    #[test]
    fn insert_relaxed_intersected() {
        let ranges = new_block_ranges([30..=50, 80..=100, 130..=150]);

        let mut r = ranges.clone();
        r.insert_relaxed(29..=29).unwrap();
        assert_eq!(&r.0[..], &[29..=50, 80..=100, 130..=150][..]);

        let mut r = ranges.clone();
        r.insert_relaxed(1..=29).unwrap();
        assert_eq!(&r.0[..], &[1..=50, 80..=100, 130..=150][..]);

        let mut r = ranges.clone();
        r.insert_relaxed(29..=35).unwrap();
        assert_eq!(&r.0[..], &[29..=50, 80..=100, 130..=150][..]);

        let mut r = ranges.clone();
        r.insert_relaxed(29..=55).unwrap();
        assert_eq!(&r.0[..], &[29..=55, 80..=100, 130..=150][..]);

        let mut r = ranges.clone();
        r.insert_relaxed(29..=78).unwrap();
        assert_eq!(&r.0[..], &[29..=78, 80..=100, 130..=150][..]);

        let mut r = ranges.clone();
        r.insert_relaxed(29..=79).unwrap();
        assert_eq!(&r.0[..], &[29..=100, 130..=150][..]);

        let mut r = ranges.clone();
        r.insert_relaxed(30..=79).unwrap();
        assert_eq!(&r.0[..], &[30..=100, 130..=150][..]);

        let mut r = ranges.clone();
        r.insert_relaxed(30..=150).unwrap();
        assert_eq!(&r.0[..], &[30..=150][..]);

        let mut r = ranges.clone();
        r.insert_relaxed(10..=170).unwrap();
        assert_eq!(&r.0[..], &[10..=170][..]);

        let mut r = ranges.clone();
        r.insert_relaxed(85..=129).unwrap();
        assert_eq!(&r.0[..], &[30..=50, 80..=150][..]);

        let mut r = ranges.clone();
        r.insert_relaxed(85..=129).unwrap();
        assert_eq!(&r.0[..], &[30..=50, 80..=150][..]);

        let mut r = ranges.clone();
        r.insert_relaxed(135..=170).unwrap();
        assert_eq!(&r.0[..], &[30..=50, 80..=100, 130..=170][..]);

        let mut r = ranges.clone();
        r.insert_relaxed(151..=170).unwrap();
        assert_eq!(&r.0[..], &[30..=50, 80..=100, 130..=170][..]);

        let mut r = new_block_ranges([1..=2, 4..=6, 8..=10, 15..=20, 80..=100]);
        r.insert_relaxed(3..=79).unwrap();
        assert_eq!(&r.0[..], &[1..=100][..]);
    }

    #[test]
    fn remove_relaxed() {
        let ranges = new_block_ranges([30..=50, 80..=100, 130..=150]);

        let mut r = ranges.clone();
        r.remove_relaxed(29..=29).unwrap();
        assert_eq!(&r.0[..], &[30..=50, 80..=100, 130..=150][..]);

        let mut r = ranges.clone();
        r.remove_relaxed(30..=30).unwrap();
        assert_eq!(&r.0[..], &[31..=50, 80..=100, 130..=150][..]);

        let mut r = ranges.clone();
        r.remove_relaxed(20..=40).unwrap();
        assert_eq!(&r.0[..], &[41..=50, 80..=100, 130..=150][..]);

        let mut r = ranges.clone();
        r.remove_relaxed(35..=40).unwrap();
        assert_eq!(&r.0[..], &[30..=34, 41..=50, 80..=100, 130..=150][..]);

        let mut r = ranges.clone();
        r.remove_relaxed(51..=129).unwrap();
        assert_eq!(&r.0[..], &[30..=50, 130..=150][..]);

        let mut r = ranges.clone();
        r.remove_relaxed(50..=130).unwrap();
        assert_eq!(&r.0[..], &[30..=49, 131..=150][..]);

        let mut r = ranges.clone();
        r.remove_relaxed(35..=49).unwrap();
        assert_eq!(&r.0[..], &[30..=34, 50..=50, 80..=100, 130..=150][..]);

        let mut r = ranges.clone();
        r.remove_relaxed(35..=50).unwrap();
        assert_eq!(&r.0[..], &[30..=34, 80..=100, 130..=150][..]);

        let mut r = ranges.clone();
        r.remove_relaxed(35..=55).unwrap();
        assert_eq!(&r.0[..], &[30..=34, 80..=100, 130..=150][..]);

        let mut r = ranges.clone();
        r.remove_relaxed(35..=135).unwrap();
        assert_eq!(&r.0[..], &[30..=34, 136..=150][..]);

        let mut r = ranges.clone();
        r.remove_relaxed(35..=170).unwrap();
        assert_eq!(&r.0[..], &[30..=34][..]);

        let mut r = ranges.clone();
        r.remove_relaxed(10..=135).unwrap();
        assert_eq!(&r.0[..], &[136..=150][..]);

        let mut r = ranges.clone();
        r.remove_relaxed(10..=170).unwrap();
        assert!(r.0.is_empty());

        let mut r = new_block_ranges([1..=10, 12..=12, 14..=14]);
        r.remove_relaxed(12..=12).unwrap();
        assert_eq!(&r.0[..], &[1..=10, 14..=14][..]);

        let mut r = new_block_ranges([1..=u64::MAX]);
        r.remove_relaxed(12..=12).unwrap();
        assert_eq!(&r.0[..], &[1..=11, 13..=u64::MAX][..]);

        let mut r = new_block_ranges([1..=u64::MAX]);
        r.remove_relaxed(1..=1).unwrap();
        assert_eq!(&r.0[..], &[2..=u64::MAX][..]);

        let mut r = new_block_ranges([1..=u64::MAX]);
        r.remove_relaxed(u64::MAX..=u64::MAX).unwrap();
        assert_eq!(&r.0[..], &[1..=u64::MAX - 1][..]);
    }

    #[test]
    fn edges() {
        let r = BlockRanges::default();
        let edges = r.edges();
        assert!(edges.is_empty());

        let r = new_block_ranges([6..=6, 8..=100, 200..=201, 300..=310]);
        let edges = r.edges();
        assert_eq!(
            &edges.0[..],
            &[6..=6, 8..=8, 100..=100, 200..=201, 300..=300, 310..=310][..]
        );
    }

    #[test]
    fn inverse() {
        let inverse = !BlockRanges::new();
        assert_eq!(&inverse.0[..], &[1..=u64::MAX][..]);

        let inverse = !new_block_ranges([1..=u64::MAX]);
        assert!(inverse.is_empty());

        let inverse = !new_block_ranges([10..=u64::MAX]);
        assert_eq!(&inverse.0[..], &[1..=9][..]);

        let inverse = !new_block_ranges([1..=9]);
        assert_eq!(&inverse.0[..], &[10..=u64::MAX][..]);

        let inverse = !new_block_ranges([10..=100, 200..=200, 400..=600, 700..=800, 900..=900]);
        assert_eq!(
            &inverse.0[..],
            &[
                1..=9,
                101..=199,
                201..=399,
                601..=699,
                801..=899,
                901..=u64::MAX
            ][..]
        );
        assert_eq!(
            &(!inverse).0[..],
            &[10..=100, 200..=200, 400..=600, 700..=800, 900..=900][..]
        );

        let inverse = !new_block_ranges([1..=100, 200..=200, 400..=600, 700..=800, 900..=900]);
        assert_eq!(
            &inverse.0[..],
            &[101..=199, 201..=399, 601..=699, 801..=899, 901..=u64::MAX][..]
        );
        assert_eq!(
            &(!inverse).0[..],
            &[1..=100, 200..=200, 400..=600, 700..=800, 900..=900][..]
        );

        let inverse = !new_block_ranges([2..=100, 400..=600, 700..=(u64::MAX - 1)]);
        assert_eq!(
            &inverse.0[..],
            &[1..=1, 101..=399, 601..=699, u64::MAX..=u64::MAX][..]
        );
        assert_eq!(
            &(!inverse).0[..],
            &[2..=100, 400..=600, 700..=(u64::MAX - 1)][..]
        );
    }

    #[test]
    fn bitwise_and() {
        let a = new_block_ranges([10..=100, 200..=200, 400..=600, 700..=800, 900..=900]);
        let b = new_block_ranges([50..=50, 55..=250, 300..=750, 800..=1000]);

        assert_eq!(
            a & b,
            new_block_ranges([
                50..=50,
                55..=100,
                200..=200,
                400..=600,
                700..=750,
                800..=800,
                900..=900
            ])
        );
    }

    #[test]
    fn partition() {
        assert!(BlockRanges::new().partitions().is_none());

        let ranges = new_block_ranges([1..=101]);
        let (left, middle, right) = ranges.partitions().unwrap();
        assert_eq!(left, new_block_ranges([1..=50]));
        assert_eq!(middle, 51);
        assert_eq!(right, new_block_ranges([52..=101]));

        let ranges = new_block_ranges([10..=10]);
        let (left, middle, right) = ranges.partitions().unwrap();
        assert!(left.is_empty());
        assert_eq!(middle, 10);
        assert!(right.is_empty());

        let ranges = new_block_ranges([10..=14, 20..=24, 100..=109]);
        let (left, middle, right) = ranges.partitions().unwrap();
        assert_eq!(left, new_block_ranges([10..=14, 20..=23]));
        assert_eq!(middle, 24);
        assert_eq!(right, new_block_ranges([100..=109]));

        let ranges = new_block_ranges([10..=19, 90..=94, 100..=104]);
        let (left, middle, right) = ranges.partitions().unwrap();
        assert_eq!(left, new_block_ranges([10..=18]));
        assert_eq!(middle, 19);
        assert_eq!(right, new_block_ranges([90..=94, 100..=104]));

        let ranges = new_block_ranges([10..=14, 20..=25, 100..=109]);
        let (left, middle, right) = ranges.partitions().unwrap();
        assert_eq!(left, new_block_ranges([10..=14, 20..=24]));
        assert_eq!(middle, 25);
        assert_eq!(right, new_block_ranges([100..=109]));

        let ranges = new_block_ranges([10..=14, 20..=24, 99..=109]);
        let (left, middle, right) = ranges.partitions().unwrap();
        assert_eq!(left, new_block_ranges([10..=14, 20..=24]));
        assert_eq!(middle, 99);
        assert_eq!(right, new_block_ranges([100..=109]));

        let ranges = new_block_ranges([10..=14, 20..=24, 30..=30, 100..=109]);
        let (left, middle, right) = ranges.partitions().unwrap();
        assert_eq!(left, new_block_ranges([10..=14, 20..=24]));
        assert_eq!(middle, 30);
        assert_eq!(right, new_block_ranges([100..=109]));

        let ranges = new_block_ranges([10..=19, 30..=30, 90..=94, 100..=104]);
        let (left, middle, right) = ranges.partitions().unwrap();
        assert_eq!(left, new_block_ranges([10..=19]));
        assert_eq!(middle, 30);
        assert_eq!(right, new_block_ranges([90..=94, 100..=104]));

        let ranges = new_block_ranges([10..=14, 20..=24, 30..=31, 100..=109]);
        let (left, middle, right) = ranges.partitions().unwrap();
        assert_eq!(left, new_block_ranges([10..=14, 20..=24, 30..=30]));
        assert_eq!(middle, 31);
        assert_eq!(right, new_block_ranges([100..=109]));

        let ranges = new_block_ranges([10..=19, 30..=31, 90..=94, 100..=104]);
        let (left, middle, right) = ranges.partitions().unwrap();
        assert_eq!(left, new_block_ranges([10..=19, 30..=30]));
        assert_eq!(middle, 31);
        assert_eq!(right, new_block_ranges([90..=94, 100..=104]));

        let ranges = new_block_ranges([10..=14, 20..=24, 30..=32, 100..=109]);
        let (left, middle, right) = ranges.partitions().unwrap();
        assert_eq!(left, new_block_ranges([10..=14, 20..=24, 30..=30]));
        assert_eq!(middle, 31);
        assert_eq!(right, new_block_ranges([32..=32, 100..=109]));

        let ranges = new_block_ranges([10..=19, 30..=32, 90..=94, 100..=104]);
        let (left, middle, right) = ranges.partitions().unwrap();
        assert_eq!(left, new_block_ranges([10..=19, 30..=30]));
        assert_eq!(middle, 31);
        assert_eq!(right, new_block_ranges([32..=32, 90..=94, 100..=104]));
    }

    #[test]
    fn block_ranges_len() {
        let r = BlockRanges::default();
        assert!(r.is_empty());
        assert_eq!(r.len(), 0);
        assert_eq!(r.count(), 0);

        let r = new_block_ranges([4..=4]);
        assert!(!r.is_empty());
        assert_eq!(r.len(), 1);

        let r = new_block_ranges([4..=4, 100..=101]);
        assert!(!r.is_empty());
        assert_eq!(r.len(), 3);
        assert_eq!(r.count(), 3);

        let r = new_block_ranges([250..=300]);
        assert!(!r.is_empty());
        assert_eq!(r.len(), 51);
        assert_eq!(r.count(), 51);

        let r = new_block_ranges([4..=4, 100..=101, 250..=300]);
        assert!(!r.is_empty());
        assert_eq!(r.len(), 54);
        assert_eq!(r.count(), 54);

        let r = new_block_ranges([4..=10, 100..=101, 250..=300]);
        assert!(!r.is_empty());
        assert_eq!(r.len(), 60);
        assert_eq!(r.count(), 60);

        let r = new_block_ranges([4..=4, 100..=101, 250..=300, 500..=500]);
        assert!(!r.is_empty());
        assert_eq!(r.len(), 55);
        assert_eq!(r.count(), 55);
    }
}
