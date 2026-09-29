use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

use celestia_types::{Blob, Error, Share, nmt::Namespace};

struct TrackingAllocator;
static MAX_ALLOCATION: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for TrackingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        MAX_ALLOCATION.fetch_max(layout.size(), Ordering::Relaxed);
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static ALLOCATOR: TrackingAllocator = TrackingAllocator;

#[test]
fn missing_continuations_do_not_allocate_declared_size() {
    let blob = Blob::new(Namespace::new_v0(&[1, 2, 3]).unwrap(), vec![1], None).unwrap();
    let mut raw = blob.to_shares().unwrap()[0].to_vec();
    raw[30..34].copy_from_slice(&(16_u32 * 1024 * 1024).to_be_bytes());
    let share = Share::from_raw(&raw).unwrap();
    MAX_ALLOCATION.store(0, Ordering::Relaxed);

    assert!(matches!(
        Blob::reconstruct([&share]),
        Err(Error::MissingShares)
    ));
    assert!(MAX_ALLOCATION.load(Ordering::Relaxed) < 1024 * 1024);
}
