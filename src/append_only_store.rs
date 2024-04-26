#[cfg(not(loom))]
use loom_compat_unsafecell::UnsafeCell;
#[cfg(not(loom))]
use std::alloc::{alloc, dealloc, Layout};
#[cfg(not(loom))]
use std::sync::atomic::{AtomicUsize, Ordering};

#[cfg(loom)]
use loom::alloc::{alloc, dealloc, Layout};
#[cfg(loom)]
use loom::cell::UnsafeCell;
#[cfg(loom)]
use loom::sync::atomic::{AtomicUsize, Ordering};

#[cfg(not(loom))]
mod loom_compat_unsafecell {
    #[derive(Debug)]
    pub(crate) struct UnsafeCell<T>(std::cell::UnsafeCell<T>);

    impl<T> UnsafeCell<T> {
        pub(crate) fn new(data: T) -> UnsafeCell<T> {
            UnsafeCell(std::cell::UnsafeCell::new(data))
        }

        pub(crate) fn with<R>(&self, f: impl FnOnce(*const T) -> R) -> R {
            f(self.0.get())
        }

        pub(crate) fn with_mut<R>(&self, f: impl FnOnce(*mut T) -> R) -> R {
            f(self.0.get())
        }
    }
}

pub struct AppendOnlyStore<T> {
    claimed: AtomicUsize,
    writes: AtomicUsize,
    shards_allocated: AtomicUsize,
    max_capacity: usize,
    base_size: usize,
    shards: [UnsafeCell<*mut UnsafeCell<T>>; 16],
}

impl<T> AppendOnlyStore<T> {
    pub fn new(base_size: usize) -> Self {
        assert!(std::mem::size_of::<T>() != 0, "not suitable for ZSTs");
        assert!(base_size > 0, "base size == 0");

        let max_capacity = ((1usize << 16) - 1)
            .checked_mul(base_size)
            .expect("max capacity overflow");

        let first_slice = {
            let layout = Layout::array::<UnsafeCell<T>>(base_size).unwrap();

            // SAFETY: allocated with correct layout.
            unsafe { alloc(layout) as *mut UnsafeCell<T> }
        };

        let shards = [
            UnsafeCell::new(first_slice),
            UnsafeCell::new(std::ptr::null_mut()),
            UnsafeCell::new(std::ptr::null_mut()),
            UnsafeCell::new(std::ptr::null_mut()),
            UnsafeCell::new(std::ptr::null_mut()),
            UnsafeCell::new(std::ptr::null_mut()),
            UnsafeCell::new(std::ptr::null_mut()),
            UnsafeCell::new(std::ptr::null_mut()),
            UnsafeCell::new(std::ptr::null_mut()),
            UnsafeCell::new(std::ptr::null_mut()),
            UnsafeCell::new(std::ptr::null_mut()),
            UnsafeCell::new(std::ptr::null_mut()),
            UnsafeCell::new(std::ptr::null_mut()),
            UnsafeCell::new(std::ptr::null_mut()),
            UnsafeCell::new(std::ptr::null_mut()),
            UnsafeCell::new(std::ptr::null_mut()),
        ];

        AppendOnlyStore {
            claimed: AtomicUsize::new(0),
            writes: AtomicUsize::new(0),
            shards_allocated: AtomicUsize::new(0),
            max_capacity,
            shards,
            base_size,
        }
    }

    pub fn push(&self, item: T) -> usize {
        // 1. claim a new position. this may be in unallocated space.
        let claimed = self.claimed.fetch_add(1, Ordering::Relaxed);
        let item_ptr: *mut UnsafeCell<T> = {
            // if we are at the boundary of a slice, attempt to allocate
            // and swap.
            let shard_info = shard_info(claimed, self.base_size);

            // The first thread at a claim position is responsible for allocating the buffer.
            // 2. allocate if necessary.
            if claimed != 0 && shard_info.is_start() {
                let layout =
                    Layout::array::<UnsafeCell<T>>(self.base_size << shard_info.shard_index)
                        .unwrap();
                // SAFETY: allocated with the correct layout.
                let shard_ptr = unsafe { alloc(layout) as *mut UnsafeCell<T> };
                // SAFETY: `claimed` is atomic and only this value writes to this shard.
                self.shards[shard_info.shard_index].with_mut(
                    |ptr: *mut *mut UnsafeCell<T>| unsafe {
                        std::ptr::write(ptr, shard_ptr);
                    },
                );

                loop {
                    // SAFETY: release ordering prevents reordering with the write to self.shards.
                    if self
                        .shards_allocated
                        .compare_exchange(
                            shard_info.shard_index - 1,
                            shard_info.shard_index,
                            Ordering::Release,
                            Ordering::Relaxed,
                        )
                        .is_ok()
                    {
                        break;
                    }
                }

                shard_ptr
            } else {
                // spin until allocation has completed.
                // SAFETY: Acquire ordering prevents reordering and pairs with the release, so the write to
                //         self.shards[shard_index] is seen.
                while shard_info.shard_index != 0
                    && self.shards_allocated.load(Ordering::Acquire) < shard_info.shard_index
                {
                    #[cfg(loom)]
                    loom::thread::yield_now();
                }

                // SAFETY: shard buffer is guaranteed to be allocated at this point and claims
                //         are unique.
                self.shards[shard_info.shard_index].with(
                    |shard: *const *mut UnsafeCell<T>| unsafe {
                        (*shard).offset(shard_info.sub_index as isize)
                    },
                )
            }
        };

        // 3. write to the position.
        // SAFETY: buffer is guaranteed to be allocated above and writes cannot be reordered above.
        unsafe {
            std::ptr::write(item_ptr, UnsafeCell::new(item));
        }

        // SAFETY: propagate writes to reader threads.
        self.writes.fetch_add(1, Ordering::Release);

        claimed
    }

    /// Get an item with the given index.
    ///
    /// # Safety
    ///
    /// This may only be called with an index which was previously returned by `push` on the same
    /// instance of the struct. Anything else may lead to reading uninitialized or partially-written
    /// memory.
    pub unsafe fn get(&self, index: usize) -> &T {
        self.writes.fetch_add(1, Ordering::Acquire);

        let shard_info = shard_info(index, self.base_size);

        // SAFETY: Acquire load above pairs with the release of self.writes in `push`,
        //         which always follows writing to the shard pointer. And by the contract
        //         of this function, `index` must have been returned from `push` _after_
        //         that release. qed
        self.shards[shard_info.shard_index].with(|shard: *const *mut UnsafeCell<T>| unsafe {
            let item_ptr: *mut UnsafeCell<T> = (*shard).offset(shard_info.sub_index as isize);
            (*item_ptr).with(|inner: *const T| &*inner)
        })
    }
}

impl<T> Drop for AppendOnlyStore<T> {
    fn drop(&mut self) {
        // SAFETY: this ensures all writes from other threads have propagated.
        //         we have mutable access, therefore there are no ongoing writes.
        //         therefore there are no gaps in our arrays.
        let _writes = self.writes.load(Ordering::Acquire);
        let claimed = self.claimed.load(Ordering::Relaxed);
        let shard_info = shard_info(claimed, self.base_size);

        let drop_and_deallocate_nonempty = |ptr: *mut UnsafeCell<T>, len, size| unsafe {
            {
                // SAFETY: len is only ever incremented immediately prior to
                // an unconditional write to that position.
                // we have mutable access, therefore all writes have concluded
                let slice = std::slice::from_raw_parts_mut(ptr, len);
                std::ptr::drop_in_place(slice as *mut [UnsafeCell<T>]);
            }

            // SAFETY: we only increment len after allocating and writing the
            // array pointer.
            let layout = std::alloc::Layout::array::<UnsafeCell<T>>(size).unwrap();
            dealloc(ptr as *mut u8, layout);
        };

        // there are 0 or more full shards and 0 or 1 partially full shards
        // drop all the data in all of the shards.
        for full_shard in 0..shard_info.shard_index.saturating_sub(1) {
            let shard_size = (1 << full_shard) * self.base_size;

            // SAFETY: protected by synchronization with self.writes and mutable
            // access to self.
            self.shards[full_shard].with_mut(|shard: *mut *mut UnsafeCell<T>| unsafe {
                drop_and_deallocate_nonempty(*shard, shard_size, shard_size)
            });
        }

        // SAFETY: sub_index of zero means unallocated and unpopulated.
        if shard_info.sub_index != 0 {
            let shard_size = (1 << shard_info.shard_index) * self.base_size;
            self.shards[shard_info.shard_index].with_mut(|shard: *mut *mut UnsafeCell<T>| unsafe {
                drop_and_deallocate_nonempty(*shard, shard_size, shard_info.sub_index)
            });
        }
    }
}

// SAFETY: values may be read from any thread and dropped in (and therefore sent to) any thread.
unsafe impl<T: Send + Sync> Send for AppendOnlyStore<T> {}

// SAFETY: values may be read from any thread.
unsafe impl<T: Sync> Sync for AppendOnlyStore<T> {}

#[derive(Debug, PartialEq)]
struct ShardInfo {
    shard_index: usize,
    sub_index: usize,
}

impl ShardInfo {
    fn is_start(&self) -> bool {
        self.sub_index == 0
    }
}

fn shard_info(len: usize, base_size: usize) -> ShardInfo {
    // shard index i stores base_size * 2^i items

    // shard 1 can have 1 or 2 normalized
    // shard 2 can have 3, 4, 5, or 6 normalized

    // 0 -> shard 0
    //
    // 1 -> shard 1
    // 10 -> shard 1
    //
    // 11 -> shard 2
    // 100 -> shard 2
    // 101 -> shard 2
    // 110 -> shard 2
    //
    // 111 -> shard 3
    // 1000 -> shard 3
    // 1001 -> shard 3
    // 1010 -> shard 3
    // 1011 -> shard 3
    // 1100 -> shard 3
    // 1101 -> shard 3
    // 1110 -> shard 3
    //
    // so on
    let base_multiples = len / base_size;
    let remainder = len % base_size;

    let shard_index = usize::BITS as usize - (base_multiples + 1).leading_zeros() as usize - 1;

    let remaining_base_multiples = if shard_index == 0 {
        0
    } else {
        base_multiples.saturating_sub((1 << shard_index) - 1)
    };
    ShardInfo {
        shard_index,
        sub_index: base_size * remaining_base_multiples + remainder,
    }
}

#[test]
fn shard_info_works() {
    assert_eq!(
        shard_info(0, 10),
        ShardInfo {
            shard_index: 0,
            sub_index: 0
        }
    );
    assert_eq!(
        shard_info(9, 10),
        ShardInfo {
            shard_index: 0,
            sub_index: 9
        }
    );

    assert_eq!(
        shard_info(10, 10),
        ShardInfo {
            shard_index: 1,
            sub_index: 0
        }
    );
    assert_eq!(
        shard_info(20, 10),
        ShardInfo {
            shard_index: 1,
            sub_index: 10
        }
    );
    assert_eq!(
        shard_info(29, 10),
        ShardInfo {
            shard_index: 1,
            sub_index: 19
        }
    );

    assert_eq!(
        shard_info(30, 10),
        ShardInfo {
            shard_index: 2,
            sub_index: 0
        }
    );
}
