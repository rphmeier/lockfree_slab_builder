use std::sync::atomic::{AtomicUsize, AtomicPtr, Ordering};
use std::alloc::Layout;

pub struct AppendOnlyStore<T> {
    len: AtomicUsize,
    max_capacity: usize,
    base_size: usize,
    shards: [AtomicPtr<T>; 16],
}

impl<T> AppendOnlyStore<T> {
    pub fn new(base_size: usize) -> Self {
        assert!(std::mem::size_of::<T>() != 0, "not suitable for ZSTs");
        assert!(base_size > 0, "base size == 0");
        
        let max_capacity = ((1usize << 16) - 1)
            .checked_mul(base_size)
            .expect("max capacity overflow");
        
        let first_slice = {
            let layout = Layout::array::<T>(base_size).unwrap();
            
            // SAFETY: allocated with correct layout.
            unsafe { std::alloc::alloc(layout) as *mut T }
        };
        
        let shards = [
            AtomicPtr::new(first_slice),
            AtomicPtr::new(std::ptr::null_mut()),
            AtomicPtr::new(std::ptr::null_mut()),
            AtomicPtr::new(std::ptr::null_mut()),
            AtomicPtr::new(std::ptr::null_mut()),
            AtomicPtr::new(std::ptr::null_mut()),
            AtomicPtr::new(std::ptr::null_mut()),
            AtomicPtr::new(std::ptr::null_mut()),
            AtomicPtr::new(std::ptr::null_mut()),
            AtomicPtr::new(std::ptr::null_mut()),
            AtomicPtr::new(std::ptr::null_mut()),
            AtomicPtr::new(std::ptr::null_mut()),
            AtomicPtr::new(std::ptr::null_mut()),
            AtomicPtr::new(std::ptr::null_mut()),
            AtomicPtr::new(std::ptr::null_mut()),
            AtomicPtr::new(std::ptr::null_mut()),
        ];
        
        AppendOnlyStore {
            len: AtomicUsize::new(0),
            max_capacity,
            shards,
            base_size,
        }
    }
    
    pub fn push(&self, item: T) -> usize {
        loop {
            let len = self.len.load(Ordering::Relaxed);
            // if we are at the boundary of a slice, attempt to allocate
            // and swap.
            let shard_info = shard_info(len, self.base_size);
            if len != 0 && shard_info.is_start() {
                if !self.shards[shard_info.shard_index].load(Ordering::Relaxed).is_null() {
                    // another thread allocated successfully.
                    // spin until len is bumped by that thread.
                    continue
                }
                
                let layout = Layout::array::<T>(self.base_size << shard_info.shard_index).unwrap();
                // SAFETY: allocated with the correct layout.
                let slice_ptr = unsafe { std::alloc::alloc(layout) as *mut T };
                
                let compare_exchange_result = self.shards[shard_info.shard_index].compare_exchange(
                    std::ptr::null_mut(),
                    slice_ptr,
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                );
                
                if compare_exchange_result.is_err() {
                    // deallocate the array
                    // SAFETY: just allocated.
                    unsafe { std::alloc::dealloc(slice_ptr as *mut u8, layout) }
                    
                    continue
                }
                
                // SAFETY: all threads only increment len at this size when
                // they successfully allocated.
                //
                // Acquire prevents reordering with the subsequent write.
                // Release prevents reordering with the previous compare-exchange
                //
                // UNWRAP: we already observed "len" on this thread and no other thread
                // will win the shard compare exchange to increment.
                self.len.compare_exchange(
                    len,
                    len + 1,
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                ).unwrap();
                
                unsafe {
                    // SAFETY: just claimed ownership of the item with
                    // acquire semantics to prevent reordering.
                    std::ptr::write(slice_ptr, item);
                }
                
                return len;
            } else {
                let compare_exchange_result = self.len.compare_exchange(
                    len,
                    len + 1,
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                );
                
                if compare_exchange_result.is_ok() {
                    let shard_ptr = self.shards[shard_info.shard_index].load(Ordering::Relaxed);
                    assert!(!shard_ptr.is_null());
                    
                    // SAFETY: just claimed ownership of the item with Acquire
                    // semantics to prevent reordering.
                    unsafe {
                        let item_ptr = shard_ptr.offset(shard_info.sub_index as isize);
                        std::ptr::write(item_ptr, item);
                    }
                    
                    return len;
                }
            }
        }
    }

    pub fn get(&self, index: usize) -> &T {
        let len = self.len.load(Ordering::Relaxed);
        assert!(index < len, "index {} out of bounds", {index});
        let shard_info = shard_info(index, self.base_size);

        let shard_ptr = loop {
            let shard_ptr = self.shards[shard_info.shard_index].load(Ordering::Relaxed);
            if !shard_ptr.is_null() {
                break shard_ptr
            }
        };

        unsafe {
            // SAFETY: shard is allocated and sub index calculation is correct.
            let item_ptr = shard_ptr.offset(shard_info.sub_index as isize);
            &*item_ptr
        }
    }
}

impl<T> Drop for AppendOnlyStore<T> {
    fn drop(&mut self) {
        std::sync::atomic::fence(Ordering::Acquire);
        let len = self.len.load(Ordering::Acquire);
        let shard_info = shard_info(len, self.base_size);
        
        let drop_and_deallocate_nonempty = |ptr: *mut T, len, size| unsafe {
            {
                // SAFETY: len is only ever incremented immediately prior to
                // an unconditional write to that position.
                // we have mutable access, therefore all writes have concluded
                let slice = std::slice::from_raw_parts_mut(ptr, len);
                std::ptr::drop_in_place(slice as *mut [T]);
            }
            
            // SAFETY: we only increment len after allocating and writing the
            // array pointer. 
            let layout = std::alloc::Layout::array::<T>(size).unwrap();
            std::alloc::dealloc(ptr as *mut u8, layout);
        };
        
        // there are 0 or more full shards and 0 or 1 partially full shards
        // drop all the data in all of the shards.
        for full_shard in 0..shard_info.shard_index.saturating_sub(1) {
            let shard_size = (1 << full_shard) * self.base_size;
            let shard_ptr = self.shards[full_shard].load(Ordering::Relaxed);
            drop_and_deallocate_nonempty(shard_ptr, shard_size, shard_size);
        }
        
        // SAFETY: sub_index of zero means unallocated and unpopulated.
        if shard_info.sub_index != 0 {
            let shard_size = (1 << shard_info.shard_index) * self.base_size;
            let shard_ptr = self.shards[shard_info.shard_index].load(Ordering::Relaxed);
            drop_and_deallocate_nonempty(shard_ptr, shard_size, shard_info.sub_index);
        }
    }
} 

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
    
    let remaining_base_multiples = if shard_index == 0 { 0 } else {
        base_multiples.saturating_sub((1 << shard_index) - 1)
    };
    ShardInfo {
        shard_index,
        sub_index: base_size * remaining_base_multiples + remainder,
    }
}

#[test]
fn shard_info_works() {
    assert_eq!(shard_info(0, 10), ShardInfo { shard_index: 0, sub_index: 0 });
    assert_eq!(shard_info(9, 10), ShardInfo { shard_index: 0, sub_index: 9 });
    
    assert_eq!(shard_info(10, 10), ShardInfo { shard_index: 1, sub_index: 0 });
    assert_eq!(shard_info(20, 10), ShardInfo { shard_index: 1, sub_index: 10 });
    assert_eq!(shard_info(29, 10), ShardInfo { shard_index: 1, sub_index: 19 });
    
    assert_eq!(shard_info(30, 10), ShardInfo { shard_index: 2, sub_index: 0 });
}








