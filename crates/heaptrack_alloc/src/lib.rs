//! Generic heap‑track‑enabled allocator wrapper
#![no_std]

use core::alloc::{GlobalAlloc, Layout};

unsafe extern "C" {
    fn ht_report_alloc(ptr: *mut core::ffi::c_void, size: usize);
    fn ht_report_free(ptr: *mut core::ffi::c_void);
    fn ht_report_realloc(
        old_ptr: *mut core::ffi::c_void,
        new_size: usize,
        new_ptr: *mut core::ffi::c_void,
    );
}

/// `HeaptrackAlloc<T>` forwards to `T` but tells heaptrack about everything.
///
/// ```no_run
/// use heaptrack_alloc::HeaptrackAlloc;
/// use snmalloc_rs::SnMalloc;
///
/// #[global_allocator]
/// static GLOBAL: HeaptrackAlloc<SnMalloc> = HeaptrackAlloc::new(SnMalloc);
/// ```
pub struct HeaptrackAlloc<T: GlobalAlloc> {
    inner: T,
}

impl<T: GlobalAlloc> HeaptrackAlloc<T> {
    pub const fn new(inner: T) -> Self {
        Self { inner }
    }
}

unsafe impl<T: GlobalAlloc> GlobalAlloc for HeaptrackAlloc<T> {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        unsafe {
            let ptr = self.inner.alloc(layout);
            if !ptr.is_null() {
                ht_report_alloc(ptr.cast(), layout.size());
            }
            ptr
        }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe {
            ht_report_free(ptr.cast());
            self.inner.dealloc(ptr, layout);
        }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        unsafe {
            let new_ptr = self.inner.realloc(ptr, layout, new_size);
            if !new_ptr.is_null() {
                ht_report_realloc(ptr.cast(), new_size, new_ptr.cast());
            }
            new_ptr
        }
    }
}
