//! C raw types
//!
//! Why not use [`io_uring`] crate provided types and methods?
//!
//! [`io_uring`] provided types and methods are too low-level, and miss a good doc to guide
//! developers how to init the `io_uring_buf_ring` correctly. Instead, the `liburing` c libs has a
//! better doc, its provider better helper functions, we can use rust imitate its codes easily

use std::fmt::{Debug, Formatter};
use std::hash::{Hash, Hasher};
use std::marker::PhantomData;
use std::{fmt, mem, slice};

#[repr(C)]
#[derive(Default)]
pub struct __IncompleteArrayField<T>(PhantomData<T>, [T; 0]);
impl<T> __IncompleteArrayField<T> {
    #[inline]
    pub const fn new() -> Self {
        __IncompleteArrayField(PhantomData, [])
    }
    #[inline]
    pub fn as_ptr(&self) -> *const T {
        self as *const _ as *const T
    }
    #[inline]
    pub fn as_mut_ptr(&mut self) -> *mut T {
        self as *mut _ as *mut T
    }
    #[inline]
    pub unsafe fn as_slice(&self, len: usize) -> &[T] {
        slice::from_raw_parts(self.as_ptr(), len)
    }
    #[inline]
    pub unsafe fn as_mut_slice(&mut self, len: usize) -> &mut [T] {
        slice::from_raw_parts_mut(self.as_mut_ptr(), len)
    }
}
impl<T> Debug for __IncompleteArrayField<T> {
    fn fmt(&self, fmt: &mut Formatter<'_>) -> fmt::Result {
        fmt.write_str("__IncompleteArrayField")
    }
}
#[repr(C)]
pub struct __BindgenUnionField<T>(PhantomData<T>);
impl<T> __BindgenUnionField<T> {
    #[inline]
    pub const fn new() -> Self {
        __BindgenUnionField(PhantomData)
    }
    #[inline]
    pub unsafe fn as_ref(&self) -> &T {
        mem::transmute(self)
    }
    #[inline]
    pub unsafe fn as_mut(&mut self) -> &mut T {
        mem::transmute(self)
    }
}
impl<T> Default for __BindgenUnionField<T> {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}
impl<T> Clone for __BindgenUnionField<T> {
    #[inline]
    fn clone(&self) -> Self {
        *self
    }
}
impl<T> Copy for __BindgenUnionField<T> {}
impl<T> Debug for __BindgenUnionField<T> {
    fn fmt(&self, fmt: &mut Formatter<'_>) -> fmt::Result {
        fmt.write_str("__BindgenUnionField")
    }
}
impl<T> Hash for __BindgenUnionField<T> {
    fn hash<H: Hasher>(&self, _state: &mut H) {}
}
impl<T> PartialEq for __BindgenUnionField<T> {
    fn eq(&self, _other: &__BindgenUnionField<T>) -> bool {
        true
    }
}
impl<T> Eq for __BindgenUnionField<T> {}

#[repr(C)]
#[derive(Copy, Clone)]
#[allow(non_camel_case_types)]
pub struct io_uring_buf {
    pub addr: u64,
    pub len: u32,
    pub bid: u16,
    pub resv: u16,
}

#[repr(C)]
#[allow(non_camel_case_types)]
pub struct io_uring_buf_ring {
    pub __bindgen_anon_1: io_uring_buf_ring__bindgen_ty_1,
}

#[repr(C)]
#[allow(non_camel_case_types)]
pub struct io_uring_buf_ring__bindgen_ty_1 {
    pub __bindgen_anon_1: __BindgenUnionField<io_uring_buf_ring__bindgen_ty_1__bindgen_ty_1>,
    pub bufs: __BindgenUnionField<[io_uring_buf; 0usize]>,
    pub bindgen_union_field: [u64; 2usize],
}

#[repr(C)]
#[derive(Copy, Clone)]
#[allow(non_camel_case_types)]
pub struct io_uring_buf_ring__bindgen_ty_1__bindgen_ty_1 {
    pub resv1: u64,
    pub resv2: u32,
    pub resv3: u16,
    pub tail: u16,
}
