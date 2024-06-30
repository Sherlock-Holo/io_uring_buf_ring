//! # io_uring_buf_ring
//!
//! Helper user create an io-uring buffer-ring, user no need to care manage the underlying ring
//!
//! ## Example
//!
//! ```rust
//! use std::io::Write;
//! use std::net::{Ipv6Addr, TcpListener, TcpStream};
//! use std::{io, ptr, thread};
//! use std::os::fd::AsRawFd;
//! use io_uring::cqueue::buffer_select;
//! use io_uring::IoUring;
//! use io_uring::opcode::Read;
//! use io_uring::squeue::Flags;
//! use io_uring::types::Fd;
//! use io_uring_buf_ring::{BorrowedBuffer, Buffer, IoUringBufRing};
//!
//! let mut io_uring = IoUring::new(1024).unwrap();
//! let buf_ring = IoUringBufRing::new(&io_uring, 1, 1, 4).unwrap();
//!
//! let listener = TcpListener::bind((Ipv6Addr::LOCALHOST, 0)).unwrap();
//! let addr = listener.local_addr().unwrap();
//! thread::spawn(move || {
//!     let mut stream = listener.accept().unwrap().0;
//!     stream.write_all(b"test").unwrap();
//! });
//!
//! let stream = TcpStream::connect(addr).unwrap();
//!
//! let buffer = read_tcp(&mut io_uring, &buf_ring, 1, &stream, 0).unwrap();
//! assert_eq!(buffer.as_ref(), b"test");
//! drop(buffer);
//!
//! let buffer = read_tcp(&mut io_uring, &buf_ring, 1, &stream, 0).unwrap();
//! assert!(buffer.is_empty());
//! drop(buffer);
//!
//! unsafe { buf_ring.release(&io_uring).unwrap() }
//!
//! fn read_tcp<'a, B: Buffer>(
//!     ring: &mut IoUring,
//!     buf_ring: &'a IoUringBufRing<B>,
//!     buf_group: u16,
//!     stream: &TcpStream,
//!     len: impl Into<Option<usize>>,
//! ) -> io::Result<BorrowedBuffer<'a, B>> {
//!     let sqe = Read::new(
//!         Fd(stream.as_raw_fd()),
//!         ptr::null_mut(),
//!         len.into().unwrap_or(0) as _,
//!     )
//!     .offset(0)
//!     .buf_group(buf_group)
//!     .build()
//!     .flags(Flags::BUFFER_SELECT);
//!
//!     unsafe {
//!         ring.submission().push(&sqe).unwrap();
//!     }
//!
//!     ring.submit_and_wait(1)?;
//!
//!     let cqe = ring.completion().next().unwrap();
//!     let res = cqe.result();
//!     if res < 0 {
//!         return Err(io::Error::from_raw_os_error(-res));
//!     }
//!
//!     let bid = buffer_select(cqe.flags()).unwrap();
//!     let buffer = unsafe { buf_ring.get_buf(bid, res as _) }.unwrap();
//!
//!     Ok(buffer)
//! }
//! ```

use std::cell::UnsafeCell;
use std::io::ErrorKind;
use std::mem::{size_of, ManuallyDrop, MaybeUninit};
use std::ops::{Deref, DerefMut};
use std::ptr::NonNull;
use std::sync::atomic::{AtomicU16, Ordering};
use std::{io, ptr, slice};

#[cfg(feature = "bytes")]
use bytes::BytesMut;
use io_uring::types::BufRingEntry;
use io_uring::{cqueue, squeue, IoUring};
use rustix::mm::{mmap_anonymous, munmap, MapFlags, ProtFlags};

struct BufRingMmap {
    ptr: NonNull<BufRingEntry>,
    len: usize,
    size: usize,
}

// Safety: it can send to other thread
unsafe impl Send for BufRingMmap {}

impl BufRingMmap {
    fn new(len: usize) -> io::Result<Self> {
        let size = len * size_of::<BufRingEntry>();
        // SAFETY: correctly aligned.
        let ptr = unsafe {
            mmap_anonymous(
                ptr::null_mut(),
                size,
                ProtFlags::READ | ProtFlags::WRITE,
                MapFlags::PRIVATE,
            )
        }?;
        // SAFETY: rustix checkes the pointer.
        let ptr = unsafe { NonNull::new_unchecked(ptr.cast()) };

        Ok(Self { ptr, len, size })
    }

    /// The length of entries allocated.
    pub fn len(&self) -> usize {
        self.len
    }

    fn as_slice_uninit_mut(&mut self) -> &mut [MaybeUninit<BufRingEntry>] {
        // SAFETY: the pointer is valid
        unsafe { slice::from_raw_parts_mut(self.ptr.as_ptr().cast(), self.len) }
    }

    fn atomic_tail(&self) -> &AtomicU16 {
        // Safety: no one read/write tail ptr without atomic operation after init
        unsafe { AtomicU16::from_ptr(BufRingEntry::tail(self.ptr.as_ptr()).cast_mut()) }
    }

    /// # Safety
    ///
    /// Caller must ensure the ring is not full, and the `buf` lifecycle is long enough.
    unsafe fn add_buffer(
        &mut self,
        buf: &mut [MaybeUninit<u8>],
        bid: u16,
        mask: u16,
        buf_offset: usize,
    ) {
        let tail = self.atomic_tail();
        let index = ((tail.load(Ordering::Acquire) + (buf_offset as u16)) & mask) as usize;

        // SAFETY: only write plain data here
        let buf_ring_entry = self.as_slice_uninit_mut()[index].assume_init_mut();

        buf_ring_entry.set_addr(buf.as_ptr() as _);
        buf_ring_entry.set_len(buf.len() as _);
        buf_ring_entry.set_bid(bid);
    }

    /// # Safety
    ///
    /// The advanced count should be equal to the added count.
    unsafe fn advance_buffers(&self, count: u16) {
        self.atomic_tail().fetch_add(count, Ordering::Release);
    }
}

impl Drop for BufRingMmap {
    fn drop(&mut self) {
        // Safety: we own the buf_ring
        let _ = unsafe { munmap(self.ptr.as_ptr().cast(), self.size) };
    }
}

/// # Safety
///
/// Implementor must make sure all methods implement are safe
#[allow(clippy::len_without_is_empty)]
pub unsafe trait Buffer {
    /// Return the buffer pointer
    fn ptr(&self) -> *mut MaybeUninit<u8>;

    /// Return the buffer length
    fn len(&self) -> usize;

    /// Drop the buffer
    fn drop(self);
}

/// # Safety
///
/// The implement is safe
unsafe impl Buffer for Vec<u8> {
    fn ptr(&self) -> *mut MaybeUninit<u8> {
        self.as_ptr().cast_mut().cast()
    }

    fn len(&self) -> usize {
        self.capacity()
    }

    fn drop(self) {
        drop(self)
    }
}

/// # Safety
///
/// The implement is safe
unsafe impl<'a> Buffer for &'a mut [u8] {
    fn ptr(&self) -> *mut MaybeUninit<u8> {
        self.as_ptr().cast_mut().cast()
    }

    fn len(&self) -> usize {
        <[u8]>::len(self)
    }

    fn drop(self) {}
}

unsafe impl<'a, const N: usize> Buffer for &'a mut [u8; N] {
    fn ptr(&self) -> *mut MaybeUninit<u8> {
        self.as_ptr().cast_mut().cast()
    }

    fn len(&self) -> usize {
        self.as_ref().len()
    }

    fn drop(self) {}
}

#[cfg(feature = "bytes")]
unsafe impl Buffer for BytesMut {
    fn ptr(&self) -> *mut MaybeUninit<u8> {
        self.as_ptr().cast_mut().cast()
    }

    fn len(&self) -> usize {
        self.capacity()
    }

    fn drop(self) {
        drop(self);
    }
}

pub trait BufferExt: Buffer {
    fn as_uninit_slice(&self) -> &[MaybeUninit<u8>] {
        // Safety: ptr and len are valid
        unsafe { slice::from_raw_parts(self.ptr(), self.len()) }
    }

    /// # Safety
    ///
    /// len data must be initialize
    unsafe fn as_slice(&self, len: usize) -> &[u8] {
        debug_assert!(len <= self.len());
        slice::from_raw_parts(self.ptr().cast(), len)
    }

    fn as_uninit_slice_mut(&mut self) -> &mut [MaybeUninit<u8>] {
        // Safety: ptr and len are valid
        unsafe { slice::from_raw_parts_mut(self.ptr(), self.len()) }
    }

    /// # Safety
    ///
    /// len data must be initialize
    unsafe fn as_slice_mut(&mut self, len: usize) -> &mut [u8] {
        debug_assert!(len <= self.len());
        slice::from_raw_parts_mut(self.ptr().cast(), len)
    }
}

impl<T: Buffer> BufferExt for T {}

/// Buffer ring
///
/// register buffer ring for io-uring provided buffers
pub struct IoUringBufRing<B: Buffer> {
    buf_ring_mmap: ManuallyDrop<UnsafeCell<BufRingMmap>>,
    bufs: ManuallyDrop<UnsafeCell<Vec<B>>>,
    buf_group: u16,
}

impl IoUringBufRing<Vec<u8>> {
    /// Create new [`IoUringBufRing`] with given `buf_group`, buffer size is `buf_size`, the buffer
    /// ring entry size will be `ring_entries.next_power_of_two()`
    pub fn new<S, C>(
        ring: &IoUring<S, C>,
        ring_entries: u16,
        buf_group: u16,
        buf_size: usize,
    ) -> io::Result<Self>
    where
        S: squeue::EntryMarker,
        C: cqueue::EntryMarker,
    {
        let bufs = (0..ring_entries).map(|_| Vec::with_capacity(buf_size));

        Self::new_with_buffers(ring, bufs, buf_group)
    }
}

impl<B: Buffer> IoUringBufRing<B> {
    /// Create new [`IoUringBufRing`] with given `buf_group` and custom buffers, the buf sizes in
    /// buffers can be different.
    ///
    /// # Note
    ///
    /// if `buffers.len()` should be power of **2**, otherwise the entries will be extended.
    /// Users can call [`IoUringBufRing::add`] to fill the spare space with new buffers.
    pub fn new_with_buffers<S, C, Buffers>(
        ring: &IoUring<S, C>,
        buffers: Buffers,
        buf_group: u16,
    ) -> io::Result<Self>
    where
        S: squeue::EntryMarker,
        C: cqueue::EntryMarker,
        Buffers: IntoIterator<Item = B>,
    {
        let bufs = buffers.into_iter().collect::<Vec<_>>();
        if bufs.is_empty() {
            return Err(io::Error::new(ErrorKind::InvalidInput, "empty buffers"));
        }
        let ring_entries = bufs.len().next_power_of_two();

        let buf_ring_mmap = Self::create_buf_ring(ring, ring_entries as _, buf_group)?;
        let mut this = Self {
            buf_ring_mmap: ManuallyDrop::new(UnsafeCell::new(buf_ring_mmap)),
            bufs: ManuallyDrop::new(UnsafeCell::new(bufs)),
            buf_group,
        };

        this.init_bufs_with_iter();

        Ok(this)
    }

    /// # Safety
    ///
    /// caller must make sure there is only one [`BorrowedBuffer`] with the `id` at the same
    /// time
    pub unsafe fn get_buf(&self, id: u16, available_len: usize) -> Option<BorrowedBuffer<B>> {
        let buf = (*self.bufs.get()).get_mut(id as usize)?;
        debug_assert!(available_len <= buf.len());

        Some(BorrowedBuffer {
            buf,
            len: available_len,
            id,
            buf_ring: self,
        })
    }

    /// # Safety
    ///
    /// caller must make sure release [`IoUringBufRing`] with correct `ring`
    pub unsafe fn release<S, C>(mut self, ring: &IoUring<S, C>) -> io::Result<()>
    where
        S: squeue::EntryMarker,
        C: cqueue::EntryMarker,
    {
        ring.submitter().unregister_buf_ring(self.buf_group)?;

        ManuallyDrop::drop(&mut self.buf_ring_mmap);
        ManuallyDrop::drop(&mut self.bufs);
        Ok(())
    }

    fn init_bufs_with_iter(&mut self) {
        let mask = self.mask();
        let mmap = self.buf_ring_mmap.get_mut();
        let bufs = self.bufs.get_mut();
        for (id, buf) in bufs.iter_mut().enumerate() {
            // Safety: all arguments are valid
            unsafe {
                mmap.add_buffer(
                    // The buffers are created with `with_capacity`.
                    buf.as_uninit_slice_mut(),
                    id as _,
                    mask,
                    id,
                );
            }
        }

        unsafe {
            mmap.advance_buffers(bufs.len() as _);
        }
    }

    /// # Safety
    ///
    /// * Caller must make sure release valid buffer
    unsafe fn release_borrowed_buffer(&self, buf: &mut [MaybeUninit<u8>], bid: u16) {
        let mmap = &mut *self.buf_ring_mmap.get();
        mmap.add_buffer(buf, bid, self.mask(), 0);
        mmap.advance_buffers(1);
    }

    fn create_buf_ring<S, C>(
        ring: &IoUring<S, C>,
        ring_entries: u16,
        buf_group: u16,
    ) -> io::Result<BufRingMmap>
    where
        S: squeue::EntryMarker,
        C: cqueue::EntryMarker,
    {
        let mut buf_ring_mmap = BufRingMmap::new(ring_entries as _)?;

        let slice = buf_ring_mmap.as_slice_uninit_mut();
        unsafe {
            // Safety: ring_entries are valid
            ring.submitter()
                .register_buf_ring(slice.as_ptr() as _, slice.len() as _, buf_group)?;

            // Safety: no one write the tail at this moment
            *(BufRingEntry::tail(slice.as_ptr().cast()).cast_mut()) = 0;
        }

        Ok(buf_ring_mmap)
    }

    fn mask(&self) -> u16 {
        // Safety: we just get the bufs len to calculate the mask
        unsafe { (*self.buf_ring_mmap.get()).len() as u16 - 1 }
    }

    /// Add a new buffer to the ring.
    ///
    /// # Safety
    ///
    /// Caller should ensure that the ring is not full.
    pub unsafe fn add_unchecked(&mut self, buf: impl Into<B>) -> u16 {
        let mask = self.mask();
        let bufs = self.bufs.get_mut();
        let bufs_len = bufs.len();
        let bid = bufs_len as _;
        let mut buf = buf.into();

        let mmap = self.buf_ring_mmap.get_mut();
        mmap.add_buffer(buf.as_uninit_slice_mut(), bid, mask, 0);
        mmap.advance_buffers(1);

        bufs.push(buf);

        bid
    }

    /// Add a new buffer to the ring, and return error if the ring is full.
    pub fn add(&mut self, buf: impl Into<B>) -> io::Result<u16> {
        let bufs_len = self.bufs.get_mut().len();
        let entries_len = self.buf_ring_mmap.get_mut().len();
        if bufs_len >= entries_len {
            return Err(io::Error::new(
                ErrorKind::Other,
                "no spare space for new buffer",
            ));
        }
        Ok(unsafe { self.add_unchecked(buf) })
    }
}

/// Borrowed buffer from [`IoUringBufRing`]
pub struct BorrowedBuffer<'a, B: Buffer> {
    buf: &'a mut B,
    len: usize,
    id: u16,
    buf_ring: &'a IoUringBufRing<B>,
}

impl<B: Buffer> Deref for BorrowedBuffer<'_, B> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        // Safety: self.len is valid
        unsafe { self.buf.as_slice(self.len) }
    }
}

impl<B: Buffer> DerefMut for BorrowedBuffer<'_, B> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        // Safety: self.len is valid
        unsafe { self.buf.as_slice_mut(self.len) }
    }
}

impl<B: Buffer> Drop for BorrowedBuffer<'_, B> {
    fn drop(&mut self) {
        // Safety: release to the correct buffer ring
        unsafe {
            self.buf_ring
                .release_borrowed_buffer(self.buf.as_uninit_slice_mut(), self.id)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::{Read as _, Write};
    use std::net::{Ipv6Addr, TcpListener, TcpStream};
    use std::os::fd::AsRawFd;
    use std::thread;

    use io_uring::cqueue::buffer_select;
    use io_uring::opcode::Read;
    use io_uring::squeue::Flags;
    use io_uring::types::Fd;

    use super::*;

    #[test]
    fn create_buf_ring() {
        let io_uring = IoUring::new(1024).unwrap();
        let buf_ring = IoUringBufRing::new(&io_uring, 1, 1, 4).unwrap();

        unsafe { buf_ring.release(&io_uring).unwrap() }
    }

    #[test]
    fn create_with_custom_buffers_buf_ring() {
        let io_uring = IoUring::new(1024).unwrap();
        let buf_ring = IoUringBufRing::new_with_buffers(
            &io_uring,
            [Vec::with_capacity(1), Vec::with_capacity(2)],
            1,
        )
        .unwrap();

        unsafe { buf_ring.release(&io_uring).unwrap() }
    }

    #[test]
    fn create_and_add_more_buffer() {
        let io_uring = IoUring::new(1024).unwrap();
        let mut buf_ring = IoUringBufRing::new(&io_uring, 3, 1, 2).unwrap();
        buf_ring.add([0u8; 4]).unwrap();

        unsafe { buf_ring.release(&io_uring).unwrap() }
    }

    fn read_tcp<'a, B: Buffer>(
        ring: &mut IoUring,
        buf_ring: &'a IoUringBufRing<B>,
        buf_group: u16,
        stream: &TcpStream,
        len: impl Into<Option<usize>>,
    ) -> io::Result<BorrowedBuffer<'a, B>> {
        let sqe = Read::new(
            Fd(stream.as_raw_fd()),
            ptr::null_mut(),
            len.into().unwrap_or(0) as _,
        )
        .offset(0)
        .buf_group(buf_group)
        .build()
        .flags(Flags::BUFFER_SELECT);

        unsafe {
            ring.submission().push(&sqe).unwrap();
        }

        ring.submit_and_wait(1)?;

        let cqe = ring.completion().next().unwrap();
        let res = cqe.result();
        if res < 0 {
            return Err(io::Error::from_raw_os_error(-res));
        }

        let bid = buffer_select(cqe.flags()).unwrap();
        let buffer = unsafe { buf_ring.get_buf(bid, res as _) }.unwrap();

        Ok(buffer)
    }

    #[test]
    fn read() {
        let mut io_uring = IoUring::new(1024).unwrap();
        let buf_ring = IoUringBufRing::new(&io_uring, 1, 1, 4).unwrap();

        let listener = TcpListener::bind((Ipv6Addr::LOCALHOST, 0)).unwrap();
        let addr = listener.local_addr().unwrap();
        thread::spawn(move || {
            let mut stream = listener.accept().unwrap().0;
            stream.write_all(b"test").unwrap();
        });

        let stream = TcpStream::connect(addr).unwrap();

        let buffer = read_tcp(&mut io_uring, &buf_ring, 1, &stream, 0).unwrap();
        assert_eq!(buffer.as_ref(), b"test");
        drop(buffer);

        let buffer = read_tcp(&mut io_uring, &buf_ring, 1, &stream, 0).unwrap();
        assert!(buffer.is_empty());
        drop(buffer);

        unsafe { buf_ring.release(&io_uring).unwrap() }
    }

    #[test]
    fn read_with_multi_borrowed_buf() {
        let mut io_uring = IoUring::new(1024).unwrap();
        let buf_ring = IoUringBufRing::new(&io_uring, 2, 1, 2).unwrap();

        let listener = TcpListener::bind((Ipv6Addr::LOCALHOST, 0)).unwrap();
        let addr = listener.local_addr().unwrap();
        thread::spawn(move || {
            let mut stream = listener.accept().unwrap().0;
            stream.write_all(b"test").unwrap();
        });

        let stream = TcpStream::connect(addr).unwrap();

        let buffer1 = read_tcp(&mut io_uring, &buf_ring, 1, &stream, 0).unwrap();
        assert_eq!(buffer1.as_ref(), b"te");

        let buffer2 = read_tcp(&mut io_uring, &buf_ring, 1, &stream, 0).unwrap();
        assert_eq!(buffer2.as_ref(), b"st");
        drop(buffer2);

        let eof_buffer = read_tcp(&mut io_uring, &buf_ring, 1, &stream, 0).unwrap();
        assert!(eof_buffer.is_empty());
        drop(eof_buffer);

        drop(buffer1);

        unsafe { buf_ring.release(&io_uring).unwrap() }
    }

    #[test]
    fn custom_bufs_read_with_multi_borrowed_buf() {
        let mut io_uring = IoUring::new(1024).unwrap();
        let buf_ring = IoUringBufRing::new_with_buffers(
            &io_uring,
            [Vec::with_capacity(2), Vec::with_capacity(4)],
            1,
        )
        .unwrap();

        let listener = TcpListener::bind((Ipv6Addr::LOCALHOST, 0)).unwrap();
        let addr = listener.local_addr().unwrap();
        thread::spawn(move || {
            let mut stream = listener.accept().unwrap().0;
            stream.write_all(b"test").unwrap();
        });

        let stream = TcpStream::connect(addr).unwrap();

        let buffer1 = read_tcp(&mut io_uring, &buf_ring, 1, &stream, 0).unwrap();
        assert_eq!(buffer1.as_ref(), b"te");

        let buffer2 = read_tcp(&mut io_uring, &buf_ring, 1, &stream, 0).unwrap();
        assert_eq!(buffer2.as_ref(), b"st");

        drop(buffer1);

        let eof_buffer = read_tcp(&mut io_uring, &buf_ring, 1, &stream, 0).unwrap();
        assert!(eof_buffer.is_empty());
        drop(eof_buffer);
        drop(buffer2);

        unsafe { buf_ring.release(&io_uring).unwrap() }
    }

    #[test]
    fn read_then_write() {
        let mut io_uring = IoUring::new(1024).unwrap();
        let buf_ring = IoUringBufRing::new(&io_uring, 1, 1, 4).unwrap();

        let listener = TcpListener::bind((Ipv6Addr::LOCALHOST, 0)).unwrap();
        let addr = listener.local_addr().unwrap();
        let join_handle = thread::spawn(move || {
            let mut stream = listener.accept().unwrap().0;
            stream.write_all(b"test").unwrap();

            let mut buf = [0; 4];
            stream.read_exact(&mut buf).unwrap();

            assert_eq!(buf.as_ref(), b"aest");
        });

        let mut stream = TcpStream::connect(addr).unwrap();

        let mut buffer = read_tcp(&mut io_uring, &buf_ring, 1, &stream, 0).unwrap();
        assert_eq!(buffer.as_ref(), b"test");

        buffer[0] = b'a';
        stream.write_all(&buffer).unwrap();
        drop(buffer);

        unsafe { buf_ring.release(&io_uring).unwrap() }

        join_handle.join().unwrap();
    }

    #[test]
    fn read_with_slice() {
        let mut io_uring = IoUring::new(1024).unwrap();
        let mut buf = [0; 4];
        let buf_ring = IoUringBufRing::new_with_buffers(&io_uring, [&mut buf], 1).unwrap();

        let listener = TcpListener::bind((Ipv6Addr::LOCALHOST, 0)).unwrap();
        let addr = listener.local_addr().unwrap();
        thread::spawn(move || {
            let mut stream = listener.accept().unwrap().0;
            stream.write_all(b"test").unwrap();
        });

        let stream = TcpStream::connect(addr).unwrap();

        let buffer = read_tcp(&mut io_uring, &buf_ring, 1, &stream, 0).unwrap();
        assert_eq!(buffer.as_ref(), b"test");
        drop(buffer);

        let buffer = read_tcp(&mut io_uring, &buf_ring, 1, &stream, 0).unwrap();
        assert!(buffer.is_empty());
        drop(buffer);

        unsafe { buf_ring.release(&io_uring).unwrap() }
    }
}
