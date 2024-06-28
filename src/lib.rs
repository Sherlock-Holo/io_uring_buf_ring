use std::cell::{RefCell, UnsafeCell};
use std::io::ErrorKind;
use std::mem::MaybeUninit;
use std::ops::{Deref, DerefMut};
use std::ptr::NonNull;
use std::sync::atomic::{AtomicU16, Ordering};
use std::{io, ptr};

use io_uring::types::BufRingEntry;
use io_uring::{cqueue, squeue, IoUring};
use rustix::mm::{mmap_anonymous, munmap, MapFlags, ProtFlags};

struct BufRingMmap {
    ptr: NonNull<BufRingEntry>,
    len: usize,
    size: usize,
}

impl BufRingMmap {
    fn new(len: usize) -> io::Result<Self> {
        let size = len * std::mem::size_of::<BufRingEntry>();
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

    fn as_slice_uninit_mut(&mut self) -> &mut [MaybeUninit<BufRingEntry>] {
        // SAFETY: the pointer is valid
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr().cast(), self.len) }
    }

    fn atomic_tail(&self) -> &AtomicU16 {
        // Safety: no one read/write tail ptr without atomic operation after init
        unsafe { AtomicU16::from_ptr(BufRingEntry::tail(self.ptr.as_ptr()).cast_mut()) }
    }

    /// # Safety
    ///
    /// Caller must ensure the ring is not full.
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
        let buf_ring_entry = (&mut self.as_slice_uninit_mut()[index]).assume_init_mut();

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

/// Buffer ring
///
/// register buffer ring for io-uring provided buffers
pub struct IoUringBufRing {
    buf_ring_mmap: RefCell<BufRingMmap>,
    bufs: UnsafeCell<Vec<Vec<u8>>>,
    buf_group: u16,
}

impl IoUringBufRing {
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

    /// Create new [`IoUringBufRing`] with given `buf_group` and custom buffers, the buf sizes in
    /// buffers can be different
    ///
    /// # Note
    ///
    /// if `buffers.len()` should be power of **2**, otherwise will return error
    pub fn new_with_buffers<S, C, B>(
        ring: &IoUring<S, C>,
        buffers: B,
        buf_group: u16,
    ) -> io::Result<Self>
    where
        S: squeue::EntryMarker,
        C: cqueue::EntryMarker,
        B: Iterator<Item = Vec<u8>>,
    {
        let bufs = buffers.collect::<Vec<_>>();
        if bufs.len() == 0 {
            return Err(io::Error::new(ErrorKind::InvalidInput, "empty buffers"));
        }
        let ring_entries = bufs.len().next_power_of_two();

        let buf_ring_mmap = Self::create_buf_ring(ring, ring_entries as _, buf_group)?;
        let mut this = Self {
            buf_ring_mmap: RefCell::new(buf_ring_mmap),
            bufs: UnsafeCell::new(bufs),
            buf_group,
        };

        this.init_bufs_with_iter();

        Ok(this)
    }

    /// # Safety
    ///
    /// caller must make sure there is only one [`BorrowedBuffer`] with the `id` at the same
    /// time
    pub unsafe fn get_buf(&self, id: u16, available_len: usize) -> Option<BorrowedBuffer> {
        let buf = (*self.bufs.get()).get_mut(id as usize)?;
        debug_assert!(available_len <= buf.capacity());
        buf.set_len(available_len);

        Some(BorrowedBuffer {
            buf,
            id,
            buf_ring: self,
        })
    }

    /// # Safety
    ///
    /// caller must make sure release [`IoUringBufRing`] with correct `ring`
    pub unsafe fn release<S, C>(self, ring: &IoUring<S, C>) -> io::Result<()>
    where
        S: squeue::EntryMarker,
        C: cqueue::EntryMarker,
    {
        ring.submitter().unregister_buf_ring(self.buf_group)?;

        Ok(())
    }

    fn init_bufs_with_iter(&mut self) {
        let mask = self.mask();
        let mut mmap = self.buf_ring_mmap.borrow_mut();
        let bufs = self.bufs.get_mut();
        for (id, buf) in bufs.iter_mut().enumerate() {
            // Safety: all arguments are valid
            unsafe {
                mmap.add_buffer(
                    // The buffers are created with `with_capacity`.
                    buf.spare_capacity_mut(),
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
    /// caller must make sure release valid buffer
    unsafe fn release_borrowed_buffer(&self, buf: &mut [MaybeUninit<u8>], bid: u16) {
        self.buf_ring_mmap
            .borrow_mut()
            .add_buffer(buf, bid, self.mask(), 0);

        self.buf_ring_mmap.borrow_mut().advance_buffers(1);
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
        unsafe { (*self.bufs.get()).len().next_power_of_two() as u16 - 1 }
    }
}

/// Borrowed buffer from [`IoUringBufRing`]
pub struct BorrowedBuffer<'a> {
    buf: &'a mut Vec<u8>,
    id: u16,
    buf_ring: &'a IoUringBufRing,
}

impl Deref for BorrowedBuffer<'_> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.buf
    }
}

impl DerefMut for BorrowedBuffer<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.buf
    }
}

impl Drop for BorrowedBuffer<'_> {
    fn drop(&mut self) {
        // Safety: release to the correct buffer ring
        unsafe {
            self.buf_ring.release_borrowed_buffer(
                std::slice::from_raw_parts_mut(self.buf.as_mut_ptr().cast(), self.buf.len()),
                self.id,
            )
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
            [Vec::with_capacity(1), Vec::with_capacity(2)].into_iter(),
            1,
        )
        .unwrap();

        unsafe { buf_ring.release(&io_uring).unwrap() }
    }

    fn read_tcp<'a>(
        ring: &mut IoUring,
        buf_ring: &'a IoUringBufRing,
        buf_group: u16,
        stream: &TcpStream,
        len: impl Into<Option<usize>>,
    ) -> io::Result<BorrowedBuffer<'a>> {
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
            [Vec::with_capacity(2), Vec::with_capacity(4)].into_iter(),
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
}
