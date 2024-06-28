use std::cell::UnsafeCell;
use std::ffi::c_int;
use std::io::ErrorKind;
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};
use std::ptr::{addr_of_mut, NonNull};
use std::sync::atomic::{AtomicU16, Ordering};
use std::{io, ptr, slice};

use io_uring::{cqueue, squeue, IoUring};
use rustix::mm::{mmap_anonymous, munmap, MapFlags, ProtFlags};

use crate::raw_types::{io_uring_buf, io_uring_buf_ring};

mod raw_types;

fn br_setup<S, C>(
    ring: &IoUring<S, C>,
    ring_entries: u16,
    bgid: u16,
) -> io::Result<NonNull<io_uring_buf_ring>>
where
    S: squeue::EntryMarker,
    C: cqueue::EntryMarker,
{
    let ring_size = ring_entries as usize * size_of::<io_uring_buf>();

    unsafe {
        // Safety: we will check the ptr
        let ptr = mmap_anonymous(
            ptr::null_mut(),
            ring_size,
            ProtFlags::READ | ProtFlags::WRITE,
            MapFlags::PRIVATE,
        )?;
        let ptr = NonNull::new(ptr)
            .ok_or_else(|| io::Error::new(ErrorKind::Other, "mmap return null ptr"))?;

        // Safety: ptr and nentries are valid
        match ring
            .submitter()
            .register_buf_ring(ptr.as_ptr() as _, ring_entries, bgid)
        {
            Err(err) => {
                let _ = munmap(ptr.as_ptr(), ring_size);

                Err(err)
            }

            Ok(_) => Ok(ptr.cast()),
        }
    }
}

fn io_uring_setup_buf_ring<S, C>(
    ring: &IoUring<S, C>,
    ring_entries: u16,
    bgid: u16,
) -> io::Result<NonNull<io_uring_buf_ring>>
where
    S: squeue::EntryMarker,
    C: cqueue::EntryMarker,
{
    let buf_ring_ptr = br_setup(ring, ring_entries, bgid)?;

    unsafe {
        // Safety: buf_ring_ptr is valid
        (*buf_ring_ptr.as_ptr())
            .__bindgen_anon_1
            .__bindgen_anon_1
            .as_mut()
            .tail = 0;
    }

    Ok(buf_ring_ptr)
}

/// Buffer ring
///
/// register buffer ring for io-uring provided buffers
pub struct IoUringBufRing {
    ptr: NonNull<io_uring_buf_ring>,
    bufs: UnsafeCell<Vec<Vec<u8>>>,
    bgid: u16,

    _marker: PhantomData<io_uring_buf_ring>,
}

impl IoUringBufRing {
    /// Create new [`IoUringBufRing`] with given `bgid`, buffer size is `buf_size`, the buffer ring
    /// entry size will be `ring_entries.next_power_of_two()`
    pub fn new<S, C>(
        ring: &IoUring<S, C>,
        mut ring_entries: u16,
        bgid: u16,
        buf_size: usize,
    ) -> io::Result<Self>
    where
        S: squeue::EntryMarker,
        C: cqueue::EntryMarker,
    {
        ring_entries = ring_entries.next_power_of_two();
        let mut ptr = io_uring_setup_buf_ring(ring, ring_entries, bgid)?;
        let mut bufs = vec![Vec::with_capacity(buf_size); ring_entries as _];

        for (id, buf) in bufs.iter_mut().enumerate() {
            // Safety: all arguments are valid
            unsafe {
                io_uring_buf_ring_add(
                    ptr.as_mut(),
                    buf.as_mut_ptr(),
                    buf.capacity(),
                    id as _,
                    (ring_entries - 1) as _,
                    id as _,
                );
            }
        }

        unsafe {
            // Safety: we own the ptr
            io_uring_buf_ring_advance(ptr.as_mut(), ring_entries);
        }

        Ok(Self {
            ptr,
            bufs: UnsafeCell::new(bufs),
            bgid,
            _marker: Default::default(),
        })
    }

    /// # Safety
    ///
    /// caller must make sure there is only one [`BorrowedBuffer`] with the `id` at the same
    /// time
    pub unsafe fn get_buf(&self, id: u16, available_len: usize) -> Option<BorrowedBuffer> {
        let buf = (*self.bufs.get()).get_mut(id as usize)?;

        Some(BorrowedBuffer {
            id,
            ptr: buf.as_mut_ptr(),
            len: available_len,
            buf_ring: self,
            _marker: Default::default(),
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
        ring.submitter().unregister_buf_ring(self.bgid)?;

        Ok(())
    }
}

impl Drop for IoUringBufRing {
    fn drop(&mut self) {
        let _ = unsafe {
            munmap(
                self.ptr.as_ptr() as _,
                self.bufs.get_mut().len() * size_of::<io_uring_buf>(),
            )
        };
    }
}

/// Borrowed buffer from [`IoUringBufRing`]
pub struct BorrowedBuffer<'a> {
    id: u16,
    ptr: *mut u8,
    len: usize,

    buf_ring: &'a IoUringBufRing,

    _marker: PhantomData<&'a mut [u8]>,
}

impl Deref for BorrowedBuffer<'_> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        unsafe { slice::from_raw_parts(self.ptr, self.len) }
    }
}

impl DerefMut for BorrowedBuffer<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { slice::from_raw_parts_mut(self.ptr, self.len) }
    }
}

impl Drop for BorrowedBuffer<'_> {
    fn drop(&mut self) {
        unsafe {
            io_uring_buf_ring_add(
                &mut *self.buf_ring.ptr.as_ptr(),
                self.ptr,
                self.len,
                self.id,
                (*self.buf_ring.bufs.get()).len() as c_int - 1,
                self.id as _,
            );

            io_uring_buf_ring_advance(&mut *self.buf_ring.ptr.as_ptr(), 1);
        }
    }
}

fn io_uring_buf_ring_advance(buf_ring_ptr: &mut io_uring_buf_ring, count: u16) {
    // Safety: buf_ring_ptr is valid and no one read/write tail ptr without atomic operation
    unsafe {
        let tail = AtomicU16::from_ptr(addr_of_mut!(
            buf_ring_ptr.__bindgen_anon_1.__bindgen_anon_1.as_mut().tail
        ));
        tail.fetch_add(count, Ordering::Release);
    }
}

/// # Safety:
///
/// caller must make sure all arguments are valid
unsafe fn io_uring_buf_ring_add(
    buf_ring_ptr: &mut io_uring_buf_ring,
    buf_addr: *mut u8,
    buf_len: usize,
    bid: u16,
    mask: c_int,
    buf_offset: c_int,
) {
    let tail = AtomicU16::from_ptr(addr_of_mut!(
        buf_ring_ptr.__bindgen_anon_1.__bindgen_anon_1.as_mut().tail
    ));
    let index = ((tail.load(Ordering::Acquire) as c_int + buf_offset) & mask) as _;
    let ptr = buf_ring_ptr
        .__bindgen_anon_1
        .bufs
        .as_mut()
        .as_mut_ptr()
        .offset(index);

    (*ptr).addr = buf_addr as _;
    (*ptr).len = buf_len as _;
    (*ptr).bid = bid;
}

#[cfg(test)]
mod tests {
    use std::io::Write;
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
        IoUringBufRing::new(&io_uring, 1, 1, 4).unwrap();
    }

    #[test]
    fn create_and_release_buf_ring() {
        let io_uring = IoUring::new(1024).unwrap();
        let buf_ring = IoUringBufRing::new(&io_uring, 1, 1, 4).unwrap();

        unsafe { buf_ring.release(&io_uring).unwrap() }
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

        loop {
            let sqe = Read::new(Fd(stream.as_raw_fd()), ptr::null_mut(), 2)
                .offset(0)
                .buf_group(1)
                .build()
                .flags(Flags::BUFFER_SELECT);

            unsafe {
                io_uring.submission().push(&sqe).unwrap();
            }

            io_uring.submit_and_wait(1).unwrap();

            let cqe = io_uring.completion().next().unwrap();
            let res = cqe.result();
            if res < 0 {
                panic!("{}", io::Error::from_raw_os_error(-res));
            }
            if res == 0 {
                break;
            }

            let bid = buffer_select(cqe.flags()).unwrap();
            let buffer = unsafe { buf_ring.get_buf(bid, res as _) }.unwrap();
            println!("{}", String::from_utf8_lossy(&buffer));
        }

        unsafe { buf_ring.release(&io_uring).unwrap() }
    }
}
