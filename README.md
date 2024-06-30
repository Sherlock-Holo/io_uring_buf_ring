# io_uring_buf_ring

Help user create an io-uring buffer-ring, user no need to manage the underlying ring

## Example

```rust
use std::io::Write;
use std::net::{Ipv6Addr, TcpListener, TcpStream};
use std::{io, ptr, thread};
use std::os::fd::AsRawFd;
use io_uring::cqueue::buffer_select;
use io_uring::IoUring;
use io_uring::opcode::Read;
use io_uring::squeue::Flags;
use io_uring::types::Fd;
use io_uring_buf_ring::{BorrowedBuffer, Buffer, IoUringBufRing};

fn example() {
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
}
```
