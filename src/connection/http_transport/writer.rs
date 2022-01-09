use super::TRANSPORT_BUFFER_SIZE;
use crossbeam::channel::Sender;
use std::io::{Error, ErrorKind, Write};

/// HTTP Transport writer that can be used
/// in custom closures in [Connection::import_from_closure](crate::Connection).
pub struct ExaWriter {
    sender: Sender<Vec<u8>>,
    buf: Vec<u8>,
}

impl ExaWriter {
    pub fn new(sender: Sender<Vec<u8>>) -> Self {
        Self {
            sender,
            buf: Vec::with_capacity(TRANSPORT_BUFFER_SIZE),
        }
    }
}

impl Write for ExaWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.buf.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        let mut old_buf = Vec::with_capacity(TRANSPORT_BUFFER_SIZE);
        std::mem::swap(&mut self.buf, &mut old_buf);
        self.sender.send(old_buf).map_err(|_| {
            Error::new(
                ErrorKind::BrokenPipe,
                "Could not send data chunk to worker thread",
            )
        })
    }
}
