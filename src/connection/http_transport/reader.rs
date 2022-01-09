use crossbeam::channel::{IntoIter, Receiver};
use std::io::Read;

/// HTTP Transport reader that can be used
/// in custom closures in [Connection::export_to_closure](crate::Connection).
pub struct ExaReader {
    receiver: IntoIter<Vec<u8>>,
    buf: Vec<u8>,
    pos: usize,
}

impl ExaReader {
    pub fn new(receiver: Receiver<Vec<u8>>) -> Self {
        Self {
            receiver: receiver.into_iter(),
            buf: Vec::new(),
            pos: 0,
        }
    }
}

impl Read for ExaReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let mut num_bytes = (&self.buf[self.pos..]).read(buf)?;

        if num_bytes == 0 {
            match self.receiver.next() {
                None => num_bytes = 0,
                Some(v) => {
                    self.buf = v;
                    self.pos = 0;
                    num_bytes = (&self.buf[self.pos..]).read(buf)?;
                }
            }
        }

        self.pos += num_bytes;
        Ok(num_bytes)
    }
}
