use std::{
    io::{Read, Result as IoResult, Write},
    task::{ready, Context, Poll},
};

use sqlx_core::net::Socket;

pub struct SyncSocket<S>
where
    S: Socket,
{
    pub socket: S,
    wants_read: bool,
    wants_write: bool,
}

impl<S> SyncSocket<S>
where
    S: Socket,
{
    pub fn new(socket: S) -> Self {
        Self {
            socket,
            wants_read: false,
            wants_write: false,
        }
    }
    pub fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<IoResult<()>> {
        if self.wants_write {
            ready!(self.socket.poll_write_ready(cx))?;
            self.wants_write = false;
        }

        if self.wants_read {
            ready!(self.socket.poll_read_ready(cx))?;
            self.wants_read = false;
        }

        Poll::Ready(Ok(()))
    }
}

impl<S> Read for SyncSocket<S>
where
    S: Socket,
{
    fn read(&mut self, mut buf: &mut [u8]) -> IoResult<usize> {
        self.wants_read = true;
        let read = self.socket.try_read(&mut buf)?;
        self.wants_read = false;

        Ok(read)
    }
}

impl<S> Write for SyncSocket<S>
where
    S: Socket,
{
    fn write(&mut self, buf: &[u8]) -> IoResult<usize> {
        self.wants_write = true;
        let written = self.socket.try_write(buf)?;
        self.wants_write = false;

        Ok(written)
    }

    fn flush(&mut self) -> IoResult<()> {
        // NOTE: TCP sockets and unix sockets are both no-ops for flushes
        Ok(())
    }
}
