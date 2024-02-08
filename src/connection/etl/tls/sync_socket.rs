use std::{
    io::{Read, Result as IoResult, Write},
    task::{Context, Poll},
};

use sqlx_core::net::Socket;

/// Wrapper emulating a synchronous socket from an async one.
/// Needed by the TLS backends as they need a type implementing [`Read`] and [`Write`].
pub struct SyncSocket<S: Socket>(pub S);

impl<S> SyncSocket<S>
where
    S: Socket,
{
    pub fn poll_read_ready(&mut self, cx: &mut Context<'_>) -> Poll<IoResult<()>> {
        self.0.poll_read_ready(cx)
    }

    pub fn poll_write_ready(&mut self, cx: &mut Context<'_>) -> Poll<IoResult<()>> {
        self.0.poll_write_ready(cx)
    }
}

impl<S> Read for SyncSocket<S>
where
    S: Socket,
{
    fn read(&mut self, mut buf: &mut [u8]) -> IoResult<usize> {
        self.0.try_read(&mut buf)
    }
}

impl<S> Write for SyncSocket<S>
where
    S: Socket,
{
    fn write(&mut self, buf: &[u8]) -> IoResult<usize> {
        self.0.try_write(buf)
    }

    fn flush(&mut self) -> IoResult<()> {
        // NOTE: TCP sockets and unix sockets are both no-ops for flushes
        Ok(())
    }
}
