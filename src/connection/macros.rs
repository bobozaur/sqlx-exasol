/// The output closure is for defining the concrete types that we need to pass around as generics
/// for [`crate::connection::QueryResultStream`].
///
/// What we're really interested in is defining the `F` future for retrieving the next data chunk in
/// the result set.
///
/// However, we in fact need a factory of these `F` future types, which is the closure here, aka the
/// future maker. This is because we will generally have to retrieve chunks until a result set is
/// depleted, so we need the ability to create new ones.
///
/// Since the future uses the exclusive mutable reference to the websocket, to satisfy the borrow
/// checker we return the mutable reference after the future is done it with, so it can be passed to
/// the future maker again and create a new future.
macro_rules! fetcher_closure {
    ($lt:lifetime) => {
        move |ws: &$lt mut $crate::connection::ExaWebSocket, handle: u16, pos: usize| {
            let fetch_size = ws.attributes.fetch_size;
            let cmd = ExaCommand::new_fetch(handle, pos, fetch_size).try_into()?;
            let future = async { ws.fetch_chunk(cmd).await.map(|d| (d, ws)) };
            Ok(future)
        }
    }
}

pub(crate) use fetcher_closure;
