use std::net::SocketAddrV4;

use futures_core::future::BoxFuture;
use futures_util::future::{select, try_join_all, Either};
use sqlx_core::error::{BoxDynError, Error as SqlxError};

use crate::{
    connection::websocket::socket::ExaSocket, error::ExaResultExt, etl::socket_spawners,
    ExaConnection, ExaQueryResult,
};

/// Type of the future that executes the ETL job.
type JobFuture<'a> = BoxFuture<'a, Result<ExaQueryResult, BoxDynError>>;
/// Output type resulting from preparing an ETL job.
pub type JobPrepOutput<'a, T> = Result<(JobFuture<'a>, Vec<T>), SqlxError>;

pub trait EtlJob: Send + Sync {
    type Worker;

    fn prepare<'a, 'c>(
        &'a self,
        con: &'c mut ExaConnection,
    ) -> BoxFuture<'a, JobPrepOutput<'c, Self::Worker>>
    where
        'c: 'a,
    {
        Box::pin(async move {
            let ips = con.ws.get_hosts().await?;
            let port = con.ws.socket_addr().port();
            let with_tls = con.attributes().encryption_enabled;
            let with_compression = self
                .use_compression()
                .unwrap_or(con.attributes().compression_enabled);

            let (futures, rxs): (Vec<_>, Vec<_>) =
                socket_spawners(self.num_workers(), ips, port, with_tls)
                    .await?
                    .into_iter()
                    .unzip();

            let addrs_fut = try_join_all(rxs);
            let sockets_fut = try_join_all(futures);

            let (addrs, sockets_fut): (Vec<_>, BoxFuture<'_, Result<Vec<ExaSocket>, SqlxError>>) =
                match select(addrs_fut, sockets_fut).await {
                    Either::Left((addrs, sockets_fut)) => {
                        (addrs.to_sqlx_err()?, Box::pin(sockets_fut))
                    }
                    Either::Right((sockets, addrs_fut)) => (
                        addrs_fut.await.to_sqlx_err()?,
                        Box::pin(async move { sockets }),
                    ),
                };

            let query = self.query(addrs, with_tls, with_compression);
            let query_fut: JobFuture = Box::pin(con.execute_etl(query));

            let (sockets, query_fut) = match select(query_fut, sockets_fut).await {
                Either::Right((sockets, f)) => (sockets?, f),
                _ => unreachable!("ETL cannot finish before we use the sockets"),
            };

            let sockets = self.create_workers(sockets, with_compression);

            Ok((query_fut, sockets))
        })
    }

    fn use_compression(&self) -> Option<bool>;

    fn num_workers(&self) -> usize;

    fn create_workers(&self, sockets: Vec<ExaSocket>, with_compression: bool) -> Vec<Self::Worker>;

    fn query(&self, addrs: Vec<SocketAddrV4>, with_tls: bool, with_compression: bool) -> String;
}
