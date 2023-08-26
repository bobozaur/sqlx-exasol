use std::{fmt::Debug, net::SocketAddrV4};

use crate::{
    connection::{
        etl::{append_filenames, socket_spawners, RowSeparator},
        websocket::socket::ExaSocket,
    },
    error::ExaResultExt,
    ExaConnection, ExaQueryResult,
};

use futures_core::future::BoxFuture;

use futures_util::future::{select, try_join_all, Either};
use sqlx_core::{error::BoxDynError, Error as SqlxError};

use super::{reader::ExportReader, ExaExport};

/// Export options
#[derive(Debug)]
pub struct ExportBuilder<'a> {
    num_readers: usize,
    compression: Option<bool>,
    source: QueryOrTable<'a>,
    comment: Option<&'a str>,
    encoding: Option<&'a str>,
    null: &'a str,
    row_separator: RowSeparator,
    column_separator: &'a str,
    column_delimiter: &'a str,
    with_column_names: bool,
}

impl<'a> ExportBuilder<'a> {
    pub fn new(source: QueryOrTable<'a>) -> Self {
        Self {
            num_readers: 0,
            compression: None,
            source,
            comment: None,
            encoding: None,
            null: "",
            row_separator: RowSeparator::CRLF,
            column_separator: ",",
            column_delimiter: "\"",
            with_column_names: true,
        }
    }

    pub async fn build<'b>(
        &mut self,
        con: &'b mut ExaConnection,
    ) -> Result<
        (
            BoxFuture<'b, Result<ExaQueryResult, BoxDynError>>,
            Vec<ExaExport>,
        ),
        SqlxError,
    > {
        let ips = con.ws.get_hosts().await?;
        let port = con.ws.socket_addr().port();
        let with_tls = con.attributes().encryption_enabled;
        let with_compression = self
            .compression
            .unwrap_or(con.attributes().compression_enabled);

        let (futures, rxs): (Vec<_>, Vec<_>) =
            socket_spawners(self.num_readers, ips, port, with_tls)
                .await?
                .into_iter()
                .unzip();

        let addrs_fut = try_join_all(rxs);
        let sockets_fut = try_join_all(futures);

        let (addrs, sockets_fut): (Vec<_>, BoxFuture<'_, Result<Vec<ExaSocket>, SqlxError>>) =
            match select(addrs_fut, sockets_fut).await {
                Either::Left((addrs, sockets_fut)) => (addrs.to_sqlx_err()?, Box::pin(sockets_fut)),
                Either::Right((sockets, addrs_fut)) => (
                    addrs_fut.await.to_sqlx_err()?,
                    Box::pin(async move { sockets }),
                ),
            };

        let query = self.query(addrs, with_tls, with_compression);
        let query_fut = Box::pin(con.execute_etl(query));

        let (sockets, query_fut) = match select(query_fut, sockets_fut).await {
            Either::Right((sockets, f)) => (sockets?, f),
            _ => unreachable!("ETL cannot finish before we use the sockets"),
        };

        let sockets = sockets
            .into_iter()
            .map(ExportReader::new)
            .map(|r| ExaExport::new(r, with_compression))
            .collect();

        Ok((query_fut, sockets))
    }

    /// Sets the number of reader jobs that will be started.
    /// If set to `0`, then as many as possible will be used (one per node).
    /// Providing a number bigger than the number of nodes is the same as providing `0`.
    pub fn num_readers(&mut self, num_readers: usize) -> &mut Self {
        self.num_readers = num_readers;
        self
    }

    #[cfg(feature = "compression")]
    pub fn compression(&mut self, enabled: bool) -> &mut Self {
        self.compression = Some(enabled);
        self
    }

    pub fn comment(&mut self, comment: &'a str) -> &mut Self {
        self.comment = Some(comment);
        self
    }

    pub fn encoding(&mut self, encoding: &'a str) -> &mut Self {
        self.encoding = Some(encoding);
        self
    }

    pub fn null(&mut self, null: &'a str) -> &mut Self {
        self.null = null;
        self
    }

    pub fn row_separator(&mut self, separator: RowSeparator) -> &mut Self {
        self.row_separator = separator;
        self
    }

    pub fn column_separator(&mut self, separator: &'a str) -> &mut Self {
        self.column_separator = separator;
        self
    }

    pub fn column_delimiter(&mut self, delimiter: &'a str) -> &mut Self {
        self.column_delimiter = delimiter;
        self
    }

    pub fn with_column_names(&mut self, flag: bool) -> &mut Self {
        self.with_column_names = flag;
        self
    }

    fn query(&self, addrs: Vec<SocketAddrV4>, with_tls: bool, with_compression: bool) -> String {
        let mut query = String::new();

        if let Some(com) = self.comment {
            query.push_str("/*");
            query.push_str(com);
            query.push_str("*/\n");
        }

        query.push_str("EXPORT ");

        match self.source {
            QueryOrTable::Table(t) => {
                query.push('"');
                query.push_str(t);
                query.push('"');
            }
            QueryOrTable::Query(q) => {
                query.push_str("(\n");
                query.push_str(q);
                query.push_str("\n)");
            }
        };

        query.push_str(" INTO CSV ");

        append_filenames(&mut query, addrs, with_tls, with_compression);

        if let Some(enc) = self.encoding {
            query.push_str(" ENCODING = '");
            query.push_str(enc);
            query.push('\'');
        }

        query.push_str(" NULL = '");
        query.push_str(self.null);
        query.push('\'');

        query.push_str(" ROW SEPARATOR = '");
        query.push_str(self.row_separator.as_ref());
        query.push('\'');

        query.push_str(" COLUMN SEPARATOR = '");
        query.push_str(self.column_separator);
        query.push('\'');

        query.push_str(" COLUMN DELIMITER = '");
        query.push_str(self.column_delimiter);
        query.push('\'');

        if self.with_column_names {
            query.push_str(" WITH COLUMN NAMES");
        }

        query
    }
}

#[derive(Clone, Copy, Debug)]
pub enum QueryOrTable<'a> {
    Query(&'a str),
    Table(&'a str),
}
