use std::{borrow::Cow, future};

use futures_core::{future::BoxFuture, stream::BoxStream};
use futures_util::{stream, StreamExt, TryStreamExt};
#[cfg(feature = "migrate")]
use sqlx_core::migrate::Migrate;
use sqlx_core::{
    any::{
        Any, AnyArguments, AnyColumn, AnyConnectOptions, AnyConnectionBackend, AnyQueryResult,
        AnyRow, AnyStatement, AnyTypeInfo, AnyTypeInfoKind, AnyValue, AnyValueKind,
    },
    column::Column,
    connection::{ConnectOptions, Connection},
    database::Database,
    decode::Decode,
    describe::Describe,
    executor::Executor,
    logger::QueryLogger,
    row::Row,
    transaction::TransactionManager,
    value::ValueRef,
    Either,
};

#[cfg(feature = "migrate")]
use crate::SqlxResult;
use crate::{
    connection::{
        stream::ResultStream,
        websocket::future::{Execute, ExecuteBatch, ExecutePrepared},
    },
    type_info::ExaDataType,
    ExaColumn, ExaConnectOptions, ExaConnection, ExaQueryResult, ExaRow, ExaTransactionManager,
    ExaTypeInfo, ExaValueRef, Exasol, SqlxError,
};

sqlx_core::declare_driver_with_optional_migrate!(DRIVER = Exasol);

impl AnyConnectionBackend for ExaConnection {
    fn name(&self) -> &str {
        <Exasol as Database>::NAME
    }

    fn close(self: Box<Self>) -> BoxFuture<'static, SqlxResult<()>> {
        Connection::close(*self)
    }

    fn close_hard(self: Box<Self>) -> BoxFuture<'static, SqlxResult<()>> {
        Connection::close_hard(*self)
    }

    fn ping(&mut self) -> BoxFuture<'_, SqlxResult<()>> {
        Connection::ping(self)
    }

    fn begin(&mut self, statement: Option<Cow<'static, str>>) -> BoxFuture<'_, SqlxResult<()>> {
        ExaTransactionManager::begin(self, statement)
    }

    fn commit(&mut self) -> BoxFuture<'_, SqlxResult<()>> {
        ExaTransactionManager::commit(self)
    }

    fn rollback(&mut self) -> BoxFuture<'_, SqlxResult<()>> {
        ExaTransactionManager::rollback(self)
    }

    fn start_rollback(&mut self) {
        ExaTransactionManager::start_rollback(self);
    }

    fn get_transaction_depth(&self) -> usize {
        ExaTransactionManager::get_transaction_depth(self)
    }

    fn shrink_buffers(&mut self) {
        Connection::shrink_buffers(self);
    }

    fn flush(&mut self) -> BoxFuture<'_, SqlxResult<()>> {
        Connection::flush(self)
    }

    fn should_flush(&self) -> bool {
        Connection::should_flush(self)
    }

    #[cfg(feature = "migrate")]
    fn as_migrate(&mut self) -> SqlxResult<&mut (dyn Migrate + Send + 'static)> {
        Ok(self)
    }

    fn fetch_many<'q>(
        &'q mut self,
        sql: &'q str,
        persistent: bool,
        arguments: Option<AnyArguments<'q>>,
    ) -> BoxStream<'q, SqlxResult<Either<AnyQueryResult, AnyRow>>> {
        let logger = QueryLogger::new(sql, self.log_settings.clone());
        let arguments = match arguments.as_ref().map(AnyArguments::convert_to).transpose() {
            Ok(arguments) => arguments,
            Err(error) => {
                return stream::once(future::ready(Err(sqlx_core::Error::Encode(error)))).boxed()
            }
        };

        let filter_fn = |step| async move {
            match step {
                Either::Left(qr) => Ok(Some(Either::Left(map_result(qr)))),
                Either::Right(row) => AnyRow::try_from(&row).map(Either::Right).map(Some),
            }
        };

        if let Some(arguments) = arguments {
            let future = ExecutePrepared::new(sql, persistent, arguments);
            ResultStream::new(&mut self.ws, logger, future)
                .try_filter_map(filter_fn)
                .boxed()
        } else {
            let future = ExecuteBatch::new(sql);
            ResultStream::new(&mut self.ws, logger, future)
                .try_filter_map(filter_fn)
                .boxed()
        }
    }

    fn fetch_optional<'q>(
        &'q mut self,
        sql: &'q str,
        persistent: bool,
        arguments: Option<AnyArguments<'q>>,
    ) -> BoxFuture<'q, SqlxResult<Option<AnyRow>>> {
        let logger = QueryLogger::new(sql, self.log_settings.clone());
        let arguments = arguments
            .as_ref()
            .map(AnyArguments::convert_to)
            .transpose()
            .map_err(sqlx_core::Error::Encode);

        Box::pin(async move {
            let arguments = arguments?;

            let mut stream = if let Some(arguments) = arguments {
                let future = ExecutePrepared::new(sql, persistent, arguments);
                ResultStream::new(&mut self.ws, logger, future)
            } else {
                let future = Execute::new(sql);
                ResultStream::new(&mut self.ws, logger, future)
            };

            while let Some(result) = stream.try_next().await? {
                if let Either::Right(row) = result {
                    return Ok(Some(AnyRow::try_from(&row)?));
                }
            }

            Ok(None)
        })
    }

    fn prepare_with<'c, 'q: 'c>(
        &'c mut self,
        sql: &'q str,
        _parameters: &[AnyTypeInfo],
    ) -> BoxFuture<'c, SqlxResult<AnyStatement<'q>>> {
        Box::pin(async move {
            let statement = Executor::prepare_with(self, sql, &[]).await?;
            AnyStatement::try_from_statement(
                sql,
                &statement,
                statement.metadata.column_names.clone(),
            )
        })
    }

    fn describe<'q>(&'q mut self, sql: &'q str) -> BoxFuture<'q, SqlxResult<Describe<Any>>> {
        Box::pin(async move {
            let describe = Executor::describe(self, sql).await?;
            describe.try_into_any()
        })
    }
}

impl<'a> TryFrom<&'a ExaTypeInfo> for AnyTypeInfo {
    type Error = SqlxError;

    fn try_from(type_info: &'a ExaTypeInfo) -> Result<Self, Self::Error> {
        Ok(AnyTypeInfo {
            kind: match &type_info.data_type {
                ExaDataType::Null => AnyTypeInfoKind::Null,
                ExaDataType::Boolean => AnyTypeInfoKind::Bool,
                ExaDataType::Decimal(_) => AnyTypeInfoKind::BigInt,
                ExaDataType::Double => AnyTypeInfoKind::Double,
                ExaDataType::HashType(_) | ExaDataType::Char(_) | ExaDataType::Varchar(_) => {
                    AnyTypeInfoKind::Text
                }
                _ => {
                    return Err(sqlx_core::Error::AnyDriverError(
                        format!("Any driver does not support Exasol type {type_info:?}").into(),
                    ))
                }
            },
        })
    }
}

impl<'a> TryFrom<&'a ExaColumn> for AnyColumn {
    type Error = sqlx_core::Error;

    fn try_from(column: &'a ExaColumn) -> Result<Self, Self::Error> {
        let type_info = AnyTypeInfo::try_from(&column.data_type)?;

        Ok(AnyColumn {
            ordinal: column.ordinal,
            name: column.name.to_string().into(),
            type_info,
        })
    }
}

impl<'a> TryFrom<&'a ExaRow> for AnyRow {
    type Error = sqlx_core::Error;

    fn try_from(row: &'a ExaRow) -> Result<Self, Self::Error> {
        fn decode<'r, T: Decode<'r, Exasol>>(valueref: ExaValueRef<'r>) -> SqlxResult<T> {
            Decode::decode(valueref).map_err(SqlxError::decode)
        }

        let mut row_out = AnyRow {
            column_names: row.column_names.clone(),
            columns: Vec::with_capacity(row.columns().len()),
            values: Vec::with_capacity(row.columns().len()),
        };

        for col in row.columns() {
            let i = col.ordinal();

            let any_col = AnyColumn::try_from(col)?;

            let value = row.try_get_raw(i)?;

            // Map based on the _value_ type info, not the column type info.
            let type_info = AnyTypeInfo::try_from(value.type_info().as_ref()).map_err(|e| {
                SqlxError::ColumnDecode {
                    index: col.ordinal().to_string(),
                    source: e.into(),
                }
            })?;

            let value_kind = match type_info.kind {
                k if value.is_null() => AnyValueKind::Null(k),
                AnyTypeInfoKind::Null => AnyValueKind::Null(AnyTypeInfoKind::Null),
                AnyTypeInfoKind::Bool => AnyValueKind::Bool(decode(value)?),
                AnyTypeInfoKind::SmallInt => AnyValueKind::SmallInt(decode(value)?),
                AnyTypeInfoKind::Integer => AnyValueKind::Integer(decode(value)?),
                AnyTypeInfoKind::BigInt => AnyValueKind::BigInt(decode(value)?),
                AnyTypeInfoKind::Real => AnyValueKind::Real(decode(value)?),
                AnyTypeInfoKind::Double => AnyValueKind::Double(decode(value)?),
                AnyTypeInfoKind::Text => AnyValueKind::Text(decode::<String>(value)?.into()),
                AnyTypeInfoKind::Blob => Err(SqlxError::decode(
                    "unsupported data type by the `any` driver",
                ))?,
            };

            row_out.columns.push(any_col);
            row_out.values.push(AnyValue { kind: value_kind });
        }

        Ok(row_out)
    }
}

impl<'a> TryFrom<&'a AnyConnectOptions> for ExaConnectOptions {
    type Error = sqlx_core::Error;

    fn try_from(any_opts: &'a AnyConnectOptions) -> Result<Self, Self::Error> {
        let mut opts = Self::from_url(&any_opts.database_url)?;
        opts.log_settings = any_opts.log_settings.clone();
        Ok(opts)
    }
}

fn map_result(result: ExaQueryResult) -> AnyQueryResult {
    AnyQueryResult {
        rows_affected: result.rows_affected(),
        last_insert_id: None,
    }
}
