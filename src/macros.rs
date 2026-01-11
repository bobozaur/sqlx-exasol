#[macro_export]
#[cfg_attr(docsrs, doc(cfg(feature = "macros")))]
macro_rules! query (
    ($query:expr) => ({
        $crate::sqlx_exasol_macros::expand_query!(source = $query)
    });
    ($query:expr, $($args:tt)*) => ({
        $crate::sqlx_exasol_macros::expand_query!(source = $query, args = [$($args)*])
    })
);

#[macro_export]
#[cfg_attr(docsrs, doc(cfg(feature = "macros")))]
macro_rules! query_unchecked (
    ($query:expr) => ({
        $crate::sqlx_exasol_macros::expand_query!(source = $query, checked = false)
    });
    ($query:expr, $($args:tt)*) => ({
        $crate::sqlx_exasol_macros::expand_query!(source = $query, args = [$($args)*], checked = false)
    })
);

#[macro_export]
#[cfg_attr(docsrs, doc(cfg(feature = "macros")))]
macro_rules! query_file (
    ($path:literal) => ({
        $crate::sqlx_exasol_macros::expand_query!(source_file = $path)
    });
    ($path:literal, $($args:tt)*) => ({
        $crate::sqlx_exasol_macros::expand_query!(source_file = $path, args = [$($args)*])
    })
);

#[macro_export]
#[cfg_attr(docsrs, doc(cfg(feature = "macros")))]
macro_rules! query_file_unchecked (
    ($path:literal) => ({
        $crate::sqlx_exasol_macros::expand_query!(source_file = $path, checked = false)
    });
    ($path:literal, $($args:tt)*) => ({
        $crate::sqlx_exasol_macros::expand_query!(source_file = $path, args = [$($args)*], checked = false)
    })
);

#[macro_export]
#[cfg_attr(docsrs, doc(cfg(feature = "macros")))]
macro_rules! query_as (
    ($out_struct:path, $query:expr) => ( {
        $crate::sqlx_exasol_macros::expand_query!(record = $out_struct, source = $query)
    });
    ($out_struct:path, $query:expr, $($args:tt)*) => ( {
        $crate::sqlx_exasol_macros::expand_query!(record = $out_struct, source = $query, args = [$($args)*])
    })
);

#[macro_export]
#[cfg_attr(docsrs, doc(cfg(feature = "macros")))]
macro_rules! query_file_as (
    ($out_struct:path, $path:literal) => ( {
        $crate::sqlx_exasol_macros::expand_query!(record = $out_struct, source_file = $path)
    });
    ($out_struct:path, $path:literal, $($args:tt)*) => ( {
        $crate::sqlx_exasol_macros::expand_query!(record = $out_struct, source_file = $path, args = [$($args)*])
    })
);

#[macro_export]
#[cfg_attr(docsrs, doc(cfg(feature = "macros")))]
macro_rules! query_as_unchecked (
    ($out_struct:path, $query:expr) => ( {
        $crate::sqlx_exasol_macros::expand_query!(record = $out_struct, source = $query, checked = false)
    });

    ($out_struct:path, $query:expr, $($args:tt)*) => ( {
        $crate::sqlx_exasol_macros::expand_query!(record = $out_struct, source = $query, args = [$($args)*], checked = false)
    })
);

#[macro_export]
#[cfg_attr(docsrs, doc(cfg(feature = "macros")))]
macro_rules! query_file_as_unchecked (
    ($out_struct:path, $path:literal) => ( {
        $crate::sqlx_exasol_macros::expand_query!(record = $out_struct, source_file = $path, checked = false)
    });

    ($out_struct:path, $path:literal, $($args:tt)*) => ( {
        $crate::sqlx_exasol_macros::expand_query!(record = $out_struct, source_file = $path, args = [$($args)*], checked = false)
    })
);

#[macro_export]
#[cfg_attr(docsrs, doc(cfg(feature = "macros")))]
macro_rules! query_scalar (
    ($query:expr) => (
        $crate::sqlx_exasol_macros::expand_query!(scalar = _, source = $query)
    );
    ($query:expr, $($args:tt)*) => (
        $crate::sqlx_exasol_macros::expand_query!(scalar = _, source = $query, args = [$($args)*])
    )
);

#[macro_export]
#[cfg_attr(docsrs, doc(cfg(feature = "macros")))]
macro_rules! query_file_scalar (
    ($path:literal) => (
        $crate::sqlx_exasol_macros::expand_query!(scalar = _, source_file = $path)
    );
    ($path:literal, $($args:tt)*) => (
        $crate::sqlx_exasol_macros::expand_query!(scalar = _, source_file = $path, args = [$($args)*])
    )
);

#[macro_export]
#[cfg_attr(docsrs, doc(cfg(feature = "macros")))]
macro_rules! query_scalar_unchecked (
    ($query:expr) => (
        $crate::sqlx_exasol_macros::expand_query!(scalar = _, source = $query, checked = false)
    );
    ($query:expr, $($args:tt)*) => (
        $crate::sqlx_exasol_macros::expand_query!(scalar = _, source = $query, args = [$($args)*], checked = false)
    )
);

#[macro_export]
#[cfg_attr(docsrs, doc(cfg(feature = "macros")))]
macro_rules! query_file_scalar_unchecked (
    ($path:literal) => (
        $crate::sqlx_exasol_macros::expand_query!(scalar = _, source_file = $path, checked = false)
    );
    ($path:literal, $($args:tt)*) => (
        $crate::sqlx_exasol_macros::expand_query!(scalar = _, source_file = $path, args = [$($args)*], checked = false)
    )
);
