#![cfg_attr(not(test), warn(unused_crate_dependencies))]

mod parse;

#[allow(unused_imports, reason = "built-in; conditionally compiled")]
use proc_macro::TokenStream;

#[cfg(feature = "macros")]
#[proc_macro]
pub fn expand_query(input: TokenStream) -> TokenStream {
    let input = syn::parse_macro_input!(input as sqlx_macros_core::query::QueryMacroInput);

    match sqlx_macros_core::query::expand_input(input, &[sqlx_exasol_impl::QUERY_DRIVER]) {
        Ok(ts) => parse::rewrite(ts).into(),
        Err(e) => {
            if let Some(parse_err) = e.downcast_ref::<syn::Error>() {
                parse_err.to_compile_error().into()
            } else {
                let msg = e.to_string();
                quote::quote!(::std::compile_error!(#msg)).into()
            }
        }
    }
}

#[cfg(feature = "derive")]
#[proc_macro_derive(Encode, attributes(sqlx))]
pub fn derive_encode(tokenstream: TokenStream) -> TokenStream {
    let input = syn::parse_macro_input!(tokenstream as syn::DeriveInput);
    match sqlx_macros_core::derives::expand_derive_encode(&input) {
        Ok(ts) => parse::rewrite(ts).into(),
        Err(e) => e.to_compile_error().into(),
    }
}

#[cfg(feature = "derive")]
#[proc_macro_derive(Decode, attributes(sqlx))]
pub fn derive_decode(tokenstream: TokenStream) -> TokenStream {
    let input = syn::parse_macro_input!(tokenstream as syn::DeriveInput);
    match sqlx_macros_core::derives::expand_derive_decode(&input) {
        Ok(ts) => parse::rewrite(ts).into(),
        Err(e) => e.to_compile_error().into(),
    }
}

#[cfg(feature = "derive")]
#[proc_macro_derive(Type, attributes(sqlx))]
pub fn derive_type(tokenstream: TokenStream) -> TokenStream {
    let input = syn::parse_macro_input!(tokenstream as syn::DeriveInput);
    match sqlx_macros_core::derives::expand_derive_type_encode_decode(&input) {
        Ok(ts) => parse::rewrite(ts).into(),
        Err(e) => e.to_compile_error().into(),
    }
}

#[cfg(feature = "derive")]
#[proc_macro_derive(FromRow, attributes(sqlx))]
pub fn derive_from_row(input: TokenStream) -> TokenStream {
    let input = syn::parse_macro_input!(input as syn::DeriveInput);

    match sqlx_macros_core::derives::expand_derive_from_row(&input) {
        Ok(ts) => parse::rewrite(ts).into(),
        Err(e) => e.to_compile_error().into(),
    }
}

#[cfg(feature = "migrate")]
#[proc_macro]
pub fn migrate(input: TokenStream) -> TokenStream {
    let input = syn::parse_macro_input!(input as Option<syn::LitStr>);
    match sqlx_macros_core::migrate::expand(input) {
        // Wrap the TokenStream in a block so it can be parsed as a syn::Stmt.
        // Otherwise we'd have to special case this to a syn::Expr.
        //
        // NOTE: Stmt::Expr variant does not normally apply here!
        Ok(ts) => parse::rewrite(quote::quote!({#ts})).into(),
        Err(e) => {
            if let Some(parse_err) = e.downcast_ref::<syn::Error>() {
                parse_err.to_compile_error().into()
            } else {
                let msg = e.to_string();
                quote::quote!(::std::compile_error!(#msg)).into()
            }
        }
    }
}

#[cfg(feature = "macros")]
#[proc_macro_attribute]
pub fn test(args: TokenStream, input: TokenStream) -> TokenStream {
    let input = syn::parse_macro_input!(input as syn::ItemFn);

    match sqlx_macros_core::test_attr::expand(args.into(), input) {
        Ok(ts) => parse::rewrite(ts).into(),
        Err(e) => {
            if let Some(parse_err) = e.downcast_ref::<syn::Error>() {
                parse_err.to_compile_error().into()
            } else {
                let msg = e.to_string();
                quote::quote!(::std::compile_error!(#msg)).into()
            }
        }
    }
}
