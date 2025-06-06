#![cfg_attr(not(test), warn(unused_crate_dependencies))]

use proc_macro::TokenStream;
use quote::quote;
use sqlx_exasol_impl::QUERY_DRIVER;
use sqlx_macros_core::query;

#[proc_macro]
pub fn expand_query(input: TokenStream) -> TokenStream {
    let input = syn::parse_macro_input!(input as query::QueryMacroInput);

    match query::expand_input(input, &[QUERY_DRIVER]) {
        Ok(ts) => ts.into(),
        Err(e) => {
            if let Some(parse_err) = e.downcast_ref::<syn::Error>() {
                parse_err.to_compile_error().into()
            } else {
                let msg = e.to_string();
                quote!(::std::compile_error!(#msg)).into()
            }
        }
    }
}
