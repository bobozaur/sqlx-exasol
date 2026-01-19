#![cfg_attr(not(test), warn(unused_crate_dependencies))]

#[cfg(feature = "macros")]
#[proc_macro]
pub fn expand_query(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input = syn::parse_macro_input!(input as sqlx_macros_core::query::QueryMacroInput);

    match sqlx_macros_core::query::expand_input(input, &[sqlx_exasol_impl::QUERY_DRIVER]) {
        Ok(ts) => ts.into(),
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
