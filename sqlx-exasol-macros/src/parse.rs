use proc_macro2::TokenStream;
use syn::{parse::Parse, visit_mut::VisitMut, ItemUse, Path, Stmt, UseTree};

/// Rewrites `::sqlx::` to `::sqlx_exasol::`.
pub fn rewrite(token_stream: TokenStream) -> TokenStream {
    let mut stmts: Vec<Stmt> = match syn::parse2(token_stream) {
        Ok(Stmts(stmts)) => stmts,
        Err(err) => return err.to_compile_error(),
    };

    for stmt in &mut stmts {
        SqlxToSqlxExasol.visit_stmt_mut(stmt);
    }

    quote::quote!(#(#stmts)*)
}

struct Stmts(Vec<Stmt>);

impl Parse for Stmts {
    fn parse(input: syn::parse::ParseStream<'_>) -> syn::Result<Self> {
        let mut items = Vec::new();
        while !input.is_empty() {
            items.push(input.parse()?);
        }
        Ok(Self(items))
    }
}

struct SqlxToSqlxExasol;

impl VisitMut for SqlxToSqlxExasol {
    fn visit_path_mut(&mut self, path: &mut Path) {
        // Recurse to also match inner paths such as generics.
        syn::visit_mut::visit_path_mut(self, path);

        // Match ::sqlx::...
        if path.leading_colon.is_some() {
            if let Some(segment) = path.segments.first_mut() {
                if segment.ident == "sqlx" {
                    segment.ident = syn::Ident::new("sqlx_exasol", segment.ident.span());
                }
            }
        }
    }

    fn visit_item_use_mut(&mut self, item_use: &mut ItemUse) {
        // Match ::sqlx::...
        if item_use.leading_colon.is_some() {
            if let UseTree::Path(path) = &mut item_use.tree {
                if path.ident == "sqlx" {
                    path.ident = syn::Ident::new("sqlx_exasol", path.ident.span());
                }
            }
        }
    }
}
