/// The EXPORT source type, which can either directly be a table or an entire query.
#[derive(Clone, Copy, Debug)]
pub enum ExportSource<'a> {
    Query(&'a str),
    Table(&'a str),
}
