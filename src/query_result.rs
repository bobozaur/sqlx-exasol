#[derive(Debug, Default)]
pub struct ExaQueryResult {
    rows_affected: u64,
}

impl ExaQueryResult {
    pub fn new(rows_affected: u64) -> Self {
        Self { rows_affected }
    }
}

impl Extend<ExaQueryResult> for ExaQueryResult {
    fn extend<T: IntoIterator<Item = ExaQueryResult>>(&mut self, iter: T) {
        for elem in iter {
            self.rows_affected += elem.rows_affected
        }
    }
}
