use std::ops::{Add, AddAssign};

/// A query result containing the number of affected rows.
#[derive(Copy, Clone, Debug, Default)]
pub struct ExaQueryResult {
    rows_affected: u64,
}

impl ExaQueryResult {
    #[must_use]
    pub fn new(rows_affected: u64) -> Self {
        Self { rows_affected }
    }

    #[must_use]
    pub fn rows_affected(&self) -> u64 {
        self.rows_affected
    }
}

impl Extend<ExaQueryResult> for ExaQueryResult {
    fn extend<T: IntoIterator<Item = ExaQueryResult>>(&mut self, iter: T) {
        for elem in iter {
            self.rows_affected += elem.rows_affected;
        }
    }
}

impl Add for ExaQueryResult {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        Self {
            rows_affected: self.rows_affected + rhs.rows_affected,
        }
    }
}

impl AddAssign for ExaQueryResult {
    fn add_assign(&mut self, rhs: Self) {
        self.rows_affected += rhs.rows_affected;
    }
}
