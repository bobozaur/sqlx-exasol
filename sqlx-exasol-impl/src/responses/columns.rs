use std::sync::Arc;

use serde::Deserialize;

use crate::ExaColumn;

/// Helper type for deserializing columns ready for shared ownership while also setting the ordinal
/// value of the column in the array.
#[derive(Debug, Deserialize)]
#[serde(transparent)]
pub struct ExaColumns(pub Vec<ExaColumn>);

impl From<ExaColumns> for Arc<[ExaColumn]> {
    fn from(mut value: ExaColumns) -> Self {
        value
            .0
            .iter_mut()
            .enumerate()
            .for_each(|(idx, c)| c.ordinal = idx);

        value.0.into()
    }
}
