use crate::{
    destinations::DestinationError, sources::SourceError, stages::StageError,
    templating::TemplateError,
};

pub type Result<T> = core::result::Result<T, AqueductsError>;

#[derive(Debug, thiserror::Error)]
pub enum AqueductsError {
    #[error(transparent)]
    Template(#[from] TemplateError),

    #[error(transparent)]
    Source(#[from] SourceError),

    #[error(transparent)]
    Stage(#[from] StageError),

    #[error(transparent)]
    Destination(#[from] DestinationError),
}
