use crate::{
    destinations::DestinationError, sources::SourceError, stages::StageError,
    templating::TemplateError,
};

use miette::Diagnostic;

pub type Result<T> = core::result::Result<T, Error>;

#[derive(Debug, thiserror::Error, Diagnostic)]
pub enum Error {
    #[error(transparent)]
    #[diagnostic(transparent)]
    Source(#[from] SourceError),

    #[error(transparent)]
    #[diagnostic(transparent)]
    Stage(#[from] StageError),

    #[error(transparent)]
    #[diagnostic(transparent)]
    Destination(#[from] DestinationError),

    #[error(transparent)]
    #[diagnostic(transparent)]
    Template(#[from] TemplateError),
}
