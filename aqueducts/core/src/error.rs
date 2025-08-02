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
    Source(Box<SourceError>),

    #[error(transparent)]
    #[diagnostic(transparent)]
    Stage(Box<StageError>),

    #[error(transparent)]
    #[diagnostic(transparent)]
    Destination(Box<DestinationError>),

    #[error(transparent)]
    #[diagnostic(transparent)]
    Template(Box<TemplateError>),
}

impl From<SourceError> for Error {
    fn from(error: SourceError) -> Self {
        Error::Source(Box::new(error))
    }
}

impl From<StageError> for Error {
    fn from(error: StageError) -> Self {
        Error::Stage(Box::new(error))
    }
}

impl From<DestinationError> for Error {
    fn from(error: DestinationError) -> Self {
        Error::Destination(Box::new(error))
    }
}

impl From<TemplateError> for Error {
    fn from(error: TemplateError) -> Self {
        Error::Template(Box::new(error))
    }
}
