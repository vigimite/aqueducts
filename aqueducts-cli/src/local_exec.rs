use std::{collections::HashMap, path::PathBuf, sync::Arc};

use aqueducts::prelude::*;
use miette::{IntoDiagnostic, Result};
use tracing::{debug, info};

pub async fn run_local(file: PathBuf, params: HashMap<String, String>) -> Result<()> {
    info!("Running pipeline locally from file: {}", file.display());

    let format = format_from_path(&file);
    let aqueduct = Aqueduct::from_file(&file, format, params)?;

    debug!("Creating SessionContext");
    let mut ctx = datafusion::prelude::SessionContext::new();

    aqueducts::custom_udfs::register_all(&mut ctx).into_diagnostic()?;

    let progress_tracker = Arc::new(LoggingProgressTracker);

    debug!("Starting pipeline execution");
    run_pipeline(Arc::new(ctx), aqueduct, Some(progress_tracker)).await?;

    debug!("Pipeline execution completed successfully");
    Ok(())
}
