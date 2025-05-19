use std::{collections::HashMap, path::PathBuf, sync::Arc};

use anyhow::Context;
use aqueducts::{progress_tracker::LoggingProgressTracker, run_pipeline};
use tracing::{debug, info};

use crate::parse_aqueduct_file;

pub async fn run_local(
    file: PathBuf,
    params: HashMap<String, String>,
) -> Result<(), anyhow::Error> {
    info!("Running pipeline locally from file: {}", file.display());

    aqueducts::register_handlers();
    let aqueduct = parse_aqueduct_file(&file, params)?;

    debug!("Creating SessionContext");
    let mut ctx = datafusion::prelude::SessionContext::new();
    datafusion_functions_json::register_all(&mut ctx)
        .context("Failed to register JSON functions")?;

    let progress_tracker = Arc::new(LoggingProgressTracker);

    debug!("Starting pipeline execution");
    run_pipeline(Arc::new(ctx), aqueduct, Some(progress_tracker))
        .await
        .context("Failure during execution of aqueducts file")?;

    debug!("Pipeline execution completed successfully");
    Ok(())
}
