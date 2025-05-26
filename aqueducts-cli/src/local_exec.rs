use std::{collections::HashMap, path::PathBuf, sync::Arc};

use anyhow::Context;
use aqueducts::prelude::*;
use tracing::{debug, info};

pub async fn run_local(file: PathBuf, params: HashMap<String, String>) -> anyhow::Result<()> {
    info!("Running pipeline locally from file: {}", file.display());

    let aqueduct = Aqueduct::from_file(&file, params)?;

    debug!("Creating SessionContext");
    let mut ctx = datafusion::prelude::SessionContext::new();

    aqueducts::custom_udfs::register_all(&mut ctx)?;

    let progress_tracker = Arc::new(LoggingProgressTracker);

    debug!("Starting pipeline execution");
    run_pipeline(Arc::new(ctx), aqueduct, Some(progress_tracker))
        .await
        .context("Failure during execution of aqueducts file")?;

    debug!("Pipeline execution completed successfully");
    Ok(())
}
