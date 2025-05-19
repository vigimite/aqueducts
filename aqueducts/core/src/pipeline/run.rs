//! Pipeline execution logic

use std::{collections::HashMap, sync::Arc, time::Instant};

use datafusion::execution::context::SessionContext;
use regex::Regex;
use tokio::task::JoinHandle;
use tracing::{debug, instrument, warn};

use crate::error;
use crate::execution::traits::{DestinationProvider, SourceProvider, StageProvider};
use crate::model::stages::Stage;
use crate::pipeline::progress_tracker::{ProgressEvent, ProgressTracker};
use crate::pipeline::Result;

/// Execute an `Aqueduct` pipeline with optional progress tracking
/// The progress tracker receives events during execution to monitor status
/// Returns the provided context once the pipeline completes
#[instrument(skip_all, err)]
pub async fn run_pipeline(
    ctx: Arc<SessionContext>,
    aqueduct: crate::Aqueduct,
    progress_tracker: Option<Arc<dyn ProgressTracker>>,
) -> Result<Arc<SessionContext>> {
    let mut stage_ttls: HashMap<String, usize> = HashMap::new();
    let start_time = Instant::now();

    debug!("Running Aqueduct ...");

    if let Some(tracker) = &progress_tracker {
        tracker.on_progress(ProgressEvent::Started);
    }

    if let Some(destination) = &aqueduct.destination {
        let time = Instant::now();

        destination.register(ctx.clone()).await?;

        debug!(
            "Created destination ... Elapsed time: {:.2?}",
            time.elapsed()
        );
    }

    let handles = aqueduct
        .sources
        .iter()
        .map(|source| {
            let time = Instant::now();
            let source_ = source.clone();
            let ctx_ = ctx.clone();
            let source_name = source.name();

            let handle = tokio::spawn(async move {
                source_.register(ctx_).await?;

                Ok(())
            });

            (source_name, time, handle)
        })
        .collect::<Vec<(String, Instant, JoinHandle<Result<()>>)>>();

    for (source_name, time, handle) in handles {
        handle.await.expect("failed to join task")?;

        debug!(
            "Registered source {source_name} ... Elapsed time: {:.2?}",
            time.elapsed()
        );

        if let Some(tracker) = &progress_tracker {
            tracker.on_progress(ProgressEvent::SourceRegistered { name: source_name });
        }
    }

    for (pos, parallel) in aqueduct.stages.iter().enumerate() {
        let mut handles: Vec<JoinHandle<Result<()>>> = Vec::new();

        for (sub, stage) in parallel.iter().enumerate() {
            let stage_ = stage.clone();
            let ctx_ = ctx.clone();
            let name = stage.name.clone();
            let tracker = progress_tracker.clone();

            let handle = tokio::spawn(async move {
                let time = Instant::now();
                debug!("Running stage {} #{pos}:{sub}", name);

                if let Some(tracker_ref) = &tracker {
                    tracker_ref.on_progress(ProgressEvent::StageStarted {
                        name: name.clone(),
                        position: pos,
                        sub_position: sub,
                    });
                }

                let df = stage_.execute(ctx_.clone()).await?;
                // Instead of wrapping in another Arc, pass the DataFusion view directly
                ctx_.register_table(stage_.name.as_str(), df.into_view())
                    .map_err(error::Error::ReadTableError)?;

                let elapsed = time.elapsed();
                debug!(
                    "Finished processing stage {name} #{pos}:{sub} ... Elapsed time: {:.2?}",
                    elapsed
                );

                if let Some(tracker) = &tracker {
                    tracker.on_progress(ProgressEvent::StageCompleted {
                        name: name.clone(),
                        position: pos,
                        sub_position: sub,
                        duration_ms: elapsed.as_millis() as u64,
                    });
                }

                Ok(())
            });

            calculate_ttl(&mut stage_ttls, stage.name.as_str(), pos, &aqueduct.stages)?;
            handles.push(handle);
        }

        for handle in handles {
            handle.await.expect("failed to join task")?;
        }

        deregister_stages(ctx.clone(), &stage_ttls, pos)?;
    }

    if let (Some(last_stage), Some(destination)) = (
        aqueduct.stages.last().and_then(|s| s.last()),
        &aqueduct.destination,
    ) {
        let time = Instant::now();

        let df = ctx.table(last_stage.name.as_str()).await?;
        destination.write(ctx.clone(), df).await?;

        ctx.deregister_table(last_stage.name.as_str())?;

        let elapsed = time.elapsed();
        debug!(
            "Finished writing to destination ... Elapsed time: {:.2?}",
            elapsed
        );

        // Emit destination completed event
        if let Some(tracker) = &progress_tracker {
            tracker.on_progress(ProgressEvent::DestinationCompleted);
        }
    } else {
        warn!("No destination defined ... skipping write");
    }

    let total_duration = start_time.elapsed();
    debug!(
        "Finished processing pipeline ... Total time: {:.2?}",
        total_duration
    );

    // Emit completed event
    if let Some(tracker) = &progress_tracker {
        tracker.on_progress(ProgressEvent::Completed {
            duration_ms: total_duration.as_millis() as u64,
        });
    }

    Ok(ctx)
}

// calculate time to live for a stage based on the position of the stage
pub(crate) fn calculate_ttl<'a>(
    stage_ttls: &'a mut HashMap<String, usize>,
    stage_name: &'a str,
    stage_pos: usize,
    stages: &[Vec<Stage>],
) -> Result<()> {
    let stage_name_r = format!("\\s{stage_name}(\\s|\\;|\\n|\\)|\\.|$)");
    let regex = Regex::new(stage_name_r.as_str())?;

    let ttl = stages
        .iter()
        .enumerate()
        .skip(stage_pos + 1)
        .flat_map(|(forward_pos, parallel)| parallel.iter().map(move |stage| (forward_pos, stage)))
        .filter_map(|(forward_pos, stage)| {
            if regex.is_match(stage.query.as_str()) {
                debug!("Registering TTL for {stage_name}. STAGE_POS={stage_pos} TTL={forward_pos}");
                Some(forward_pos)
            } else {
                None
            }
        })
        .next_back()
        .unwrap_or(stage_pos + 1);

    stage_ttls
        .entry(stage_name.to_string())
        .and_modify(|e| *e = ttl)
        .or_insert_with(|| ttl);

    Ok(())
}

// deregister stages from context if the current position matches the ttl of the stages
pub(crate) fn deregister_stages(
    ctx: Arc<SessionContext>,
    ttls: &HashMap<String, usize>,
    current_pos: usize,
) -> Result<()> {
    ttls.iter().try_for_each(|(table, ttl)| {
        if *ttl == current_pos {
            debug!("Deregistering table {table}, current_pos {current_pos}, ttl {ttl}");
            ctx.deregister_table(table).map(|_| ())
        } else {
            Ok(())
        }
    })?;

    Ok(())
}
