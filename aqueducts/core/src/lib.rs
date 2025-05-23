use std::{
    collections::{HashMap, HashSet},
    path::Path,
    sync::{Arc, OnceLock},
    time::Instant,
};

use datafusion::execution::context::SessionContext;
use regex::Regex;
use serde::{Deserialize, Serialize};
use tokio::task::JoinHandle;
use tracing::{debug, error, instrument, warn};

pub mod destinations;
pub mod error;
pub mod progress_tracker;
pub mod sources;
pub mod stages;

use destinations::*;
use progress_tracker::*;
use sources::*;
use stages::*;

/// Prelude to import all relevant models and functions
pub mod prelude {
    pub use super::destinations::*;
    pub use super::progress_tracker::{ProgressEvent, ProgressTracker};
    pub use super::run_pipeline;
    pub use super::sources::*;
    pub use super::stages::*;
    pub use super::{Aqueduct, AqueductBuilder};
}

pub type Result<T> = core::result::Result<T, error::Error>;

static PARAM_REGEX: OnceLock<Regex> = OnceLock::new();
/// Definition for an `Aqueduct` data pipeline
#[derive(Debug, Clone, Serialize, Deserialize, derive_new::new)]
#[cfg_attr(feature = "schema_gen", derive(schemars::JsonSchema))]
pub struct Aqueduct {
    /// Definition of the data sources for this pipeline
    pub sources: Vec<Source>,

    /// A sequential list of transformations to execute within the context of this pipeline
    /// Nested stages are executed in parallel
    pub stages: Vec<Vec<Stage>>,

    /// Destination for the final step of the `Aqueduct`
    /// takes the last stage as input for the write operation
    pub destination: Option<Destination>,
}

impl Aqueduct {
    /// Builder for an Aqueduct pipeline
    pub fn builder() -> AqueductBuilder {
        AqueductBuilder::default()
    }

    /// Load an Aqueduct table definition from a local fs path containing a json file
    /// Provided params will be substituted throughout the file (format: `${param}`) with the corresponding value
    #[cfg(feature = "json")]
    pub fn try_from_json<P>(path: P, params: HashMap<String, String>) -> Result<Self>
    where
        P: AsRef<Path>,
    {
        let raw = std::fs::read_to_string(path)?;
        let parsed = serde_json::from_str::<serde_json::Value>(raw.as_str())?;
        let parsed = serde_json::to_string(&parsed)?;
        let definition = Self::substitute_params(parsed.as_str(), params)?;
        let aqueduct = serde_json::from_str::<Aqueduct>(definition.as_str())?;

        Ok(aqueduct)
    }

    /// Load an Aqueduct table definition from a local fs path containing a toml file
    /// Provided params will be substituted throughout the file (format: `${param}`) with the corresponding value
    #[cfg(feature = "toml")]
    pub fn try_from_toml<P>(path: P, params: HashMap<String, String>) -> Result<Self>
    where
        P: AsRef<Path>,
    {
        let raw = std::fs::read_to_string(path)?;
        let parsed = toml::from_str::<toml::Value>(raw.as_str())?;
        let parsed = toml::to_string(&parsed)?;
        let definition = Self::substitute_params(parsed.as_str(), params)?;
        let aqueduct = toml::from_str::<Aqueduct>(definition.as_str())?;

        Ok(aqueduct)
    }

    /// Load an Aqueduct table definition from a local fs path containing a yaml configuration file
    /// Provided params will be substituted throughout the file (format: `${param}`) with the corresponding value
    #[cfg(feature = "yaml")]
    pub fn try_from_yml<P>(path: P, params: HashMap<String, String>) -> Result<Self>
    where
        P: AsRef<Path>,
    {
        let raw = std::fs::read_to_string(path)?;
        let parsed = serde_yml::from_str::<serde_yml::Value>(raw.as_str())?;
        let parsed = serde_yml::to_string(&parsed)?;
        let definition = Self::substitute_params(parsed.as_str(), params)?;
        let aqueduct = serde_yml::from_str::<Aqueduct>(definition.as_str())?;

        Ok(aqueduct)
    }

    /// Load an Aqueduct table definition from a &str containing a json string
    /// Provided params will be substituted throughout the file (format: `${param}`) with the corresponding value
    #[cfg(feature = "json")]
    pub fn try_from_json_str(contents: &str, params: HashMap<String, String>) -> Result<Self> {
        let parsed = serde_json::from_str::<serde_json::Value>(contents)?;
        let parsed = serde_json::to_string(&parsed)?;
        let definition = Self::substitute_params(parsed.as_str(), params)?;
        let aqueduct = serde_json::from_str::<Aqueduct>(definition.as_str())?;

        Ok(aqueduct)
    }

    /// Load an Aqueduct table definition from a &str containing a toml string
    /// Provided params will be substituted throughout the file (format: `${param}`) with the corresponding value
    #[cfg(feature = "toml")]
    pub fn try_from_toml_str(contents: &str, params: HashMap<String, String>) -> Result<Self> {
        let parsed = toml::from_str::<toml::Value>(contents)?;
        let parsed = toml::to_string(&parsed)?;
        let definition = Self::substitute_params(parsed.as_str(), params)?;
        let aqueduct = toml::from_str::<Aqueduct>(definition.as_str())?;

        Ok(aqueduct)
    }

    /// Load an Aqueduct table definition from a &str containing a yaml string
    /// Provided params will be substituted throughout the file (format: `${param}`) with the corresponding value
    #[cfg(feature = "yaml")]
    pub fn try_from_yml_str(contents: &str, params: HashMap<String, String>) -> Result<Self> {
        let parsed = serde_yml::from_str::<serde_yml::Value>(contents)?;
        let parsed = serde_yml::to_string(&parsed)?;
        let definition = Self::substitute_params(parsed.as_str(), params)?;
        let aqueduct = serde_yml::from_str::<Aqueduct>(definition.as_str())?;

        Ok(aqueduct)
    }

    fn substitute_params(raw: &str, params: HashMap<String, String>) -> Result<String> {
        let mut definition = raw.to_string();

        params.into_iter().for_each(|(name, value)| {
            let template = format!("${{{name}}}");
            definition = definition.replace(template.as_str(), value.as_str());
        });

        let captures = PARAM_REGEX
            .get_or_init(|| Regex::new("\\$\\{([a-zA-Z0-9_]+)\\}").expect("invalid regex"))
            .captures_iter(definition.as_str());

        let missing_params = captures
            .map(|capture| {
                let param = capture
                    .get(1)
                    .expect("no capture group found")
                    .as_str()
                    .to_string();
                param
            })
            .collect::<HashSet<String>>();

        if !missing_params.is_empty() {
            let error = error::Error::MissingParams(missing_params);

            error!("{error}");
            return Err(error);
        }

        Ok(definition)
    }
}

/// Builder for an Aqueduct pipeline
#[derive(Debug, Clone, Default, Serialize, Deserialize, derive_new::new)]
pub struct AqueductBuilder {
    sources: Vec<Source>,
    stages: Vec<Vec<Stage>>,
    destination: Option<Destination>,
}

impl AqueductBuilder {
    /// Add source to builder
    pub fn source(mut self, source: Source) -> Self {
        self.sources.push(source);
        self
    }

    /// Add stage to builder
    pub fn stage(mut self, stage: Stage) -> Self {
        self.stages.push(vec![stage]);
        self
    }

    /// Set destination to builder
    pub fn destination(mut self, destination: Destination) -> Self {
        self.destination = Some(destination);
        self
    }

    /// Build Aqueduct pipeline
    pub fn build(self) -> Aqueduct {
        Aqueduct::new(self.sources, self.stages, self.destination)
    }
}

/// Register handlers for the object stores enabled by the selected feature flags (s3, gcs, azure)
/// Stores can alternatively be provided by a custom context passed to `run_pipeline`
#[instrument()]
pub fn register_handlers() {
    aqueducts_utils::store::register_handlers();
}

/// Execute an `Aqueduct` pipeline with optional progress tracking
/// The progress tracker receives events during execution to monitor status
/// Returns the provided context once the pipeline completes
#[instrument(skip_all, err)]
pub async fn run_pipeline(
    ctx: Arc<SessionContext>,
    aqueduct: Aqueduct,
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

        register_destination(ctx.clone(), destination).await?;

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
                register_source(ctx_, source_).await?;

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

                process_stage(ctx_, stage_, tracker.as_ref()).await?;

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
        write_to_destination(ctx.clone(), destination, df).await?;

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
fn calculate_ttl<'a>(
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
        .last()
        .unwrap_or(stage_pos + 1);

    stage_ttls
        .entry(stage_name.to_string())
        .and_modify(|e| *e = ttl)
        .or_insert_with(|| ttl);

    Ok(())
}

// deregister stages from context if the current position matches the ttl of the stages
fn deregister_stages(
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
