use datafusion::execution::context::SessionContext;
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, path::Path, sync::OnceLock, time::Instant};
use tracing::{debug, error, info, instrument, warn};

pub mod destinations;
pub mod error;
pub mod sources;
pub mod stages;

use destinations::*;
use sources::*;
use stages::*;

/// Prelude to import all relevant models and functions
pub mod prelude {
    pub use super::destinations::*;
    pub use super::sources::*;
    pub use super::stages::*;
    pub use super::{Aqueduct, AqueductBuilder};

    pub use super::run_pipeline;
}

pub type Result<T> = core::result::Result<T, error::Error>;

static PARAM_REGEX: OnceLock<Regex> = OnceLock::new();

/// Definition for an `Aqueduct` data pipeline
#[derive(Debug, Clone, Serialize, Deserialize, derive_new::new)]
pub struct Aqueduct {
    /// Definition of the data sources for this pipeline
    pub sources: Vec<Source>,

    /// A sequential list of transformations to execute within the context of this pipeline
    pub stages: Vec<Stage>,

    /// Destiantion for the final step of the `Aqueduct`
    /// takes the last stage as input for the write operation
    pub destination: Option<Destination>,
}

impl Aqueduct {
    /// Builder for an Aqueduct pipeline
    pub fn builder() -> AqueductBuilder {
        AqueductBuilder::default()
    }

    /// Load an Aqueduct table definition from a local fs path containing a json file
    /// Provided params will be subsituted throughout the file (format: `${param}`) with the corresponding value
    pub fn try_from_json<P>(path: P, params: HashMap<String, String>) -> Result<Self>
    where
        P: AsRef<Path>,
    {
        let raw = std::fs::read_to_string(path)?;
        let definition = Self::subsitute_params(raw, params)?;
        let aqueduct = serde_json::from_str::<Aqueduct>(definition.as_str())?;

        Ok(aqueduct)
    }

    /// Load an Aqueduct table definition from a local fs path containing a yaml configuration file
    /// Provided params will be subsituted throughout the file (format: `${param}`) with the corresponding value
    pub fn try_from_yml<P>(path: P, params: HashMap<String, String>) -> Result<Self>
    where
        P: AsRef<Path>,
    {
        let raw = std::fs::read_to_string(path)?;
        let definition = Self::subsitute_params(raw, params)?;
        let aqueduct = serde_yml::from_str::<Aqueduct>(definition.as_str())?;

        Ok(aqueduct)
    }

    fn subsitute_params(raw: String, params: HashMap<String, String>) -> Result<String> {
        let mut definition = raw;

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
            .collect::<Vec<String>>();

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
    stages: Vec<Stage>,
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
        self.stages.push(stage);
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

/// Execute an `Aqueduct` pipeline, optionally with a provided datafusion `SessionContext`
#[instrument(skip(ctx, aqueduct), err)]
pub async fn run_pipeline(aqueduct: Aqueduct, ctx: Option<SessionContext>) -> Result<()> {
    let mut stage_ttls: HashMap<String, usize> = HashMap::new();

    let ctx = ctx.unwrap_or_default();
    let start_time = Instant::now();

    info!("Running Aqueduct ...");

    if let Some(destination) = &aqueduct.destination {
        let time = Instant::now();

        create_destination(&ctx, destination).await?;

        info!(
            "Created destination ... Elapsed time: {:.2?}",
            time.elapsed()
        );
    }

    for (pos, source) in aqueduct.sources.iter().enumerate() {
        let time = Instant::now();

        register_source(&ctx, source.clone()).await?;

        info!(
            "Registered source #{pos} ... Elapsed time: {:.2?}",
            time.elapsed()
        );
    }

    for (pos, stage) in aqueduct.stages.iter().enumerate() {
        let time = Instant::now();

        calculate_ttl(&mut stage_ttls, stage.name.as_str(), pos, &aqueduct.stages)?;
        process_stage(&ctx, stage.clone()).await?;
        deregister_stages(&ctx, &stage_ttls, pos)?;

        info!(
            "Finished processing stage #{pos} ... Elapsed time: {:.2?}",
            time.elapsed()
        );
    }

    if let (Some(last_stage), Some(destination)) = (aqueduct.stages.last(), &aqueduct.destination) {
        let time = Instant::now();

        let df = ctx.table(last_stage.name.as_str()).await?;
        write_to_destination(destination, df).await?;

        info!(
            "Finished writing to destination ... Elapsed time: {:.2?}",
            time.elapsed()
        );
    } else {
        warn!("No destination defined ... skipping write");
    }

    info!(
        "Finished processing pipeline ... Total time: {:.2?}",
        start_time.elapsed()
    );

    Ok(())
}

// calculate time to live for a stage based on the position of the stage
fn calculate_ttl<'a>(
    stage_ttls: &'a mut HashMap<String, usize>,
    stage_name: &'a str,
    stage_pos: usize,
    stages: &Vec<Stage>,
) -> Result<()> {
    let stage_name_r = format!("\\s{stage_name}(\\s|\\;|\\n|\\.|$)");
    let regex = Regex::new(stage_name_r.as_str())?;

    let ttl = stages
        .iter()
        .enumerate()
        .skip(stage_pos + 1)
        .filter_map(|(forward_pos, f)| {
            if regex.is_match(f.query.as_str()) {
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
    ctx: &SessionContext,
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
